package gitmcp

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/jchen0426/agentos/internal/faults"
	"github.com/jchen0426/agentos/internal/gitenv"
	"github.com/jchen0426/agentos/internal/statedir"
)

// Client runs the git MCP server once per call, inside the sandbox its launcher builds, and
// calls one tool. A fresh process per call keeps calls apart: a server that a crafted
// repository managed to subvert holds no state for the next one. Make one with NewClient.
type Client struct {
	server  string
	sandbox []string
	deny    string // Options.DenyRead, as JSON
	timeout time.Duration
	log     *slog.Logger
	slots   chan struct{} // servers running at once
	limits  Limits
	calls   *atomic.Int64 // calls made, by every client WithOwnSlots made of this one too: each scope's number
}

// Options configure a Client.
type Options struct {
	Server string // the agentos-mcp-git binary
	// Sandbox is the launcher and its leading arguments: run-tool.sh; empty runs unsandboxed. A
	// launcher sets the hard CPU time limit above the soft one (see cpuLimited).
	Sandbox []string
	// DenyRead lists paths the launcher keeps out of reach on top of its own rules (agentd's
	// data directory, the other mirrors), passed as AGENTOS_SANDBOX_DENY_READ, a JSON array:
	// any character may be in a path.
	DenyRead []string
	Timeout  time.Duration // per call, from the moment it has a slot
	Parallel int           // servers running at once; 4 when 0
	Log      *slog.Logger  // gets the server's stderr when a call fails or is refused; the error does not carry it
	// Limits, if set, hold each call to its resource limits (Linux, M5 C3a): the launcher gets what
	// Limits.Env gives it, and runs the call in a systemd user scope of its own. A call whose server
	// failed because the kernel killed it at its memory limit is refused (a ToolError), as one past the
	// CPU time limit is: the same call goes over it every time.
	Limits Limits
}

// Limits are how a call's launcher holds it to the calls' resource limits: a *scope.Set.
type Limits interface {
	// Env is what call n's launcher gets: the name of its scope ("" for none), and the variables that
	// say what holds it, or that the limits are off.
	Env(n int64) (unit string, env []string)
	// OOMKilled reports, once what ran in unit has exited, whether the kernel killed it at its memory
	// limit; its error says it could not be told.
	OOMKilled(unit string) (bool, error)
	// Memory is the memory limit, as a refusal names it.
	Memory() string
}

// maxReply bounds one message from the server: a diff at its limit, or one file read_files
// reads at its limit with its path, base64-encoded, and room. A subverted server's message can
// cost agentd some tens of times its size to decode, so the bound stays small: ReadFiles asks
// for one file at a time.
const maxReply = 2*MaxDiffBytes + 64<<10

// maxValues bounds the JSON values in an answer: the honest ones hold a dozen or so, and each
// of a subverted server's millions of tiny values could become a struct of its own when the
// answer is decoded into its type.
const maxValues = 1000

// maxToolError bounds a text of the server's that goes on to the workflow and maybe a public
// report (Reason). The server's own are far shorter.
const maxToolError = 4 << 10

// stateRoot holds the per-call state directories, /tmp/agentos-tools.<pid>.<random> (statedir).
// sandbox-runtime, like the Worker's launcher, keeps its state under /tmp.
const stateRoot = statedir.Root

// stateKind names this package's state directories.
const stateKind = "agentos-tools"

// slotWait bounds the wait for a free slot; the Invoke activity heartbeats meanwhile.
const slotWait = 2 * time.Minute

// NewClient makes a client, and removes the state directories that agentd processes no longer
// running left behind (one killed mid-call cannot clean up).
func NewClient(o Options) (*Client, error) {
	statedir.Sweep(stateKind)
	deny, err := json.Marshal(o.DenyRead)
	if err != nil {
		return nil, err
	}
	n := o.Parallel
	if n <= 0 {
		n = 4
	}
	return &Client{server: o.Server, sandbox: o.Sandbox, deny: string(deny), timeout: o.Timeout, log: o.Log,
		slots: make(chan struct{}, n), limits: o.Limits, calls: new(atomic.Int64)}, nil
}

// WithOwnSlots is a client like c whose calls take n slots of their own (4 when n is not
// positive, as in Options): they never wait for c's calls, nor c's for theirs.
func (c *Client) WithOwnSlots(n int) *Client {
	if n <= 0 {
		n = 4
	}
	d := *c
	d.slots = make(chan struct{}, n)
	return &d
}

// ToolError is the server's refusal: git failed on what it was given.
type ToolError struct{ Msg string }

func (e *ToolError) Error() string { return e.Msg }

// refusal is the server's refusal as a ToolError, its text through Reason.
func refusal(mirror string, res *mcp.CallToolResult) *ToolError {
	var text []string
	for _, c := range res.Content {
		if t, ok := c.(*mcp.TextContent); ok {
			text = append(text, t.Text)
		}
	}
	return &ToolError{Msg: Reason(mirror, strings.Join(text, "\n"))}
}

// Reason is a text of the server's that goes on to the workflow, and maybe a public report (a
// refusal, why a patch does not apply): cut short, and with the mirror's path as agentd names
// it taken out. (A subverted server could still spell that path some other way, or put in up
// to maxToolError of anything it can read.)
func Reason(mirror, text string) string {
	text = strings.ReplaceAll(text, mirror, "<mirror>")
	if len(text) > maxToolError {
		text = strings.ToValidUTF8(text[:maxToolError], "") + " [truncated]"
	}
	return text
}

// entryTypes is the type git ls-tree shows for each mode it shows; neither when the commit has
// nothing at a path.
var entryTypes = map[string]string{"": "", "100644": "blob", "100755": "blob", "120000": "blob", "040000": "tree", "160000": "commit"}

// Caller calls the server's tools: a Client, or a test's stand-in.
type Caller interface {
	Call(ctx context.Context, mirror, tool string, args, out any) error
}

// ResolveCommit is the full SHA of the one commit that digits (7 to 40 hexadecimal) name in the
// mirror, as the server's resolve_commit finds it. Its refusals are *ToolError.
func ResolveCommit(ctx context.Context, c Caller, mirror, digits string) (string, error) {
	if !commitDigits.MatchString(digits) {
		return "", &ToolError{Msg: "commit must be 7 to 40 hexadecimal digits"}
	}
	var out ResolveCommitOut
	if err := c.Call(ctx, mirror, "resolve_commit", ResolveCommitIn{Commit: digits}, &out); err != nil {
		return "", err
	}
	// As far as agentd can check the server: a commit the digits begin.
	if !fullSHA.MatchString(out.Commit) || !strings.HasPrefix(out.Commit, digits) {
		return "", fmt.Errorf("git tool resolve_commit: unexpected answer %.100q for %s", out.Commit, digits)
	}
	return out.Commit, nil
}

// ReadFiles is what commit has at each of paths in the mirror, in their order, as the server's
// read_files reads it, with each File's Content decoded. It asks for one path at a time, so
// that no answer is larger than maxReply, and, from a Client, in one slot: the calls queue for
// a slot once. Its refusals are *ToolError.
func ReadFiles(ctx context.Context, c Caller, mirror, commit string, paths []string) ([]File, error) {
	if s, ok := c.(interface {
		InOneSlot(context.Context, func(Caller) error) error
	}); ok {
		var files []File
		err := s.InOneSlot(ctx, func(c Caller) error {
			var err error
			files, err = readEach(ctx, c, mirror, commit, paths)
			return err
		})
		return files, err
	}
	return readEach(ctx, c, mirror, commit, paths)
}

func readEach(ctx context.Context, c Caller, mirror, commit string, paths []string) ([]File, error) {
	files := make([]File, 0, len(paths))
	for _, p := range paths {
		var out ReadFilesOut
		if err := c.Call(ctx, mirror, "read_files", ReadFilesIn{Commit: commit, Paths: []string{p}}, &out); err != nil {
			return nil, err
		}
		if len(out.Files) != 1 {
			return nil, fmt.Errorf("git tool read_files: %d answers for 1 path", len(out.Files))
		}
		f, err := checked(out.Files[0], p)
		if err != nil {
			return nil, err
		}
		files = append(files, f)
	}
	return files, nil
}

// checked is the server's answer for path p, as far as agentd can check it: an entry git could
// show, or nothing and maybe a directory above p that is something else; and the content that
// goes with it, decoded.
func checked(f File, p string) (File, error) {
	typ, known := entryTypes[f.Mode]
	content, err := base64.StdEncoding.DecodeString(f.ContentBase64)
	readable := f.Type == "blob" && f.Size <= MaxReadBytes
	switch {
	case f.Path != p:
		return File{}, fmt.Errorf("git tool read_files: an answer for %.200q in place of %.200q", f.Path, p)
	case !known || f.Type != typ || f.NotDir != "" && (f.Mode != "" || !strings.HasPrefix(f.Path, f.NotDir+"/")):
		return File{}, fmt.Errorf("git tool read_files: an unexpected entry for %.200q", f.Path)
	case err != nil || f.Size < 0 || f.Type != "blob" && f.Size != 0 || readable && int64(len(content)) != f.Size || !readable && len(content) > 0:
		return File{}, fmt.Errorf("git tool read_files: unexpected content for %.200q", f.Path)
	}
	f.Content, f.ContentBase64 = content, ""
	return f, nil
}

// Call runs tool on the mirror and decodes its structured result into out. A call that runs
// past the timeout fails, to be retried: the launcher's CPU time limit is what refuses a
// commit too costly to diff, the same way every time, while a call can be slow because the
// machine is. So is its memory limit, on Linux (Options.Limits), for a commit too large.
func (c *Client) Call(ctx context.Context, mirror, tool string, args, out any) error {
	release, err := c.slot(ctx)
	if err != nil {
		return err
	}
	defer release()
	return c.call(ctx, mirror, tool, args, out)
}

// InOneSlot runs fn with a Caller whose calls all take the one slot of c's that InOneSlot
// waits for first: a sequence of calls queues for a slot once, not once for each.
func (c *Client) InOneSlot(ctx context.Context, fn func(Caller) error) error {
	release, err := c.slot(ctx)
	if err != nil {
		return err
	}
	defer release()
	return fn(inSlot{c})
}

// inSlot calls in a slot already taken.
type inSlot struct{ c *Client }

func (s inSlot) Call(ctx context.Context, mirror, tool string, args, out any) error {
	return s.c.call(ctx, mirror, tool, args, out)
}

// slot waits for a free slot, at most slotWait, and returns what gives it back.
func (c *Client) slot(ctx context.Context) (func(), error) {
	wait, stop := context.WithTimeout(ctx, slotWait)
	defer stop()
	select {
	case c.slots <- struct{}{}:
		return func() { <-c.slots }, nil
	case <-wait.Done():
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("no free slot for the git tool server within %s", slotWait)
	}
}

// call is Call in a slot already taken.
func (c *Client) call(ctx context.Context, mirror, tool string, args, out any) (err error) {
	callCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	// A directory of this call's own, removed afterwards however the server ends: the
	// launcher keeps its state there, and an unsandboxed server its temporary files.
	state, err := statedir.Make(stateKind)
	if err != nil {
		return err
	}
	defer statedir.Remove(state)
	argv := []string{c.server, "--mirror", mirror}
	if len(c.sandbox) > 0 {
		argv = append(append(slices.Clone(c.sandbox), mirror, "--"), argv...)
	}
	cmd := exec.CommandContext(callCtx, argv[0], argv[1:]...)
	// The launcher clears the environment again; this is what it, or a server without one,
	// may see.
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + state, "TMPDIR=" + state, "AGENTOS_TMPDIR=" + state,
		"LANG=C.UTF-8", "AGENTOS_SANDBOX_STATE=" + state, "AGENTOS_SANDBOX_DENY_READ=" + c.deny}
	var unit string // the call's systemd user scope, if it has one
	if c.limits != nil {
		var limits []string
		unit, limits = c.limits.Env(c.calls.Add(1))
		cmd.Env = append(cmd.Env, limits...)
	}
	// The launcher and everything it starts form a process group, killed whole: on a timeout
	// the server goes too, not just its launcher. (A process that starts a session of its own
	// escapes this; on Linux bwrap's --die-with-parent still ends it.) The group is a session's
	// too, which has no controlling terminal: sandbox-runtime lets what it runs ioctl /dev/tty,
	// so on the terminal agentd runs on, a subverted server could type into it (TIOCSTI).
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = 5 * time.Second // a straggler holding the pipes open does not hold agentd
	stderr := &limitedBuffer{max: 16 << 10}
	cmd.Stderr = stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		c.logFailure(tool, fmt.Errorf("start: %w", err), stderr.String())
		return failedOn(tool)
	}
	// What went wrong is logged with the server's stderr, which is whole once it has exited. Whether the
	// kernel killed a server that failed at the call's memory limit, systemd says once it has exited: such
	// a call is refused, since it goes over the limit every time, as over the CPU time limit (cpuLimitError).
	var failure error
	var refused *ToolError
	defer func() {
		_ = cmd.Wait()
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) // anything of the group still running
		msg := stderr.String()
		if failure != nil && unit != "" {
			switch killed, oerr := c.limits.OOMKilled(unit); {
			case oerr != nil:
				c.logFailure(tool, fmt.Errorf("%w; and systemd could not say whether the call went over its memory limit: %w",
					failure, oerr), msg)
				return
			case killed:
				err = &ToolError{Msg: "the commit is too large for the git tool server to read within its memory limit of " +
					c.limits.Memory()}
				if c.log != nil {
					c.log.Warn("the git tool call went over its memory limit, and the kernel killed it: refused", "tool", tool,
						"memory_limit", c.limits.Memory(), "scope", unit, "error", failure, "stderr", msg)
				}
				return
			}
		}
		switch {
		case failure != nil:
			c.logFailure(tool, failure, msg)
		case refused != nil && msg != "" && c.log != nil:
			c.log.Info("the git tool server refused", "tool", tool, "refusal", refused.Msg, "stderr", msg)
		}
	}()

	client := mcp.NewClient(&mcp.Implementation{Name: "agentd", Version: "0.1"}, nil)
	session, err := client.Connect(callCtx, &mcp.IOTransport{Reader: stdout, Writer: stdin, MaxLineLength: maxReply}, nil)
	if err != nil {
		failure = fmt.Errorf("connect: %w", err)
		return failedOn(tool)
	}
	defer session.Close()
	faults.StallInTool(callCtx) // a test's hold, with the server up (agentd's --fault-script)
	res, err := session.CallTool(callCtx, &mcp.CallToolParams{Name: tool, Arguments: args})
	if err != nil {
		failure = err
		return failedOn(tool)
	}
	if res.IsError {
		refused = refusal(mirror, res)
		return refused
	}
	switch n, ok := values(res.StructuredContent, maxValues); {
	case !ok:
		return fmt.Errorf("git tool %s: an answer agentd cannot count the values of", tool)
	case n > maxValues:
		return fmt.Errorf("git tool %s: an answer of more than %d values", tool, maxValues)
	}
	b, err := json.Marshal(res.StructuredContent)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, out); err != nil {
		return fmt.Errorf("git tool %s: unexpected result: %w", tool, err)
	}
	return nil
}

// SelfCheck calls every tool through the launcher, on a repository of its own: agentd runs it
// at start, so that a launcher that cannot work (sandbox-runtime not installed, bwrap without
// user namespaces, a temporary directory the server cannot write), or a server without a tool
// agentd uses, stops agentd instead of failing every run.
func (c *Client) SelfCheck(ctx context.Context) error {
	dir, err := statedir.Make(stateKind)
	if err != nil {
		return err
	}
	defer statedir.Remove(dir)
	git := func(args ...string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...)
		cmd.Env = gitenv.Environ("GIT_AUTHOR_NAME=agentos", "GIT_AUTHOR_EMAIL=agentos@agentos.invalid",
			"GIT_COMMITTER_NAME=agentos", "GIT_COMMITTER_EMAIL=agentos@agentos.invalid")
		out, err := cmd.Output()
		if err != nil {
			return nil, fmt.Errorf("git %s: %w", args[0], err)
		}
		return out, nil
	}
	if _, err := git("init", "-q"); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "check.txt"), []byte("ok\n"), 0o600); err != nil {
		return err
	}
	if _, err := git("add", "check.txt"); err != nil {
		return err
	}
	if _, err := git("commit", "-q", "-m", "self check"); err != nil {
		return err
	}
	head, err := git("rev-parse", "HEAD")
	if err != nil {
		return err
	}
	commit := strings.TrimSpace(string(head))
	if full, err := ResolveCommit(ctx, c, dir, commit[:7]); err != nil {
		return err
	} else if full != commit {
		return fmt.Errorf("the git tool server resolved %s to %s, not %s", commit[:7], full, commit)
	}
	var out ShowDiffOut
	if err := c.Call(ctx, dir, "show_diff", ShowDiffIn{Commit: commit}, &out); err != nil {
		return err
	}
	diff, err := out.Diff()
	if err != nil || !bytes.Contains(diff, []byte("check.txt")) {
		return fmt.Errorf("the git tool server answered with an unexpected diff: %q", out.DiffBase64)
	}
	files, err := ReadFiles(ctx, c, dir, commit, []string{"check.txt"})
	if err != nil {
		return err
	}
	if f := files[0]; f.Mode != "100644" || f.Type != "blob" || string(f.Content) != "ok\n" {
		return fmt.Errorf("the git tool server read check.txt as %s %s %q", f.Mode, f.Type, f.Content)
	}
	// apply_check writes its throwaway index and objects to the one directory the server may write.
	var check ApplyCheckOut
	patch := "--- a/check.txt\n+++ b/check.txt\n@@ -1 +1 @@\n-ok\n+checked\n"
	if err := c.Call(ctx, dir, "apply_check", ApplyCheckIn{Commit: commit, Patch: patch, Paths: []string{"check.txt"}}, &check); err != nil {
		return err
	}
	if !check.Applies {
		return fmt.Errorf("the git tool server refused a patch that applies: %s", check.Reason)
	}
	if want := (IndexEntry{Path: "check.txt", Mode: "100644", Object: blobID([]byte("checked\n"))}); check.Index == nil ||
		len(*check.Index) != 1 || (*check.Index)[0] != want {
		return fmt.Errorf("the git tool server said git made %+v of a patch that makes %+v", check.Index, want)
	}
	return nil
}

// blobID is the name git gives a file of content b (as internal/patch.BlobID, which this package
// cannot import).
func blobID(b []byte) string {
	h := sha1.New()
	fmt.Fprintf(h, "blob %d\x00", len(b))
	h.Write(b)
	return hex.EncodeToString(h.Sum(nil))
}

// ServerFailure is the error of a call whose server failed: it did not start, stopped, timed out, or
// answered nothing agentd could read; but for one the kernel killed at the call's memory limit, which
// is refused (a ToolError). Wrapped (%w) by whatever reads through the server, it stays one
// (IsServerFailure): the gateway records it as such, and agentctl runs explain tells it by that.
type ServerFailure struct{ Tool string }

func (e *ServerFailure) Error() string {
	return "the git tool server failed on " + e.Tool + "; agentd's log has the details"
}

// failedOn is the error of a call that did not get an answer. What went wrong goes to agentd's
// log only (logFailure): the error reaches the workflow, and from there maybe a public report,
// and the server's stderr can name local paths (a launcher's "run npm ci in ..." does).
func failedOn(tool string) error { return &ServerFailure{Tool: tool} }

// IsServerFailure reports whether err is, or wraps, a ServerFailure.
func IsServerFailure(err error) bool {
	var f *ServerFailure
	return errors.As(err, &f)
}

func (c *Client) logFailure(tool string, err error, stderr string) {
	if c.log != nil {
		c.log.Warn("the git tool server failed", "tool", tool, "error", err, "stderr", stderr)
	}
}

// limitedBuffer keeps the first max bytes written to it: a process's stderr.
type limitedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
	max int
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if room := b.max - b.buf.Len(); room > 0 {
		b.buf.Write(p[:min(len(p), room)])
	}
	return len(p), nil
}

func (b *limitedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.TrimSpace(b.buf.String())
}

// values counts the JSON values in v, as the SDK decodes an answer into any, until it is past
// limit. A value of a type it does not know is not ok: it could hold anything.
func values(v any, limit int) (int, bool) {
	n := 1
	switch v := v.(type) {
	case nil, bool, float64, string, json.Number:
	case map[string]any:
		for _, e := range v {
			if n > limit {
				break
			}
			m, ok := values(e, limit-n)
			if !ok {
				return n, false
			}
			n += m
		}
	case []any:
		for _, e := range v {
			if n > limit {
				break
			}
			m, ok := values(e, limit-n)
			if !ok {
				return n, false
			}
			n += m
		}
	default:
		return n, false
	}
	return n, true
}

// IsToolError reports whether err is the server's refusal rather than a failure to run it.
func IsToolError(err error) bool {
	var t *ToolError
	return errors.As(err, &t)
}
