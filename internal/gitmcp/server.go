// Package gitmcp is the first task's read-only git MCP server (design doc, sections 3 and 4)
// and agentd's client for it. agentd runs the server once per call, in a sandbox that holds one
// mirror read-only, a private temporary directory, and no network. The server never writes the
// mirror: apply_check applies the patch to a throwaway index.
//
// The commits it reads are untrusted, the mirror's own configuration is not: git runs in
// gitenv's environment and shows every file as text, and what it reads is sized up first.
package gitmcp

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/jchen0426/agentos/internal/gitenv"
)

// Limits on what goes in and out. show_diff sizes up a commit before git diffs it: git holds
// both sides of a file in memory, with some 60 bytes more for every line, and a small crafted
// repository can hold huge files. A 4 MiB file of random short lines, the worst shape measured,
// takes git 30 s and 360 MB to diff on an M-series Mac; 8 MiB, over a minute and 660 MB.
const (
	MaxDiffBytes   = 1 << 20   // a diff larger than this is refused, not cut
	MaxFileBytes   = 4 << 20   // show_diff refuses a commit that changes a larger file...
	MaxDiffInput   = 256 << 20 // ...or more than this in all, both sides of every file counted
	MaxCommitBytes = 1 << 20   // a larger commit object is refused unread
	MaxPatchBytes  = 256 << 10 // as internal/patch
	MaxPaths       = 20        // files a patch may touch, as internal/patch
	// MaxRequest bounds one message to the server: a patch at its limit with every byte
	// escaped in JSON (a control character takes six), and room.
	MaxRequest = 6*MaxPatchBytes + 64<<10
)

var fullSHA = regexp.MustCompile(`^[0-9a-f]{40}$`)

// zeroID stands for "no file" on one side of a change.
const zeroID = "0000000000000000000000000000000000000000"

// Server serves the git tools on one mirror.
type Server struct {
	Mirror  string    // the read-only mirror
	TempDir string    // where apply_check keeps its throwaway index; os.TempDir() when empty
	Stderr  io.Writer // gets what git writes to its stderr, for agentd's log; nil discards it
}

type ShowDiffIn struct {
	Commit string `json:"commit" jsonschema:"the commit to show, a full 40-character SHA"`
}

type ShowDiffOut struct {
	// The diff's exact bytes, base64-encoded: a commit need not be UTF-8, and base64 stays as
	// compact in JSON whatever the diff holds.
	DiffBase64 string `json:"diff_base64" jsonschema:"the commit's changes against its first parent, as a patch, base64-encoded"`
}

// Diff decodes the diff.
func (o ShowDiffOut) Diff() ([]byte, error) { return base64.StdEncoding.DecodeString(o.DiffBase64) }

type ApplyCheckIn struct {
	Commit string   `json:"commit" jsonschema:"the commit the patch is for, a full 40-character SHA"`
	Patch  string   `json:"patch" jsonschema:"a unified diff"`
	Paths  []string `json:"paths" jsonschema:"the files the patch touches"`
}

type ApplyCheckOut struct {
	Applies bool   `json:"applies"`
	Reason  string `json:"reason,omitempty" jsonschema:"why git apply refuses the patch"`
}

// MCP is the MCP server with the two tools.
func (s *Server) MCP() *mcp.Server {
	srv := mcp.NewServer(&mcp.Implementation{Name: "agentos-git", Version: "0.1"}, nil)
	mcp.AddTool(srv, &mcp.Tool{Name: "show_diff",
		Description: "The changes a commit makes against its first parent, as a patch."}, s.showDiff)
	mcp.AddTool(srv, &mcp.Tool{Name: "apply_check",
		Description: "Whether git apply accepts a patch on top of a commit, without changing anything."}, s.applyCheck)
	return srv
}

// Errors come back two ways. A tool error (IsError) says git refused what it was given: an
// unknown commit, a patch that does not apply, a diff over a limit. A protocol error says
// the server could not do its work (git missing, an unreadable mirror): the call may succeed
// later. Tool errors quote what the commit names, a path say, cut short: they reach the
// workflow and its report.
func unavailable(err error) error {
	return &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: err.Error()}
}

// failed is a git command's failure as the tool's error: a refusal when git ran out of the
// memory or the CPU time it may use, since it always will on that commit, and unavailable
// otherwise.
func failed(err error) error {
	var g *gitError
	var cpu *cpuLimitError
	switch {
	case errors.As(err, &g) && gitenv.TooLarge(g.msg):
		return errors.New("the commit is too large for git to read within its memory limit")
	case errors.As(err, &cpu):
		return errors.New("the commit is too costly for git to diff within its CPU time limit")
	}
	return unavailable(err)
}

// result is the tool's own short text: the structured output carries the data once, instead
// of a second, escaped copy in the text.
func result(text string) *mcp.CallToolResult {
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: text}}}
}

func (s *Server) showDiff(ctx context.Context, _ *mcp.CallToolRequest, in ShowDiffIn) (*mcp.CallToolResult, ShowDiffOut, error) {
	if err := s.commit(ctx, in.Commit); err != nil {
		return nil, ShowDiffOut{}, err
	}
	if err := s.fileSizes(ctx, in.Commit); err != nil {
		return nil, ShowDiffOut{}, err
	}
	// --root shows a first commit whole; a merge is shown against its first parent only (-m
	// --first-parent would show it against each parent in turn); --text shows every file,
	// binary or not. No external diff drivers or text conversion: those would run programs.
	out, err := s.git(ctx, nil, nil, MaxDiffBytes, "diff-tree", "-p", "--text", "--no-commit-id", "--root",
		"--diff-merges=first-parent", "--no-color", "--no-ext-diff", "--no-textconv", in.Commit)
	var tooLong *outputLimitError
	switch {
	case errors.As(err, &tooLong):
		return nil, ShowDiffOut{}, fmt.Errorf("the diff is over the limit of %d bytes", MaxDiffBytes)
	case err != nil:
		return nil, ShowDiffOut{}, failed(err)
	}
	return result(fmt.Sprintf("a diff of %d bytes", len(out))), ShowDiffOut{DiffBase64: base64.StdEncoding.EncodeToString(out)}, nil
}

func (s *Server) applyCheck(ctx context.Context, _ *mcp.CallToolRequest, in ApplyCheckIn) (*mcp.CallToolResult, ApplyCheckOut, error) {
	if len(in.Patch) > MaxPatchBytes {
		return nil, ApplyCheckOut{}, fmt.Errorf("the patch is %d bytes, over the limit of %d", len(in.Patch), MaxPatchBytes)
	}
	if len(in.Paths) == 0 || len(in.Paths) > MaxPaths {
		return nil, ApplyCheckOut{}, fmt.Errorf("a patch touches 1 to %d files, not %d", MaxPaths, len(in.Paths))
	}
	if err := s.commit(ctx, in.Commit); err != nil {
		return nil, ApplyCheckOut{}, err
	}
	dir, err := os.MkdirTemp(s.TempDir, "apply-check-")
	if err != nil {
		return nil, ApplyCheckOut{}, unavailable(err)
	}
	defer os.RemoveAll(dir)
	// A throwaway index holding only the files the patch touches, as the commit has them: the
	// rest of the tree, however large, is never read, and neither the mirror nor any worktree
	// changes.
	env := []string{"GIT_INDEX_FILE=" + filepath.Join(dir, "index")}
	entries, err := s.git(ctx, nil, nil, 1<<20, append([]string{"ls-tree", "-z", "--full-tree", in.Commit, "--"}, in.Paths...)...)
	if err != nil {
		return nil, ApplyCheckOut{}, failed(err)
	}
	if _, err := s.git(ctx, env, bytes.NewReader(entries), 0, "update-index", "-z", "--index-info"); err != nil {
		return nil, ApplyCheckOut{}, failed(err)
	}
	_, err = s.git(ctx, env, strings.NewReader(in.Patch), 0, "apply", "--cached", "--check", "-")
	var refused *gitError
	switch {
	case err == nil:
		return result("the patch applies"), ApplyCheckOut{Applies: true}, nil
	case errors.As(err, &refused) && (refused.code == 1 || refused.code == 128) && !gitenv.TooLarge(refused.msg):
		// git apply's own "no": the patch does not fit the commit (1), or is not a patch git
		// can apply at all (128). The index was just built, so neither is about the mirror.
		return result("the patch does not apply"), ApplyCheckOut{Applies: false, Reason: refused.Error()}, nil
	}
	return nil, ApplyCheckOut{}, failed(err)
}

// commit checks that c names a commit of at most MaxCommitBytes in the mirror, from the
// object's header alone: git reads a commit whole, and a crafted one can hold gigabytes that
// pack into megabytes. Another kind of object, or a commit over the limit, is a tool error,
// which no retry changes. A missing one is not: agentd found the run's commit in this mirror
// when the run started, so the server cannot see what agentd can (a mirror being cloned
// again, objects in an alternate outside the sandbox).
func (s *Server) commit(ctx context.Context, c string) error {
	if !fullSHA.MatchString(c) {
		return errors.New("commit must be a full 40-character SHA")
	}
	out, err := s.git(ctx, nil, strings.NewReader(c+"\n"), 0, "cat-file", "--batch-check=%(objecttype) %(objectsize)")
	if err != nil {
		return failed(err)
	}
	f := strings.Fields(string(out)) // "commit 251", or "<c> missing"
	switch {
	case len(f) == 2 && f[1] == "missing":
		return unavailable(fmt.Errorf("no commit %s in the mirror", c))
	case len(f) != 2:
		return unavailable(fmt.Errorf("git cat-file: unexpected answer %q", out))
	case f[0] != "commit":
		return fmt.Errorf("%s is a %s, not a commit", c, f[0])
	}
	if n, err := strconv.ParseInt(f[1], 10, 64); err != nil || n > MaxCommitBytes {
		return fmt.Errorf("commit %s is %s bytes, over the limit of %d", c, f[1], MaxCommitBytes)
	}
	return nil
}

// fileSizes refuses a commit that changes a file over MaxFileBytes, before or after, or more
// than MaxDiffInput in all.
func (s *Server) fileSizes(ctx context.Context, c string) error {
	// A listing longer than this belongs to a diff far over MaxDiffBytes.
	raw, err := s.git(ctx, nil, nil, 4<<20, "diff-tree", "-r", "-z", "--raw", "--no-abbrev", "--no-commit-id", "--root",
		"--diff-merges=first-parent", c)
	var tooLong *outputLimitError
	switch {
	case errors.As(err, &tooLong):
		return fmt.Errorf("the commit changes too many files: its diff is over the limit of %d bytes", MaxDiffBytes)
	case err != nil:
		return failed(err)
	}
	// Each change is ":<mode> <mode> <object> <object> <status>" and a path, each ending in NUL.
	var objects []string
	paths := map[string]string{}
	fields := bytes.Split(raw, []byte{0})
	for i := 0; i+1 < len(fields); i += 2 {
		f := strings.Fields(strings.TrimPrefix(string(fields[i]), ":"))
		if len(f) != 5 {
			return unavailable(fmt.Errorf("git diff-tree: unexpected change %q", fields[i]))
		}
		for side := range 2 {
			// A submodule's object is a commit in another repository: git shows its ID only.
			if mode, object := f[side], f[2+side]; mode != "160000" && object != zeroID {
				objects = append(objects, object)
				paths[object] = string(fields[i+1])
			}
		}
	}
	if len(objects) == 0 {
		return nil
	}
	sizes, err := s.git(ctx, nil, strings.NewReader(strings.Join(objects, "\n")+"\n"), 0, "cat-file",
		"--batch-check=%(objectname) %(objecttype) %(objectsize)")
	if err != nil {
		return failed(err)
	}
	var total int64
	for _, line := range strings.Split(strings.TrimSpace(string(sizes)), "\n") {
		f := strings.Fields(line) // or "<object> missing": the diff reports that
		if len(f) != 3 || f[1] != "blob" {
			continue
		}
		n, err := strconv.ParseInt(f[2], 10, 64)
		if err != nil || n > MaxFileBytes {
			return fmt.Errorf("%.200q: %s bytes, over the limit of %d for a file the diff shows", paths[f[0]], f[2], MaxFileBytes)
		}
		total += n
	}
	if total > MaxDiffInput {
		return fmt.Errorf("the commit changes %d bytes of files, over the limit of %d for a diff", total, MaxDiffInput)
	}
	return nil
}

// gitError is git's own refusal: its exit code and message.
type gitError struct {
	code int
	msg  string
}

func (e *gitError) Error() string { return e.msg }

// outputLimitError means git wrote more than the caller allows; git was stopped there.
type outputLimitError struct{}

func (*outputLimitError) Error() string { return "output over the limit" }

// cpuLimitError means git used up the CPU time the launcher allows it.
type cpuLimitError struct{ cmd string }

func (e *cpuLimitError) Error() string { return "git " + e.cmd + ": killed at its CPU time limit" }

// cpuLimited reports whether a process died at its CPU time limit: of SIGXCPU at the soft
// limit, or of SIGKILL having used as much CPU as the limit (Linux sends SIGKILL, not SIGXCPU,
// at a hard limit equal to the soft one). The server has the same limits as git: they are
// the launcher's.
func cpuLimited(st *os.ProcessState) bool {
	ws, ok := st.Sys().(syscall.WaitStatus)
	if !ok || !ws.Signaled() {
		return false
	}
	switch ws.Signal() {
	case syscall.SIGXCPU:
		return true
	case syscall.SIGKILL:
		var rl syscall.Rlimit // seconds; RLIM_INFINITY, whatever its value on this OS, is far more
		if err := syscall.Getrlimit(syscall.RLIMIT_CPU, &rl); err != nil || rl.Cur > 1<<31 {
			return false
		}
		return st.UserTime()+st.SystemTime() >= time.Duration(rl.Cur)*time.Second
	}
	return false
}

// git runs a read-only git command in the mirror. With limit > 0, it reads at most limit bytes
// of output and stops git past that: a small crafted repository can make a huge diff. Its
// errors name the mirror "<mirror>": they reach the workflow, and from there maybe a public
// report.
func (s *Server) git(ctx context.Context, env []string, stdin io.Reader, limit int, args ...string) ([]byte, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", s.Mirror}, args...)...)
	cmd.Env = gitenv.ForMirror(s.Mirror, env...)
	cmd.Stdin = stdin
	stderr := &limitedBuffer{max: 16 << 10}
	cmd.Stderr = stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("git %s: %w", args[0], err)
	}
	r := io.Reader(stdout)
	if limit > 0 {
		r = io.LimitReader(stdout, int64(limit)+1)
	}
	out, readErr := io.ReadAll(r)
	if limit > 0 && len(out) > limit {
		cancel() // stop git; whatever it says now does not matter
		_ = cmd.Wait()
		return nil, &outputLimitError{}
	}
	err = cmd.Wait()
	if msg := stderr.String(); msg != "" && s.Stderr != nil {
		fmt.Fprintf(s.Stderr, "git %s: %s\n", args[0], msg)
	}
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			if cpuLimited(exit.ProcessState) {
				return nil, &cpuLimitError{cmd: args[0]}
			}
			if exit.ExitCode() > 0 {
				msg := strings.ReplaceAll(strings.TrimSpace(stderr.String()), s.Mirror, "<mirror>")
				return nil, &gitError{code: exit.ExitCode(), msg: fmt.Sprintf("git %s: %s", args[0], msg)}
			}
		}
		return nil, fmt.Errorf("git %s: %w", args[0], err)
	}
	if readErr != nil {
		return nil, fmt.Errorf("git %s: %w", args[0], readErr)
	}
	return out, nil
}
