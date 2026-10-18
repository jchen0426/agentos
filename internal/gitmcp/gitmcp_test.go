package gitmcp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const safe = "def find_user(conn, name):\n    return conn.execute(\"SELECT * FROM users WHERE name = ?\", (name,))\n"
const vulnerable = "def find_user(conn, name):\n    return conn.execute(\"SELECT * FROM users WHERE name = '%s'\" % name)\n"

const fix = `diff --git a/app/db.py b/app/db.py
--- a/app/db.py
+++ b/app/db.py
@@ -1,2 +1,2 @@
 def find_user(conn, name):
-    return conn.execute("SELECT * FROM users WHERE name = '%s'" % name)
+    return conn.execute("SELECT * FROM users WHERE name = ?", (name,))
`

type repo struct {
	t   *testing.T
	dir string
}

func newRepo(t *testing.T) *repo {
	t.Helper()
	r := &repo{t: t, dir: t.TempDir()}
	r.git("init", "-q", "-b", "main")
	return r
}

func (r *repo) git(args ...string) string {
	r.t.Helper()
	cmd := exec.Command("git", append([]string{"-C", r.dir}, args...)...)
	cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com",
		"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com")
	out, err := cmd.CombinedOutput()
	if err != nil {
		r.t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

// gitIn runs git with stdin.
func (r *repo) gitIn(stdin string, args ...string) string {
	r.t.Helper()
	cmd := exec.Command("git", append([]string{"-C", r.dir}, args...)...)
	cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com",
		"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com")
	cmd.Stdin = strings.NewReader(stdin)
	out, err := cmd.CombinedOutput()
	if err != nil {
		r.t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func (r *repo) write(path, body string) {
	r.t.Helper()
	full := filepath.Join(r.dir, path)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		r.t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
		r.t.Fatal(err)
	}
}

func (r *repo) commit(msg string) string {
	r.t.Helper()
	r.git("add", "-A")
	r.git("commit", "-q", "-m", msg)
	return r.git("rev-parse", "HEAD")
}

// mirror makes a repository whose last commit introduces the SQL injection. It returns its
// path and the first and last commits.
func mirror(t *testing.T) (string, string, string) {
	t.Helper()
	r := newRepo(t)
	r.write("app/db.py", safe)
	first := r.commit("init")
	r.write("app/db.py", vulnerable)
	return r.dir, first, r.commit("find users by name")
}

// connect serves s in this process and returns a client session for it.
func connect(t *testing.T, s *Server) *mcp.ClientSession {
	t.Helper()
	ctx := context.Background()
	st, ct := mcp.NewInMemoryTransports()
	if _, err := s.MCP().Connect(ctx, st, nil); err != nil {
		t.Fatal(err)
	}
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test"}, nil).Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cs.Close() })
	return cs
}

// call returns the tool's structured output, or the tool error's text, or fails the test on a
// protocol error unless protocolErr is set, in which case it returns that error's text.
func call(t *testing.T, cs *mcp.ClientSession, tool string, args any, out any) (toolErr string, protocolErr error) {
	t.Helper()
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: tool, Arguments: args})
	if err != nil {
		return "", err
	}
	if res.IsError {
		return res.Content[0].(*mcp.TextContent).Text, nil
	}
	b, _ := json.Marshal(res.StructuredContent)
	if err := json.Unmarshal(b, out); err != nil {
		t.Fatal(err)
	}
	return "", nil
}

func diffOf(t *testing.T, cs *mcp.ClientSession, commit string) []byte {
	t.Helper()
	var out ShowDiffOut
	if msg, err := call(t, cs, "show_diff", ShowDiffIn{Commit: commit}, &out); msg != "" || err != nil {
		t.Fatalf("show_diff %s: %q %v", commit, msg, err)
	}
	d, err := out.Diff()
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestShowDiff(t *testing.T) {
	dir, first, head := mirror(t)
	cs := connect(t, &Server{Mirror: dir})
	if d := diffOf(t, cs, head); !bytes.Contains(d, []byte(`+    return conn.execute("SELECT * FROM users WHERE name = '%s'" % name)`)) {
		t.Fatalf("show_diff: %s", d)
	}
	if d := diffOf(t, cs, first); !bytes.Contains(d, []byte("+def find_user")) { // a first commit shows whole
		t.Fatalf("show_diff of the first commit: %s", d)
	}
	for _, bad := range []string{"HEAD", "--output=/tmp/x", head[:12]} {
		if msg, err := call(t, cs, "show_diff", ShowDiffIn{Commit: bad}, &ShowDiffOut{}); msg == "" || err != nil {
			t.Errorf("show_diff %q: %q %v, want a tool error", bad, msg, err)
		}
	}
	// agentd found the run's commit in this mirror: one the server cannot see is its failure.
	if msg, err := call(t, cs, "show_diff", ShowDiffIn{Commit: strings.Repeat("0", 40)}, &ShowDiffOut{}); err == nil {
		t.Errorf("an unknown commit: %q, want a protocol error", msg)
	}
}

// A crafted commit cannot hide its change from the diff: not with attributes in the checked-out
// tree, not with a NUL byte, not with a replace ref that swaps in another commit.
func TestShowDiffShowsWhatTheCommitDoes(t *testing.T) {
	r := newRepo(t)
	r.write("app/db.py", safe)
	r.write(".gitattributes", "*.py -diff\n")
	r.commit("init")
	r.write("app/db.py", vulnerable)
	r.write("app/blob.js", "// \x00\nsteal(cookies)\n")
	evil := r.commit("backdoor")
	cs := connect(t, &Server{Mirror: r.dir})
	d := diffOf(t, cs, evil)
	for _, want := range []string{"name = '%s'", "steal(cookies)"} {
		if !bytes.Contains(d, []byte(want)) {
			t.Errorf("the diff hides %q:\n%s", want, d)
		}
	}

	r.write("app/db.py", "harmless\n")
	benign := r.commit("benign")
	r.git("replace", evil, benign)
	if d := diffOf(t, cs, evil); !bytes.Contains(d, []byte("name = '%s'")) {
		t.Errorf("a replace ref swapped the commit:\n%s", d)
	}

	// No GIT_ variable of the server's own environment reaches git: here, another repository.
	other := newRepo(t)
	t.Setenv("GIT_DIR", filepath.Join(other.dir, ".git"))
	if d := diffOf(t, cs, evil); !bytes.Contains(d, []byte("name = '%s'")) {
		t.Errorf("GIT_DIR reached git:\n%s", d)
	}
}

// The diff comes back byte for byte, UTF-8 or not, and one over the limit is refused without
// reading all of it.
func TestShowDiffBytesAndLimit(t *testing.T) {
	r := newRepo(t)
	r.write("a.txt", "x\n")
	r.commit("init")
	r.write("legacy.py", "# caf\xe9 \xd6\xd0\xce\xc4 <&>\n")
	latin := r.commit("latin-1")
	cs := connect(t, &Server{Mirror: r.dir})
	if d := diffOf(t, cs, latin); !bytes.Contains(d, []byte("# caf\xe9 \xd6\xd0\xce\xc4 <&>")) {
		t.Errorf("the bytes changed: %q", d)
	}

	// 300 paths sharing one blob: a small repository, a huge diff.
	big := strings.Repeat("<", 64<<10) + "\n"
	for i := range 300 {
		r.write(fmt.Sprintf("bomb/%03d.txt", i), big)
	}
	bomb := r.commit("bomb")
	start := time.Now()
	msg, err := call(t, cs, "show_diff", ShowDiffIn{Commit: bomb}, &ShowDiffOut{})
	if err != nil || !strings.Contains(msg, "over the limit") {
		t.Fatalf("a huge diff: %q %v", msg, err)
	}
	if time.Since(start) > 10*time.Second {
		t.Errorf("refusing a huge diff took %s", time.Since(start))
	}

	// One file of empty lines just over the limit, added and then removed: tiny in the
	// repository, hundreds of megabytes for git to diff. Neither side is diffed.
	r.write("lines.txt", strings.Repeat("\n", MaxFileBytes+1))
	added := r.commit("add a huge file")
	r.git("rm", "-q", "lines.txt")
	removed := r.commit("remove it")
	for _, c := range []string{added, removed} {
		msg, err := call(t, cs, "show_diff", ShowDiffIn{Commit: c}, &ShowDiffOut{})
		if want := fmt.Sprintf("%q: %d bytes, over the limit", "lines.txt", MaxFileBytes+1); err != nil || !strings.Contains(msg, want) {
			t.Errorf("a huge file: %q %v", msg, err)
		}
	}
	// A file at the limit (of long lines, which git diffs cheaply) and a submodule, whose
	// object is a commit git only names, pass; the diff is then too long.
	r.write("lines.txt", strings.Repeat(strings.Repeat("x", 1023)+"\n", MaxFileBytes/1024))
	r.git("add", "lines.txt")
	r.git("update-index", "--add", "--cacheinfo", "160000,"+bomb+",vendor/lib")
	r.git("commit", "-q", "-m", "a file at the limit and a submodule")
	msg, err = call(t, cs, "show_diff", ShowDiffIn{Commit: r.git("rev-parse", "HEAD")}, &ShowDiffOut{})
	if err != nil || !strings.Contains(msg, "the diff is over the limit") {
		t.Errorf("a file at the limit: %q %v", msg, err)
	}
}

// What git would read whole is sized up from object headers first: a commit object, the files
// a commit changes, all of them together. Refusals quote a path cut short.
func TestShowDiffSizesUpTheCommitFirst(t *testing.T) {
	r := newRepo(t)
	r.write("a.txt", "x\n")
	base := r.commit("init")
	cs := connect(t, &Server{Mirror: r.dir})

	// A commit message just over the limit (gigabytes pack into megabytes, too).
	msgFile := filepath.Join(t.TempDir(), "message")
	if err := os.WriteFile(msgFile, []byte(strings.Repeat("x", MaxCommitBytes+1)), 0o600); err != nil {
		t.Fatal(err)
	}
	huge := r.git("commit-tree", base+"^{tree}", "-p", base, "-F", msgFile)
	if msg, err := call(t, cs, "show_diff", ShowDiffIn{Commit: huge}, &ShowDiffOut{}); err != nil ||
		!strings.Contains(msg, "commit "+huge) || !strings.Contains(msg, fmt.Sprintf("over the limit of %d", MaxCommitBytes)) {
		t.Errorf("a huge commit: %q %v", msg, err)
	}
	// An annotated tag's own ID is not a commit's.
	r.git("tag", "-a", "v1", "-m", "a tag", base)
	if msg, err := call(t, cs, "show_diff", ShowDiffIn{Commit: r.git("rev-parse", "v1")}, &ShowDiffOut{}); err != nil ||
		!strings.Contains(msg, "is a tag, not a commit") {
		t.Errorf("a tag: %q %v", msg, err)
	}

	// One file at the limit, at enough paths to go over the limit for all of them together.
	big := filepath.Join(t.TempDir(), "big")
	if err := os.WriteFile(big, []byte(strings.Repeat("y\n", MaxFileBytes/2)), 0o600); err != nil {
		t.Fatal(err)
	}
	blob := r.git("hash-object", "-w", big)
	var index strings.Builder
	for i := range MaxDiffInput/MaxFileBytes + 1 {
		fmt.Fprintf(&index, "100644 %s\tmany/%03d.txt\n", blob, i)
	}
	r.gitIn(index.String(), "update-index", "--index-info")
	r.git("commit", "-q", "-m", "many big files")
	if msg, err := call(t, cs, "show_diff", ShowDiffIn{Commit: r.git("rev-parse", "HEAD")}, &ShowDiffOut{}); err != nil ||
		!strings.Contains(msg, fmt.Sprintf("over the limit of %d for a diff", MaxDiffInput)) {
		t.Errorf("many big files: %q %v", msg, err)
	}

	// A path of 1,000 characters, quoted cut short.
	long := strings.Repeat(strings.Repeat("d", 249)+"/", 4) + "big.txt"
	r.gitIn(fmt.Sprintf("100644 %s\t%s\n", r.gitIn(strings.Repeat("z", MaxFileBytes+1), "hash-object", "-w", "--stdin"), long),
		"update-index", "--index-info")
	r.git("commit", "-q", "-m", "a long path")
	msg, err := call(t, cs, "show_diff", ShowDiffIn{Commit: r.git("rev-parse", "HEAD")}, &ShowDiffOut{})
	if err != nil || !strings.Contains(msg, "over the limit") || len(msg) > 400 {
		t.Errorf("a long path: %d bytes: %q %v", len(msg), msg, err)
	}
}

// A merge is shown against its first parent: what it brings in, not what the first parent
// gained since the branch point.
func TestShowDiffShowsAMergeAgainstItsFirstParent(t *testing.T) {
	r := newRepo(t)
	r.write("a.txt", "x\n")
	r.commit("init")
	r.git("checkout", "-q", "-b", "feature")
	r.write("feature.py", "brought in by the merge\n")
	r.commit("feature")
	r.git("checkout", "-q", "main")
	r.write("mainline.py", "already on main\n")
	r.commit("main moves on")
	r.git("merge", "-q", "--no-edit", "feature")
	cs := connect(t, &Server{Mirror: r.dir})
	d := diffOf(t, cs, r.git("rev-parse", "HEAD"))
	if !bytes.Contains(d, []byte("feature.py")) || bytes.Contains(d, []byte("mainline.py")) {
		t.Errorf("the merge's diff:\n%s", d)
	}
}

// A mirror git cannot read is the server's failure, a protocol error that is retried, not a
// refusal of the commit; and git never reads the repository around it instead.
func TestServerReportsAnUnusableMirror(t *testing.T) {
	dir, _, head := mirror(t)
	inside := filepath.Join(dir, "app", "not-a-mirror") // a directory in a checkout
	if err := os.Mkdir(inside, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, m := range []string{t.TempDir(), inside} {
		cs := connect(t, &Server{Mirror: m})
		if msg, err := call(t, cs, "show_diff", ShowDiffIn{Commit: head}, &ShowDiffOut{}); err == nil {
			t.Errorf("show_diff in %s: %q, want a protocol error", m, msg)
		}
		if msg, err := call(t, cs, "apply_check", ApplyCheckIn{Commit: head, Patch: fix, Paths: []string{"app/db.py"}}, &ApplyCheckOut{}); err == nil {
			t.Errorf("apply_check in %s: %q, want a protocol error", m, msg)
		}
	}
}

func TestApplyCheck(t *testing.T) {
	dir, first, head := mirror(t)
	cs := connect(t, &Server{Mirror: dir})
	before, _ := os.ReadFile(filepath.Join(dir, ".git", "index"))
	paths := []string{"app/db.py"}

	var out ApplyCheckOut
	if msg, err := call(t, cs, "apply_check", ApplyCheckIn{Commit: head, Patch: fix, Paths: paths}, &out); msg != "" || err != nil || !out.Applies {
		t.Fatalf("the fix: %+v %q %v", out, msg, err)
	}
	// The fix does not apply to the commit before the injection: git's own "no".
	out = ApplyCheckOut{}
	if msg, err := call(t, cs, "apply_check", ApplyCheckIn{Commit: first, Patch: fix, Paths: paths}, &out); msg != "" || err != nil ||
		out.Applies || !strings.Contains(out.Reason, "app/db.py") || strings.Contains(out.Reason, dir) {
		t.Fatalf("the fix on the first commit: %+v %q %v", out, msg, err)
	}
	if msg, _ := call(t, cs, "apply_check", ApplyCheckIn{Commit: "main", Patch: fix, Paths: paths}, &out); msg == "" {
		t.Error("a branch name instead of a commit was accepted")
	}
	// A patch git cannot apply at all, here one without hunks: a "no", not a failure to retry.
	out = ApplyCheckOut{}
	if msg, err := call(t, cs, "apply_check", ApplyCheckIn{Commit: head, Patch: "diff --git a/app/db.py b/app/db.py\n", Paths: paths}, &out); msg != "" ||
		err != nil || out.Applies || out.Reason == "" {
		t.Fatalf("a patch without hunks: %+v %q %v", out, msg, err)
	}

	after, _ := os.ReadFile(filepath.Join(dir, ".git", "index"))
	if !bytes.Equal(before, after) {
		t.Error("apply_check changed the mirror's index")
	}
	if status, err := exec.Command("git", "-C", dir, "status", "--porcelain").Output(); err != nil || len(status) != 0 {
		t.Errorf("the mirror changed: %q %v", status, err)
	}

	// A server that cannot do its work (here: nowhere to put the index) says so as a protocol
	// error, which the client retries, not as a refusal of the patch.
	broken := connect(t, &Server{Mirror: dir, TempDir: filepath.Join(t.TempDir(), "missing")})
	if msg, err := call(t, broken, "apply_check", ApplyCheckIn{Commit: head, Patch: fix, Paths: paths}, &out); msg != "" || err == nil {
		t.Fatalf("no temporary directory: %q %v, want a protocol error", msg, err)
	}
}

func build(t *testing.T) string {
	t.Helper()
	if bin := os.Getenv("AGENTOS_TEST_MCP_GIT"); bin != "" { // prebuilt, where there is no Go toolchain
		return bin
	}
	bin := filepath.Join(t.TempDir(), "agentos-mcp-git")
	if out, err := exec.Command("go", "build", "-o", bin, "github.com/jchen0426/agentos/cmd/agentos-mcp-git").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	return bin
}

// The client runs the real binary, as agentd does, here without a sandbox.
func TestClientRunsTheServerPerCall(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the server")
	}
	bin := build(t)
	dir, _, head := mirror(t)
	c, err := NewClient(Options{Server: bin, Timeout: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.SelfCheck(context.Background()); err != nil {
		t.Fatal(err)
	}
	var diff ShowDiffOut
	if err := c.Call(context.Background(), dir, "show_diff", ShowDiffIn{Commit: head}, &diff); err != nil {
		t.Fatal(err)
	}
	if d, err := diff.Diff(); err != nil || !bytes.Contains(d, []byte("app/db.py")) {
		t.Fatalf("diff: %q %v", d, err)
	}
	if err := c.Call(context.Background(), dir, "show_diff", ShowDiffIn{Commit: "HEAD"}, &diff); !IsToolError(err) {
		t.Fatalf("a branch name: %v", err)
	}
	if err := c.Call(context.Background(), dir, "show_diff", ShowDiffIn{Commit: strings.Repeat("0", 40)}, &diff); err == nil || IsToolError(err) {
		t.Fatalf("an unknown commit: %v, want a failure", err)
	}
	// The largest patch, every byte of it escaped in the request (\u0001 takes six), gets through.
	line := "+" + strings.Repeat("\x01", 1023) + "\n"
	n := (MaxPatchBytes - 128) / len(line)
	patch := fmt.Sprintf("diff --git a/new.txt b/new.txt\nnew file mode 100644\n--- /dev/null\n+++ b/new.txt\n@@ -0,0 +1,%d @@\n%s",
		n, strings.Repeat(line, n))
	var check ApplyCheckOut
	err = c.Call(context.Background(), dir, "apply_check", ApplyCheckIn{Commit: head, Patch: patch, Paths: []string{"new.txt"}}, &check)
	if err != nil || !check.Applies || len(patch) > MaxPatchBytes {
		t.Fatalf("a patch of %d bytes: %+v %v", len(patch), check, err)
	}

	// A launcher that fails says why on stderr, which may name local paths: that stays out of
	// the error, which reaches the workflow.
	c2, err := NewClient(Options{Server: bin, Timeout: 30 * time.Second,
		Sandbox: []string{"/bin/sh", "-c", "echo run npm ci in /Users/someone/secret-place >&2; exit 1", "sh"}})
	if err != nil {
		t.Fatal(err)
	}
	err = c2.Call(context.Background(), dir, "show_diff", ShowDiffIn{Commit: head}, &diff)
	if err == nil || IsToolError(err) || strings.Contains(err.Error(), "secret-place") {
		t.Fatalf("a failing launcher: %v", err)
	}
	if c2.SelfCheck(context.Background()) == nil {
		t.Error("the self check passed with a launcher that cannot work")
	}
	if left := callDirs(); len(left) != 0 {
		t.Errorf("left behind: %v", left)
	}
}

// A commit too costly to diff is refused the same way every time: git dies at the launcher's
// CPU time limit. A subverted server cannot leave its call's directory behind by taking the
// permissions off it.
func TestClientLimits(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the server")
	}
	bin := build(t)
	r := newRepo(t)
	r.write("lines.txt", "x\n")
	r.commit("init")
	// Random short lines: git's diff costs it seconds (some 10 s for 2 MiB on an M-series Mac).
	var old, new strings.Builder
	ro, rn := rand.New(rand.NewPCG(1, 1)), rand.New(rand.NewPCG(2, 2))
	for range 1 << 20 {
		old.WriteString([]string{"\n", "x\n"}[ro.IntN(2)])
		new.WriteString([]string{"\n", "x\n"}[rn.IntN(2)])
	}
	r.write("lines.txt", old.String())
	r.commit("old")
	r.write("lines.txt", new.String())
	slow := r.commit("new")
	// Soft and hard limit alike: SIGXCPU on macOS, SIGKILL on Linux.
	launcher := []string{"/bin/sh", "-c", `ulimit -t 1; shift 2; exec "$@"`, "sh"} // <mirror> -- <server> ...
	c, err := NewClient(Options{Server: bin, Sandbox: launcher, Timeout: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	err = c.Call(context.Background(), r.dir, "show_diff", ShowDiffIn{Commit: slow}, &ShowDiffOut{})
	if !IsToolError(err) || !strings.Contains(err.Error(), "CPU time") {
		t.Fatalf("a commit too costly to diff: %v", err)
	}

	locker := []string{"/bin/sh", "-c", `mkdir -p "$AGENTOS_SANDBOX_STATE/a/b" && touch "$AGENTOS_SANDBOX_STATE/a/b/f" &&
		chmod 000 "$AGENTOS_SANDBOX_STATE/a/b" "$AGENTOS_SANDBOX_STATE/a"; exit 1`, "sh"}
	c2, err := NewClient(Options{Server: bin, Sandbox: locker, Timeout: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := c2.Call(context.Background(), r.dir, "show_diff", ShowDiffIn{Commit: slow}, &ShowDiffOut{}); err == nil {
		t.Fatal("no error from a launcher that fails")
	}
	if left := callDirs(); len(left) != 0 {
		t.Errorf("left behind: %v", left)
	}
}

// callDirs lists this process's per-call directories.
func callDirs() []string {
	dirs, _ := filepath.Glob(filepath.Join(stateRoot, fmt.Sprintf("agentos-tools.%d.*", os.Getpid())))
	return dirs
}

// A server that does not answer is killed with everything it started, and its call's
// directory is gone when Call returns.
func TestClientKillsAServerThatHangs(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "still-running")
	// The "launcher" starts a child that would outlive it, then hangs.
	launcher := []string{"/bin/sh", "-c", fmt.Sprintf("(sleep 30; touch %s) & sleep 30", marker), "sh"}
	c, err := NewClient(Options{Server: "/bin/true", Sandbox: launcher, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	// The machine may be what is slow: a failure to retry, not a refusal.
	err = c.Call(context.Background(), t.TempDir(), "show_diff", ShowDiffIn{Commit: strings.Repeat("a", 40)}, &ShowDiffOut{})
	if err == nil || IsToolError(err) {
		t.Fatalf("a server that hangs: %v", err)
	}
	if time.Since(start) > 10*time.Second {
		t.Errorf("Call took %s", time.Since(start))
	}
	if left := callDirs(); len(left) != 0 {
		t.Errorf("left behind: %v", left)
	}
	out, _ := exec.Command("pgrep", "-f", "touch "+marker).Output()
	if len(bytes.TrimSpace(out)) != 0 {
		t.Errorf("the server's child still runs: %s", out)
	}
}
