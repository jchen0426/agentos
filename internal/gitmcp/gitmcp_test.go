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
	"maps"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/jchen0426/agentos/internal/gitenv"
	"github.com/jchen0426/agentos/internal/gitmcp/gitmcptest"
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

// rawTree writes a tree object as given, unchecked: each entry is "<mode> <name>" and an
// object. git mktree refuses what a crafted repository can still hold.
func (r *repo) rawTree(entries ...[2]string) string {
	r.t.Helper()
	var b bytes.Buffer
	for _, e := range entries {
		id, err := hex.DecodeString(e[1])
		if err != nil {
			r.t.Fatal(err)
		}
		b.WriteString(e[0])
		b.WriteByte(0)
		b.Write(id)
	}
	return r.gitIn(b.String(), "hash-object", "-t", "tree", "-w", "--literally", "--stdin")
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
		if msg, err := call(t, cs, "resolve_commit", ResolveCommitIn{Commit: head[:7]}, &ResolveCommitOut{}); err == nil {
			t.Errorf("resolve_commit in %s: %q, want a protocol error", m, msg)
		}
		if msg, err := call(t, cs, "read_files", ReadFilesIn{Commit: head, Paths: []string{"app/db.py"}}, &ReadFilesOut{}); err == nil {
			t.Errorf("read_files in %s: %q, want a protocol error", m, msg)
		}
	}
}

// gitID is the ID git gives an object of the kind with body.
func gitID(kind string, body []byte) string {
	h := sha1.New()
	fmt.Fprintf(h, "%s %d\x00", kind, len(body))
	h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}

// sharedDigits finds objects whose IDs begin with the same 7 digits, by trying commits (on the
// empty tree) and blobs in turn: a commit and a blob, and two commits. It returns their bodies.
func sharedDigits() (commitAndBlob, twoCommits [2][]byte) {
	commits, blobs := map[string][]byte{}, map[string][]byte{}
	for i := 0; commitAndBlob[0] == nil || twoCommits[0] == nil; i++ {
		c := fmt.Appendf(nil, "tree %s\nauthor t <t@example.com> 1700000000 +0000\ncommitter t <t@example.com> 1700000000 +0000\n\ncommit %d\n",
			gitenv.EmptyTree, i)
		b := fmt.Appendf(nil, "blob %d\n", i)
		cd, bd := gitID("commit", c)[:7], gitID("blob", b)[:7]
		switch {
		case twoCommits[0] == nil && commits[cd] != nil:
			twoCommits = [2][]byte{commits[cd], c}
		case commitAndBlob[0] == nil && blobs[cd] != nil:
			commitAndBlob = [2][]byte{c, blobs[cd]}
		case commitAndBlob[0] == nil && commits[bd] != nil:
			commitAndBlob = [2][]byte{commits[bd], b}
		}
		commits[cd], blobs[bd] = c, b
	}
	return commitAndBlob, twoCommits
}

func resolved(t *testing.T, cs *mcp.ClientSession, digits string) (string, string) {
	t.Helper()
	var out ResolveCommitOut
	msg, err := call(t, cs, "resolve_commit", ResolveCommitIn{Commit: digits}, &out)
	if err != nil {
		t.Fatalf("resolve_commit %s: %v", digits, err)
	}
	return out.Commit, msg
}

// Digits name one commit, from object headers alone: not a blob or a tag that shares them, not
// two commits, not one too large to read.
func TestResolveCommit(t *testing.T) {
	dir, _, head := mirror(t)
	r := &repo{t: t, dir: dir}
	cs := connect(t, &Server{Mirror: dir})
	for _, digits := range []string{head, head[:7], head[:12]} {
		if got, msg := resolved(t, cs, digits); got != head || msg != "" {
			t.Errorf("%s: %q %q", digits, got, msg)
		}
	}
	for _, bad := range []string{"HEAD", "main", head[:6], "ABCDEF0", "--output=/tmp/x", head + "0"} {
		if _, msg := resolved(t, cs, bad); !strings.Contains(msg, "7 to 40 hexadecimal digits") {
			t.Errorf("%q: %q, want a refusal", bad, msg)
		}
	}
	r.git("tag", "-a", "v1", "-m", "a tag", head)
	for _, other := range []string{"0000000", r.git("rev-parse", head+":app/db.py"), r.git("rev-parse", "v1"), r.git("rev-parse", head+"^{tree}")} {
		if _, msg := resolved(t, cs, other[:7]); !strings.Contains(msg, "not found in the mirror") {
			t.Errorf("%s: %q, want not found", other, msg)
		}
	}
	msgFile := filepath.Join(t.TempDir(), "message")
	if err := os.WriteFile(msgFile, []byte(strings.Repeat("x", MaxCommitBytes+1)), 0o600); err != nil {
		t.Fatal(err)
	}
	huge := r.git("commit-tree", head+"^{tree}", "-p", head, "-F", msgFile)
	if _, msg := resolved(t, cs, huge[:7]); !strings.Contains(msg, fmt.Sprintf("over the limit of %d", MaxCommitBytes)) {
		t.Errorf("a huge commit: %q", msg)
	}

	commitAndBlob, twoCommits := sharedDigits()
	commit := r.gitIn(string(commitAndBlob[0]), "hash-object", "-t", "commit", "-w", "--stdin")
	blob := r.gitIn(string(commitAndBlob[1]), "hash-object", "-w", "--stdin")
	if commit[:7] != blob[:7] {
		t.Fatalf("%s and %s do not share 7 digits", commit, blob)
	}
	if got, msg := resolved(t, cs, commit[:7]); got != commit || msg != "" {
		t.Errorf("digits a blob shares: %q %q", got, msg)
	}
	first := r.gitIn(string(twoCommits[0]), "hash-object", "-t", "commit", "-w", "--stdin")
	second := r.gitIn(string(twoCommits[1]), "hash-object", "-t", "commit", "-w", "--stdin")
	if _, msg := resolved(t, cs, first[:7]); first[:7] != second[:7] || !strings.Contains(msg, "names 2 commits") {
		t.Errorf("digits two commits share (%s, %s): %q", first, second, msg)
	}
	n := 7
	for first[n] == second[n] {
		n++
	}
	if got, msg := resolved(t, cs, second[:n+1]); got != second || msg != "" {
		t.Errorf("enough digits to tell the two apart: %q %q", got, msg)
	}
}

// readFiles calls read_files and fails the test on a refusal or a protocol error.
func readFiles(t *testing.T, cs *mcp.ClientSession, commit string, paths ...string) []File {
	t.Helper()
	var out ReadFilesOut
	if msg, err := call(t, cs, "read_files", ReadFilesIn{Commit: commit, Paths: paths}, &out); msg != "" || err != nil {
		t.Fatalf("read_files %v: %q %v", paths, msg, err)
	}
	if len(out.Files) != len(paths) {
		t.Fatalf("read_files %v: %d answers", paths, len(out.Files))
	}
	for i := range out.Files {
		b, err := base64.StdEncoding.DecodeString(out.Files[i].ContentBase64)
		if err != nil {
			t.Fatal(err)
		}
		out.Files[i].Content, out.Files[i].ContentBase64 = b, ""
	}
	return out.Files
}

// Each path's entry as the commit has it, whatever it is, and the content of the files small
// enough to read, byte for byte; or, where nothing is, whether a directory above is something
// else.
func TestReadFiles(t *testing.T) {
	r := newRepo(t)
	r.write("app/db.py", vulnerable)
	r.write("copy.py", vulnerable)   // the same blob at two paths
	legacy := "# caf\xe9 \x00 <&>\n" // neither UTF-8 nor text
	r.write("app/legacy.py", legacy)
	r.write("app/run.sh", "#!/bin/sh\n")
	if err := os.Chmod(filepath.Join(r.dir, "app/run.sh"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("db.py", filepath.Join(r.dir, "app/link.py")); err != nil {
		t.Fatal(err)
	}
	r.write("limit.txt", strings.Repeat("y", MaxReadBytes))
	r.write("big.txt", strings.Repeat("z", MaxReadBytes+1))
	r.write("d1/d2/f", "a file where a directory could be\n")
	r.write("empty.txt", "")
	base := r.commit("files")
	r.git("update-index", "--add", "--cacheinfo", "160000,"+base+",vendor/lib")
	r.git("commit", "-q", "-m", "a submodule")
	head := r.git("rev-parse", "HEAD")
	cs := connect(t, &Server{Mirror: r.dir})

	type want struct {
		mode, typ, notDir string
		size              int64
		content           string
	}
	cases := map[string]want{
		"app/db.py":                 {"100644", "blob", "", int64(len(vulnerable)), vulnerable},
		"copy.py":                   {"100644", "blob", "", int64(len(vulnerable)), vulnerable},
		"app/legacy.py":             {"100644", "blob", "", int64(len(legacy)), legacy},
		"app/run.sh":                {"100755", "blob", "", 10, "#!/bin/sh\n"},
		"app/link.py":               {"120000", "blob", "", 5, "db.py"},
		"limit.txt":                 {"100644", "blob", "", MaxReadBytes, strings.Repeat("y", MaxReadBytes)},
		"big.txt":                   {"100644", "blob", "", MaxReadBytes + 1, ""}, // sized up, not read
		"empty.txt":                 {"100644", "blob", "", 0, ""},
		"app":                       {"040000", "tree", "", 0, ""},
		"vendor/lib":                {"160000", "commit", "", 0, ""},
		"app/missing.py":            {},
		"new/dir/x.py":              {}, // nothing on the way either
		"d1/d2/new/d4/x.py":         {},
		"app/link.py/inside.py":     {notDir: "app/link.py"},
		"app/db.py/x":               {notDir: "app/db.py"},
		"vendor/lib/x.py":           {notDir: "vendor/lib"},
		"d1/d2/f/d4/d5/d6/d7/x.py":  {notDir: "d1/d2/f"},
		"d1/d2/f/x":                 {notDir: "d1/d2/f"},
		"d1/d2":                     {"040000", "tree", "", 0, ""},
		"app/legacy.py/a/b/c/d/e/f": {notDir: "app/legacy.py"},
	}
	paths := slices.Sorted(maps.Keys(cases))
	for i, f := range readFiles(t, cs, head, paths...) {
		w := cases[paths[i]]
		if f.Path != paths[i] || f.Mode != w.mode || f.Type != w.typ || f.NotDir != w.notDir || f.Size != w.size || string(f.Content) != w.content {
			t.Errorf("%s: %s %s not_dir %q, %d bytes, content %.40q; want %+v", paths[i], f.Mode, f.Type, f.NotDir, f.Size, f.Content, w)
		}
	}
	// A directory and a file in it, asked for together, each come back as itself.
	if got := readFiles(t, cs, head, "app", "app/db.py"); got[0].Type != "tree" || string(got[1].Content) != vulnerable {
		t.Errorf("a directory and a file in it: %+v", got)
	}

	// A replace ref changes neither the commit's tree nor a file in it.
	r.git("replace", r.git("rev-parse", head+":app/db.py"), r.git("rev-parse", head+":app/run.sh"))
	if got := readFiles(t, cs, head, "app/db.py"); string(got[0].Content) != vulnerable {
		t.Errorf("a replace ref swapped the file: %q", got[0].Content)
	}
}

// Every path is looked up on its own: one file among thousands in its directory is found
// without git listing the rest, and so is a new one.
func TestReadFilesInALargeDirectory(t *testing.T) {
	r := newRepo(t)
	blob := r.gitIn("x\n", "hash-object", "-w", "--stdin")
	name := func(i int) string { return fmt.Sprintf("big/%s-%04d.txt", strings.Repeat("n", 200), i) }
	var index strings.Builder
	for i := range 3000 {
		fmt.Fprintf(&index, "100644 %s\t%s\n", blob, name(i))
	}
	r.gitIn(index.String(), "update-index", "--index-info")
	r.git("commit", "-q", "-m", "a large directory")
	cs := connect(t, &Server{Mirror: r.dir})
	got := readFiles(t, cs, r.git("rev-parse", "HEAD"), name(1234), "big/new.txt")
	if string(got[0].Content) != "x\n" || got[1].Type != "" || got[1].NotDir != "" {
		t.Errorf("in a large directory: %+v", got)
	}
}

// Paths are taken byte for byte, also in a repository made on macOS, where git would otherwise
// turn a decomposed name into the composed one.
func TestReadFilesTakesPathsByteForByte(t *testing.T) {
	r := newRepo(t)
	r.git("config", "core.precomposeUnicode", "true")
	decomposed, composed := "cafe\u0301.txt", "caf\u00e9.txt"
	blob := r.gitIn("x\n", "hash-object", "-w", "--stdin")
	c := r.git("commit-tree", r.rawTree([2]string{"100644 " + decomposed, blob}), "-m", "a decomposed name")
	cs := connect(t, &Server{Mirror: r.dir})
	got := readFiles(t, cs, c, decomposed, composed)
	if string(got[0].Content) != "x\n" || got[1].Type != "" {
		t.Errorf("decomposed %+v, composed %+v", got[0], got[1])
	}
}

// git says on stderr when it could not read some objects, and goes on: finding no commit then
// is the server's failure, not a refusal of the digits.
func TestResolveCommitWithObjectsGitCannotRead(t *testing.T) {
	dir, _, head := mirror(t)
	if err := os.WriteFile(filepath.Join(dir, ".git", "objects", "info", "alternates"), []byte(filepath.Join(t.TempDir(), "gone")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cs := connect(t, &Server{Mirror: dir})
	if got, msg := resolved(t, cs, head[:7]); got != head || msg != "" {
		t.Errorf("a commit it can read: %q %q", got, msg)
	}
	if msg, err := call(t, cs, "resolve_commit", ResolveCommitIn{Commit: "0000000"}, &ResolveCommitOut{}); err == nil {
		t.Errorf("nothing found, with errors: %q, want a protocol error", msg)
	}
}

// An object rev-parse listed and cat-file could not read means the listing cannot be trusted,
// like a complaint on stderr: only a repack or a prune going on makes one without the other.
func TestCommits(t *testing.T) {
	c, b := strings.Repeat("c", 40), strings.Repeat("b", 40)
	found, missing := commits([]byte(c + " commit 250\n" + b + " blob 3\n" + strings.Repeat("d", 40) + " missing\n"))
	if len(found) != 1 || found[0][0] != c || !missing {
		t.Errorf("found %q, missing %v", found, missing)
	}
	if _, missing := commits([]byte(c + " commit 250\n")); missing {
		t.Error("missing without a missing object")
	}
}

// What read_files refuses: a commit it does not take, paths that are not plain paths from the
// top of the tree, too many or too few, one twice, and a crafted tree that lists a path twice.
// A commit the mirror lacks is the server's failure.
func TestReadFilesRefuses(t *testing.T) {
	dir, _, head := mirror(t)
	r := &repo{t: t, dir: dir}
	cs := connect(t, &Server{Mirror: dir})
	refused := func(commit string, paths []string, want string) {
		t.Helper()
		msg, err := call(t, cs, "read_files", ReadFilesIn{Commit: commit, Paths: paths}, &ReadFilesOut{})
		if err != nil || !strings.Contains(msg, want) {
			t.Errorf("%.20s %.100q: %q %v, want a refusal about %q", commit, paths, msg, err, want)
		}
	}
	db := []string{"app/db.py"}
	refused("HEAD", db, "full 40-character SHA")
	refused(head[:12], db, "full 40-character SHA")
	r.git("tag", "-a", "v1", "-m", "a tag", head)
	refused(r.git("rev-parse", "v1"), db, "is a tag, not a commit")
	refused(head, nil, "1 to 20 files, not 0")
	many := make([]string, MaxPaths+1)
	for i := range many {
		many[i] = fmt.Sprintf("f%d", i)
	}
	refused(head, many, "1 to 20 files, not 21")
	refused(head, []string{"app/db.py", "app/db.py"}, "asked for twice")
	refused(head, []string{""}, "1 to 4096 bytes, not 0")
	refused(head, []string{strings.Repeat("a", MaxPathBytes+1)}, "not 4097")
	for _, p := range []string{"/etc/passwd", "a/../b", "../x", ".", "..", "a//b", "a/", "./a", "app/./db.py"} {
		refused(head, []string{p}, "not a plain path")
	}
	for _, p := range []string{"a\nb", "a\x00b", "a\tb", "a\x7fb"} {
		refused(head, []string{p}, "without control characters")
	}

	// A crafted tree may hold a name twice; git lists both.
	blob := r.git("rev-parse", head+":app/db.py")
	tree := r.gitIn(fmt.Sprintf("100644 blob %s\tdup\n100644 blob %s\tdup\n", blob, blob), "mktree")
	dup := r.git("commit-tree", tree, "-m", "a name twice")
	refused(dup, []string{"dup"}, "lists it more than once")
	// Or give a file's mode to a tree or a commit, which git shows as a blob.
	for kind, object := range map[string]string{"tree": r.git("rev-parse", head+"^{tree}"), "commit": head} {
		crafted := r.git("commit-tree", r.rawTree([2]string{"100644 x", object}), "-m", "a "+kind+" as a file")
		refused(crafted, []string{"x"}, "a file's mode but a "+kind+" object")
	}
	// Or hold a directory on the way twice.
	app := r.git("rev-parse", head+":app")
	twice := r.git("commit-tree", r.rawTree([2]string{"40000 app", app}, [2]string{"40000 app", app}), "-m", "a directory twice")
	refused(twice, []string{"app/db.py"}, `"app": the commit's tree lists it more than once`)

	// Directories thousands deep, each on the way listed with its whole path, are too much.
	deep := strings.Repeat("d/", 2000) + "x.py"
	r.gitIn(fmt.Sprintf("commit refs/heads/deep\ncommitter t <t@example.com> 1700000000 +0000\ndata 4\ndeep\nM 100644 inline %s\ndata 2\nx\n\n", deep),
		"fast-import", "--quiet")
	refused(r.git("rev-parse", "deep"), []string{deep}, "lists too much on the way to it")

	if msg, err := call(t, cs, "read_files", ReadFilesIn{Commit: strings.Repeat("0", 40), Paths: db}, &ReadFilesOut{}); err == nil {
		t.Errorf("an unknown commit: %q, want a protocol error", msg)
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
	// What git made of the file: the safe version, as a blob git names.
	if want := []IndexEntry{{Path: "app/db.py", Mode: "100644", Object: blobID([]byte(safe))}}; out.Index == nil || !slices.Equal(*out.Index, want) {
		t.Fatalf("the fix made %+v, want %+v", out.Index, want)
	}
	// A patch that deletes the file leaves nothing in the index; one that adds a file adds it.
	out = ApplyCheckOut{}
	gone := "diff --git a/app/db.py b/app/db.py\ndeleted file mode 100644\n--- a/app/db.py\n+++ /dev/null\n@@ -1,2 +0,0 @@\n" +
		"-def find_user(conn, name):\n-    return conn.execute(\"SELECT * FROM users WHERE name = '%s'\" % name)\n" +
		"diff --git a/app/new.py b/app/new.py\nnew file mode 100644\n--- /dev/null\n+++ b/app/new.py\n@@ -0,0 +1 @@\n+x = 1\n"
	if msg, err := call(t, cs, "apply_check", ApplyCheckIn{Commit: head, Patch: gone, Paths: []string{"app/db.py", "app/new.py"}}, &out); msg != "" ||
		err != nil || !out.Applies || out.Index == nil || !slices.Equal(*out.Index, []IndexEntry{{Path: "app/new.py", Mode: "100644", Object: blobID([]byte("x = 1\n"))}}) {
		t.Fatalf("a deletion and a new file: %+v %+v %q %v", out, out.Index, msg, err)
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
	// The blobs git made went to the throwaway object directory, not the mirror's.
	if out, err := exec.Command("git", "-C", dir, "cat-file", "-e", blobID([]byte("x = 1\n"))).CombinedOutput(); err == nil {
		t.Errorf("apply_check wrote a blob to the mirror: %s", out)
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

// What git ls-files -s says of the index is read whole, or not at all.
func TestIndexEntries(t *testing.T) {
	blob := blobID([]byte("x\n"))
	got, err := indexEntries([]byte("100644 " + blob + " 0\ta/b.txt\x00100644 " + blob + " 2\tc\td.txt\x00"))
	if err != nil || !slices.Equal(got, []IndexEntry{{Path: "a/b.txt", Mode: "100644", Object: blob}, {Path: "c\td.txt", Mode: "100644", Object: blob, Stage: 2}}) {
		t.Fatalf("%+v %v", got, err)
	}
	for _, bad := range []string{"100644 abc 0\tx\x00", "100644 " + blob + "\tx\x00", "100644 " + blob + " zero\tx\x00", "100644 " + blob + " 0 x\x00"} {
		if _, err := indexEntries([]byte(bad)); err == nil {
			t.Errorf("%q: read", bad)
		}
	}
}

// The client runs the real binary, as agentd does, here without a sandbox.
func TestClientRunsTheServerPerCall(t *testing.T) {
	bin := gitmcptest.Server(t)
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
	// agentd resolves a run's commit and reads a patch's files through the server too.
	if full, err := ResolveCommit(context.Background(), c, dir, head[:7]); err != nil || full != head {
		t.Fatalf("resolve_commit: %q %v", full, err)
	}
	if _, err := ResolveCommit(context.Background(), c, dir, "0000000"); !IsToolError(err) || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("resolve_commit of an unknown commit: %v, want a refusal", err)
	}
	files, err := ReadFiles(context.Background(), c, dir, head, []string{"app/db.py", "app/new.py"})
	if err != nil || string(files[0].Content) != vulnerable || files[1].Type != "" {
		t.Fatalf("read_files: %+v %v", files, err)
	}
	if _, err := ReadFiles(context.Background(), c, dir, head, []string{"../x"}); !IsToolError(err) {
		t.Fatalf("read_files of ../x: %v, want a refusal", err)
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

// The largest answer read_files gives, every file at its limit and none compressible, gets
// through one reply.
func TestClientReadsTheLargestAnswer(t *testing.T) {
	bin := gitmcptest.Server(t)
	r := newRepo(t)
	rnd := rand.New(rand.NewPCG(3, 3))
	want := map[string][]byte{}
	paths := make([]string, MaxPaths)
	for i := range paths {
		b := make([]byte, MaxReadBytes)
		for j := range b {
			b[j] = byte(rnd.Uint32())
		}
		paths[i] = fmt.Sprintf("random/%02d.bin", i)
		want[paths[i]] = b
		r.write(paths[i], string(b))
	}
	head := r.commit("random files")
	c, err := NewClient(Options{Server: bin, Timeout: 30 * time.Second, Parallel: 1}) // in one slot, or never
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	files, err := ReadFiles(ctx, c, r.dir, head, paths)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if !bytes.Equal(f.Content, want[f.Path]) {
			t.Errorf("%s: %d bytes, not the file's", f.Path, len(f.Content))
		}
	}
}

// answers stands in for a server that answers every call with out, as a subverted one could.
type answers struct{ out any }

func (a answers) Call(_ context.Context, _, _ string, _, out any) error {
	b, err := json.Marshal(a.out)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, out)
}

// agentd checks what it can of the server's answers: an answer it cannot take is a failure,
// not a refusal.
func TestClientChecksTheAnswers(t *testing.T) {
	ctx := context.Background()
	commit := strings.Repeat("a", 40)
	for _, answer := range []string{strings.Repeat("b", 40), "abcdef0", "", "abcdef0" + strings.Repeat("0", 34)} {
		if full, err := ResolveCommit(ctx, answers{ResolveCommitOut{Commit: answer}}, "m", "abcdef0"); err == nil || IsToolError(err) {
			t.Errorf("resolve_commit answered %q: %q %v", answer, full, err)
		}
	}
	if full, err := ResolveCommit(ctx, answers{ResolveCommitOut{Commit: "abcdef0" + strings.Repeat("1", 33)}}, "m", "abcdef0"); err != nil ||
		full != "abcdef0"+strings.Repeat("1", 33) {
		t.Errorf("a good answer: %q %v", full, err)
	}

	for _, digits := range []string{"", "abc", "ABCDEF0", "abcdef0 ", strings.Repeat("a", 41)} {
		if _, err := ResolveCommit(ctx, noCall{t}, "m", digits); !IsToolError(err) {
			t.Errorf("digits %q: %v, want a refusal", digits, err)
		}
	}

	b64 := base64.StdEncoding.EncodeToString
	for name, files := range map[string][]File{
		"none":                     {},
		"two":                      {{Path: "a.py"}, {Path: "b.py"}},
		"another path":             {{Path: "b.py"}},
		"not base64":               {{Path: "a.py", Mode: "100644", Type: "blob", Size: 2, ContentBase64: "!!"}},
		"short content":            {{Path: "a.py", Mode: "100644", Type: "blob", Size: 3, ContentBase64: b64([]byte("ab"))}},
		"no content":               {{Path: "a.py", Mode: "100644", Type: "blob", Size: 3}},
		"content of a directory":   {{Path: "a.py", Mode: "040000", Type: "tree", ContentBase64: b64([]byte("x"))}},
		"content over the limit":   {{Path: "a.py", Mode: "100644", Type: "blob", Size: MaxReadBytes + 1, ContentBase64: b64([]byte("x"))}},
		"a size below zero":        {{Path: "a.py", Mode: "100644", Type: "blob", Size: -1}},
		"content of nothing there": {{Path: "a.py", ContentBase64: b64([]byte("x"))}},
		"a mode git never shows":   {{Path: "a.py", Mode: "100664", Type: "blob"}},
		"a type for nothing":       {{Path: "a.py", Type: "blob"}},
		"a mode without a type":    {{Path: "a.py", Mode: "100644"}},
		"a mode of another type":   {{Path: "a.py", Mode: "040000", Type: "blob"}},
		"a size of a directory":    {{Path: "a.py", Mode: "040000", Type: "tree", Size: 7}},
		"not_dir with an entry":    {{Path: "a.py", Mode: "100644", Type: "blob", NotDir: "a"}},
		"not_dir not above":        {{Path: "a.py", NotDir: "a"}},
		"not_dir the path itself":  {{Path: "a.py", NotDir: "a.py"}},
	} {
		if got, err := ReadFiles(ctx, answers{ReadFilesOut{Files: files}}, "m", commit, []string{"a.py"}); err == nil || IsToolError(err) {
			t.Errorf("%s: %+v %v", name, got, err)
		}
	}
	got, err := ReadFiles(ctx, answers{ReadFilesOut{Files: []File{{Path: "a.py", Mode: "100644", Type: "blob", Size: 2, ContentBase64: b64([]byte("ok"))}}}},
		"m", commit, []string{"a.py"})
	if err != nil || string(got[0].Content) != "ok" || got[0].ContentBase64 != "" {
		t.Errorf("a good answer: %+v %v", got, err)
	}
}

// An answer of millions of tiny values is refused before it is decoded into its type; an
// honest one is a dozen values or so.
func TestValues(t *testing.T) {
	hostile := map[string]any{"files": make([]any, 1<<20)}
	start := time.Now()
	if n, ok := values(hostile, maxValues); !ok || n <= maxValues || time.Since(start) > time.Second {
		t.Errorf("a million values counted as %d (%v) in %s", n, ok, time.Since(start))
	}
	var honest any
	b, _ := json.Marshal(ReadFilesOut{Files: []File{{Path: "a.py", Mode: "100644", Type: "blob", Size: 2, ContentBase64: "b2s="}}})
	if err := json.Unmarshal(b, &honest); err != nil {
		t.Fatal(err)
	}
	if n, ok := values(honest, maxValues); !ok || n > 20 {
		t.Errorf("an honest answer counted as %d values (%v)", n, ok)
	}
	// What the guard cannot count, it does not let through: an SDK that kept the raw JSON would
	// otherwise pass any answer as one value.
	for _, v := range []any{json.RawMessage(`{"files":[0,0,0]}`), []byte("x"), struct{}{}, map[string]any{"a": []string{"b"}}} {
		if _, ok := values(v, maxValues); ok {
			t.Errorf("%T counted", v)
		}
	}
	// One file at read_files' limit, with the longest path twice, every byte of it escaped,
	// fits one reply.
	if one := 4*MaxReadBytes/3 + 2*6*MaxPathBytes + 1<<10; one > maxReply {
		t.Errorf("one file's answer, %d bytes, is over the reply limit of %d", one, maxReply)
	}
}

// recording answers read_files for each path it is asked, empty, and records the calls.
type recording struct{ calls [][]string }

func (r *recording) Call(_ context.Context, _, _ string, args, out any) error {
	paths := args.(ReadFilesIn).Paths
	r.calls = append(r.calls, paths)
	files := make([]File, len(paths))
	for i, p := range paths {
		files[i] = File{Path: p}
	}
	*out.(*ReadFilesOut) = ReadFilesOut{Files: files}
	return nil
}

// ReadFiles asks for one path at a time, so that no answer is larger than maxReply.
func TestReadFilesAsksForOnePathAtATime(t *testing.T) {
	var rec recording
	files, err := ReadFiles(context.Background(), &rec, "m", strings.Repeat("a", 40), []string{"a.py", "b/c.py", "d"})
	if err != nil || len(files) != 3 || files[1].Path != "b/c.py" || len(rec.calls) != 3 {
		t.Fatalf("%+v %v, calls %q", files, err, rec.calls)
	}
	for _, c := range rec.calls {
		if len(c) != 1 {
			t.Errorf("a call for %q", c)
		}
	}
}

// Calls through WithOwnSlots never wait for the slots of the client it was made from: a run's
// start does not wait for the tool calls of the runs going on.
func TestWithOwnSlots(t *testing.T) {
	dir, _, head := mirror(t)
	c, err := NewClient(Options{Server: gitmcptest.Server(t), Timeout: 30 * time.Second, Parallel: 1})
	if err != nil {
		t.Fatal(err)
	}
	c.slots <- struct{}{} // every slot taken
	starts := c.WithOwnSlots(1)
	soon, cancelSoon := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelSoon()
	if full, err := ResolveCommit(soon, starts, dir, head[:7]); err != nil || full != head {
		t.Fatalf("with slots of its own: %q %v", full, err)
	}
	// Calls in one slot take no other: with every other slot taken, they go on.
	<-c.slots
	err = c.InOneSlot(soon, func(in Caller) error {
		for range 2 {
			if _, err := ResolveCommit(soon, in, dir, head[:7]); err != nil {
				return err
			}
		}
		_, err := ReadFiles(soon, in, dir, head, []string{"app/db.py", "app/new.py"})
		return err
	})
	if err != nil {
		t.Fatalf("calls in one slot: %v", err)
	}
	c.slots <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if _, err := ResolveCommit(ctx, c, dir, head[:7]); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("with every slot taken: %v, want to wait", err)
	}
}

// fakeServerEnv makes this test binary a git tool server that answers every call as its value
// says, the way a subverted server could: see fakeServer.
const fakeServerEnv = "AGENTOS_TEST_FAKE_SERVER"

func TestMain(m *testing.M) {
	if answer := os.Getenv(fakeServerEnv); answer != "" {
		fakeServer(answer)
		return
	}
	os.Exit(m.Run())
}

// fakeServer serves every tool on stdio with one answer: "many values" (an answer of
// hundreds of thousands of tiny values, under the reply limit), "long refusal" (a refusal
// that names the mirror and runs on), or "failure" (a protocol error). It says something on
// stderr first, as git would.
func fakeServer(answer string) {
	mirror := ""
	for i, a := range os.Args {
		if a == "--mirror" && i+1 < len(os.Args) {
			mirror = os.Args[i+1]
		}
	}
	srv := mcp.NewServer(&mcp.Implementation{Name: "fake"}, nil)
	for _, tool := range []string{"show_diff", "apply_check", "resolve_commit", "read_files"} {
		mcp.AddTool(srv, &mcp.Tool{Name: tool}, func(context.Context, *mcp.CallToolRequest, map[string]any) (*mcp.CallToolResult, map[string]any, error) {
			fmt.Fprintln(os.Stderr, "the fake server's own words about", mirror)
			switch answer {
			case "many values":
				return result("x"), map[string]any{"files": make([]any, 300_000)}, nil
			case "long refusal":
				return nil, nil, errors.New("in " + mirror + ": " + strings.Repeat("x", 100_000))
			}
			return nil, nil, unavailable(errors.New("it broke"))
		})
	}
	if err := srv.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
}

// logs collects what a client logs.
type logs struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logs) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *logs) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// fake is a client whose server is this test binary, answering as fakeServer does.
func fake(t *testing.T, answer string, log *logs) *Client {
	t.Helper()
	bin, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	launcher := []string{"/bin/sh", "-c", `answer=$1; shift 3; ` + fakeServerEnv + `=$answer exec "$@"`, "sh", answer} // <answer> <mirror> -- <server> ...
	c, err := NewClient(Options{Server: bin, Sandbox: launcher, Timeout: 30 * time.Second,
		Log: slog.New(slog.NewTextHandler(log, &slog.HandlerOptions{Level: slog.LevelInfo}))})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// Call's own defences against a subverted server, through a real process: an answer of too
// many values is a failure, not decoded; a refusal's text is cut short and does not name the
// mirror; and what the server said on stderr reaches agentd's log, after a refusal and after
// a failure alike (it used to be read before the server had finished writing it).
func TestCallAgainstALyingServer(t *testing.T) {
	mirror := t.TempDir()
	var log logs
	err := fake(t, "many values", &log).Call(context.Background(), mirror, "read_files", ReadFilesIn{}, &ReadFilesOut{})
	if err == nil || IsToolError(err) || !strings.Contains(err.Error(), "more than 1000 values") {
		t.Errorf("many values: %v", err)
	}
	err = fake(t, "long refusal", &log).Call(context.Background(), mirror, "show_diff", ShowDiffIn{}, &ShowDiffOut{})
	if !IsToolError(err) || strings.Contains(err.Error(), mirror) || len(err.Error()) > maxToolError+20 {
		t.Errorf("a long refusal: %d bytes: %.100q", len(err.Error()), err)
	}
	if !strings.Contains(log.String(), "the git tool server refused") || !strings.Contains(log.String(), "the fake server's own words") {
		t.Errorf("the refusal's log: %s", log.String())
	}
	for i := range 50 { // the race it guards against lost the stderr of a few calls in a hundred
		var log logs
		err := fake(t, "failure", &log).Call(context.Background(), mirror, "resolve_commit", ResolveCommitIn{}, &ResolveCommitOut{})
		if err == nil || IsToolError(err) || strings.Contains(err.Error(), "it broke") {
			t.Fatalf("a failure: %v", err)
		}
		if !strings.Contains(log.String(), "it broke") || !strings.Contains(log.String(), "the fake server's own words") {
			t.Fatalf("call %d: the failure's log lacks the server's stderr: %s", i, log.String())
		}
	}
}

// noCall fails the test if it is called.
type noCall struct{ t *testing.T }

func (n noCall) Call(context.Context, string, string, any, any) error {
	n.t.Error("the server was called")
	return nil
}

// A refusal's text reaches the workflow and maybe a public report: whatever a subverted server
// puts in it, it is cut short and does not name the mirror.
func TestRefusalText(t *testing.T) {
	long := &mcp.CallToolResult{IsError: true, Content: []mcp.Content{
		&mcp.TextContent{Text: "/srv/mirrors/payments: " + strings.Repeat("é", 10000)}}}
	err := refusal("/srv/mirrors/payments", long)
	if strings.Contains(err.Msg, "/srv/mirrors") || len(err.Msg) > maxToolError+20 || !strings.HasSuffix(err.Msg, " [truncated]") ||
		!utf8.ValidString(err.Msg) {
		t.Errorf("%d bytes: %.80q", len(err.Msg), err.Msg)
	}
}

// A commit too costly to diff is refused the same way every time: git dies at the launcher's
// CPU time limit. A subverted server cannot leave its call's directory behind by taking the
// permissions off it.
func TestClientLimits(t *testing.T) {
	bin := gitmcptest.Server(t)
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
	// The soft limit below the hard one, as run-tool.sh sets them, so that git dies of SIGXCPU
	// on Linux as on macOS. With the two equal, Linux sends SIGKILL instead, on a CPU clock it
	// samples at each tick: with other processes waking for short spells on the same CPU, that
	// clock ran a tenth or more ahead of the CPU time wait reports, and the server took git's
	// death for a failure.
	launcher := []string{"/bin/sh", "-c", `ulimit -S -t 1 && ulimit -H -t 2 && shift 2 && exec "$@"`, "sh"} // <mirror> -- <server> ...
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
