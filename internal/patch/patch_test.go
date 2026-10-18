package patch

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jchen0426/agentos/internal/gitmcp"
	"github.com/jchen0426/agentos/internal/gitmcp/gitmcptest"
)

const vulnerable = "def find_user(conn, name):\n    return conn.execute(\"SELECT * FROM users WHERE name = '%s'\" % name)\n"

const fix = `diff --git a/app/db.py b/app/db.py
--- a/app/db.py
+++ b/app/db.py
@@ -1,2 +1,2 @@
 def find_user(conn, name):
-    return conn.execute("SELECT * FROM users WHERE name = '%s'" % name)
+    return conn.execute("SELECT * FROM users WHERE name = ?", (name,))
`

func files(m map[string]string) Source {
	return func(paths []string) (map[string][]byte, error) {
		out := map[string][]byte{}
		for _, p := range paths {
			if s, ok := m[p]; ok {
				out[p] = []byte(s)
			}
		}
		return out, nil
	}
}

func refused(t *testing.T, err error, want string) {
	t.Helper()
	var v *Violation
	if !errors.As(err, &v) {
		t.Fatalf("got %v, want a violation about %q", err, want)
	}
	if !strings.Contains(v.Reason, want) {
		t.Fatalf("violation %q does not mention %q", v.Reason, want)
	}
}

func TestApplyFix(t *testing.T) {
	p, err := Parse([]byte(fix))
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Check([]string{".github/**", "Makefile"}); err != nil {
		t.Fatal(err)
	}
	changes, err := p.Apply(files(map[string]string{"app/db.py": vulnerable}))
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 || changes[0].Path != "app/db.py" || !strings.Contains(string(changes[0].Content), "name = ?") {
		t.Fatalf("changes = %+v", changes)
	}
}

func TestPlainDiffDropsThePrefix(t *testing.T) {
	plain := strings.TrimPrefix(fix, "diff --git a/app/db.py b/app/db.py\n")
	p, err := Parse([]byte(plain))
	if err != nil {
		t.Fatal(err)
	}
	if got := p.Paths(); len(got) != 1 || got[0] != "app/db.py" {
		t.Fatalf("paths = %v", got)
	}
}

func TestCheckRefuses(t *testing.T) {
	newFile := func(name, mode string) string {
		return "diff --git a/" + name + " b/" + name + "\nnew file mode " + mode + "\n--- /dev/null\n+++ b/" + name +
			"\n@@ -0,0 +1 @@\n+x\n"
	}
	for _, tc := range []struct{ patch, want string }{
		{newFile(".github/workflows/x.yml", "100644"), "protects .github/**"},
		{newFile("makefile", "100644"), "protects Makefile"}, // case-insensitive
		{newFile("sub/.git/hooks/pre-commit", "100644"), ".git"},
		{newFile(".gitattributes", "100644"), ".gitattributes"},
		{newFile("run.sh", "100755"), "mode 100644"},
		{newFile("link", "120000"), "mode 100644"},
		{"diff --git a/x b/x\nold mode 100644\nnew mode 100755\n", "mode changes"},
		{"diff --git a/a.py b/b.py\nsimilarity index 100%\nrename from a.py\nrename to b.py\n", "renames"},
		{"diff --git a/img.png b/img.png\nindex 1111111..2222222 100644\nBinary files a/img.png and b/img.png differ\n", "binary"},
		{newFile("a/../../etc/passwd", "100644"), "not a plain relative path"},
		{newFile(":/app/db.py", "100644"), "only letters"},       // git pathspec magic
		{newFile("\u017fcripts/x.sh", "100644"), "only letters"}, // folds to "scripts" in some places
		{newFile(".g\u200cit/config", "100644"), "only letters"}, // a zero-width joiner
		{"diff --git a/app/db.py b/app/db.py\n", "changes nothing"},
		{"diff --git a/app/db.py b/app/db.py\nindex 1111111..2222222 100644\n", "changes nothing"},
	} {
		p, err := Parse([]byte(tc.patch))
		if err == nil {
			err = p.Check([]string{".github/**", "Makefile"})
		}
		refused(t, err, tc.want)
	}
}

func TestApplyRefuses(t *testing.T) {
	p, err := Parse([]byte(fix))
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.Apply(files(map[string]string{"app/db.py": "something else\n"}))
	refused(t, err, "does not apply")
	_, err = p.Apply(files(map[string]string{}))
	refused(t, err, "no such file")
	_, err = Parse([]byte("not a patch\n"))
	refused(t, err, "changes no file")
}

// The patch's limits are what the git tool server reads.
func TestLimitsMatchTheServer(t *testing.T) {
	if MaxFiles != gitmcp.MaxPaths || MaxFileBytes != gitmcp.MaxReadBytes || MaxBytes != gitmcp.MaxPatchBytes {
		t.Errorf("patch: %d files of %d bytes, %d bytes in all; the git tool server: %d, %d, %d",
			MaxFiles, MaxFileBytes, MaxBytes, gitmcp.MaxPaths, gitmcp.MaxReadBytes, gitmcp.MaxPatchBytes)
	}
}

type mirrorRepo struct {
	t   *testing.T
	dir string
}

func newMirror(t *testing.T) *mirrorRepo {
	t.Helper()
	r := &mirrorRepo{t: t, dir: t.TempDir()}
	r.git("init", "-q", "-b", "main")
	return r
}

func (r *mirrorRepo) git(args ...string) string {
	r.t.Helper()
	return r.gitIn("", args...)
}

func (r *mirrorRepo) gitIn(stdin string, args ...string) string {
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

func (r *mirrorRepo) write(name string, body []byte, mode os.FileMode) {
	r.t.Helper()
	full := filepath.Join(r.dir, name)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		r.t.Fatal(err)
	}
	if err := os.WriteFile(full, body, mode); err != nil {
		r.t.Fatal(err)
	}
}

// server is the real git tool server, unsandboxed, as MirrorSource calls it.
func server(t *testing.T) *gitmcp.Client {
	t.Helper()
	c, err := gitmcp.NewClient(gitmcp.Options{Server: gitmcptest.Server(t), Timeout: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// MirrorSource reads through the git tool server: regular files only, under directories, and
// none over the limit.
func TestMirrorSource(t *testing.T) {
	r := newMirror(t)
	r.write("app/db.py", []byte(vulnerable), 0o644)
	r.write("app/run.sh", []byte("#!/bin/sh\n"), 0o755)
	r.write("app/big.py", make([]byte, MaxFileBytes+1), 0o644)
	if err := os.Symlink("db.py", filepath.Join(r.dir, "app", "link.py")); err != nil {
		t.Fatal(err)
	}
	r.git("add", ".")
	r.git("commit", "-q", "-m", "x")
	base := r.git("rev-parse", "HEAD")
	r.git("update-index", "--add", "--cacheinfo", "160000,"+base+",vendor/lib")
	r.git("commit", "-q", "-m", "a submodule")
	head := r.git("rev-parse", "HEAD")
	git := server(t)
	read := func(paths ...string) (map[string][]byte, error) {
		return MirrorSource(context.Background(), git, r.dir, head)(paths)
	}

	got, err := read("app/db.py", "app/missing.py", "new/dir/file.py")
	if err != nil || len(got) != 1 || string(got["app/db.py"]) != vulnerable {
		t.Fatalf("app/db.py, a missing file, one in a new directory: %q %v", got, err)
	}
	for path, want := range map[string]string{
		"app/link.py":           "not a regular file at the commit (120000 blob)",
		"app/run.sh":            "not a regular file at the commit (100755 blob)",
		"app":                   "not a regular file at the commit (040000 tree)",
		"vendor/lib":            "not a regular file at the commit (160000 commit)",
		"app/link.py/inside.py": "app/link.py is not a directory", // a new file under a symlink
		"vendor/lib/x.py":       "vendor/lib is not a directory",
		"app/big.py":            "over the limit",
	} {
		_, err := read("app/db.py", path)
		refused(t, err, want)
	}

	// The server's refusal is a violation: here a crafted tree that lists a name twice.
	blob := r.git("rev-parse", head+":app/db.py")
	tree := r.gitIn("100644 blob "+blob+"\tdup\n100644 blob "+blob+"\tdup\n", "mktree")
	dup := r.git("commit-tree", tree, "-m", "a name twice")
	_, err = MirrorSource(context.Background(), git, r.dir, dup)([]string{"dup"})
	refused(t, err, "lists it more than once")

	// The server's failure is not: the call is tried again. Errors never carry the mirror's
	// path: they can end up in a public report.
	_, err = MirrorSource(context.Background(), git, r.dir, strings.Repeat("0", 40))([]string{"app/db.py"})
	var v *Violation
	if err == nil || errors.As(err, &v) || strings.Contains(err.Error(), r.dir) {
		t.Fatalf("an unknown commit: %v, want a failure", err)
	}
}

// A patch that creates a file at the top of the tree, or in a directory the commit does not
// have yet, applies.
func TestApplyCreatesNewFiles(t *testing.T) {
	r := newMirror(t)
	r.write("app/db.py", []byte(vulnerable), 0o644)
	r.git("add", ".")
	r.git("commit", "-q", "-m", "x")
	git, head := server(t), r.git("rev-parse", "HEAD")
	for _, name := range []string{"SECURITY.md", "tests/sql/test_db.py"} {
		p, err := Parse([]byte("diff --git a/" + name + " b/" + name + "\nnew file mode 100644\n" +
			"--- /dev/null\n+++ b/" + name + "\n@@ -0,0 +1 @@\n+new\n"))
		if err != nil {
			t.Fatal(err)
		}
		changes, err := p.Apply(MirrorSource(context.Background(), git, r.dir, head))
		if err != nil || len(changes) != 1 || changes[0].Path != name || string(changes[0].Content) != "new\n" {
			t.Errorf("%s: changes %+v, %v", name, changes, err)
		}
	}
}
