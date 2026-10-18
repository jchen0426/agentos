package patch

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
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
	return func(n string) ([]byte, bool, error) {
		s, ok := m[n]
		return []byte(s), ok, nil
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

func TestMirrorSource(t *testing.T) {
	dir := t.TempDir()
	run := func(args ...string) string {
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com",
			"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	run("init", "-q", "-b", "main")
	if err := os.MkdirAll(filepath.Join(dir, "app"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "app", "db.py"), []byte(vulnerable), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("db.py", filepath.Join(dir, "app", "link.py")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "app", "big.py"), make([]byte, MaxFileBytes+1), 0o644); err != nil {
		t.Fatal(err)
	}
	run("add", ".")
	run("commit", "-q", "-m", "x")
	src := MirrorSource(context.Background(), dir, run("rev-parse", "HEAD"))

	b, ok, err := src("app/db.py")
	if err != nil || !ok || string(b) != vulnerable {
		t.Fatalf("app/db.py: %q %v %v", b, ok, err)
	}
	if _, ok, err := src("app/missing.py"); err != nil || ok {
		t.Fatalf("missing file: ok=%v err=%v", ok, err)
	}
	_, _, err = src("app/link.py")
	refused(t, err, "not a regular file")
	_, _, err = src("app")
	refused(t, err, "not a regular file")
	_, _, err = src("app/link.py/inside.py") // a new file under a symlink
	refused(t, err, "not a directory")
	_, _, err = src("app/big.py")
	refused(t, err, "over the limit")

	// Errors never carry the mirror's path: they can end up in a public report.
	_, _, err = MirrorSource(context.Background(), dir, strings.Repeat("0", 40))("app/db.py")
	if err == nil || strings.Contains(err.Error(), dir) {
		t.Fatalf("got %v", err)
	}
}
