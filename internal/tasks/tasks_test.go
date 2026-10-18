package tasks

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jchen0426/agentos/internal/gitmcp"
)

func git(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com",
		"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func TestResolveCommit(t *testing.T) {
	dir := t.TempDir()
	git(t, dir, "init", "-q", "-b", "main")
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, dir, "add", "a.txt")
	git(t, dir, "commit", "-q", "-m", "init")
	head := git(t, dir, "rev-parse", "HEAD")
	ctx := context.Background()

	for _, name := range []string{head, head[:7]} {
		if got, err := resolveCommit(ctx, dir, name); err != nil || got != head {
			t.Errorf("%s: %q %v", name, got, err)
		}
	}
	// Not a commit: unknown, a blob, an annotated tag's own object.
	git(t, dir, "tag", "-a", "v1", "-m", "a tag")
	for _, name := range []string{"0000000", git(t, dir, "rev-parse", "HEAD:a.txt"), git(t, dir, "rev-parse", "v1")} {
		if _, err := resolveCommit(ctx, dir, name); err == nil || !strings.Contains(err.Error(), "not found") {
			t.Errorf("%s: %v", name, err)
		}
	}
	// A commit over the limit is refused from its header, unread.
	msg := filepath.Join(t.TempDir(), "message")
	if err := os.WriteFile(msg, []byte(strings.Repeat("x", gitmcp.MaxCommitBytes+1)), 0o600); err != nil {
		t.Fatal(err)
	}
	huge := git(t, dir, "commit-tree", head+"^{tree}", "-p", head, "-F", msg)
	if _, err := resolveCommit(ctx, dir, huge); err == nil || !strings.Contains(err.Error(), "over the limit") {
		t.Errorf("a huge commit: %v", err)
	}
	// A mirror git cannot read says why, without its path; one inside a checkout is not the
	// checkout.
	inside := filepath.Join(dir, "not-a-mirror")
	if err := os.Mkdir(inside, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, m := range []string{t.TempDir(), inside} {
		if _, err := resolveCommit(ctx, m, head); err == nil || strings.Contains(err.Error(), m) ||
			!strings.Contains(err.Error(), "not a git repository") {
			t.Errorf("%s: %v", m, err)
		}
	}
}

// A prefix names one commit: blobs and trees that share it do not count, and two commits that
// share it are refused.
func TestResolveCommitPrefixes(t *testing.T) {
	dir := t.TempDir()
	git(t, dir, "init", "-q", "-b", "main")
	// Thousands of commits and blobs in one go, so that some 4-digit prefixes repeat.
	var stream strings.Builder
	for i := range 2000 {
		fmt.Fprintf(&stream, "blob\ndata %d\nblob %d\n\n", len(fmt.Sprintf("blob %d\n", i)), i)
		msg := fmt.Sprintf("commit %d\n", i)
		fmt.Fprintf(&stream, "commit refs/heads/main\ncommitter t <t@example.com> 1700000000 +0000\ndata %d\n%s\n", len(msg), msg)
	}
	cmd := exec.Command("git", "-C", dir, "fast-import", "--quiet")
	cmd.Stdin = strings.NewReader(stream.String())
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("fast-import: %v\n%s", err, out)
	}
	types := map[string]map[string][]string{} // prefix -> type -> objects
	for line := range strings.Lines(git(t, dir, "cat-file", "--batch-all-objects", "--batch-check=%(objectname) %(objecttype)")) {
		f := strings.Fields(line)
		if types[f[0][:4]] == nil {
			types[f[0][:4]] = map[string][]string{}
		}
		types[f[0][:4]][f[1]] = append(types[f[0][:4]][f[1]], f[0])
	}
	var shared, twice string
	for prefix, byType := range types {
		switch c := len(byType["commit"]); {
		case c == 1 && len(byType["blob"]) > 0 && shared == "":
			shared = prefix
		case c == 2 && twice == "":
			twice = prefix
		}
	}
	if shared == "" || twice == "" {
		t.Fatalf("no prefix to test with: shared %q, twice %q", shared, twice)
	}
	ctx := context.Background()
	if got, err := resolveCommit(ctx, dir, shared); err != nil || got != types[shared]["commit"][0] {
		t.Errorf("a prefix a blob shares: %q %v", got, err)
	}
	if _, err := resolveCommit(ctx, dir, twice); err == nil || !strings.Contains(err.Error(), "names 2 commits") {
		t.Errorf("a prefix two commits share: %v", err)
	}
}
