package githubsim

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-github/v92/github"
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

func write(t *testing.T, name, body string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, []byte(body), mode); err != nil {
		t.Fatal(err)
	}
}

// repo is a repository with every kind of tree entry; it returns its directory and last commit.
func repo(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	git(t, dir, "init", "-q", "-b", "main")
	write(t, filepath.Join(dir, "app", "db.py"), "x\n", 0o644)
	write(t, filepath.Join(dir, "run.sh"), "#!/bin/sh\n", 0o755)
	if err := os.Symlink("app/db.py", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	git(t, dir, "add", ".")
	git(t, dir, "commit", "-q", "-m", "one")
	first := git(t, dir, "rev-parse", "HEAD")
	git(t, dir, "update-index", "--add", "--cacheinfo", "160000,"+first+",vendor")
	git(t, dir, "commit", "-q", "-m", "two")
	return dir, git(t, dir, "rev-parse", "HEAD")
}

func client(t *testing.T, s *Server) *github.Client {
	t.Helper()
	srv := httptest.NewServer(s)
	t.Cleanup(srv.Close)
	url := srv.URL + "/"
	gh, err := github.NewClient(github.WithURLs(&url, &url))
	if err != nil {
		t.Fatal(err)
	}
	return gh
}

// GitHub has the repository's own commits and trees, as a local copy holds them: each tree
// listed one level at a time, as GitHub's Git Trees API lists it.
func TestTreesComeFromTheRepository(t *testing.T) {
	dir, head := repo(t)
	gh := client(t, &Server{Repos: map[string]string{"acme/payments": dir}})
	ctx := context.Background()

	c, _, err := gh.Git.GetCommit(ctx, "acme", "payments", head)
	if err != nil {
		t.Fatal(err)
	}
	if c.GetSHA() != head || c.GetTree().GetSHA() != git(t, dir, "rev-parse", head+"^{tree}") ||
		len(c.Parents) != 1 || c.Parents[0].GetSHA() != git(t, dir, "rev-parse", head+"^") {
		t.Fatalf("commit %+v", c)
	}
	tree, _, err := gh.Git.GetTree(ctx, "acme", "payments", c.GetTree().GetSHA(), false)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, e := range tree.Entries {
		got[e.GetPath()] = strings.TrimSpace(e.GetMode() + " " + e.GetType() + " " + e.GetSHA() + " " +
			map[bool]string{true: "", false: "size"}[e.Size == nil])
	}
	want := map[string]string{
		"app":    "040000 tree " + git(t, dir, "rev-parse", head+":app"),
		"link":   "120000 blob " + git(t, dir, "rev-parse", head+":link") + " size",
		"run.sh": "100755 blob " + git(t, dir, "rev-parse", head+":run.sh") + " size",
		"vendor": "160000 commit " + git(t, dir, "rev-parse", head+"^"),
	}
	if len(got) != len(want) || tree.GetTruncated() || tree.GetSHA() != c.GetTree().GetSHA() {
		t.Fatalf("tree %+v", tree)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s: %q, want %q", k, got[k], v)
		}
	}
	sub, _, err := gh.Git.GetTree(ctx, "acme", "payments", git(t, dir, "rev-parse", head+":app"), false)
	if err != nil || len(sub.Entries) != 1 || sub.Entries[0].GetPath() != "db.py" || sub.Entries[0].GetSize() != 2 ||
		sub.Entries[0].GetSHA() != "587be6b4c3f93f93c489c0111bba5596147a26cb" {
		t.Fatalf("app: %+v %v", sub, err)
	}

	// A commit is listed as its tree, as GitHub does.
	if c, _, err := gh.Git.GetTree(ctx, "acme", "payments", head, false); err != nil || len(c.Entries) != len(want) {
		t.Fatalf("tree of the commit: %+v %v", c, err)
	}

	// What the repository does not have, GitHub does not either; nor a commit or tree that is
	// another kind of object (GitHub's answers, checked on github.com: 404, and 422 for the tree
	// of a blob).
	blob := git(t, dir, "rev-parse", head+":run.sh")
	for _, tc := range []struct {
		sha          string
		commit, tree int
	}{{strings.Repeat("1", 40), 404, 404}, {blob, 404, 422}, {c.GetTree().GetSHA(), 404, 200}} {
		if _, resp, err := gh.Git.GetCommit(ctx, "acme", "payments", tc.sha); resp.StatusCode != tc.commit {
			t.Errorf("commit %s: %v", tc.sha, err)
		}
		if _, resp, err := gh.Git.GetTree(ctx, "acme", "payments", tc.sha, false); resp.StatusCode != tc.tree {
			t.Errorf("tree %s: %v", tc.sha, err)
		}
	}
}

// The fault file can make GitHub disagree with the repository about an entry.
func TestFaultsChangeAnEntry(t *testing.T) {
	dir, head := repo(t)
	faults := filepath.Join(t.TempDir(), "faults.json")
	b, _ := json.Marshal(Faults{Entries: map[string]Entry{"db.py": {Mode: "100755"}}})
	if err := os.WriteFile(faults, b, 0o600); err != nil {
		t.Fatal(err)
	}
	gh := client(t, &Server{Repos: map[string]string{"acme/payments": dir}, FaultsPath: faults})
	sub, _, err := gh.Git.GetTree(context.Background(), "acme", "payments", git(t, dir, "rev-parse", head+":app"), false)
	if err != nil || len(sub.Entries) != 1 || sub.Entries[0].GetMode() != "100755" ||
		sub.Entries[0].GetSHA() != "587be6b4c3f93f93c489c0111bba5596147a26cb" {
		t.Fatalf("app: %+v %v", sub, err)
	}
}

// A repository the simulator was not given keeps the old stand-in: any commit exists.
func TestOtherRepositoriesHaveAnyCommit(t *testing.T) {
	gh := client(t, &Server{})
	sha := strings.Repeat("1", 40)
	if c, _, err := gh.Git.GetCommit(context.Background(), "acme", "other", sha); err != nil || c.GetSHA() != sha {
		t.Fatalf("%+v %v", c, err)
	}
}

// Every repository is there to see, as for a token that may see it.
func TestRepositoriesCanBeSeen(t *testing.T) {
	gh := client(t, &Server{})
	if r, _, err := gh.Repositories.Get(context.Background(), "acme", "payments"); err != nil || r.GetFullName() != "acme/payments" {
		t.Fatalf("%+v %v", r, err)
	}
}
