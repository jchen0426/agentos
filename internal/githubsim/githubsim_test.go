package githubsim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
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

// GitHub has the repository's own branches, as the local copy holds them. A branch the API
// created, or a test set, comes first: the copy is what the repository held to begin with.
func TestBranchesComeFromTheRepository(t *testing.T) {
	dir, head := repo(t)
	first := git(t, dir, "rev-parse", head+"^")
	git(t, dir, "branch", "release/1.0", first)
	s := &Server{Repos: map[string]string{"acme/payments": dir}}
	gh := client(t, s)
	ctx := context.Background()
	for branch, want := range map[string]string{"main": head, "release/1.0": first} {
		ref, _, err := gh.Git.GetRef(ctx, "acme", "payments", "heads/"+branch)
		if err != nil || ref.GetRef() != "refs/heads/"+branch || ref.GetObject().GetSHA() != want ||
			ref.GetObject().GetType() != "commit" {
			t.Errorf("%s: %+v %v", branch, ref, err)
		}
	}
	if _, resp, err := gh.Git.GetRef(ctx, "acme", "payments", "heads/nosuch"); resp.StatusCode != 404 {
		t.Errorf("a branch the repository does not have: %v", err)
	}
	// GitHub finds "release//1.0" as release/1.0, and answers under the name it was asked for;
	// a name that ends in "/" it does not find (checked on github.com, octocat/Hello-World).
	if ref, _, err := gh.Git.GetRef(ctx, "acme", "payments", "heads/release//1.0"); err != nil ||
		ref.GetRef() != "refs/heads/release//1.0" || ref.GetObject().GetSHA() != first {
		t.Errorf("release//1.0: %+v %v", ref, err)
	}
	if _, resp, err := gh.Git.GetRef(ctx, "acme", "payments", "heads/main/"); resp.StatusCode != 404 {
		t.Errorf("main/: %v", err)
	}
	s.SetRef("acme/payments", "main", first) // someone moved the branch
	if ref, _, err := gh.Git.GetRef(ctx, "acme", "payments", "heads/main"); err != nil || ref.GetObject().GetSHA() != first {
		t.Errorf("main after it moved: %+v %v", ref, err)
	}
}

// GitHub compares two commits of the repository as its compare API does: whether the second
// is in the first's history (behind, identical) or has commits of its own (ahead, diverged).
// Like GitHub, it compares a commit no branch holds, as a fork's commit is (GitHub serves those
// through the parent repository). The answers follow github.com's (checked 2026-10-19 on
// jchen0426/agentos-audit-target, octocat/Spoon-Knife and select2/select2): commits lists the
// second's own commits, oldest first, and files what they change; anything not there, or
// something other than a commit, is a 404, and so are two commits with no history in common.
func TestCompareLikeGitHub(t *testing.T) {
	dir, head := repo(t)
	first := git(t, dir, "rev-parse", head+"^")
	git(t, dir, "checkout", "-q", "-b", "feature")
	write(t, filepath.Join(dir, "app", "db.py"), "y\n", 0o644)
	git(t, dir, "add", "app/db.py") // not -a: the submodule has no directory here, and would go
	git(t, dir, "commit", "-q", "-m", "feature")
	feature := git(t, dir, "rev-parse", "HEAD")
	git(t, dir, "checkout", "-q", "main")
	fork := git(t, dir, "commit-tree", "-p", first, "-m", "fork", head+"^{tree}") // adds vendor, as head does
	orphan := git(t, dir, "commit-tree", "-m", "orphan", head+"^{tree}")
	gh := client(t, &Server{Repos: map[string]string{"acme/payments": dir}})
	ctx := context.Background()

	for _, tc := range []struct {
		base, head            string
		status                string
		ahead, behind         int
		baseCommit, mergeBase string
		commits               []string
		files                 []string // "status filename"
	}{
		{"main", head, "identical", 0, 0, head, head, nil, nil},
		{"main", first, "behind", 0, 1, head, first, nil, nil},
		{head, first, "behind", 0, 1, head, first, nil, nil},
		{"main", feature, "ahead", 1, 0, head, head, []string{feature}, []string{"modified app/db.py"}},
		{"main", fork, "diverged", 1, 1, head, first, []string{fork}, []string{"added vendor"}},
		{"feature", "main", "behind", 0, 1, feature, head, nil, nil},
	} {
		c, _, err := gh.Repositories.CompareCommits(ctx, "acme", "payments", tc.base, tc.head, nil)
		if err != nil {
			t.Errorf("%s...%s: %v", tc.base, tc.head, err)
			continue
		}
		var commits, files []string
		for _, rc := range c.Commits {
			commits = append(commits, rc.GetSHA())
		}
		for _, f := range c.Files {
			files = append(files, f.GetStatus()+" "+f.GetFilename())
		}
		if c.GetStatus() != tc.status || c.GetAheadBy() != tc.ahead || c.GetBehindBy() != tc.behind ||
			c.GetTotalCommits() != tc.ahead || c.GetBaseCommit().GetSHA() != tc.baseCommit ||
			c.GetMergeBaseCommit().GetSHA() != tc.mergeBase || !slices.Equal(commits, tc.commits) || !slices.Equal(files, tc.files) {
			t.Errorf("%s...%s: %s, ahead %d, behind %d, total %d, base %s, merge base %s, commits %v, files %v",
				tc.base, tc.head, c.GetStatus(), c.GetAheadBy(), c.GetBehindBy(), c.GetTotalCommits(),
				c.GetBaseCommit().GetSHA(), c.GetMergeBaseCommit().GetSHA(), commits, files)
		}
	}

	// Its commits carry their messages, as GitHub's do (without git's last newline); per_page
	// cuts the list of commits, not the counts.
	c, _, err := gh.Repositories.CompareCommits(ctx, "acme", "payments", first, "feature", &github.ListOptions{PerPage: 1})
	if err != nil || c.GetAheadBy() != 2 || len(c.Commits) != 1 || c.Commits[0].GetSHA() != head ||
		c.Commits[0].GetCommit().GetMessage() != "two" || c.GetBaseCommit().GetCommit().GetMessage() != "one" {
		t.Errorf("%s...feature, one per page: %+v %v", first, c, err)
	}

	tree, blob := git(t, dir, "rev-parse", head+"^{tree}"), git(t, dir, "rev-parse", head+":run.sh")
	for _, tc := range []struct{ repo, base, head, message string }{
		{"payments", "main", strings.Repeat("1", 40), "Not Found"},
		{"payments", "nosuch", head, "Not Found"},
		{"payments", strings.Repeat("1", 40), head, "Not Found"},
		{"payments", "main", tree, "Not Found"},
		{"payments", "main", blob, "Not Found"},
		{"payments", "main", orphan, "No common ancestor between main and " + orphan + "."},
		{"other", "main", head, "Not Found"}, // a repository the simulator was not given
	} {
		_, resp, err := gh.Repositories.CompareCommits(ctx, "acme", tc.repo, tc.base, tc.head, nil)
		var answer *github.ErrorResponse
		if resp == nil || resp.StatusCode != 404 || !errors.As(err, &answer) || answer.Message != tc.message {
			t.Errorf("%s: %s...%s: %v", tc.repo, tc.base, tc.head, err)
		}
	}
}

// The log says what each tree and commit the API created was made on: a tree's base tree, a
// commit's tree and parents. A test can then tell what a branch holds.
func TestTheLogSaysWhatTreesAndCommitsAreMadeOn(t *testing.T) {
	dir, head := repo(t)
	log := filepath.Join(t.TempDir(), "github.jsonl")
	gh := client(t, &Server{Repos: map[string]string{"acme/payments": dir}, LogPath: log})
	ctx := context.Background()
	base := git(t, dir, "rev-parse", head+"^{tree}")
	tree, _, err := gh.Git.CreateTree(ctx, "acme", "payments", base, []*github.TreeEntry{
		{Path: new("app/db.py"), Mode: new("100644"), Type: new("blob"), Content: new("y\n")}})
	if err != nil {
		t.Fatal(err)
	}
	commit, _, err := gh.Git.CreateCommit(ctx, "acme", "payments", github.Commit{Message: new("m"),
		Tree: &github.Tree{SHA: new(tree.GetSHA())}, Parents: []*github.Commit{{SHA: new(head)}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	var entries []map[string]any
	for line := range strings.Lines(string(b)) {
		var e map[string]any
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, e)
	}
	if len(entries) != 2 || entries[0]["action"] != "create_tree" || entries[0]["sha"] != tree.GetSHA() ||
		entries[0]["base_tree"] != base {
		t.Fatalf("create_tree: %v", entries)
	}
	if entries[1]["action"] != "create_commit" || entries[1]["sha"] != commit.GetSHA() || entries[1]["tree"] != tree.GetSHA() ||
		fmt.Sprint(entries[1]["parents"]) != fmt.Sprint([]any{head}) {
		t.Fatalf("create_commit: %v", entries[1])
	}
}

// The repository's own branches are there for the writes too: GitHub refuses to create one
// that exists (422), and a pull request onto a branch it does not have.
func TestWritesSeeTheRepositorysBranches(t *testing.T) {
	dir, head := repo(t)
	gh := client(t, &Server{Repos: map[string]string{"acme/payments": dir}})
	ctx := context.Background()
	if _, resp, err := gh.Git.CreateRef(ctx, "acme", "payments", github.CreateRef{Ref: "refs/heads/main", SHA: head}); resp.StatusCode != 422 {
		t.Errorf("create main: %v", err)
	}
	if _, _, err := gh.Git.CreateRef(ctx, "acme", "payments", github.CreateRef{Ref: "refs/heads/fix", SHA: head}); err != nil {
		t.Fatal(err)
	}
	for base, want := range map[string]int{"release": 422, "main": 201} {
		_, resp, err := gh.PullRequests.Create(ctx, "acme", "payments", github.CreatePullRequest{Title: new("x"), Head: "fix", Base: base})
		if resp == nil || resp.StatusCode != want {
			t.Errorf("a pull request onto %s: %v, want %d", base, err, want)
		}
	}
}
