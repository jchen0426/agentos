package githubtool

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jchen0426/agentos/internal/githubsim"
	"github.com/jchen0426/agentos/internal/tools"
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

const workflowID = "0f8b1c2e-1111-4222-8333-444455556666"

type blobs map[string][]byte

func (b blobs) PutRunBlob(_ context.Context, workflowID string, v []byte) (string, error) {
	sum := sha256.Sum256(v)
	ref := "sha256:" + hex.EncodeToString(sum[:])
	b[workflowID+" "+ref] = v
	return ref, nil
}

func (b blobs) GetRunBlob(_ context.Context, workflowID, ref string) ([]byte, error) {
	v, ok := b[workflowID+" "+ref]
	if !ok {
		return nil, errors.New("the blob was not produced by this run")
	}
	return v, nil
}

type effects struct{ pr int64 }

func (e effects) LatestEffectResult(context.Context, string, string) (map[string]any, bool, error) {
	if e.pr == 0 {
		return nil, false, nil
	}
	return map[string]any{"number": json.Number(strconv.FormatInt(e.pr, 10))}, true, nil
}

func proceed(context.Context) error { return nil }

type fixture struct {
	sim    *githubsim.Server
	client *Client
	blobs  blobs
	repos  map[string]Repo
	commit string
}

func setup(t *testing.T) *fixture {
	t.Helper()
	dir := t.TempDir()
	git := func(args ...string) string {
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com",
			"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "-q", "-b", "main")
	if err := os.MkdirAll(filepath.Join(dir, "app"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "app", "db.py"), []byte(vulnerable), 0o644); err != nil {
		t.Fatal(err)
	}
	git("add", ".")
	git("commit", "-q", "-m", "x")

	sim := &githubsim.Server{Login: "agentos-bot", LogPath: filepath.Join(t.TempDir(), "github.jsonl")}
	srv := httptest.NewServer(sim)
	t.Cleanup(srv.Close)
	client, err := New(Config{APIURL: srv.URL + "/", Token: "test", RequestTimeout: 5 * time.Second, Settle: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{sim: sim, client: client, blobs: blobs{},
		repos: map[string]Repo{"acme/payments": {Mirror: dir, Owner: "acme", Name: "payments"}}, commit: git("rev-parse", "HEAD")}
}

func (f *fixture) request(t *testing.T, args map[string]any) tools.Request {
	t.Helper()
	created, _ := time.Parse(time.RFC3339, "2026-10-18T09:00:00Z")
	return tools.Request{WorkflowID: workflowID, RunCreatedAt: created, EffectID: strings.Repeat("ab", 32), Args: args}
}

func (f *fixture) log(t *testing.T, action string) int {
	t.Helper()
	b, err := os.ReadFile(f.sim.LogPath)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var e map[string]any
		if json.Unmarshal([]byte(line), &e) == nil && e["action"] == action && e["status"] == float64(201) {
			n++
		}
	}
	return n
}

func (f *fixture) allCarriedTheToken(t *testing.T) bool {
	t.Helper()
	b, err := os.ReadFile(f.sim.LogPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var e map[string]any
		if err := json.Unmarshal([]byte(line), &e); err != nil || e["token"] != true {
			return false
		}
	}
	return true
}

func TestOpenDraftPROpensOnceAndFindsItAgain(t *testing.T) {
	f := setup(t)
	ref, _ := f.blobs.PutRunBlob(context.Background(), workflowID, []byte(fix))
	tool := &OpenDraftPR{Client: f.client, Repos: f.repos, Blobs: f.blobs}
	req := f.request(t, map[string]any{"repo": "acme/payments", "commit": f.commit, "base_branch": "main", "patch_ref": ref})
	if err := tool.Args().Validate(req.Args); err != nil {
		t.Fatal(err)
	}
	payload, err := tool.Prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	again, err := tool.Prepare(context.Background(), req)
	if err != nil || string(again) != string(payload) {
		t.Fatalf("Prepare is not deterministic: %v", err)
	}
	if !strings.Contains(string(payload), "name = ?") {
		t.Fatalf("the payload does not hold the patched file: %s", payload)
	}

	first, err := tool.Execute(context.Background(), payload, "key", proceed)
	if err != nil {
		t.Fatal(err)
	}
	// A retry, say after a crash that lost the answer: the same pull request, not a second one.
	second, err := tool.Execute(context.Background(), payload, "key", proceed)
	if err != nil {
		t.Fatal(err)
	}
	if !f.allCarriedTheToken(t) {
		t.Error("a request went to GitHub without the token")
	}
	if first["url"] != second["url"] || f.log(t, "create_pull") != 1 || f.log(t, "create_ref") != 1 {
		t.Fatalf("first %v, second %v, pulls %d, refs %d", first, second, f.log(t, "create_pull"), f.log(t, "create_ref"))
	}
	found, ok, err := tool.Find(context.Background(), payload)
	if err != nil || !ok || found["url"] != first["url"] {
		t.Fatalf("Find: %v %v %v", found, ok, err)
	}
	if err := tool.Result().Validate(first); err != nil {
		t.Fatalf("the tool's own result breaks its result schema: %v", err)
	}
}

func TestOpenDraftPRLeavesAForeignBranchAlone(t *testing.T) {
	f := setup(t)
	ref, _ := f.blobs.PutRunBlob(context.Background(), workflowID, []byte(fix))
	tool := &OpenDraftPR{Client: f.client, Repos: f.repos, Blobs: f.blobs}
	req := f.request(t, map[string]any{"repo": "acme/payments", "commit": f.commit, "base_branch": "main", "patch_ref": ref})
	payload, err := tool.Prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	var p prPayload
	_ = json.Unmarshal(payload, &p)
	f.sim.SetRef("acme/payments", p.Branch, strings.Repeat("1", 40))
	_, err = tool.Execute(context.Background(), payload, "key", proceed)
	var conflict *tools.ConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("got %v, want a conflict", err)
	}
	if f.log(t, "create_pull") != 0 {
		t.Fatal("opened a pull request on a branch it does not own")
	}
}

func TestOpenDraftPRDeniesProtectedPaths(t *testing.T) {
	f := setup(t)
	// No deny_paths in the policy: the paths GitHub acts on are refused anyway.
	tool := &OpenDraftPR{Client: f.client, Repos: f.repos, Blobs: f.blobs}
	for _, path := range []string{".github/workflows/ci.yml", "CODEOWNERS", "docs/CODEOWNERS", ".devcontainer/devcontainer.json",
		".devcontainer.json"} {
		newFile := "diff --git a/" + path + " b/" + path + "\nnew file mode 100644\n--- /dev/null\n+++ b/" + path +
			"\n@@ -0,0 +1 @@\n+x\n"
		ref, _ := f.blobs.PutRunBlob(context.Background(), workflowID, []byte(newFile))
		_, err := tool.Prepare(context.Background(), f.request(t, map[string]any{
			"repo": "acme/payments", "commit": f.commit, "base_branch": "main", "patch_ref": ref}))
		var denied *tools.DeniedError
		if !errors.As(err, &denied) {
			t.Errorf("%s: got %v, want denied", path, err)
		}
	}
}

func TestCommentCommit(t *testing.T) {
	f := setup(t)
	report := `{"summary": "Adds SQL injection. @someone see https://evil.example", "patch": null, "findings": [
	  {"severity": "high", "cwe": "CWE-89", "file": "app/db.py", "line": 2, "title": "SQL injection",
	   "explanation": "Formats name into the query. ` + "````" + ` [click](https://evil.example) \u202egnp.exe"}]}`
	ref, _ := f.blobs.PutRunBlob(context.Background(), workflowID, []byte(report))
	tool := &CommentCommit{Client: f.client, Repos: f.repos, Blobs: f.blobs, Effects: effects{pr: 7}}
	req := f.request(t, map[string]any{"repo": "acme/payments", "commit": f.commit, "report_ref": ref,
		"outcome": "ok", "pr_url": "https://evil.example/not-ours"})
	if err := tool.Args().Validate(req.Args); err != nil {
		t.Fatal(err)
	}
	payload, err := tool.Prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	var p commentPayload
	_ = json.Unmarshal(payload, &p)
	head, fenced, ok := strings.Cut(p.Body, "`````text\n")
	if !ok {
		t.Fatalf("the model's text is not in a fence longer than its own backticks:\n%s", p.Body)
	}
	if strings.ContainsRune(p.Body, '\u202e') {
		t.Error("a bidirectional override reached the comment")
	}
	if strings.Contains(head, "evil") || strings.Contains(head, "@") || !strings.Contains(head, "#7") ||
		!strings.Contains(head, "1 high") || !strings.Contains(fenced, "@someone") {
		t.Fatalf("body:\n%s", p.Body)
	}

	// Someone else posts a comment with the same marker first, and agentd posted one with the
	// marker inside its fenced text (a workflow can compute another effect's ID): the lookup
	// takes neither.
	f.sim.AddComment("acme/payments", f.commit, "mallory", "fake\n"+p.Marker+"\n")
	f.sim.AddComment("acme/payments", f.commit, "agentos-bot", "```text\n"+p.Marker+"\n```\n\n<!-- agent-effect:other -->\n")
	if _, found, err := tool.Find(context.Background(), payload); err != nil || found {
		t.Fatalf("Find took a comment that is not this effect's: %v %v", found, err)
	}
	posted, err := tool.Execute(context.Background(), payload, "key", proceed)
	if err != nil {
		t.Fatal(err)
	}
	found, ok, err := tool.Find(context.Background(), payload)
	if err != nil || !ok || found["url"] != posted["url"] {
		t.Fatalf("Find: %v %v %v", found, ok, err)
	}
}

func TestRenderKeepsWithinGitHubsLimit(t *testing.T) {
	body := render(strings.Repeat("a", 40), strings.Repeat("`", 30000)+strings.Repeat("x", 1990),
		&report{Summary: strings.Repeat("`", 30000)}, 0, marker(strings.Repeat("ab", 32)))
	if len(body) > 65536 || strings.Contains(body, strings.Repeat("`", maxBackticks+2)) {
		t.Fatalf("%d characters, longest backtick run %d", len(body), longestRun(body, '`'))
	}
	if lastLine(body) != marker(strings.Repeat("ab", 32)) {
		t.Fatal("the marker is not the last line")
	}
}

func TestACancelStopsThePullRequest(t *testing.T) {
	f := setup(t)
	ref, _ := f.blobs.PutRunBlob(context.Background(), workflowID, []byte(fix))
	tool := &OpenDraftPR{Client: f.client, Repos: f.repos, Blobs: f.blobs}
	payload, err := tool.Prepare(context.Background(), f.request(t, map[string]any{
		"repo": "acme/payments", "commit": f.commit, "base_branch": "main", "patch_ref": ref}))
	if err != nil {
		t.Fatal(err)
	}
	asked := 0
	cancelled := func(context.Context) error {
		asked++
		return fmt.Errorf("%w: cancelled", tools.ErrNotSent)
	}
	if _, err := tool.Execute(context.Background(), payload, "key", cancelled); !errors.Is(err, tools.ErrNotSent) {
		t.Fatalf("got %v", err)
	}
	if asked != 1 || f.log(t, "create_ref") != 0 || f.log(t, "create_pull") != 0 {
		t.Fatalf("asked %d, refs %d, pulls %d", asked, f.log(t, "create_ref"), f.log(t, "create_pull"))
	}
}

func TestNotConfiguredOnlyDryRuns(t *testing.T) {
	tool := &OpenDraftPR{}
	if tool.Live() {
		t.Fatal("a tool without a client claims to reach GitHub")
	}
	if _, err := tool.Execute(context.Background(), json.RawMessage(`{}`), "key", proceed); !errors.Is(err, tools.ErrNotSent) {
		t.Fatalf("got %v, want not sent", err)
	}
}
