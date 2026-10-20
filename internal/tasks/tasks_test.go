package tasks

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
	"github.com/jchen0426/agentos/internal/policy"
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

// server is the real git tool server, unsandboxed, as agentd calls it.
func server(t *testing.T) *gitmcp.Client {
	t.Helper()
	c, err := gitmcp.NewClient(gitmcp.Options{Server: gitmcptest.Server(t), Timeout: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// The commit is resolved by the git tool server (gitmcp's tests cover how). What it refuses
// refuses the parameter; where it fails, the start may work later.
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
	srv := server(t)

	for _, name := range []string{head, head[:7]} {
		if got, err := resolveCommit(ctx, srv, dir, name); err != nil || got != head {
			t.Errorf("%s: %q %v", name, got, err)
		}
	}
	// Not a commit: unknown, a blob, an annotated tag's own object.
	git(t, dir, "tag", "-a", "v1", "-m", "a tag")
	var unavailable *Unavailable
	for _, name := range []string{"0000000", git(t, dir, "rev-parse", "HEAD:a.txt"), git(t, dir, "rev-parse", "v1")} {
		if _, err := resolveCommit(ctx, srv, dir, name); err == nil || errors.As(err, &unavailable) || !strings.Contains(err.Error(), "not found") {
			t.Errorf("%s: %v", name, err)
		}
	}
	// A commit over the limit is refused from its header, unread.
	msg := filepath.Join(t.TempDir(), "message")
	if err := os.WriteFile(msg, []byte(strings.Repeat("x", gitmcp.MaxCommitBytes+1)), 0o600); err != nil {
		t.Fatal(err)
	}
	huge := git(t, dir, "commit-tree", head+"^{tree}", "-p", head, "-F", msg)
	if _, err := resolveCommit(ctx, srv, dir, huge); err == nil || errors.As(err, &unavailable) || !strings.Contains(err.Error(), "over the limit") {
		t.Errorf("a huge commit: %v", err)
	}
	// A mirror git cannot read is the server's failure, which agentd's log explains; the error
	// does not carry the mirror's path. One inside a checkout is not the checkout.
	inside := filepath.Join(dir, "not-a-mirror")
	if err := os.Mkdir(inside, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, m := range []string{t.TempDir(), inside} {
		if _, err := resolveCommit(ctx, srv, m, head); !errors.As(err, &unavailable) || strings.Contains(err.Error(), m) {
			t.Errorf("%s: %v, want Unavailable", m, err)
		}
	}
}

// Resolve passes the server's failure on as Unavailable, and checks the parameters before it
// asks the server anything.
func TestSecurityAuditResolve(t *testing.T) {
	dir := t.TempDir()
	git(t, dir, "init", "-q", "-b", "main")
	git(t, dir, "commit", "-q", "--allow-empty", "-m", "init")
	head := git(t, dir, "rev-parse", "HEAD")
	pol := &policy.Policy{Repos: []string{"acme/payments", "acme/broken"}}
	task := SecurityAudit(pol, map[string]string{"acme/payments": dir, "acme/broken": t.TempDir()}, server(t))
	ctx := context.Background()

	input, params, err := task.Resolve(ctx, map[string]string{"repo": "acme/payments", "commit": head[:7], "base": "main"})
	if err != nil || input.(AuditInput).Commit != head || params["commit"] != head {
		t.Fatalf("resolve: %+v %v %v", input, params, err)
	}
	var unavailable *Unavailable
	if _, _, err := task.Resolve(ctx, map[string]string{"repo": "acme/broken", "commit": head[:7], "base": "main"}); !errors.As(err, &unavailable) {
		t.Errorf("a mirror the server cannot read: %v, want Unavailable", err)
	}
	for _, bad := range []map[string]string{
		{"repo": "acme/payments", "commit": "--output=/tmp/x", "base": "main"},
		{"repo": "acme/payments", "commit": head[:6], "base": "main"},
		{"repo": "acme/other", "commit": head[:7], "base": "main"},
	} {
		if _, _, err := task.Resolve(ctx, bad); err == nil || errors.As(err, &unavailable) {
			t.Errorf("%v: %v, want a refusal", bad, err)
		}
	}
}

// The base branch is a name git itself would make (`git check-ref-format --branch` sorts these
// lists): GitHub reads a branch by its exact name, while go-github resolves dot segments in the
// path it asks for, so with "a/./b" open_draft_pr would check a/b and open the pull request
// against "a/./b".
func TestSecurityAuditTakesOnlyBranchNamesGitMakes(t *testing.T) {
	dir := t.TempDir()
	git(t, dir, "init", "-q", "-b", "main")
	git(t, dir, "commit", "-q", "--allow-empty", "-m", "init")
	head := git(t, dir, "rev-parse", "HEAD")
	task := SecurityAudit(&policy.Policy{Repos: []string{"acme/payments"}}, map[string]string{"acme/payments": dir}, server(t))
	ctx := context.Background()
	for _, base := range []string{"a/./b", "main/.", "main/", "stable//3.5", "a/.hidden", "main.lock", "a/b.lock", "x.lock/y",
		"main.", "a..b", "release/../main", "HEAD"} {
		if _, _, err := task.Resolve(ctx, map[string]string{"repo": "acme/payments", "commit": head[:7], "base": base}); err == nil {
			t.Errorf("%q: taken", base)
		}
	}
	for _, base := range []string{"main", "release/1.0", "feature/x-y_z", "v1.2.3", "a.lock.b", "a/-b"} {
		if _, params, err := task.Resolve(ctx, map[string]string{"repo": "acme/payments", "commit": head[:7], "base": base}); err != nil ||
			params["base"] != base {
			t.Errorf("%q: %v %v", base, params, err)
		}
	}
}

// answers is a run's accepted audits, newest first.
type answers []map[string]any

func (a answers) ModelOutputs(context.Context, string) ([]map[string]any, error) { return a, nil }

// The reviewer of a patch sees the findings of the audit that proposed it: a workflow can ask for
// several audits and send one's patch with another's findings. A patch no audit proposed (a
// reviewer's edit) gets the latest audit's, and the card says so.
func TestApprovalContextShowsTheFindingsOfThePatchsAudit(t *testing.T) {
	audit := func(patch, cwe string) map[string]any {
		return map[string]any{"patch_ref": patch, "summary": "audit with " + patch,
			"findings": []any{map[string]any{"severity": "high", "cwe": cwe, "file": "app/db.py", "line": 2, "title": "t", "explanation": "e"}}}
	}
	run := answers{audit("sha256:new", "CWE-79"), audit("sha256:old", "CWE-89")}
	for patch, want := range map[string]struct {
		cwe      string
		proposed bool
	}{
		"sha256:old":    {"CWE-89", true},
		"sha256:new":    {"CWE-79", true},
		"sha256:edited": {"CWE-79", false},
	} {
		c := auditFindings(context.Background(), run, map[string]any{"patch_ref": patch})
		findings := c["findings"].([]any)
		if got := findings[0].(map[string]any)["cwe"]; got != want.cwe || c["patch_from_this_audit"] != want.proposed || c["audits"] != 2 {
			t.Errorf("%s: %v", patch, c)
		}
		if _, ok := findings[0].(map[string]any)["explanation"]; ok {
			t.Errorf("%s: the card carries the model's explanation: %v", patch, findings[0])
		}
	}
}
