package policy

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDesignDocPolicy(t *testing.T) {
	p, err := Load("../../config/policy.yaml")
	if err != nil {
		t.Fatal(err)
	}
	repo := map[string]any{"repo": "acme/payments"}
	cases := []struct {
		tool string
		args map[string]any
		want Decision
	}{
		{"git.show_diff", repo, Allow},
		{"github.open_draft_pr", repo, RequireApproval},
		{"github.push", repo, Deny},                                             // not listed
		{"git.show_diff", map[string]any{"repo": "evil/repo"}, Deny},            // repo not in the policy
		{"git.show_diff", map[string]any{"repo": []any{"acme/payments"}}, Deny}, // not a string
		{"git.show_diff", map[string]any{"commit": "3f2a"}, Deny},               // no repo at all
	}
	for _, c := range cases {
		if got := p.EvaluateTool(c.tool, c.args, nil).Decision; got != c.want {
			t.Errorf("%s %v: got %v, want %v", c.tool, c.args, got, c.want)
		}
	}
	if got := p.EvaluateModel("audit_v1", "fake").Decision; got != Deny {
		t.Errorf("fake provider: got %v, want deny", got)
	}
	if got := p.EvaluateModel("audit_v1", "anthropic").Decision; got != Allow {
		t.Errorf("anthropic: got %v, want allow", got)
	}
}

func TestLoadRejectsTyposAndUnknownValues(t *testing.T) {
	for name, body := range map[string]string{
		"unknown key":      "repos: []\ntool: {}\n",
		"unknown decision": "tools:\n  x: {decision: maybe, effect: none}\n",
		"unknown effect":   "tools:\n  x: {decision: allow, effect: sometimes}\n",
	} {
		path := filepath.Join(t.TempDir(), "policy.yaml")
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(path); err == nil {
			t.Errorf("%s: loaded without error", name)
		}
	}
}

// A policy need not state a tool's effect class: the tool declares its own (section 6), and
// the gateway refuses a policy that states a weaker one.
func TestEffectClassIsOptional(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(path, []byte("tools:\n  x: {decision: allow}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	p, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := p.EvaluateTool("x", nil, nil).Effect; got != "" {
		t.Errorf("effect: got %q, want none stated", got)
	}
}

func TestToolCallsStayInTheRunsScope(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policy.yaml")
	body := "repos: [acme/payments, acme/ledger]\ntools:\n  github.comment_commit: {decision: allow, effect: lookup}\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	p, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	sha := "3f2a9c1d00000000000000000000000000000000"
	scope := map[string]string{"repo": "acme/payments", "commit": sha, "base_branch": "main"}
	cases := []struct {
		args map[string]any
		want Decision
	}{
		{map[string]any{"repo": "acme/payments", "commit": sha}, Allow},
		{map[string]any{"repo": "acme/payments", "commit": sha, "base_branch": "main"}, Allow},
		{map[string]any{"repo": "acme/ledger", "commit": sha}, Deny},                    // in the policy, not this run's
		{map[string]any{"repo": "acme/payments", "commit": "0000"}, Deny},               // another commit
		{map[string]any{"repo": "acme/payments", "base_branch": "release"}, Deny},       // another base branch
		{map[string]any{"repo": "acme/payments", "commit": []any{sha}}, Deny},           // not a string
		{map[string]any{"repo": "acme/payments", "report_ref": "blob:sha256:1"}, Allow}, // unscoped arguments pass
	}
	for _, c := range cases {
		if got := p.EvaluateTool("github.comment_commit", c.args, scope).Decision; got != c.want {
			t.Errorf("%v: got %v, want %v", c.args, got, c.want)
		}
	}
}
