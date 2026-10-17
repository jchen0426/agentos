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
		if got := p.EvaluateTool(c.tool, c.args).Decision; got != c.want {
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

func TestUndeclaredEffectIsUnsafe(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(path, []byte("tools:\n  x: {decision: allow}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	p, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := p.EvaluateTool("x", nil).Effect; got != EffectUnsafe {
		t.Errorf("effect: got %q, want unsafe", got)
	}
}
