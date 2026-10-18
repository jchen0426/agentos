package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jchen0426/agentos/internal/models"
	"github.com/jchen0426/agentos/internal/policy"
	"github.com/jchen0426/agentos/internal/pricing"
)

// priced checks that every model each task may reach has a price, as agentd does at start.
func priced(t *testing.T, m *models.Config, pricesFile string) {
	t.Helper()
	prices, err := pricing.Load(pricesFile)
	if err != nil {
		t.Fatal(err)
	}
	for name := range m.Tasks {
		r, err := m.Route(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := prices.Bound(r.Candidates, 0, r.MaxTokens); err != nil {
			t.Errorf("task %s: %v", name, err)
		}
	}
}

func TestDevConfigLoads(t *testing.T) {
	c, err := Load("../../config/dev/agentd.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{c.DataDir, c.ControlSocket, c.PolicyFile, c.ModelsFile, c.PricesFile, c.Repos["acme/payments"].Mirror} {
		if !filepath.IsAbs(p) {
			t.Errorf("%q is not absolute", p)
		}
	}
	if c.TaskQueues.Gateway != "gateway" || c.TaskQueues.Workflows != "agentos-workflows" {
		t.Errorf("task queues: %+v", c.TaskQueues)
	}
	if _, err := policy.Load(c.PolicyFile); err != nil {
		t.Error(err)
	}
	m, err := models.Load(c.ModelsFile)
	if err != nil {
		t.Fatal(err)
	}
	priced(t, m, c.PricesFile)
}

func TestDevClaudeConfigLoads(t *testing.T) {
	c, err := Load("../../config/dev/agentd-claude.yaml")
	if err != nil {
		t.Fatal(err)
	}
	m, err := models.Load(c.ModelsFile)
	if err != nil {
		t.Fatal(err)
	}
	priced(t, m, c.PricesFile)
	pol, err := policy.Load(c.PolicyFile)
	if err != nil {
		t.Fatal(err)
	}
	r, _ := m.Route("audit_v1")
	if v := pol.EvaluateModel("audit_v1", r.Provider); v.Decision != policy.Allow {
		t.Errorf("the dev policy refuses %s: %s", r.Provider, v.Reason)
	}
	// Until approvals arrive (M4), a real model and real GitHub writes without approval never
	// meet: injected text in a diff could open a pull request. This config only dry-runs.
	if c.GitHub != nil {
		t.Error("agentd-claude.yaml configures GitHub: with the dev policy, nothing would ask before writing")
	}
}

func TestReposAndGitHub(t *testing.T) {
	dir := t.TempDir()
	write := func(body string) (*Config, error) {
		path := filepath.Join(dir, "agentd.yaml")
		if err := os.WriteFile(path, []byte("policy_file: p\nmodels_file: m\nprices_file: x\n"+body), 0o600); err != nil {
			t.Fatal(err)
		}
		return Load(path)
	}
	c, err := write("repos:\n  acme/payments: mirror\n  acme/ledger: {mirror: m2, github: jchen0426/audit-target}\n" +
		"github: {api_url: \"http://127.0.0.1:9/\"}\n")
	if err != nil {
		t.Fatal(err)
	}
	if r := c.Repos["acme/payments"]; r.Mirror != filepath.Join(dir, "mirror") || r.GitHub != "acme/payments" {
		t.Errorf("acme/payments: %+v", r)
	}
	if r := c.Repos["acme/ledger"]; r.GitHub != "jchen0426/audit-target" {
		t.Errorf("acme/ledger: %+v", r)
	}
	if g := c.GitHub; g.RequestTimeout != 30*time.Second || g.Settle != time.Minute || g.AuthorEmail != "agentos@agentos.invalid" {
		t.Errorf("github defaults: %+v", g)
	}
	for name, body := range map[string]string{
		"no token for GitHub itself": "github: {api_url: \"https://api.github.com/\"}\n",
		"github not owner/name":      "repos:\n  a/b: {mirror: m, github: nope}\n",
		"no mirror":                  "repos:\n  a/b: {github: o/n}\n",
	} {
		if _, err := write(body); err == nil {
			t.Errorf("%s: loaded", name)
		}
	}
}

func TestDesignDocModelsLoad(t *testing.T) {
	m, err := models.Load("../../config/models.yaml")
	if err != nil {
		t.Fatal(err)
	}
	r, err := m.Route("audit_v1")
	if err != nil {
		t.Fatal(err)
	}
	if r.Provider != "anthropic" || r.Adapter != "anthropic" || r.Model != "claude-opus-5-5" || r.Effort != "high" || r.MaxTokens <= 0 {
		t.Errorf("route: %+v", r)
	}
	priced(t, m, "../../config/prices.yaml")
}
