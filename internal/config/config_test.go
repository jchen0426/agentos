package config

import (
	"os"
	"path/filepath"
	"strings"
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
	if len(c.GitTools.Sandbox) != 1 {
		t.Fatalf("git_tools.sandbox: %v", c.GitTools.Sandbox)
	}
	if _, err := os.Stat(c.GitTools.Sandbox[0]); err != nil {
		t.Errorf("the git tool launcher: %v", err)
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

// The tests and the README give a Worker the repository as its code directory, and its sandbox
// lets it read all of that directory (and, on Linux, connect to any socket in it). So the dev
// configs keep agentd's data, its sockets and the mirror out of the repository, in the home
// directory, which the macOS sandbox cannot read and the Linux one does not mount.
func TestDevConfigsKeepDataOutOfTheRepository(t *testing.T) {
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"agentd.yaml", "agentd-claude.yaml"} {
		c, err := Load(filepath.Join(repo, "config/dev", name))
		if err != nil {
			t.Fatal(err)
		}
		paths := []string{c.DataDir, c.ControlSocket}
		if p, ok := strings.CutPrefix(c.RuntimeListen, "unix://"); ok {
			paths = append(paths, p)
		}
		for _, r := range c.Repos {
			paths = append(paths, r.Mirror)
		}
		for _, p := range paths {
			if within(p, repo) || !within(p, home) {
				t.Errorf("%s: %s is in the repository, or outside the home directory", name, p)
			}
		}
	}
}

func within(path, dir string) bool {
	rel, err := filepath.Rel(dir, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func TestPathsInTheHomeDirectory(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "agentd.yaml")
	if err := os.WriteFile(path, []byte("data_dir: ~/agentos-data\npolicy_file: p\nmodels_file: ~/m.yaml\nprices_file: x\n"+
		"repos:\n  a/b: ~/mirrors/b\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range [][2]string{
		{c.DataDir, filepath.Join(home, "agentos-data")},
		{c.ControlSocket, filepath.Join(home, "agentos-data", "control", "control.sock")},
		{c.ModelsFile, filepath.Join(home, "m.yaml")},
		{c.Repos["a/b"].Mirror, filepath.Join(home, "mirrors", "b")},
		{c.PolicyFile, filepath.Join(dir, "p")}, // relative: next to the config
	} {
		if p[0] != p[1] {
			t.Errorf("got %s, want %s", p[0], p[1])
		}
	}

	// Without a home directory, a ~/ path is an error, not a directory named ~.
	t.Setenv("HOME", "")
	if c, err := Load(path); err == nil {
		t.Errorf("loaded without a home directory: data_dir %s", c.DataDir)
	}
	if err := os.WriteFile(path, []byte("data_dir: d\npolicy_file: p\nmodels_file: m\nprices_file: x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err != nil {
		t.Errorf("a config without ~/ paths needs no home directory: %v", err)
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
