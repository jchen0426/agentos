package config

import (
	"path/filepath"
	"testing"

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
	for _, p := range []string{c.DataDir, c.ControlSocket, c.PolicyFile, c.ModelsFile, c.PricesFile, c.Repos["acme/payments"]} {
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
