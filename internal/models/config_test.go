package models

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func loadModels(t *testing.T, body string) (*Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "models.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return Load(path)
}

const providers = `
providers:
  anthropic: {adapter: anthropic, key: "keyring:k"}
  openai:    {adapter: openai_compat, api: responses, base_url: "https://api.openai.com/v1", key: "keyring:o", efforts: [low, high]}
  local:     {adapter: openai_compat, api: chat, base_url: "http://127.0.0.1:11434/v1", context_tokens: 65536}
`

func TestCrossProviderFallbackRoutes(t *testing.T) {
	c, err := loadModels(t, providers+`
tasks:
  audit_v1:
    model: anthropic/claude-opus-5-5
    max_tokens: 16000
    effort: high
    fallback: [anthropic/claude-opus-4-8]
    cross_provider_fallback: [openai/gpt-6.1-sol, local/gpt-oss:20b]
`)
	if err != nil {
		t.Fatal(err)
	}
	r, err := c.Route("audit_v1")
	if err != nil {
		t.Fatal(err)
	}
	// The request's own candidates are Claude's; each other provider is a request of its own.
	if strings.Join(r.Candidates, ",") != "anthropic/claude-opus-5-5,anthropic/claude-opus-4-8" || len(r.Next) != 2 {
		t.Fatalf("route %+v", r)
	}
	for i, want := range []struct{ provider, adapter, model string }{
		{"openai", "openai_compat", "gpt-6.1-sol"}, {"local", "openai_compat", "gpt-oss:20b"},
	} {
		n := r.Next[i]
		if n.Provider != want.provider || n.Adapter != want.adapter || n.Model != want.model ||
			len(n.Candidates) != 1 || n.MaxTokens != 16000 || n.Effort != "high" || len(n.Next) != 0 {
			t.Errorf("next %d: %+v", i, n)
		}
	}
	for _, p := range []string{"anthropic", "openai", "local"} {
		if !c.Uses(p) {
			t.Errorf("Uses(%s) = false", p)
		}
	}
	// Claude is sent the task's effort; OpenAI, which lists low and high, the highest not above it.
	if c.EffortSent(r) != "high" || c.EffortSent(r.Next[0]) != "high" || c.EffortSent(r.Next[1]) != "" {
		t.Errorf("efforts sent: %q, %q, %q", c.EffortSent(r), c.EffortSent(r.Next[0]), c.EffortSent(r.Next[1]))
	}
}

func TestModelsRefusesBadRoutes(t *testing.T) {
	task := func(lines string) string {
		return providers + "tasks:\n  audit_v1:\n    max_tokens: 1000\n" + lines
	}
	for name, body := range map[string]string{
		// A fallback within the request exists only on Claude, and only on the same provider.
		"fallback to another provider": task("    model: anthropic/claude-opus-5-5\n    fallback: [openai/gpt-6.1-sol]\n"),
		"fallback on openai":           task("    model: openai/gpt-6.1-sol\n    fallback: [openai/gpt-6-astra]\n"),
		"cross to the same provider":   task("    model: anthropic/claude-opus-5-5\n    cross_provider_fallback: [anthropic/claude-opus-4-8]\n"),
		"cross listed twice":           task("    model: anthropic/claude-opus-5-5\n    cross_provider_fallback: [openai/x, openai/x]\n"),
		"cross to unknown provider":    task("    model: anthropic/claude-opus-5-5\n    cross_provider_fallback: [mistral/large]\n"),
		// Ollama's cloud models run on Ollama's servers.
		"ollama cloud model":  task("    model: local/gpt-oss:120b-cloud\n"),
		"ollama cloud tag":    task("    model: local/qwen3-coder:480b-cloud\n"),
		"ollama cloud suffix": task("    model: local/deepseek-v4:cloud\n"),
		"ollama cloud upper":  task("    model: local/GPT-OSS:120B-CLOUD\n"),
		"cloud on responses": "providers:\n  ollama: {adapter: openai_compat, api: responses, base_url: http://127.0.0.1:11434/v1}\n" +
			"tasks: {audit_v1: {model: ollama/gpt-oss:120b-cloud, max_tokens: 10}}\n",
		"unknown task effort": task("    model: anthropic/claude-opus-5-5\n    effort: extreme\n"),
		"chat without context_tokens": "providers:\n  local: {adapter: openai_compat, api: chat, base_url: http://127.0.0.1:11434/v1}\n" +
			"tasks: {audit_v1: {model: local/m, max_tokens: 10}}\n",
		"no api":         "providers:\n  openai: {adapter: openai_compat, base_url: https://api.openai.com/v1}\ntasks: {}\n",
		"no base_url":    "providers:\n  openai: {adapter: openai_compat, api: responses}\ntasks: {}\n",
		"unknown effort": "providers:\n  openai: {adapter: openai_compat, api: responses, base_url: x, efforts: [extreme]}\ntasks: {}\n",
		"api on claude":  "providers:\n  anthropic: {adapter: anthropic, api: responses}\ntasks: {}\n",
	} {
		if _, err := loadModels(t, body); err == nil {
			t.Errorf("%s: loaded", name)
		}
	}
}
