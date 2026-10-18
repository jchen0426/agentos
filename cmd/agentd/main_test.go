package main

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/jchen0426/agentos/internal/config"
	"github.com/jchen0426/agentos/internal/models"
)

// The git tool server's launcher gets everything of agentd's it must keep out of reach.
func TestToolDenyRead(t *testing.T) {
	cfg := &config.Config{
		DataDir:       "/srv/agentos/data",
		ControlSocket: "/run/agentos/control/control.sock",
		RuntimeListen: "unix:///run/agentos/runtime/runtime.sock",
		Temporal:      config.Temporal{TLS: &config.TLS{CertFile: "/etc/agentos/agentd.crt", KeyFile: "/etc/agentos/agentd.key"}},
		Repos: map[string]config.Repo{
			"acme/payments": {Mirror: "/srv/mirrors/payments"},
			"acme/ledger":   {Mirror: "/srv/mirrors/acme:ledger"},
		},
	}
	got := toolDenyRead(cfg)
	want := []string{"/srv/agentos/data", "/run/agentos/control", "/run/agentos/runtime", "/etc/agentos/agentd.crt",
		"/etc/agentos/agentd.key", "/srv/mirrors/acme:ledger", "/srv/mirrors/payments"}
	if !slices.Equal(got, want) {
		t.Errorf("toolDenyRead:\n got %q\nwant %q", got, want)
	}
	// A Runtime endpoint on TCP has no file to deny; the sandbox has no network.
	cfg.RuntimeListen = "tcp://127.0.0.1:7233"
	if slices.Contains(toolDenyRead(cfg), "127.0.0.1:7233") || len(toolDenyRead(cfg)) != len(want)-1 {
		t.Errorf("a TCP Runtime endpoint: %q", toolDenyRead(cfg))
	}
}

// A provider elsewhere is reached only over https; a key for a Chat Completions server is read
// only when models.yaml names it.
func TestModelAdapterKeys(t *testing.T) {
	load := func(body string) *models.Config {
		t.Helper()
		path := filepath.Join(t.TempDir(), "models.yaml")
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		m, err := models.Load(path)
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	task := "tasks: {audit_v1: {model: local/m, max_tokens: 10}}\n"
	for name, c := range map[string]struct {
		provider, wantErr string
	}{
		"key over http":       {`{adapter: openai_compat, api: chat, base_url: "http://10.0.0.5:8000/v1", context_tokens: 4096, key: "keyring:k"}`, "only over https"},
		"no key, http vLLM":   {`{adapter: openai_compat, api: chat, base_url: "http://10.0.0.5:8000/v1", context_tokens: 4096}`, "only over https"},
		"no key, https vLLM":  {`{adapter: openai_compat, api: chat, base_url: "https://gpu.lan:8000/v1", context_tokens: 4096}`, ""},
		"no key, Ollama here": {`{adapter: openai_compat, api: responses, base_url: "http://127.0.0.1:11434/v1"}`, ""},
		"Claude over http":    {`{adapter: anthropic, base_url: "http://proxy.lan:8080", key: "keyring:k"}`, "only over https"},
	} {
		m := load("providers:\n  local: " + c.provider + "\n" + task)
		_, err := modelAdapters(m, nil, t.TempDir())
		if (c.wantErr == "") != (err == nil) || (err != nil && !strings.Contains(err.Error(), c.wantErr)) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// The safety identifier is random, made once and kept.
func TestSafetyIdentifier(t *testing.T) {
	dir := t.TempDir()
	first, err := safetyIdentifier(dir)
	if err != nil || !strings.HasPrefix(first, "agentos-") || len(first) < 30 {
		t.Fatalf("safetyIdentifier = %q, %v", first, err)
	}
	if again, _ := safetyIdentifier(dir); again != first {
		t.Errorf("not kept: %q, then %q", first, again)
	}
	if other, _ := safetyIdentifier(t.TempDir()); other == first {
		t.Error("two data directories, one identifier")
	}
	if info, err := os.Stat(filepath.Join(dir, "safety-identifier")); err != nil || info.Mode().Perm() != 0o600 {
		t.Errorf("kept as %v, %v", info.Mode(), err)
	}
}
