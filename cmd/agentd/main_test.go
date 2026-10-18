package main

import (
	"slices"
	"testing"

	"github.com/jchen0426/agentos/internal/config"
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
