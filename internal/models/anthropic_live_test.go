package models

import (
	"context"
	"os"
	"testing"

	"github.com/jchen0426/agentos/internal/secrets"
)

// liveAdapter is the real models.yaml route to Claude, with the key from the OS keyring. Live
// tests reach the real API, so they run only with AGENTOS_LIVE=1: the CI has neither the key
// nor the network for them.
func liveAdapter(t *testing.T) (*Anthropic, Route, memBlobs) {
	t.Helper()
	if os.Getenv("AGENTOS_LIVE") == "" {
		t.Skip("set AGENTOS_LIVE=1 to call the real API")
	}
	cfg, err := Load("../../config/models.yaml")
	if err != nil {
		t.Fatal(err)
	}
	route, err := cfg.Route("audit_v1")
	if err != nil {
		t.Fatal(err)
	}
	key, err := secrets.Resolve(cfg.Providers[route.Provider].Key)
	if err != nil {
		t.Fatal(err)
	}
	blobs := memBlobs{}
	return NewAnthropic(route.Provider, key, cfg.Providers[route.Provider].BaseURL, blobs), route, blobs
}

// TestLiveCountTokens checks the key and the request shape against the real API. Token
// counting is free.
func TestLiveCountTokens(t *testing.T) {
	a, route, blobs := liveAdapter(t)
	req := Request{WorkflowID: "wf", RequestID: "live", Route: route,
		Args: map[string]any{"diff_ref": blobs.put("wf", []byte(diff))}}
	tokens, err := a.CountTokens(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if tokens < 100 {
		t.Errorf("%d input tokens: the prompt and the schema alone are more", tokens)
	}
	t.Logf("%d input tokens for %v (the larger count)", tokens, route.Candidates)
}
