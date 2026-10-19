package models

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jchen0426/agentos/internal/secrets"
)

// The live checks of M2c-b's two models, before T1 spends anything on them (README): OpenAI's
// key and the request shape, by counting tokens; the Ollama server's version, model and context
// window, for free. Like TestLiveCountTokens, they run only with AGENTOS_LIVE=1.

// liveRoute is config/models.yaml's audit_v1 sent to model instead, as the harness's
// AGENTOS_MODEL=openai and AGENTOS_MODEL=local send it.
func liveRoute(t *testing.T, model string) (*Config, Route) {
	t.Helper()
	if os.Getenv("AGENTOS_LIVE") == "" {
		t.Skip("set AGENTOS_LIVE=1 to call the real API")
	}
	cfg, err := Load("../../config/models.yaml")
	if err != nil {
		t.Fatal(err)
	}
	task := cfg.Tasks["audit_v1"]
	task.Model, task.Fallback, task.MaxTokens = model, nil, 8000
	cfg.Tasks["audit_v1"] = task
	route, err := cfg.Route("audit_v1")
	if err != nil {
		t.Fatal(err)
	}
	return cfg, route
}

func TestLiveOpenAI(t *testing.T) {
	cfg, route := liveRoute(t, "openai/gpt-6.1-sol")
	p := cfg.Providers[route.Provider]
	key, err := secrets.Resolve(p.Key)
	if err != nil {
		t.Fatalf("the OpenAI key, keychain item agentos-openai-api-key (README): %v", err)
	}
	blobs := memBlobs{}
	a, err := NewOpenAICompat(route.Provider, p, key, "agentos-live-check", blobs)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.CheckRoute(route); err != nil {
		t.Fatal(err)
	}
	req := Request{WorkflowID: "wf", RequestID: "live", Route: route, Args: map[string]any{"diff_ref": blobs.put("wf", []byte(diff))}}
	tokens, err := a.CountTokens(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if tokens < 100 {
		t.Errorf("%d input tokens: the prompt and the schema alone are more", tokens)
	}
	t.Logf("%d input tokens for %s", tokens, route.Name())
}

func TestLiveOllama(t *testing.T) {
	cfg, route := liveRoute(t, "local/gpt-oss:20b")
	p := cfg.Providers[route.Provider]
	root := strings.TrimSuffix(p.BaseURL, "/v1")
	ask := func(method, path string, body, out any) int {
		t.Helper()
		b, _ := json.Marshal(body) // "null" for no body, which GET ignores
		req, err := http.NewRequest(method, root+path, bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := (&http.Client{Timeout: 5 * time.Minute}).Do(req)
		if err != nil {
			t.Fatalf("the Ollama server at %s (README): %v", root, err)
		}
		defer resp.Body.Close()
		if out != nil && resp.StatusCode == http.StatusOK {
			if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
				t.Fatalf("%s %s: %v", method, path, err)
			}
		}
		return resp.StatusCode
	}

	var version struct{ Version string }
	ask(http.MethodGet, "/api/version", nil, &version)
	var major, minor int
	if _, err := fmt.Sscanf(version.Version, "%d.%d", &major, &minor); err != nil || major == 0 && minor < 40 {
		t.Fatalf("Ollama %q: M2c-b wants 0.40 or later", version.Version)
	}

	var show struct {
		RemoteHost  string `json:"remote_host"`
		RemoteModel string `json:"remote_model"`
	}
	if code := ask(http.MethodPost, "/api/show", map[string]string{"model": route.Model}, &show); code != http.StatusOK {
		t.Fatalf("%s: %d from /api/show; ollama pull %s", route.Model, code, route.Model)
	}
	if show.RemoteHost != "" || show.RemoteModel != "" {
		t.Fatalf("%s runs on %s, not here", route.Model, show.RemoteHost)
	}

	// Loaded (an empty prompt only loads it), the model runs with the server's window, which
	// must be the context_tokens agentd checks requests against.
	ask(http.MethodPost, "/api/generate", map[string]any{"model": route.Model, "prompt": "", "keep_alive": "10m"}, nil)
	var ps struct {
		Models []struct {
			Name          string `json:"name"`
			ContextLength int64  `json:"context_length"`
		} `json:"models"`
	}
	ask(http.MethodGet, "/api/ps", nil, &ps)
	found := false
	for _, m := range ps.Models {
		if m.Name == route.Model {
			found = true
			if m.ContextLength != p.ContextTokens {
				t.Errorf("%s runs with a %d-token window, models.yaml says %d: start Ollama with OLLAMA_CONTEXT_LENGTH=%d",
					route.Model, m.ContextLength, p.ContextTokens, p.ContextTokens)
			}
		}
	}
	if !found {
		t.Errorf("%s is not loaded: %+v", route.Model, ps.Models)
	}

	blobs := memBlobs{}
	a, err := NewOpenAICompat(route.Provider, p, "", "", blobs)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.CheckRoute(route); err != nil {
		t.Fatal(err)
	}
	req := Request{WorkflowID: "wf", RequestID: "live", Route: route, Args: map[string]any{"diff_ref": blobs.put("wf", []byte(diff))}}
	tokens, err := a.CountTokens(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Ollama %s, %s with a %d-token window; about %d input tokens", version.Version, route.Model, p.ContextTokens, tokens)
}
