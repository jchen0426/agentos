package models

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jchen0426/agentos/internal/pricing"
)

// fakeOpenAI stands in for an OpenAI-compatible server and records every request it gets.
type fakeOpenAI struct {
	*httptest.Server
	mu      sync.Mutex
	headers []http.Header
	paths   []string
	bodies  []map[string]any
	answer  func(w http.ResponseWriter, path string) // answers /v1/responses and /v1/chat/completions
	show    map[string]any                           // Ollama's /api/show answer; nil answers 404
	refuse  int                                      // /api/show answers this status instead, when set
	counted func(w http.ResponseWriter)              // answers /v1/responses/input_tokens, when set
}

func newFakeOpenAI(t *testing.T) *fakeOpenAI {
	f := &fakeOpenAI{}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(b, &body)
		f.mu.Lock()
		f.headers, f.paths, f.bodies = append(f.headers, r.Header.Clone()), append(f.paths, r.URL.Path), append(f.bodies, body)
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/responses/input_tokens":
			if f.counted != nil {
				f.counted(w)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"object": "response.input_tokens", "input_tokens": 1234})
		case "/v1/responses", "/v1/chat/completions":
			f.answer(w, r.URL.Path)
		case "/api/show":
			if f.refuse != 0 {
				w.WriteHeader(f.refuse)
				return
			}
			if f.show == nil {
				http.NotFound(w, r)
				return
			}
			_ = json.NewEncoder(w).Encode(f.show)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(f.Close)
	return f
}

func (f *fakeOpenAI) last(path string) (http.Header, map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := len(f.paths) - 1; i >= 0; i-- {
		if f.paths[i] == path {
			return f.headers[i], f.bodies[i]
		}
	}
	return nil, nil
}

func (f *fakeOpenAI) count(path string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, p := range f.paths {
		if p == path {
			n++
		}
	}
	return n
}

const reportJSON = `{"summary":"s","findings":[],"patch":null}`

func responsesAnswer(status, incomplete string, content ...map[string]any) func(http.ResponseWriter, string) {
	return tieredAnswer(status, incomplete, "default", "", content...)
}

// tieredAnswer is an answer from the given service tier; a failed one carries errorCode.
func tieredAnswer(status, incomplete, tier, errorCode string, content ...map[string]any) func(http.ResponseWriter, string) {
	return func(w http.ResponseWriter, _ string) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": "resp_1", "object": "response", "model": "gpt-6.1-sol-2026-09-30", "status": status,
			"service_tier": tier, "error": map[string]any{"code": errorCode, "message": "scripted"},
			"incomplete_details": map[string]any{"reason": incomplete},
			"output": []any{
				map[string]any{"type": "reasoning", "id": "rs_1", "summary": []any{}},
				map[string]any{"type": "message", "id": "msg_1", "role": "assistant", "status": "completed", "content": content},
			},
			"usage": map[string]any{
				"input_tokens": 1000, "output_tokens": 500, "total_tokens": 1500,
				"input_tokens_details":  map[string]any{"cached_tokens": 200, "cache_write_tokens": 300},
				"output_tokens_details": map[string]any{"reasoning_tokens": 400},
			},
		})
	}
}

func chatAnswer(finish, content, refusal string, promptTokens int) func(http.ResponseWriter, string) {
	return func(w http.ResponseWriter, _ string) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": "chatcmpl-1", "object": "chat.completion", "model": "gpt-oss:20b",
			"choices": []any{map[string]any{"index": 0, "finish_reason": finish,
				"message": map[string]any{"role": "assistant", "content": content, "refusal": refusal}}},
			"usage": map[string]any{"prompt_tokens": promptTokens, "completion_tokens": 300, "total_tokens": promptTokens + 300,
				"prompt_tokens_details": map[string]any{"cached_tokens": 100}},
		})
	}
}

func openaiRequest(t *testing.T, blobs memBlobs, effort string) Request {
	t.Helper()
	ref := blobs.put("run-1", []byte("diff --git a/app.py b/app.py\n+print(1)\n"))
	schema := map[string]any{"type": "object", "additionalProperties": false, "required": []any{"summary"},
		"properties": map[string]any{"summary": map[string]any{"type": "string"}}}
	return Request{WorkflowID: "run-1", RequestID: "r1", Args: map[string]any{"diff_ref": ref},
		Route: Route{Task: "audit_v1", Model: "gpt-6.1-sol", MaxTokens: 16000, Effort: effort,
			Prompt: "You audit commits.", Schema: schema, Candidates: []string{"openai/gpt-6.1-sol"}}}
}

// The client reads nothing from the environment: not the key, the base URL, the
// organization or the project.
func setWrongOpenAIEnvironment(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "sk-from-the-environment")
	t.Setenv("OPENAI_BASE_URL", "http://127.0.0.1:1/v1")
	t.Setenv("OPENAI_ORG_ID", "org-from-the-environment")
	t.Setenv("OPENAI_PROJECT_ID", "proj-from-the-environment")
}

func TestOpenAIResponses(t *testing.T) {
	setWrongOpenAIEnvironment(t)
	f := newFakeOpenAI(t)
	blobs := memBlobs{}
	p := Provider{Adapter: "openai_compat", API: "responses", BaseURL: f.URL + "/v1", Efforts: []string{"low", "medium", "high", "xhigh", "max"}}
	a, err := NewOpenAICompat("openai", p, "sk-given", "agentos-test", blobs)
	if err != nil {
		t.Fatal(err)
	}
	req := openaiRequest(t, blobs, "high")
	ctx := context.Background()

	if n, err := a.CountTokens(ctx, req); err != nil || n != 1234 {
		t.Fatalf("CountTokens = %d, %v", n, err)
	}
	_, counted := f.last("/v1/responses/input_tokens")
	if counted["model"] != "gpt-6.1-sol" || counted["instructions"] != "You audit commits." ||
		!strings.Contains(counted["input"].(string), "+print(1)") {
		t.Errorf("the count is not of the request: %v", counted)
	}

	f.answer = responsesAnswer("completed", "", map[string]any{"type": "output_text", "text": reportJSON, "annotations": []any{}})
	resp, err := a.Call(ctx, req)
	if err != nil || resp.Status != "ok" || resp.Output["summary"] != "s" || resp.Effort != "high" {
		t.Fatalf("Call = %+v, %v", resp, err)
	}
	// Billed as the model asked for, not the dated snapshot that answered; input split into
	// plain, written and read.
	want := []Billed{{Model: "openai/gpt-6.1-sol", Usage: pricing.Usage{InputTokens: 500, CacheWriteTokens: 300, CacheReadTokens: 200, OutputTokens: 500}}}
	if resp.Model != "openai/gpt-6.1-sol" || len(resp.Billed) != 1 || resp.Billed[0] != want[0] {
		t.Errorf("billed %+v as %s", resp.Billed, resp.Model)
	}
	h, body := f.last("/v1/responses")
	if h.Get("Authorization") != "Bearer sk-given" || h.Get("OpenAI-Organization") != "" || h.Get("OpenAI-Project") != "" {
		t.Errorf("headers %v", h)
	}
	format, _ := body["text"].(map[string]any)["format"].(map[string]any)
	reasoning, _ := body["reasoning"].(map[string]any)
	if body["store"] != false || body["safety_identifier"] != "agentos-test" || body["max_output_tokens"] != float64(16000) ||
		body["prompt_cache_key"] != "audit_v1" || body["service_tier"] != "default" || format["type"] != "json_schema" || format["strict"] != true ||
		reasoning["effort"] != "high" || !strings.Contains(body["input"].(string), "<diff_") {
		t.Errorf("request %v", body)
	}

	for name, c := range map[string]struct {
		answer            func(http.ResponseWriter, string)
		status, stop, cat string
	}{
		"refusal":        {responsesAnswer("completed", "", map[string]any{"type": "refusal", "refusal": "I can't help with that."}), "refused", "completed", "refusal"},
		"content filter": {responsesAnswer("incomplete", "content_filter"), "refused", "incomplete", "content_filter"},
		"max tokens":     {responsesAnswer("incomplete", "max_output_tokens", map[string]any{"type": "output_text", "text": `{"summ`, "annotations": []any{}}), "ok", "max_tokens", ""},
	} {
		f.answer = c.answer
		resp, err := a.Call(ctx, req)
		if err != nil || resp.Status != c.status || resp.StopReason != c.stop || (c.cat != "" && resp.Value["category"] != c.cat) {
			t.Errorf("%s: %+v, %v", name, resp, err)
		}
		if c.status == "ok" && resp.Output != nil {
			t.Errorf("%s: output cut off at max_tokens decoded: %v", name, resp.Output)
		}
		if len(resp.Billed) != 1 || resp.Billed[0].Usage.OutputTokens != 500 {
			t.Errorf("%s: what the model wrote before it stopped is billed: %+v", name, resp.Billed)
		}
	}
	// Served from another tier than the one asked for: billed under a name the price table
	// does not know, so the gateway charges the whole reservation.
	f.answer = tieredAnswer("completed", "", "priority", "", map[string]any{"type": "output_text", "text": reportJSON, "annotations": []any{}})
	if resp, err := a.Call(ctx, req); err != nil || resp.Status != "ok" || resp.Billed[0].Model != "openai/gpt-6.1-sol@priority" {
		t.Errorf("priority tier: %+v, %v", resp, err)
	}
	// A failed response: a policy code is a refusal; any other leaves the outcome unknown.
	f.answer = tieredAnswer("failed", "", "default", "bio_policy")
	if resp, err := a.Call(ctx, req); err != nil || resp.Status != "refused" || resp.Value["category"] != "bio_policy" {
		t.Errorf("bio_policy: %+v, %v", resp, err)
	}
	f.answer = tieredAnswer("failed", "", "default", "server_error")
	var rejected *RejectedError
	if _, err := a.Call(ctx, req); err == nil || errors.Is(err, ErrNotSent) || errors.As(err, &rejected) {
		t.Errorf("server_error: %v", err)
	}
	// The server says the input does not fit: too long, and turned away unbilled.
	f.answer = func(w http.ResponseWriter, _ string) {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"message": "too many tokens", "code": "context_length_exceeded"}})
	}
	if _, err := a.Call(ctx, req); !errors.Is(err, ErrTooLong) || !errors.As(err, &rejected) || !rejected.Unbilled() {
		t.Errorf("context_length_exceeded: %v", err)
	}
	// OpenAI itself is never asked about cloud models; a server on this machine always is.
	if remote, _ := NewOpenAICompat("openai", Provider{Adapter: "openai_compat", API: "responses",
		BaseURL: "https://api.openai.com/v1"}, "sk", "", blobs); remote.probe || !a.probe {
		t.Errorf("asks OpenAI %v, a server here %v", remote.probe, a.probe)
	}
}

func TestOpenAIErrors(t *testing.T) {
	f := newFakeOpenAI(t)
	blobs := memBlobs{}
	p := Provider{Adapter: "openai_compat", API: "responses", BaseURL: f.URL + "/v1"}
	a, _ := NewOpenAICompat("openai", p, "sk", "", blobs)
	req := openaiRequest(t, blobs, "high")
	ctx := context.Background()
	reply := func(status int, code string) func(http.ResponseWriter, string) {
		return func(w http.ResponseWriter, _ string) {
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"message": "nope", "type": "x", "code": code}})
		}
	}
	for _, c := range []struct {
		status    int
		retryable bool
	}{{429, true}, {500, true}, {400, false}} {
		f.answer = reply(c.status, "")
		before := f.count("/v1/responses")
		_, err := a.Call(ctx, req)
		var rejected *RejectedError
		if !errors.As(err, &rejected) || rejected.StatusCode != c.status || rejected.Retryable() != c.retryable {
			t.Errorf("%d: %v", c.status, err)
		}
		if f.count("/v1/responses") != before+1 {
			t.Errorf("%d: the SDK retried", c.status)
		}
	}
	// OpenAI's cyber safeguards decline with an error: a refusal, billed nothing, no reasoning
	// effort recorded since none was sent (the provider lists no efforts).
	f.answer = reply(400, "cyber_policy")
	resp, err := a.Call(ctx, req)
	if err != nil || resp.Status != "refused" || resp.Value["category"] != "cyber_policy" || resp.Effort != "" ||
		len(resp.Billed) != 1 || resp.Billed[0].Usage != (pricing.Usage{}) {
		t.Errorf("cyber_policy: %+v, %v", resp, err)
	}
	if _, body := f.last("/v1/responses"); body["reasoning"] != nil {
		t.Errorf("an effort was sent to a provider that lists none: %v", body["reasoning"])
	}
	// OpenAI's usage-policy flag is a refusal too, so a fallback can take the call.
	f.answer = reply(400, "invalid_prompt")
	if resp, err := a.Call(ctx, req); err != nil || resp.Status != "refused" || resp.Value["category"] != "invalid_prompt" {
		t.Errorf("invalid_prompt: %+v, %v", resp, err)
	}
	// Declined while counting: turned away for good, not retried.
	f.counted = func(w http.ResponseWriter) {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"message": "flagged", "code": "invalid_prompt"}})
	}
	var rejected *RejectedError
	if _, err := a.CountTokens(ctx, req); !errors.As(err, &rejected) || rejected.Retryable() {
		t.Errorf("declined while counting: %v", err)
	}
	f.counted = nil
	// Nothing listening: the request never left.
	f.Close()
	if _, err := a.Call(ctx, req); !errors.Is(err, ErrNotSent) {
		t.Errorf("closed server: %v", err)
	}
	// A server TLS does not trust: the request never left either.
	untrusted := httptest.NewTLSServer(http.NotFoundHandler())
	defer untrusted.Close()
	b, _ := NewOpenAICompat("openai", Provider{Adapter: "openai_compat", API: "responses", BaseURL: untrusted.URL + "/v1"}, "sk", "", blobs)
	if _, err := b.Call(ctx, req); !errors.Is(err, ErrNotSent) {
		t.Errorf("untrusted server: %v", err)
	}
	// Without streaming, a request that may take more than ten minutes is refused at start.
	if err := a.CheckRoute(Route{MaxTokens: 32000}); err == nil {
		t.Error("max_tokens 32000 passed the start-up check")
	}
	if err := a.CheckRoute(Route{MaxTokens: 16000}); err != nil {
		t.Errorf("max_tokens 16000: %v", err)
	}
}

func TestOpenAIChat(t *testing.T) {
	setWrongOpenAIEnvironment(t)
	f := newFakeOpenAI(t)
	blobs := memBlobs{}
	p := Provider{Adapter: "openai_compat", API: "chat", BaseURL: f.URL + "/v1", ContextTokens: 32768,
		Efforts: []string{"low", "medium", "high"}}
	a, err := NewOpenAICompat("local", p, "", "agentos-test", blobs)
	if err != nil {
		t.Fatal(err)
	}
	req := openaiRequest(t, blobs, "max") // gpt-oss takes high at most
	req.Route.Model, req.Route.MaxTokens = "gpt-oss:20b", 4096
	ctx := context.Background()
	estimate, err := a.CountTokens(ctx, req)
	if err != nil || estimate <= 0 {
		t.Fatalf("CountTokens = %d, %v", estimate, err)
	}
	if f.count("/v1/responses/input_tokens") != 0 {
		t.Error("asked a chat server to count")
	}

	f.answer = chatAnswer("stop", reportJSON, "", int(estimate))
	resp, err := a.Call(ctx, req)
	if err != nil || resp.Status != "ok" || resp.Output["summary"] != "s" || resp.Effort != "high" {
		t.Fatalf("Call = %+v, %v", resp, err)
	}
	if resp.Billed[0].Usage != (pricing.Usage{InputTokens: estimate - 100, CacheReadTokens: 100, OutputTokens: 300}) {
		t.Errorf("billed %+v", resp.Billed)
	}
	h, body := f.last("/v1/chat/completions")
	format, _ := body["response_format"].(map[string]any)
	messages, _ := body["messages"].([]any)
	if h.Get("Authorization") != "" || body["max_tokens"] != float64(4096) || body["max_completion_tokens"] != nil ||
		body["reasoning_effort"] != "high" || format["type"] != "json_schema" || len(messages) != 2 {
		t.Errorf("request %v, Authorization %q", body, h.Get("Authorization"))
	}

	for name, c := range map[string]struct {
		answer       func(http.ResponseWriter, string)
		status, stop string
	}{
		"refusal":        {chatAnswer("stop", "", "I can't help with that.", int(estimate)), "refused", "stop"},
		"content filter": {chatAnswer("content_filter", "", "", int(estimate)), "refused", "content_filter"},
		"max tokens":     {chatAnswer("length", `{"summ`, "", int(estimate)), "ok", "max_tokens"},
	} {
		f.answer = c.answer
		if resp, err := a.Call(ctx, req); err != nil || resp.Status != c.status || resp.StopReason != c.stop {
			t.Errorf("%s: %+v, %v", name, resp, err)
		}
	}
	// The server read far less than it was sent: it cut the input to fit its window.
	f.answer = chatAnswer("stop", reportJSON, "", 5)
	if _, err := a.Call(ctx, req); !errors.Is(err, ErrTooLong) {
		t.Errorf("a cut input: %v", err)
	}
	// Input and answer together ran past the window: the server forgot the start of the input.
	f.answer = chatAnswer("stop", reportJSON, "", 32768-200)
	if _, err := a.Call(ctx, req); !errors.Is(err, ErrTooLong) {
		t.Errorf("past the window: %v", err)
	}
	// Ollama turns away a prompt it cannot fit when it does not shift its window.
	f.answer = func(w http.ResponseWriter, _ string) {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"message": "the prompt is longer than the context length currently available to the model"}})
	}
	if _, err := a.Call(ctx, req); !errors.Is(err, ErrTooLong) {
		t.Errorf("prompt too long: %v", err)
	}
	// Input and max_tokens over the window: not sent at all.
	req.Route.MaxTokens = 32700
	if _, err := a.CountTokens(ctx, req); !errors.Is(err, ErrTooLong) {
		t.Errorf("over the window: %v", err)
	}
	if err := a.CheckRoute(Route{MaxTokens: 32768}); err == nil {
		t.Error("max_tokens as large as the window passed the start-up check")
	}
}

func TestOpenAICloudModels(t *testing.T) {
	f := newFakeOpenAI(t)
	blobs := memBlobs{}
	// Ollama serves the Responses API too: a server on this machine is asked either way.
	for _, api := range []string{"chat", "responses"} {
		p := Provider{Adapter: "openai_compat", API: api, BaseURL: f.URL + "/v1", ContextTokens: 32768}
		a, err := NewOpenAICompat("local", p, "", "", blobs)
		if err != nil {
			t.Fatal(err)
		}
		req := openaiRequest(t, blobs, "")
		req.Route.Model, req.Route.MaxTokens = "mine", 4096 // a cloud model under a name of the user's
		f.show = map[string]any{"remote_model": "gpt-oss:120b", "remote_host": "https://ollama.com:443"}
		before := f.count("/api/show")
		if _, err := a.CountTokens(context.Background(), req); !errors.Is(err, ErrCloudModel) {
			t.Errorf("%s: CountTokens = %v", api, err)
		}
		_, err = a.Call(context.Background(), req)
		if !errors.Is(err, ErrCloudModel) || !errors.Is(err, ErrNotSent) {
			t.Errorf("%s: Call = %v", api, err)
		}
		if f.count("/api/show") != before+2 {
			t.Errorf("%s: asked %d times, not before each request", api, f.count("/api/show")-before)
		}
		if f.count("/v1/chat/completions")+f.count("/v1/responses")+f.count("/v1/responses/input_tokens") != 0 {
			t.Fatalf("%s: a request went to a cloud model", api)
		}
	}
	// A model that runs on the server itself; a key the server wants goes along.
	f.show = map[string]any{"details": map[string]any{"family": "gptoss"}}
	p := Provider{Adapter: "openai_compat", API: "chat", BaseURL: f.URL + "/v1", ContextTokens: 32768}
	a, _ := NewOpenAICompat("local", p, "sk-local", "", blobs)
	req := openaiRequest(t, blobs, "")
	req.Route.Model, req.Route.MaxTokens = "gpt-oss:20b", 4096
	if _, err := a.CountTokens(context.Background(), req); err != nil {
		t.Errorf("a local model: %v", err)
	}
	if h, _ := f.last("/api/show"); h.Get("Authorization") != "Bearer sk-local" {
		t.Errorf("asked without the key: %v", h)
	}
	// No /api/show (a vLLM that answers 405): nothing to ask. Any other answer: not sent.
	f.refuse = http.StatusMethodNotAllowed
	if _, err := a.CountTokens(context.Background(), req); err != nil {
		t.Errorf("405: %v", err)
	}
	f.refuse = http.StatusUnauthorized
	if _, err := a.CountTokens(context.Background(), req); err == nil {
		t.Error("401 from /api/show: counted anyway")
	}
	if _, err := a.Call(context.Background(), req); !errors.Is(err, ErrNotSent) {
		t.Errorf("401 from /api/show: %v", err)
	}
}

func TestCountInPieces(t *testing.T) {
	a, err := NewOpenAICompat("local", Provider{Adapter: "openai_compat", API: "chat", BaseURL: "http://127.0.0.1:1/v1",
		ContextTokens: 32768}, "", "", memBlobs{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	// Cut at line ends, a diff counts as it does whole.
	var diff strings.Builder
	for i := range 2000 {
		fmt.Fprintf(&diff, "+    total_%d = compute(items[%d], weight=%d.5)  # line %d\n", i, i, i%7, i)
	}
	whole, _ := a.codec.Count(diff.String())
	pieces, err := a.count(ctx, diff.String(), 1<<40)
	if err != nil || pieces != int64(whole) {
		t.Errorf("in pieces %d, whole %d: %v", pieces, whole, err)
	}
	// A run the tokenizer cannot split is counted a piece at a time, and only until it is too long.
	start := time.Now()
	n, err := a.count(ctx, strings.Repeat("QUFB", 1<<20), 32768)
	if err != nil || n <= 32768 || time.Since(start) > 5*time.Second {
		t.Errorf("a 4 MiB run: %d tokens in %v, %v", n, time.Since(start), err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := a.count(cancelled, diff.String(), 1<<40); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled: %v", err)
	}
}

func TestStrictSchema(t *testing.T) {
	b, err := os.ReadFile("../../config/schemas/audit_report.json")
	if err != nil {
		t.Fatal(err)
	}
	var audit map[string]any
	if err := json.Unmarshal(b, &audit); err != nil {
		t.Fatal(err)
	}
	if err := strictSchema(audit); err != nil {
		t.Errorf("the audit report schema: %v", err)
	}
	obj := func(props map[string]any, required ...any) map[string]any {
		return map[string]any{"type": "object", "additionalProperties": false, "properties": props, "required": required}
	}
	str := map[string]any{"type": "string"}
	// A property may be called anything, "not" included.
	if err := strictSchema(obj(map[string]any{"not": str}, "not")); err != nil {
		t.Errorf("a property named not: %v", err)
	}
	nullable := map[string]any{"type": []any{"object", "null"}, "additionalProperties": false, "properties": map[string]any{"b": str}, "required": []any{"b"}}
	if err := strictSchema(obj(map[string]any{"a": nullable}, "a")); err != nil {
		t.Errorf("a nullable object: %v", err)
	}
	for name, s := range map[string]map[string]any{
		"root array":         {"type": "array", "items": str},
		"optional property":  obj(map[string]any{"a": str, "b": str}, "a"),
		"open object":        {"type": "object", "properties": map[string]any{"a": str}, "required": []any{"a"}},
		"nested open object": obj(map[string]any{"a": map[string]any{"type": "object", "properties": map[string]any{}}}, "a"),
		"minLength":          obj(map[string]any{"a": map[string]any{"type": "string", "minLength": 1}}, "a"),
		"allOf in items":     obj(map[string]any{"a": map[string]any{"type": "array", "items": map[string]any{"allOf": []any{str}}}}, "a"),
		"open nullable":      obj(map[string]any{"a": map[string]any{"type": []any{"object", "null"}, "properties": map[string]any{}}}, "a"),
		"boolean items":      obj(map[string]any{"a": map[string]any{"type": "array", "items": true}}, "a"),
		"propertyNames":      obj(map[string]any{"a": map[string]any{"type": "object", "additionalProperties": false, "propertyNames": str}}, "a"),
		"required, no such":  obj(map[string]any{"a": str}, "a", "b"),
	} {
		if err := strictSchema(s); err == nil {
			t.Errorf("%s: passed", name)
		}
	}
}

func TestProviderEffort(t *testing.T) {
	gptOSS := Provider{Efforts: []string{"low", "medium", "high"}}
	for _, c := range []struct {
		p          Provider
		task, sent string
	}{
		{gptOSS, "high", "high"}, {gptOSS, "max", "high"}, {gptOSS, "medium", "medium"},
		{gptOSS, "minimal", "low"}, {gptOSS, "none", "low"}, {gptOSS, "", ""}, {Provider{}, "high", ""},
		{Provider{Efforts: []string{"max", "low"}}, "xhigh", "low"},
	} {
		if got := c.p.Effort(c.task); got != c.sent {
			t.Errorf("efforts %v, task %q: sent %q, want %q", c.p.Efforts, c.task, got, c.sent)
		}
	}
}
