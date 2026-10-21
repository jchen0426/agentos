// Package modelsim stands in for the model providers in the black-box tests (T13): it speaks
// Anthropic's Messages API, OpenAI's Responses API and Chat Completions (Ollama, vLLM), and
// answers every request the same way, so that what agentd makes of an answer or a fault can
// be compared across the two adapters and the three APIs.
//
// Faults are scripted per API in a JSON file read on every request, {"anthropic": ["429"],
// "responses": [...], "chat": [...]}, and used once each: a request takes the first fault of
// its API off the list. A fault is "429", "500", "drop" (the connection closes halfway
// through the answer, as if it timed out: the provider may have billed it), "refusal" or
// "invalid" (an answer that breaks the output schema).
//
// Answers can be scripted too, for the evaluation's tests, in a second JSON file read on every
// answer: a list of rules, [{"model": "gpt-oss:20b", "contains": "case-7", "answers": [...]}],
// each for the requests whose model it names (any model, when it names none) and whose body
// holds its text. A request takes the first answer of the first rule it matches that has one
// left, and uses it up. An answer is {"report": {...}} (the model's output), {"fault": "refusal"}
// (any of the faults above), or both (an "invalid" answer is its report without the summary),
// and may set the usage it reports: {"usage": {"input": 200, "cache_read": 800, "output": 500}},
// input being what was not read from the cache. A fault in the faults file comes first; a
// request no rule answers gets the fixture report.
//
// Like Ollama it answers /api/show, about any model: a model whose name starts with "cloud"
// is one of Ollama's cloud models, which run on Ollama's servers whatever a user names them.
package modelsim

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/jchen0426/agentos/internal/models"
)

// Usage is what every answer reports: the same on every API, so that the same prices give
// the same charge.
const (
	InputTokens  = 1000
	OutputTokens = 500
)

// Server serves the three APIs. LogPath gets one JSON line per request; FaultsPath holds
// the scripted faults, AnswersPath the scripted answers.
type Server struct {
	LogPath     string
	FaultsPath  string
	AnswersPath string

	mu sync.Mutex
}

// Rule is one rule of the answers file: its answers, for the requests it matches.
type Rule struct {
	Model    string     `json:"model,omitempty"`
	Contains string     `json:"contains,omitempty"`
	Answers  []Scripted `json:"answers"`
}

// Scripted is one scripted answer.
type Scripted struct {
	Report map[string]any `json:"report,omitempty"`
	Fault  string         `json:"fault,omitempty"`
	Usage  *Usage         `json:"usage,omitempty"`
}

// Usage is the usage an answer reports. Input is the input not read from the cache.
type Usage struct {
	Input     int64 `json:"input"`
	CacheRead int64 `json:"cache_read"`
	Output    int64 `json:"output"`
}

// request is what the log records of a request: what it asked for, whichever API's names.
type request struct {
	Model string `json:"model"`
	// max_tokens (Messages API, Chat Completions) or max_output_tokens (Responses API)
	MaxTokens       int64 `json:"max_tokens"`
	MaxOutputTokens int64 `json:"max_output_tokens"`
	// output_config.effort, reasoning.effort or reasoning_effort
	OutputConfig struct {
		Effort string `json:"effort"`
	} `json:"output_config"`
	Reasoning struct {
		Effort string `json:"effort"`
	} `json:"reasoning"`
	ReasoningEffort  string `json:"reasoning_effort"`
	ServiceTier      string `json:"service_tier"`
	Store            *bool  `json:"store"`
	SafetyIdentifier string `json:"safety_identifier"`
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var body request
	b, _ := io.ReadAll(r.Body)
	_ = json.Unmarshal(b, &body)
	if r.URL.Path == "/api/show" && r.Method == http.MethodPost {
		s.log("ollama", r.URL.Path, body, "")
		show := map[string]any{"details": map[string]any{"format": "gguf"}, "model_info": map[string]any{}}
		if strings.HasPrefix(body.Model, "cloud") {
			show["remote_model"], show["remote_host"] = "gpt-oss:120b", "https://ollama.com:443"
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(show)
		return
	}
	api := map[string]string{
		"/v1/messages": "anthropic", "/v1/messages/count_tokens": "anthropic",
		"/v1/responses": "responses", "/v1/responses/input_tokens": "responses",
		"/v1/chat/completions": "chat",
	}[r.URL.Path]
	if api == "" || r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	// Counting is free and never fails here: the faults are the answers'.
	switch r.URL.Path {
	case "/v1/messages/count_tokens":
		s.log(api, r.URL.Path, body, "")
		_ = json.NewEncoder(w).Encode(map[string]any{"input_tokens": InputTokens})
		return
	case "/v1/responses/input_tokens":
		s.log(api, r.URL.Path, body, "")
		_ = json.NewEncoder(w).Encode(map[string]any{"object": "response.input_tokens", "input_tokens": InputTokens})
		return
	}
	fault, err := s.takeFault(api)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var scripted Scripted
	if fault == "" {
		if scripted, err = s.takeAnswer(body.Model, b); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fault = scripted.Fault
	}
	s.log(api, r.URL.Path, body, fault)
	switch fault {
	case "429", "500":
		status := map[string]int{"429": http.StatusTooManyRequests, "500": http.StatusInternalServerError}[fault]
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(errorBody(api, status))
		return
	case "drop":
		// Headers and a first byte, then nothing: the answer was on its way.
		w.Header().Set("Content-Length", "1000")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		if hj, ok := w.(http.Hijacker); ok {
			if conn, _, err := hj.Hijack(); err == nil {
				_ = conn.Close()
			}
		}
		return
	}
	report := scripted.Report
	if report == nil {
		report = models.FixtureReport()
	}
	if fault == "invalid" {
		delete(report, "summary")
	}
	usage := Usage{Input: InputTokens, Output: OutputTokens}
	if scripted.Usage != nil {
		usage = *scripted.Usage
	}
	text, _ := json.Marshal(report)
	_ = json.NewEncoder(w).Encode(answer(api, body.Model, string(text), fault == "refusal", usage))
}

func errorBody(api string, status int) map[string]any {
	if api == "anthropic" {
		kind := map[int]string{429: "rate_limit_error", 500: "api_error"}[status]
		return map[string]any{"type": "error", "error": map[string]any{"type": kind, "message": "scripted fault"}}
	}
	return map[string]any{"error": map[string]any{"message": "scripted fault", "type": "server_error", "code": nil}}
}

// answer is a successful answer in the API's own shape, or a refusal, with the usage given.
// OpenAI's APIs count cached input as input too.
func answer(api, model, text string, refusal bool, u Usage) map[string]any {
	const declined = "I can't help with that."
	switch api {
	case "anthropic":
		m := map[string]any{"id": "msg_sim", "type": "message", "role": "assistant", "model": model,
			"content": []any{map[string]any{"type": "text", "text": text}}, "stop_reason": "end_turn",
			"usage": map[string]any{"input_tokens": u.Input, "output_tokens": u.Output,
				"cache_creation_input_tokens": 0, "cache_read_input_tokens": u.CacheRead}}
		if refusal {
			m["content"], m["stop_reason"] = []any{}, "refusal"
			m["stop_details"] = map[string]any{"type": "refusal", "category": "cyber", "explanation": declined}
		}
		return m
	case "responses":
		content := map[string]any{"type": "output_text", "text": text, "annotations": []any{}}
		if refusal {
			content = map[string]any{"type": "refusal", "refusal": declined}
		}
		return map[string]any{"id": "resp_sim", "object": "response", "model": model, "status": "completed", "service_tier": "default",
			"output": []any{map[string]any{"type": "message", "id": "msg_sim", "role": "assistant", "status": "completed",
				"content": []any{content}}},
			"usage": map[string]any{"input_tokens": u.Input + u.CacheRead, "output_tokens": u.Output, "total_tokens": u.Input + u.CacheRead + u.Output,
				"input_tokens_details": map[string]any{"cached_tokens": u.CacheRead, "cache_write_tokens": 0}, "output_tokens_details": map[string]any{"reasoning_tokens": 0}}}
	default:
		message := map[string]any{"role": "assistant", "content": text}
		if refusal {
			message = map[string]any{"role": "assistant", "content": "", "refusal": declined}
		}
		return map[string]any{"id": "chatcmpl-sim", "object": "chat.completion", "model": model,
			"choices": []any{map[string]any{"index": 0, "finish_reason": "stop", "message": message}},
			"usage": map[string]any{"prompt_tokens": u.Input + u.CacheRead, "completion_tokens": u.Output, "total_tokens": u.Input + u.CacheRead + u.Output,
				"prompt_tokens_details": map[string]any{"cached_tokens": u.CacheRead}}}
	}
}

// takeFault takes the first fault scripted for api off the file, if any.
func (s *Server) takeFault(api string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.FaultsPath == "" {
		return "", nil
	}
	b, err := os.ReadFile(s.FaultsPath)
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	script := map[string][]string{}
	if err := json.Unmarshal(b, &script); err != nil {
		return "", fmt.Errorf("faults %s: %w", s.FaultsPath, err)
	}
	if len(script[api]) == 0 {
		return "", nil
	}
	fault := script[api][0]
	script[api] = script[api][1:]
	out, _ := json.Marshal(script)
	tmp := s.FaultsPath + ".tmp"
	if err := os.WriteFile(tmp, out, 0o600); err != nil {
		return "", err
	}
	return fault, os.Rename(tmp, s.FaultsPath)
}

// takeAnswer takes the answer for a request to model with this body off the answers file: the
// first answer of the first rule that matches and has one left. None is the zero Scripted.
func (s *Server) takeAnswer(model string, body []byte) (Scripted, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.AnswersPath == "" {
		return Scripted{}, nil
	}
	b, err := os.ReadFile(s.AnswersPath)
	if os.IsNotExist(err) {
		return Scripted{}, nil
	}
	if err != nil {
		return Scripted{}, err
	}
	var rules []Rule
	if err := json.Unmarshal(b, &rules); err != nil {
		return Scripted{}, fmt.Errorf("answers %s: %w", s.AnswersPath, err)
	}
	for i, r := range rules {
		if len(r.Answers) == 0 || (r.Model != "" && r.Model != model) || !strings.Contains(string(body), r.Contains) {
			continue
		}
		taken := r.Answers[0]
		rules[i].Answers = r.Answers[1:]
		out, _ := json.Marshal(rules)
		tmp := s.AnswersPath + ".tmp"
		if err := os.WriteFile(tmp, out, 0o600); err != nil {
			return Scripted{}, err
		}
		return taken, os.Rename(tmp, s.AnswersPath)
	}
	return Scripted{}, nil
}

func (s *Server) log(api, path string, r request, fault string) {
	if s.LogPath == "" {
		return
	}
	effort := r.OutputConfig.Effort + r.Reasoning.Effort + r.ReasoningEffort // one of them, by API
	entry := map[string]any{"api": api, "path": path, "model": r.Model, "fault": fault, "effort": effort,
		"max_tokens": r.MaxTokens + r.MaxOutputTokens, "service_tier": r.ServiceTier, "store": r.Store,
		"safety_identifier": r.SafetyIdentifier}
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := os.OpenFile(s.LogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	defer f.Close()
	line, _ := json.Marshal(entry)
	_, _ = f.Write(append(line, '\n'))
}
