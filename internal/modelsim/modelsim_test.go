package modelsim

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

// post sends a request body naming model and holding text to path, and decodes the answer.
func post(t *testing.T, url, path, model, text string) map[string]any {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"model": model, "messages": []any{map[string]any{"role": "user", "content": text}}})
	resp, err := http.Post(url+path, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s: %d %s", path, resp.StatusCode, b)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("%s: %v: %s", path, err, b)
	}
	return out
}

// text is the output text of a Chat Completions answer.
func text(t *testing.T, answer map[string]any) map[string]any {
	t.Helper()
	msg := answer["choices"].([]any)[0].(map[string]any)["message"].(map[string]any)
	var report map[string]any
	if err := json.Unmarshal([]byte(msg["content"].(string)), &report); err != nil {
		t.Fatal(err)
	}
	return report
}

func TestScriptedAnswersGoToTheRequestsTheirRuleMatches(t *testing.T) {
	dir := t.TempDir()
	answers := filepath.Join(dir, "answers.json")
	script := []Rule{
		{Model: "m1", Contains: "case-a", Answers: []Scripted{
			{Report: map[string]any{"summary": "first", "findings": []any{}, "patch": nil}},
			{Report: map[string]any{"summary": "second", "findings": []any{}, "patch": nil}, Usage: &Usage{Input: 200, CacheRead: 800, Output: 50}},
		}},
		{Contains: "case-a", Answers: []Scripted{{Fault: "refusal"}}},
	}
	b, _ := json.Marshal(script)
	if err := os.WriteFile(answers, b, 0o600); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(&Server{AnswersPath: answers})
	defer srv.Close()

	// In turn, each once, for the model the rule names; then the next rule that matches.
	if got := text(t, post(t, srv.URL, "/v1/chat/completions", "m1", "the diff of case-a")); got["summary"] != "first" {
		t.Fatalf("first answer: %v", got)
	}
	second := post(t, srv.URL, "/v1/chat/completions", "m1", "the diff of case-a")
	if got := text(t, second); got["summary"] != "second" {
		t.Fatalf("second answer: %v", got)
	}
	// OpenAI counts cached input as input.
	usage := second["usage"].(map[string]any)
	if usage["prompt_tokens"] != 1000.0 || usage["completion_tokens"] != 50.0 ||
		usage["prompt_tokens_details"].(map[string]any)["cached_tokens"] != 800.0 {
		t.Fatalf("usage: %v", usage)
	}
	refusal := post(t, srv.URL, "/v1/chat/completions", "m1", "the diff of case-a")
	if msg := refusal["choices"].([]any)[0].(map[string]any)["message"].(map[string]any); msg["refusal"] == nil {
		t.Fatalf("third answer should be the refusal: %v", refusal)
	}
	// Nothing left that matches: the fixture report, with the default usage.
	last := post(t, srv.URL, "/v1/chat/completions", "m1", "the diff of case-a")
	if got := text(t, last); got["summary"] == nil || len(got["findings"].([]any)) != 1 {
		t.Fatalf("fixture report expected: %v", got)
	}
	if usage := last["usage"].(map[string]any); usage["prompt_tokens"] != float64(InputTokens) {
		t.Fatalf("default usage: %v", usage)
	}

	// The other model gets only the rule that names no model; another body, no rule.
	b, _ = json.Marshal(script)
	if err := os.WriteFile(answers, b, 0o600); err != nil {
		t.Fatal(err)
	}
	other := post(t, srv.URL, "/v1/messages", "m2", "the diff of case-a")
	if other["stop_reason"] != "refusal" {
		t.Fatalf("m2 should get the refusal: %v", other)
	}
	plain := post(t, srv.URL, "/v1/messages", "m1", "the diff of case-b")
	if plain["stop_reason"] != "end_turn" {
		t.Fatalf("case-b matches no rule: %v", plain)
	}
	var left []Rule
	b, _ = os.ReadFile(answers)
	if err := json.Unmarshal(b, &left); err != nil {
		t.Fatal(err)
	}
	if len(left[0].Answers) != 2 || len(left[1].Answers) != 0 {
		t.Fatalf("answers left: %+v", left)
	}
}

func TestScriptedUsageInEachAPI(t *testing.T) {
	u := Usage{Input: 300, CacheRead: 700, Output: 40}
	anthropic := answer("anthropic", "m", "{}", false, u)["usage"].(map[string]any)
	if anthropic["input_tokens"] != int64(300) || anthropic["cache_read_input_tokens"] != int64(700) || anthropic["output_tokens"] != int64(40) {
		t.Fatalf("anthropic: %v", anthropic)
	}
	responses := answer("responses", "m", "{}", false, u)["usage"].(map[string]any)
	if responses["input_tokens"] != int64(1000) || responses["input_tokens_details"].(map[string]any)["cached_tokens"] != int64(700) {
		t.Fatalf("responses: %v", responses)
	}
}

func TestAnInvalidScriptedAnswerLosesItsSummary(t *testing.T) {
	dir := t.TempDir()
	answers := filepath.Join(dir, "answers.json")
	b, _ := json.Marshal([]Rule{{Answers: []Scripted{{Report: map[string]any{"summary": "s", "findings": []any{}, "patch": nil}, Fault: "invalid"}}}})
	if err := os.WriteFile(answers, b, 0o600); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(&Server{AnswersPath: answers})
	defer srv.Close()
	got := text(t, post(t, srv.URL, "/v1/chat/completions", "m", "anything"))
	if _, ok := got["summary"]; ok || got["findings"] == nil {
		t.Fatalf("invalid answer: %v", got)
	}
}
