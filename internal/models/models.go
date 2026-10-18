// Package models routes model tasks to provider adapters (models.yaml, D11): the fake one,
// the Claude-native one (anthropic) and the OpenAI-compatible one (openai_compat), which
// speaks OpenAI's Responses API or, to Ollama and vLLM, Chat Completions.
package models

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptrace"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"go.yaml.in/yaml/v3"

	"github.com/jchen0426/agentos/internal/pricing"
)

type Provider struct {
	Adapter string `yaml:"adapter"` // fake, anthropic or openai_compat
	// openai_compat only: "responses" (OpenAI's Responses API) or "chat" (Chat Completions,
	// for Ollama and vLLM).
	API     string `yaml:"api"`
	BaseURL string `yaml:"base_url"`
	Key     string `yaml:"key"` // a secrets reference: keyring:<service>[/<account>]
	// Requests per minute the gateway sends this provider, across all runs; 0 is no limit.
	RequestsPerMinute int `yaml:"requests_per_minute"`
	// The reasoning efforts the provider's models take (none, minimal, low, medium, high,
	// xhigh, max). A task's effort is sent as the highest of these not above it, or the lowest
	// of them when all are; with none listed, no effort is sent. openai_compat only: Claude
	// takes every effort.
	Efforts []string `yaml:"efforts"`
	// api chat only: the context window the server runs with (Ollama's OLLAMA_CONTEXT_LENGTH).
	// Ollama drops what does not fit without a word, so a request whose input and max_tokens
	// may not fit is not sent.
	ContextTokens int64 `yaml:"context_tokens"`
}

// efforts is every reasoning effort, from least to most.
var efforts = []string{"none", "minimal", "low", "medium", "high", "xhigh", "max"}

// Effort is the effort to send a provider for a task's: the highest the provider takes that
// is not above it, or the lowest it takes when all are above (sending none would get the
// provider's default, often more); "" when the provider lists none or effort is "".
func (p Provider) Effort(effort string) string {
	want := slices.Index(efforts, effort)
	if want < 0 || len(p.Efforts) == 0 {
		return ""
	}
	taken := slices.SortedFunc(slices.Values(p.Efforts), func(a, b string) int {
		return slices.Index(efforts, a) - slices.Index(efforts, b)
	})
	sent := taken[0]
	for _, e := range taken {
		if slices.Index(efforts, e) <= want {
			sent = e
		}
	}
	return sent
}

// EffortSent is the effort a route's provider is sent for the task's: Claude and the fake take
// every effort, an OpenAI-compatible provider those it lists (Provider.Effort).
func (c *Config) EffortSent(r Route) string {
	if p := c.Providers[r.Provider]; p.Adapter == "openai_compat" {
		return p.Effort(r.Effort)
	}
	return r.Effort
}

// check reports a provider that cannot work as configured.
func (p Provider) check() error {
	for _, e := range p.Efforts {
		if !slices.Contains(efforts, e) {
			return fmt.Errorf("effort %q is not one of %v", e, efforts)
		}
	}
	switch p.Adapter {
	case "openai_compat":
		if p.BaseURL == "" {
			return errors.New("base_url is required")
		}
		switch p.API {
		case "responses":
		case "chat":
			if p.ContextTokens <= 0 {
				return errors.New("context_tokens is required with api chat: the context window the server runs with")
			}
		default:
			return fmt.Errorf("api %q is not responses or chat", p.API)
		}
	default:
		if p.API != "" || p.ContextTokens != 0 || len(p.Efforts) > 0 {
			return fmt.Errorf("api, context_tokens and efforts are for adapter openai_compat, not %q", p.Adapter)
		}
	}
	return nil
}

type Task struct {
	Model string `yaml:"model"` // "<provider>/<model>"
	// The most output one request may produce. Fixed here, with the model and its fallbacks,
	// so that the budget can bound what a request costs before it is sent.
	MaxTokens    int64    `yaml:"max_tokens"`
	Effort       string   `yaml:"effort"`
	Prompt       string   `yaml:"prompt"`        // system prompt file, relative to models.yaml
	OutputSchema string   `yaml:"output_schema"` // JSON Schema file of the output, relative to models.yaml
	Blobs        []string `yaml:"blobs"`         // output fields stored as blobs and passed on as <field>_ref
	// Models of the same provider that it runs, within the same request, when the model
	// declines (Claude's server-side fallback): "<provider>/<model>" each; off unless listed.
	Fallback []string `yaml:"fallback"`
	// Models of other providers asked in turn, each in a request of its own, when the answer
	// is a refusal: "<provider>/<model>" each; off unless listed. The policy must allow each
	// provider for the task.
	CrossProviderFallback []string `yaml:"cross_provider_fallback"`

	prompt        string
	promptVersion string
	schema        map[string]any
	validator     *jsonschema.Schema
}

type Config struct {
	Providers map[string]Provider `yaml:"providers"`
	Tasks     map[string]Task     `yaml:"tasks"`
}

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	var c Config
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("models %s: %w", path, err)
	}
	for name, p := range c.Providers {
		if err := p.check(); err != nil {
			return nil, fmt.Errorf("models %s: provider %s: %w", path, name, err)
		}
	}
	dir := filepath.Dir(path)
	for name, t := range c.Tasks {
		if err := t.load(dir); err != nil {
			return nil, fmt.Errorf("models %s: task %s: %w", path, name, err)
		}
		c.Tasks[name] = t
		if _, err := c.Route(name); err != nil {
			return nil, fmt.Errorf("models %s: %w", path, err)
		}
	}
	return &c, nil
}

// load reads the task's prompt and output schema. The prompt's version is its file name and
// the hash of its text, so that every call records exactly which prompt it was sent.
func (t *Task) load(dir string) error {
	if t.Effort != "" && !slices.Contains(efforts, t.Effort) {
		return fmt.Errorf("effort %q is not one of %v", t.Effort, efforts)
	}
	abs := func(p string) string {
		if filepath.IsAbs(p) {
			return p
		}
		return filepath.Join(dir, p)
	}
	t.promptVersion = "none"
	if t.Prompt != "" {
		b, err := os.ReadFile(abs(t.Prompt))
		if err != nil {
			return err
		}
		sum := sha256.Sum256(b)
		t.prompt, t.promptVersion = string(b), t.Prompt+"@sha256:"+hex.EncodeToString(sum[:6])
	}
	if t.OutputSchema != "" {
		b, err := os.ReadFile(abs(t.OutputSchema))
		if err != nil {
			return err
		}
		if err := json.Unmarshal(b, &t.schema); err != nil {
			return fmt.Errorf("output schema %s: %w", t.OutputSchema, err)
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(b))
		if err != nil {
			return err
		}
		c := jsonschema.NewCompiler()
		if err := c.AddResource("output.json", doc); err != nil {
			return err
		}
		if t.validator, err = c.Compile("output.json"); err != nil {
			return fmt.Errorf("output schema %s: %w", t.OutputSchema, err)
		}
	}
	for _, f := range t.Blobs {
		if t.schema == nil {
			return fmt.Errorf("blobs %v need an output_schema", t.Blobs)
		}
		if props, _ := t.schema["properties"].(map[string]any); props[f] == nil {
			return fmt.Errorf("blob field %q is not in the output schema", f)
		}
	}
	return nil
}

// Uses reports whether any task may reach provider, as its model or as a fallback.
func (c *Config) Uses(provider string) bool {
	for name := range c.Tasks {
		r, err := c.Route(name)
		if err != nil {
			continue
		}
		for _, hop := range append([]Route{r}, r.Next...) {
			for _, m := range hop.Candidates {
				if p, _, _ := strings.Cut(m, "/"); p == provider {
					return true
				}
			}
		}
	}
	return false
}

// Route is where one model task goes.
type Route struct {
	Task       string
	Provider   string
	Adapter    string
	Model      string
	MaxTokens  int64
	Candidates []string // "<provider>/<model>" of every model that may answer the request: the model, then its fallbacks
	// The routes asked in turn, each in a request of its own, when the answer is a refusal:
	// the task's cross-provider fallbacks.
	Next          []Route
	Effort        string
	Prompt        string         // the system prompt
	PromptVersion string         // its file and the hash of its text
	Schema        map[string]any // the output's JSON Schema, sent to providers that constrain output
	Blobs         []string
	validator     *jsonschema.Schema
}

// Check reports whether output matches the task's output schema. A task without a schema
// accepts any object.
func (r Route) Check(output map[string]any) error {
	if r.validator == nil {
		return nil
	}
	b, err := json.Marshal(output)
	if err != nil {
		return err
	}
	v, err := jsonschema.UnmarshalJSON(bytes.NewReader(b))
	if err != nil {
		return err
	}
	return r.validator.Validate(v)
}

// Name is the route's model as the price table and the ledger name it: "<provider>/<model>".
func (r Route) Name() string { return r.Provider + "/" + r.Model }

// ErrUnknownTask means the workflow named a model task that models.yaml does not list.
var ErrUnknownTask = fmt.Errorf("model task not in models.yaml")

func (c *Config) Route(task string) (Route, error) {
	t, ok := c.Tasks[task]
	if !ok {
		return Route{}, ErrUnknownTask
	}
	if t.MaxTokens <= 0 {
		return Route{}, fmt.Errorf("task %s: max_tokens is required", task)
	}
	route := func(name string) (Route, error) {
		provider, model, ok := strings.Cut(name, "/")
		p, known := c.Providers[provider]
		if !ok || model == "" || !known {
			return Route{}, fmt.Errorf("task %s: model %q is not <provider>/<model> of a listed provider", task, name)
		}
		if m := strings.ToLower(model); p.Adapter == "openai_compat" && (strings.Contains(m, ":cloud") || strings.HasSuffix(m, "-cloud")) {
			// Ollama runs these on its own servers: the task's data would leave this machine.
			// It serves both APIs; the adapter also asks it about each model under any name.
			return Route{}, fmt.Errorf("task %s: %s is an Ollama cloud model, not a local one", task, name)
		}
		return Route{Task: task, Provider: provider, Adapter: p.Adapter, Model: model, MaxTokens: t.MaxTokens,
			Candidates: []string{name}, Effort: t.Effort, Prompt: t.prompt, PromptVersion: t.promptVersion,
			Schema: t.schema, Blobs: t.Blobs, validator: t.validator}, nil
	}
	r, err := route(t.Model)
	if err != nil {
		return Route{}, err
	}
	for _, f := range t.Fallback {
		// Only Claude runs a fallback within the request; the fake adapter plays it in the tests.
		if fp, _, _ := strings.Cut(f, "/"); fp != r.Provider || (r.Adapter != "anthropic" && r.Adapter != "fake") {
			return Route{}, fmt.Errorf("task %s: fallback %q: a fallback runs within the request, on the same provider "+
				"(Claude's server-side fallback); list other providers' models in cross_provider_fallback", task, f)
		}
		if _, err := route(f); err != nil {
			return Route{}, err
		}
		r.Candidates = append(r.Candidates, f)
	}
	seen := map[string]bool{t.Model: true}
	for _, f := range t.CrossProviderFallback {
		next, err := route(f)
		switch {
		case err != nil:
			return Route{}, err
		case next.Provider == r.Provider || seen[f]:
			return Route{}, fmt.Errorf("task %s: cross_provider_fallback %q: another provider's model, listed once", task, f)
		}
		seen[f] = true
		r.Next = append(r.Next, next)
	}
	return r, nil
}

// Request is one network request to a provider, as the gateway hands it to an adapter.
type Request struct {
	WorkflowID string
	RequestID  string // unique per network request; the budget reserves for it by this ID
	Route      Route
	Args       map[string]any // the workflow's arguments; M2b turns them into messages
	Fault      FakeFault      // test only: what a fault script asks of the fake provider
}

// Response is an adapter's normalized answer.
type Response struct {
	Status     string         `json:"status"`           // "ok" or "refused"; the gateway adds "invalid_output"
	Model      string         `json:"model"`            // "<provider>/<model>" that gave the answer
	Output     map[string]any `json:"output,omitempty"` // the model's structured output, as it came
	Value      map[string]any `json:"value"`            // what the workflow gets: the gateway fills it in
	StopReason string         `json:"stop_reason"`
	// The reasoning effort the request was sent with; empty when none was: the provider takes
	// no effort, or the task names none (design doc, section 9).
	Effort string `json:"effort,omitempty"`
	// Every attempt the provider billed, in order: a server-side fallback adds one, priced
	// at the fallback model's rates.
	Billed []Billed `json:"billed"`
}

// Billed is what one attempt within a request was billed for.
type Billed struct {
	Model string `json:"model"` // "<provider>/<model>"
	pricing.Usage
}

// Adapter talks to one kind of provider API. Each Call is exactly one network request:
// provider SDK retries stay off, because the gateway reserves budget for every request that
// may be billed (design doc, section 9).
type Adapter interface {
	// CountTokens returns the input tokens req would be billed for, counted the provider's way.
	CountTokens(ctx context.Context, req Request) (int64, error)
	// Call sends req. An error wrapping ErrNotSent never reached the provider; a
	// *RejectedError is the provider's own refusal to serve it, unbilled; with any other
	// error nobody knows whether the provider billed it.
	Call(ctx context.Context, req Request) (Response, error)
}

// RouteChecker is an adapter that can tell at start which routes it cannot serve.
type RouteChecker interface {
	CheckRoute(Route) error
}

// ErrNotSent marks a request that failed before it left agentd: nothing was billed.
var ErrNotSent = errors.New("request not sent")

// traceWrite marks, on a request's context, when the whole request has been written to the
// connection. Until then nothing reached the provider, whatever error comes back: the name
// did not resolve, no connection or proxy tunnel was made, TLS did not trust the server. Once
// it has, the provider may have run and billed it, whatever error comes after. The adapters'
// SDKs make one request per call (their retries are off).
func traceWrite(ctx context.Context) (context.Context, func() bool) {
	var written atomic.Bool
	return httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		WroteRequest: func(i httptrace.WroteRequestInfo) {
			if i.Err == nil {
				written.Store(true)
			}
		},
	}), written.Load
}

// Declined is a provider declining a request before it was sent, while its tokens were being
// counted: a refusal like any, billed nothing.
type Declined struct{ Category, Explanation string }

func (d *Declined) Error() string { return fmt.Sprintf("declined (%s): %s", d.Category, d.Explanation) }

// NonStreamingLimit is the longest a request may be expected to take: the adapters do not
// stream, and the Anthropic SDK refuses a non-streaming request expected to take longer.
const NonStreamingLimit = 10 * time.Minute

// ExpectedDuration is how long a request for maxTokens of output may take without streaming:
// the Anthropic SDK's estimate, an hour for 128,000 tokens, which is slow for any provider.
// Past a million tokens, which no model takes, it stays at a million's.
func ExpectedDuration(maxTokens int64) time.Duration {
	return time.Duration(min(maxTokens, 1_000_000)) * time.Hour / 128000
}

// RejectedError is a provider's error answer: 429 or 529 (overloaded) and other 4xx mean it
// did not serve the request; after a 500, 502 or 504 it may have (Unbilled).
type RejectedError struct {
	StatusCode int
	Message    string
	Err        error // what the rejection means, when the adapter knows: ErrTooLong
}

func (e *RejectedError) Error() string {
	return fmt.Sprintf("provider answered %d: %s", e.StatusCode, e.Message)
}

func (e *RejectedError) Unwrap() error { return e.Err }

// Retryable reports whether sending the request again may succeed: a timeout, a conflict,
// rate limiting or a server error (408, 409, 429, 5xx), as the SDKs themselves retry.
func (e *RejectedError) Retryable() bool {
	return e.StatusCode == 408 || e.StatusCode == 409 || e.StatusCode == 429 || e.StatusCode >= 500
}

// Unbilled reports whether the provider certainly did not bill the request: it refused it
// (4xx) or was overloaded (529). A 500, 502 or 504 can come from a proxy after the model had
// already run, so the budget treats those as billed in full.
func (e *RejectedError) Unbilled() bool { return e.StatusCode < 500 || e.StatusCode == 529 }

// BlobStore gives an adapter the blobs a call's arguments name, but only those the calling
// run produced.
type BlobStore interface {
	GetRunBlob(ctx context.Context, workflowID, ref string) ([]byte, error)
}

// ErrBadArguments means the call's arguments cannot make a request: retrying will not help.
var ErrBadArguments = errors.New("bad arguments")
