// Package models routes model tasks to provider adapters (models.yaml, D11). M2a ships only
// the fake adapter; the Claude-native adapter arrives in M2b, the OpenAI-compatible one in M2c.
package models

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"go.yaml.in/yaml/v3"

	"github.com/jchen0426/agentos/internal/pricing"
)

type Provider struct {
	Adapter string `yaml:"adapter"` // fake; anthropic (M2b); openai_compat (M2c)
	BaseURL string `yaml:"base_url"`
	Key     string `yaml:"key"` // a secrets reference: keyring:<service>[/<account>]
	// Requests per minute the gateway sends this provider, across all runs; 0 is no limit.
	RequestsPerMinute int `yaml:"requests_per_minute"`
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
	Fallback     []string `yaml:"fallback"`      // "<provider>/<model>" each; off unless listed

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
		for _, m := range r.Candidates {
			if p, _, _ := strings.Cut(m, "/"); p == provider {
				return true
			}
		}
	}
	return false
}

// Route is where one model task goes.
type Route struct {
	Task          string
	Provider      string
	Adapter       string
	Model         string
	MaxTokens     int64
	Candidates    []string // "<provider>/<model>" of every model that may answer: the model, then its fallbacks
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
	provider, model, ok := strings.Cut(t.Model, "/")
	if !ok {
		return Route{}, fmt.Errorf("task %s: model %q is not <provider>/<model>", task, t.Model)
	}
	p, ok := c.Providers[provider]
	if !ok {
		return Route{}, fmt.Errorf("task %s: unknown provider %q", task, provider)
	}
	if t.MaxTokens <= 0 {
		return Route{}, fmt.Errorf("task %s: max_tokens is required", task)
	}
	candidates := []string{t.Model}
	for _, f := range t.Fallback {
		fp, _, ok := strings.Cut(f, "/")
		if _, known := c.Providers[fp]; !ok || !known {
			return Route{}, fmt.Errorf("task %s: fallback %q is not <provider>/<model> of a listed provider", task, f)
		}
		candidates = append(candidates, f)
	}
	return Route{Task: task, Provider: provider, Adapter: p.Adapter, Model: model, MaxTokens: t.MaxTokens,
		Candidates: candidates, Effort: t.Effort, Prompt: t.prompt, PromptVersion: t.promptVersion,
		Schema: t.schema, Blobs: t.Blobs, validator: t.validator}, nil
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

// RejectedError is a provider's error answer: 429 or 529 (overloaded) and other 4xx mean it
// did not serve the request; after a 500, 502 or 504 it may have (Unbilled).
type RejectedError struct {
	StatusCode int
	Message    string
}

func (e *RejectedError) Error() string {
	return fmt.Sprintf("provider answered %d: %s", e.StatusCode, e.Message)
}

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
