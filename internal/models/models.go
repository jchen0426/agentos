// Package models routes model tasks to provider adapters (models.yaml, D11). M2a ships only
// the fake adapter; the Claude-native adapter arrives in M2b, the OpenAI-compatible one in M2c.
package models

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"go.yaml.in/yaml/v3"

	"github.com/jchen0426/agentos/internal/pricing"
)

type Provider struct {
	Adapter string `yaml:"adapter"` // fake (M1); anthropic, openai_compat (M2)
	BaseURL string `yaml:"base_url"`
	Key     string `yaml:"key"`
}

type Task struct {
	Model string `yaml:"model"` // "<provider>/<model>"
	// The most output one request may produce. Fixed here, with the model and its fallbacks,
	// so that the budget can bound what a request costs before it is sent.
	MaxTokens    int64    `yaml:"max_tokens"`
	Effort       string   `yaml:"effort"`
	Prompt       string   `yaml:"prompt"`
	OutputSchema string   `yaml:"output_schema"`
	Fallback     []string `yaml:"fallback"` // "<provider>/<model>" each; off unless listed
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
	for name := range c.Tasks {
		if _, err := c.Route(name); err != nil {
			return nil, fmt.Errorf("models %s: %w", path, err)
		}
	}
	return &c, nil
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
	PromptVersion string
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
	version := t.Prompt
	if version == "" {
		version = "none"
	}
	return Route{Task: task, Provider: provider, Adapter: p.Adapter, Model: model, MaxTokens: t.MaxTokens,
		Candidates: candidates, Effort: t.Effort, PromptVersion: version}, nil
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
	Status     string         `json:"status"` // "ok" or "refused"
	Model      string         `json:"model"`  // "<provider>/<model>" that answered and billed it
	Value      map[string]any `json:"value"`  // the structured output, with blob references for large parts
	StopReason string         `json:"stop_reason"`
	Usage      pricing.Usage  `json:"usage"`
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

// ErrNotSent marks a request that failed before it left agentd: nothing was billed.
var ErrNotSent = errors.New("request not sent")

// RejectedError is a provider's error answer, such as 429 or 529: it did not serve the
// request and does not bill it.
type RejectedError struct {
	StatusCode int
	Message    string
}

func (e *RejectedError) Error() string {
	return fmt.Sprintf("provider answered %d: %s", e.StatusCode, e.Message)
}

// Retryable reports whether sending the request again may succeed.
func (e *RejectedError) Retryable() bool { return e.StatusCode == 429 || e.StatusCode >= 500 }

// BlobStore keeps large values out of the activity payloads.
type BlobStore interface {
	PutBlob(ctx context.Context, b []byte) (string, error)
}
