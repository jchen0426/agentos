// Package models routes model tasks to provider adapters (models.yaml, D11). M1 ships only
// the fake adapter; the Claude-native and OpenAI-compatible adapters arrive in M2.
package models

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"

	"go.yaml.in/yaml/v3"
)

type Provider struct {
	Adapter string `yaml:"adapter"` // fake (M1); anthropic, openai_compat (M2)
	BaseURL string `yaml:"base_url"`
	Key     string `yaml:"key"`
}

type Task struct {
	Model        string   `yaml:"model"` // "<provider>/<model>"
	Effort       string   `yaml:"effort"`
	Prompt       string   `yaml:"prompt"`
	OutputSchema string   `yaml:"output_schema"`
	Fallback     []string `yaml:"fallback"`
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
	Effort        string
	PromptVersion string
}

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
	version := t.Prompt
	if version == "" {
		version = "none"
	}
	return Route{Task: task, Provider: provider, Adapter: p.Adapter, Model: model, Effort: t.Effort, PromptVersion: version}, nil
}

// Request is one model call as the gateway hands it to an adapter.
type Request struct {
	WorkflowID string
	Route      Route
	Args       map[string]any // the workflow's arguments; M2 turns them into messages
}

// Response is an adapter's normalized answer.
type Response struct {
	Status     string           `json:"status"` // "ok" or "refused"
	Value      map[string]any   `json:"value"`  // the structured output, with blob references for large parts
	StopReason string           `json:"stop_reason"`
	Usage      map[string]int64 `json:"usage"`
}

type Adapter interface {
	Call(ctx context.Context, req Request) (Response, error)
}

// BlobStore keeps large values out of the activity payloads.
type BlobStore interface {
	PutBlob(ctx context.Context, b []byte) (string, error)
}
