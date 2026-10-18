package models

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"

	"github.com/jchen0426/agentos/internal/pricing"
)

// Anthropic is the Claude-native adapter (anthropic-sdk-go). Each Call is one request: the
// SDK's own retries are off, and the client reads nothing from the environment (no
// ANTHROPIC_API_KEY, profile or base URL), only the key and URL it is given.
type Anthropic struct {
	provider string // the provider's name in models.yaml, which prefixes its model names
	client   anthropic.Client
	blobs    BlobStore
}

func NewAnthropic(provider, key, baseURL string, blobs BlobStore) *Anthropic {
	opts := []option.RequestOption{option.WithoutEnvironmentDefaults(), option.WithAPIKey(key), option.WithMaxRetries(0)}
	if baseURL != "" {
		opts = append(opts, option.WithBaseURL(baseURL))
	}
	return &Anthropic{provider: provider, client: anthropic.NewClient(opts...), blobs: blobs}
}

// CountTokens counts the request with each model that may answer it and returns the most:
// the models' tokenizers can differ, and the budget's bound must hold for any of them.
func (a *Anthropic) CountTokens(ctx context.Context, req Request) (int64, error) {
	p, err := a.params(ctx, req)
	if err != nil {
		return 0, err
	}
	var most int64
	for _, model := range a.models(req.Route) {
		count := anthropic.BetaMessageCountTokensParams{
			Model: anthropic.Model(model), Messages: p.Messages, OutputConfig: p.OutputConfig,
		}
		if len(p.System) > 0 {
			count.System = anthropic.BetaMessageCountTokensParamsSystemUnion{OfBetaTextBlockArray: p.System}
		}
		r, err := a.client.Beta.Messages.CountTokens(ctx, count)
		if err != nil {
			return 0, fmt.Errorf("count tokens for %s: %w", model, err)
		}
		most = max(most, r.InputTokens)
	}
	return most, nil
}

func (a *Anthropic) Call(ctx context.Context, req Request) (Response, error) {
	p, err := a.params(ctx, req)
	if err != nil {
		return Response{}, fmt.Errorf("%w: %v", ErrNotSent, err)
	}
	msg, err := a.client.Beta.Messages.New(ctx, p)
	if err != nil {
		return Response{}, classify(err)
	}
	resp := Response{Model: a.name(string(msg.Model)), StopReason: string(msg.StopReason), Effort: req.Route.Effort,
		Billed: a.billed(msg)}
	if msg.StopReason == anthropic.BetaStopReasonRefusal {
		// A safety classifier or the model declined; with fallbacks configured, the fallback
		// model declined too. Branch on the stop reason: the details are informational.
		resp.Status = "refused"
		resp.Value = map[string]any{
			"category": string(msg.StopDetails.Category), "explanation": msg.StopDetails.Explanation,
		}
		return resp, nil
	}
	resp.Status = "ok"
	var text strings.Builder
	for _, block := range msg.Content {
		switch block.Type {
		case "fallback":
			// Before it, a model wrote some output and then declined; the answer follows it.
			text.Reset()
		case "text":
			text.WriteString(block.Text)
		}
	}
	// Output stays nil when the text is not one JSON object, for example when max_tokens cut
	// it off: the gateway treats that as an answer that breaks the schema.
	dec := json.NewDecoder(strings.NewReader(text.String()))
	dec.UseNumber()
	var out map[string]any
	if err := dec.Decode(&out); err == nil {
		resp.Output = out
	}
	return resp, nil
}

// params builds the request: the task's prompt as the cached system prompt, the arguments as
// the user message, the effort and the output schema, and the server-side fallbacks.
func (a *Anthropic) params(ctx context.Context, req Request) (anthropic.BetaMessageNewParams, error) {
	user, err := userMessage(ctx, a.blobs, req.WorkflowID, req.Args)
	if err != nil {
		return anthropic.BetaMessageNewParams{}, err
	}
	p := anthropic.BetaMessageNewParams{
		Model:     anthropic.Model(req.Route.Model),
		MaxTokens: req.Route.MaxTokens,
		Messages:  []anthropic.BetaMessageParam{anthropic.NewBetaUserMessage(anthropic.NewBetaTextBlock(user))},
	}
	if req.Route.Prompt != "" {
		p.System = []anthropic.BetaTextBlockParam{{Text: req.Route.Prompt, CacheControl: anthropic.NewBetaCacheControlEphemeralParam()}}
	}
	if req.Route.Effort != "" {
		p.OutputConfig.Effort = anthropic.BetaOutputConfigEffort(req.Route.Effort)
	}
	if req.Route.Schema != nil {
		p.OutputConfig.Format = anthropic.BetaJSONOutputFormatParam{Schema: req.Route.Schema}
	}
	// Fallbacks are listed by name, never "default": the budget's bound needs every model that
	// may answer, priced in advance.
	var fallbacks []anthropic.BetaFallbackParam
	for _, m := range a.models(req.Route)[1:] {
		fallbacks = append(fallbacks, anthropic.BetaFallbackParam{Model: anthropic.Model(m)})
	}
	if len(fallbacks) > 0 {
		p.Fallbacks = anthropic.BetaFallbacksParamUnion{OfBetaFallbackArray: fallbacks}
		p.Betas = append(p.Betas, anthropic.AnthropicBetaServerSideFallback2026_06_01)
	}
	return p, nil
}

// CheckRoute reports, at start, a route this adapter cannot serve. The SDK refuses a
// non-streaming request whose max_tokens could take over 10 minutes before sending it; the
// budget would count that as an unknown outcome and charge it in full on every retry.
func (a *Anthropic) CheckRoute(r Route) error {
	for _, m := range a.models(r) {
		if _, err := anthropic.CalculateNonStreamingTimeout(int(r.MaxTokens), anthropic.Model(m), nil); err != nil {
			return fmt.Errorf("max_tokens %d for %s needs streaming, which this adapter does not do yet: %w", r.MaxTokens, m, err)
		}
	}
	return nil
}

// models is the route's model and the fallbacks this provider serves itself, bare names.
func (a *Anthropic) models(r Route) []string {
	names := []string{r.Model}
	for _, c := range r.Candidates[1:] {
		if provider, model, _ := strings.Cut(c, "/"); provider == a.provider {
			names = append(names, model)
		}
	}
	return names
}

func (a *Anthropic) name(model string) string { return a.provider + "/" + model }

// billed lists what each attempt within the request was billed for. With a server-side
// fallback, usage.iterations holds one entry per model that ran, and the top-level usage
// covers only the last.
func (a *Anthropic) billed(msg *anthropic.BetaMessage) []Billed {
	if len(msg.Usage.Iterations) == 0 {
		u := msg.Usage
		return []Billed{{Model: a.name(string(msg.Model)),
			Usage: usage(u.InputTokens, u.CacheCreationInputTokens, u.CacheCreation, u.CacheReadInputTokens, u.OutputTokens)}}
	}
	out := make([]Billed, 0, len(msg.Usage.Iterations))
	for _, it := range msg.Usage.Iterations {
		model := string(it.Model)
		if model == "" {
			model = string(msg.Model)
		}
		out = append(out, Billed{Model: a.name(model),
			Usage: usage(it.InputTokens, it.CacheCreationInputTokens, it.CacheCreation, it.CacheReadInputTokens, it.OutputTokens)})
	}
	return out
}

// usage maps the API's counts. Cache writes the API does not split by duration are counted
// as 1-hour writes, the dearer kind.
func usage(input, cacheWrites int64, split anthropic.BetaCacheCreation, cacheReads, output int64) pricing.Usage {
	u := pricing.Usage{
		InputTokens: input, CacheWrite5mTokens: split.Ephemeral5mInputTokens, CacheWrite1hTokens: split.Ephemeral1hInputTokens,
		CacheReadTokens: cacheReads, OutputTokens: output,
	}
	if rest := cacheWrites - u.CacheWrite5mTokens - u.CacheWrite1hTokens; rest > 0 {
		u.CacheWrite1hTokens += rest
	}
	return u
}

// classify sorts an SDK error into the three kinds the budget tells apart: an HTTP error
// answer is the provider declining to serve the request, unbilled; a failure to connect
// never left agentd; after anything else (a timeout, a connection lost mid-answer) nobody
// knows whether the provider billed it.
func classify(err error) error {
	var apiErr *anthropic.Error
	if errors.As(err, &apiErr) {
		return &RejectedError{StatusCode: apiErr.StatusCode, Message: apiErr.Error()}
	}
	if unsent(err) {
		return fmt.Errorf("%w: %v", ErrNotSent, err)
	}
	return err
}
