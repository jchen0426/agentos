package models

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	"github.com/openai/openai-go/v3/responses"
	"github.com/openai/openai-go/v3/shared"
	"github.com/tiktoken-go/tokenizer"

	"github.com/jchen0426/agentos/internal/pricing"
)

// ErrTooLong means a request's input does not fit the model's context window, or the server
// cut it to fit: sending it again will not help, and an answer to part of the input is no
// answer.
var ErrTooLong = errors.New("input too long for the model's context")

// ErrCloudModel means a model server on this machine would hand the model's work to servers
// elsewhere (an Ollama cloud model): the task's data would leave the machine.
var ErrCloudModel = errors.New("a cloud model, not a local one")

// OpenAICompat is the OpenAI-compatible adapter (openai-go). It speaks OpenAI's Responses
// API to OpenAI (api: responses), and Chat Completions to Ollama and vLLM (api: chat), the
// one surface those serve. Each Call is one request: the SDK's own retries are off, and the
// clients read nothing from the environment (no OPENAI_API_KEY, OPENAI_BASE_URL,
// OPENAI_ORG_ID or OPENAI_PROJECT_ID), only the key and URL they are given.
type OpenAICompat struct {
	provider  string // the provider's name in models.yaml, which prefixes its model names
	cfg       Provider
	responses responses.ResponseService
	chat      openai.ChatCompletionService
	codec     tokenizer.Codec // api chat: estimates input tokens, for the context window
	blobs     BlobStore
	// Sent with every OpenAI request: OpenAI's cyber safeguards hold a flagged identifier to
	// account, not the whole organization (design doc, section 9).
	safetyID string

	// A server of this machine's own, or any Chat Completions server, is asked about the model
	// before each request (local).
	probe bool
	key   string
	http  *http.Client
}

// NewOpenAICompat makes the adapter for provider p. key may be empty for a server that
// takes none (Ollama).
func NewOpenAICompat(provider string, p Provider, key, safetyID string, blobs BlobStore) (*OpenAICompat, error) {
	if err := p.check(); err != nil {
		return nil, err
	}
	// The services, unlike openai.NewClient, apply no defaults from the environment.
	opts := []option.RequestOption{option.WithBaseURL(p.BaseURL), option.WithMaxRetries(0)}
	if key != "" {
		opts = append(opts, option.WithAPIKey(key))
	}
	a := &OpenAICompat{provider: provider, cfg: p, blobs: blobs, safetyID: safetyID,
		probe: p.API == "chat" || loopback(p.BaseURL), key: key, http: &http.Client{Timeout: 10 * time.Second}}
	switch p.API {
	case "responses":
		a.responses = responses.NewResponseService(opts...)
	case "chat":
		a.chat = openai.NewChatCompletionService(opts...)
		codec, err := tokenizer.Get(tokenizer.O200kBase)
		if err != nil {
			return nil, err
		}
		a.codec = codec
	}
	return a, nil
}

// loopback reports whether a URL points at this machine (config.Loopback, which this package
// cannot import).
func loopback(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	ip := net.ParseIP(u.Hostname())
	return ip != nil && ip.IsLoopback()
}

func (a *OpenAICompat) name(model string) string { return a.provider + "/" + model }

// local makes sure the model runs on the server itself. Ollama serves cloud models, which run
// on Ollama's servers, under any name a user gives them; its /api/show names their remote host.
// It is asked before every request, since any program on the machine can rename a model
// through Ollama's API. A server without /api/show (vLLM) answers 404 or 405 and has no such
// models; so does Ollama for a model it does not have, and the request itself then fails.
func (a *OpenAICompat) local(ctx context.Context, model string) error {
	if !a.probe {
		return nil
	}
	body, err := json.Marshal(map[string]string{"model": model})
	if err != nil {
		return err
	}
	root := strings.TrimSuffix(strings.TrimSuffix(a.cfg.BaseURL, "/"), "/v1")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, root+"/api/show", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if a.key != "" {
		req.Header.Set("Authorization", "Bearer "+a.key) // a server behind a proxy that wants it
	}
	resp, err := a.http.Do(req)
	if err != nil {
		return fmt.Errorf("ask %s about %s: %w", a.provider, model, err)
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusMethodNotAllowed:
		return nil
	case resp.StatusCode != http.StatusOK:
		return fmt.Errorf("ask %s about %s: %s", a.provider, model, resp.Status)
	}
	var show struct {
		RemoteHost  string `json:"remote_host"`
		RemoteModel string `json:"remote_model"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&show); err != nil {
		return fmt.Errorf("ask %s about %s: %w", a.provider, model, err)
	}
	if show.RemoteHost != "" || show.RemoteModel != "" {
		return fmt.Errorf("%w: %s runs %s on %s", ErrCloudModel, a.name(model), show.RemoteModel, show.RemoteHost)
	}
	return nil
}

// CountTokens counts the request's input. OpenAI counts it exactly, the request as it would
// be sent (its input_tokens endpoint, Responses API only). An Ollama or vLLM server has no
// such thing here, and its tokens cost nothing (agentd refuses prices for them): the count is
// an estimate, and decides whether the input and max_tokens fit the context window.
func (a *OpenAICompat) CountTokens(ctx context.Context, req Request) (int64, error) {
	if err := a.local(ctx, req.Route.Model); err != nil {
		return 0, err
	}
	user, err := userMessage(ctx, a.blobs, req.WorkflowID, req.Args)
	if err != nil {
		return 0, err
	}
	if a.cfg.API == "chat" {
		_, n, err := a.estimate(ctx, req.Route, user)
		if err != nil {
			return 0, err
		}
		if n+req.Route.MaxTokens > a.cfg.ContextTokens {
			return 0, fmt.Errorf("%w: at least about %d input tokens and max_tokens %d are over the %d-token context of %s",
				ErrTooLong, n, req.Route.MaxTokens, a.cfg.ContextTokens, a.provider)
		}
		return n, nil
	}
	p := responses.InputTokenCountParams{
		Model:     openai.String(req.Route.Model),
		Input:     responses.InputTokenCountParamsInputUnion{OfString: openai.String(user)},
		Text:      responses.InputTokenCountParamsText{Format: a.format(req.Route)},
		Reasoning: a.reasoning(req.Route),
	}
	if req.Route.Prompt != "" {
		p.Instructions = openai.String(req.Route.Prompt)
	}
	r, err := a.responses.InputTokens.Count(ctx, p)
	if err != nil {
		declined, sorted := a.failed(req.Route.Model, err)
		switch {
		case sorted != nil:
			err = sorted
		case declined.Status == "refused":
			// Declined before any request: it will be declined again, at no cost.
			err = &RejectedError{StatusCode: http.StatusBadRequest, Message: fmt.Sprintf("declined: %v", declined.Value["explanation"])}
		}
		return 0, fmt.Errorf("count tokens for %s: %w", req.Route.Model, err)
	}
	return r.InputTokens, nil
}

// estimate counts tokens with o200k_base, the encoding of OpenAI's models and of gpt-oss.
// raw is the prompt and the input, about what the server reads; padded adds the output
// schema, which some servers put in the prompt, and a fifth, since other models' tokenizers
// differ. Counting stops once it passes the context window: the input is too long either way.
func (a *OpenAICompat) estimate(ctx context.Context, r Route, user string) (raw, padded int64, err error) {
	schema, err := json.Marshal(r.Schema)
	if err != nil {
		return 0, 0, err
	}
	for i, text := range []string{r.Prompt, user, string(schema)} {
		n, err := a.count(ctx, text, a.cfg.ContextTokens)
		if err != nil {
			return 0, 0, err
		}
		n += 4 // the chat format's own tokens around each part
		if i < 2 {
			raw += n
		}
		padded += n
	}
	return raw, padded * 6 / 5, nil
}

// countPiece is how much text the tokenizer takes at a time. Its work grows with the square
// of a run of text it cannot split, a long base64 line say: pieces bound that.
const countPiece = 1024

// count counts text's tokens a piece at a time, each cut at a line end where there is one,
// checking ctx between pieces; past limit, it stops with what it has.
func (a *OpenAICompat) count(ctx context.Context, text string, limit int64) (int64, error) {
	var n int64
	for text != "" && n <= limit {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		piece := text
		if len(piece) > countPiece {
			cut := strings.LastIndexByte(piece[:countPiece], '\n') + 1
			if cut == 0 {
				for cut = countPiece; cut > 0 && !utf8.RuneStart(piece[cut]); cut-- {
				}
				if cut == 0 {
					cut = countPiece // not UTF-8: any cut will do
				}
			}
			piece = piece[:cut]
		}
		c, err := a.codec.Count(piece)
		if err != nil {
			return 0, err
		}
		n += int64(c)
		text = text[len(piece):]
	}
	return n, nil
}

func (a *OpenAICompat) Call(ctx context.Context, req Request) (Response, error) {
	if err := a.local(ctx, req.Route.Model); err != nil {
		return Response{}, fmt.Errorf("%w: %w", ErrNotSent, err)
	}
	user, err := userMessage(ctx, a.blobs, req.WorkflowID, req.Args)
	if err != nil {
		return Response{}, fmt.Errorf("%w: %v", ErrNotSent, err)
	}
	if a.cfg.API == "chat" {
		return a.callChat(ctx, req, user)
	}
	return a.callResponses(ctx, req, user)
}

// format is the output schema as OpenAI takes it, in strict mode (CheckRoute vetted it).
func (a *OpenAICompat) format(r Route) responses.ResponseFormatTextConfigUnionParam {
	if r.Schema == nil {
		return responses.ResponseFormatTextConfigUnionParam{}
	}
	return responses.ResponseFormatTextConfigUnionParam{OfJSONSchema: &responses.ResponseFormatTextJSONSchemaConfigParam{
		Name: "output", Schema: r.Schema, Strict: openai.Bool(true),
	}}
}

// reasoning is the task's effort, as the provider takes it.
func (a *OpenAICompat) reasoning(r Route) shared.ReasoningParam {
	if e := a.cfg.Effort(r.Effort); e != "" {
		return shared.ReasoningParam{Effort: shared.ReasoningEffort(e)}
	}
	return shared.ReasoningParam{}
}

// policyCodes are the codes OpenAI declines a request with, as an error or a failed response:
// refusals, billed only what the answer reports. invalid_prompt is its usage-policy flag ("your
// prompt was flagged as potentially violating our usage policy").
var policyCodes = []string{"cyber_policy", "bio_policy", "misalignment_policy_violation", "invalid_prompt"}

func (a *OpenAICompat) callResponses(ctx context.Context, req Request, user string) (Response, error) {
	name := a.name(req.Route.Model)
	p := responses.ResponseNewParams{
		Model:           req.Route.Model,
		Input:           responses.ResponseNewParamsInputUnion{OfString: openai.String(user)},
		MaxOutputTokens: openai.Int(req.Route.MaxTokens),
		Text:            responses.ResponseTextConfigParam{Format: a.format(req.Route)},
		Reasoning:       a.reasoning(req.Route),
		// The tier the price table lists: a project's default may be another, priced otherwise.
		ServiceTier: responses.ResponseNewParamsServiceTierDefault,
		// OpenAI keeps responses for new accounts unless told not to: the diff is the user's.
		Store: openai.Bool(false),
		// Requests that share the task's prompt are routed to the same cache.
		PromptCacheKey: openai.String(req.Route.Task),
	}
	if req.Route.Prompt != "" {
		p.Instructions = openai.String(req.Route.Prompt)
	}
	if a.safetyID != "" {
		p.SafetyIdentifier = openai.String(a.safetyID)
	}
	resp, err := a.responses.New(ctx, p)
	if err != nil {
		return a.failed(name, err)
	}
	u := resp.Usage
	// Billed as the model asked for: the answer may name a dated snapshot of it, which the
	// price table does not list.
	billed := name
	if resp.ServiceTier != "" && resp.ServiceTier != responses.ResponseServiceTierDefault {
		// Not the tier asked for, nor one the price table knows: no price, so the gateway
		// charges the whole reservation and logs the model under this name.
		billed = name + "@" + string(resp.ServiceTier)
	}
	out := Response{Model: name, StopReason: string(resp.Status), Effort: a.cfg.Effort(req.Route.Effort),
		Billed: []Billed{{Model: billed, Usage: pricing.Usage{
			InputTokens:      u.InputTokens - u.InputTokensDetails.CachedTokens - u.InputTokensDetails.CacheWriteTokens,
			CacheWriteTokens: u.InputTokensDetails.CacheWriteTokens, CacheReadTokens: u.InputTokensDetails.CachedTokens,
			OutputTokens: u.OutputTokens, // reasoning included
		}}}}
	switch resp.Status {
	case responses.ResponseStatusCompleted, responses.ResponseStatusIncomplete:
	case responses.ResponseStatusFailed:
		if code := string(resp.Error.Code); slices.Contains(policyCodes, code) {
			out.StopReason = code
			return refused(out, code, resp.Error.Message), nil
		}
		fallthrough
	default:
		// The model ran, or may have: whatever it cost, the outcome is not known.
		return Response{}, fmt.Errorf("%s: response %s: %s %s", name, resp.Status, resp.Error.Code, resp.Error.Message)
	}
	var text strings.Builder
	for _, item := range resp.Output {
		if item.Type != "message" {
			continue // reasoning, which is billed as output and not shown
		}
		for _, c := range item.Content {
			switch c.Type {
			case "refusal":
				return refused(out, "refusal", c.Refusal), nil
			case "output_text":
				text.WriteString(c.Text)
			}
		}
	}
	if resp.Status == responses.ResponseStatusIncomplete {
		switch resp.IncompleteDetails.Reason {
		case "content_filter":
			return refused(out, "content_filter", "the answer was stopped by OpenAI's content filter"), nil
		case "max_output_tokens":
			out.StopReason = "max_tokens"
		}
	}
	out.Status, out.Output = "ok", decodeObject(text.String())
	return out, nil
}

func (a *OpenAICompat) callChat(ctx context.Context, req Request, user string) (Response, error) {
	name := a.name(req.Route.Model)
	raw, _, err := a.estimate(ctx, req.Route, user)
	if err != nil {
		return Response{}, fmt.Errorf("%w: %v", ErrNotSent, err)
	}
	var messages []openai.ChatCompletionMessageParamUnion
	if req.Route.Prompt != "" {
		messages = append(messages, openai.SystemMessage(req.Route.Prompt))
	}
	p := openai.ChatCompletionNewParams{
		Model:    req.Route.Model,
		Messages: append(messages, openai.UserMessage(user)),
		// Ollama reads max_tokens and ignores max_completion_tokens: with it, no limit at all.
		MaxTokens: openai.Int(req.Route.MaxTokens),
	}
	if req.Route.Schema != nil {
		// Ollama turns the schema into constrained decoding and ignores strict; the gateway
		// checks the answer against the schema either way.
		p.ResponseFormat = openai.ChatCompletionNewParamsResponseFormatUnion{OfJSONSchema: &shared.ResponseFormatJSONSchemaParam{
			JSONSchema: shared.ResponseFormatJSONSchemaJSONSchemaParam{Name: "output", Schema: req.Route.Schema, Strict: openai.Bool(true)},
		}}
	}
	if e := a.cfg.Effort(req.Route.Effort); e != "" {
		p.ReasoningEffort = shared.ReasoningEffort(e)
	}
	resp, err := a.chat.New(ctx, p)
	if err != nil {
		return a.failed(name, err)
	}
	u := resp.Usage
	out := Response{Model: name, Effort: a.cfg.Effort(req.Route.Effort), Billed: []Billed{{Model: name, Usage: pricing.Usage{
		InputTokens:     u.PromptTokens - u.PromptTokensDetails.CachedTokens,
		CacheReadTokens: u.PromptTokensDetails.CachedTokens, OutputTokens: u.CompletionTokens,
	}}}}
	// Ollama cuts input that does not fit its context window to about half the window, without
	// a word, and shifts the window, forgetting the start of the input, when the answer runs past
	// it. Its prompt_tokens counts what it read, cached or not.
	switch {
	case u.PromptTokens+u.CompletionTokens > a.cfg.ContextTokens:
		return Response{}, fmt.Errorf("%w: %s read %d input tokens and wrote %d, more than the %d-token context",
			ErrTooLong, a.provider, u.PromptTokens, u.CompletionTokens, a.cfg.ContextTokens)
	case u.PromptTokens > 0 && u.PromptTokens < raw*4/5:
		return Response{}, fmt.Errorf("%w: %s read %d input tokens of about %d; is the server's context window "+
			"(OLLAMA_CONTEXT_LENGTH) at least context_tokens, %d?", ErrTooLong, a.provider, u.PromptTokens, raw, a.cfg.ContextTokens)
	}
	if len(resp.Choices) == 0 {
		return Response{}, &RejectedError{StatusCode: 502, Message: "an answer without choices"}
	}
	choice := resp.Choices[0]
	out.StopReason = choice.FinishReason
	switch {
	case choice.Message.Refusal != "":
		return refused(out, "refusal", choice.Message.Refusal), nil
	case choice.FinishReason == "content_filter":
		return refused(out, "content_filter", "the answer was stopped by the server's content filter"), nil
	case choice.FinishReason == "length":
		out.StopReason = "max_tokens"
	}
	out.Status, out.Output = "ok", decodeObject(choice.Message.Content)
	return out, nil
}

// refused is a refusal, still billed for what the model wrote before it declined.
func refused(out Response, category, explanation string) Response {
	out.Status, out.Output = "refused", nil
	out.Value = map[string]any{"category": category, "explanation": explanation}
	return out
}

// decodeObject is the answer's one JSON object, or nil when the text is not one (cut off at
// max_tokens, say): the gateway treats nil as an answer that breaks the schema.
func decodeObject(text string) map[string]any {
	dec := json.NewDecoder(strings.NewReader(text))
	dec.UseNumber()
	var out map[string]any
	if err := dec.Decode(&out); err != nil {
		return nil
	}
	return out
}

// failed sorts an SDK error as classify does for Claude. OpenAI's safeguards decline a
// request with an error, not an answer: that is a refusal, billed nothing. An input over the
// context window is turned away with a 400 by OpenAI, vLLM and Ollama alike.
func (a *OpenAICompat) failed(name string, err error) (Response, error) {
	var apiErr *openai.Error
	if errors.As(err, &apiErr) {
		if slices.Contains(policyCodes, apiErr.Code) {
			return refused(Response{Model: name, StopReason: apiErr.Code, Billed: []Billed{{Model: name}}},
				apiErr.Code, apiErr.Message), nil
		}
		rejected := &RejectedError{StatusCode: apiErr.StatusCode, Message: apiErr.Error()}
		if apiErr.StatusCode == http.StatusBadRequest &&
			(apiErr.Code == "context_length_exceeded" || strings.Contains(strings.ToLower(apiErr.Message), "context length")) {
			rejected.Err = ErrTooLong
		}
		return Response{}, rejected
	}
	if unsent(err) {
		return Response{}, fmt.Errorf("%w: %v", ErrNotSent, err)
	}
	return Response{}, err
}

// CheckRoute reports, at start, a route this adapter cannot serve: OpenAI's strict mode takes
// a subset of JSON Schema, and turns anything else away on every request; and a request that
// may run longer than the adapter, which does not stream, should wait for.
func (a *OpenAICompat) CheckRoute(r Route) error {
	if d := ExpectedDuration(r.MaxTokens); d > NonStreamingLimit {
		return fmt.Errorf("max_tokens %d may take %v without streaming, more than %v: lower it", r.MaxTokens, d, NonStreamingLimit)
	}
	if a.cfg.API == "chat" && r.MaxTokens >= a.cfg.ContextTokens {
		return fmt.Errorf("max_tokens %d leaves no room for input in the %d-token context of %s", r.MaxTokens, a.cfg.ContextTokens, a.provider)
	}
	if a.cfg.API == "responses" && r.Schema != nil {
		if err := strictSchema(r.Schema); err != nil {
			return fmt.Errorf("output schema: OpenAI's strict structured outputs refuse it: %w", err)
		}
	}
	return nil
}
