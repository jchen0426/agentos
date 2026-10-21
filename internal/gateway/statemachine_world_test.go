package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/jchen0426/agentos/internal/policy"
	"github.com/jchen0426/agentos/internal/tools"
)

// The outside world of the state machine test (statemachine_test.go): the clock, the requests the
// tools sent, and when, if ever, each took effect.

// smSettle is how long after a request went out a lookup that finds nothing means it never took
// effect (tools.Finder). The world keeps that promise: a request takes effect at most smLate after
// it went out.
const (
	smSettle = 3 * time.Minute
	smLate   = 150 * time.Second
)

// sentRequest is one request a tool sent.
type sentRequest struct {
	effectID string
	key      string
	final    bool      // it makes the write; a request before it (a branch) makes something else
	at       time.Time // when it went out
	landsAt  time.Time // when it takes effect; zero: never
}

func (r *sentRequest) landed(now time.Time) bool { return !r.landsAt.IsZero() && !r.landsAt.After(now) }

// world is shared with recovery at start, which runs in goroutines of its own: mu guards it, and
// what the walk knows of each effect (effectModel).
type world struct {
	mu   sync.Mutex
	now  time.Time
	sent []*sentRequest
}

// Now is ledger.Clock for the walk. Time moves when the walk moves it, or recovery waits; and by a
// nanosecond at each reading, so that the ledger's times are all different, and in order.
func (w *world) Now() time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.now = w.now.Add(time.Nanosecond)
	return w.now
}

func (w *world) advance(d time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.now = w.now.Add(d)
}

// made counts what an effect's write made by now: one thing per key when the outside world
// dedupes (an idempotent tool), one per request that took effect otherwise. w.mu is held.
func (w *world) made(effectID string, dedupe bool) int {
	var n int
	var keys []string
	for _, r := range w.sent {
		if r.effectID != effectID || !r.final || !r.landed(w.now) {
			continue
		}
		if dedupe {
			if slices.Contains(keys, r.key) {
				continue
			}
			keys = append(keys, r.key)
		}
		n++
	}
	return n
}

// pending reports whether a request of the effect's write may still take effect. w.mu is held.
func (w *world) pending(effectID string) bool {
	return slices.ContainsFunc(w.sent, func(r *sentRequest) bool {
		return r.effectID == effectID && r.final && !r.landsAt.IsZero() && r.landsAt.After(w.now)
	})
}

// wentOut reports whether any request of the effect went out. w.mu is held.
func (w *world) wentOut(effectID string) bool {
	return slices.ContainsFunc(w.sent, func(r *sentRequest) bool { return r.effectID == effectID })
}

// smTool is a write of the walk: a fresh one of each for every agentd, which dies with it. What
// each attempt does outside is drawn by the walk (machine.execute).
type smTool struct {
	m      *machine
	inc    *incarnation
	name   string
	class  string
	dedupe bool // the outside world makes one thing per idempotency key
}

// smFinder is an smTool whose writes can be looked up.
type smFinder struct{ *smTool }

var (
	smArgs   = tools.MustSchema(`{"type": "object"}`)
	smResult = tools.MustSchema(`{"type": "object", "required": ["id"], "properties": {"id": {"type": "integer"}}}`)
)

func (t *smTool) Name() string                          { return t.name }
func (t *smTool) Class() string                         { return t.class }
func (t *smTool) Args() *tools.Schema                   { return smArgs }
func (t *smTool) Result() *tools.Schema                 { return smResult }
func (t *smTool) Live() bool                            { return t.inc.live }
func (t *smTool) EditArg() string                       { return "patch_ref" }
func (t *smTool) DryRun(json.RawMessage) map[string]any { return map[string]any{"dry_run": true} }

// Prepare names the effect in its payload, as a marker a lookup finds; and what this agentd
// renders differs from what another would (its config, say): what goes out is what was recorded,
// never one rendered again.
func (t *smTool) Prepare(_ context.Context, req tools.Request) (json.RawMessage, error) {
	return json.Marshal(smPayload{EffectID: req.EffectID, Args: req.Args, By: t.inc.name})
}

func (t *smTool) Execute(ctx context.Context, payload json.RawMessage, key string, proceed tools.Proceed) (map[string]any, error) {
	return t.m.execute(ctx, t, payload, key, proceed)
}

func (f smFinder) Find(ctx context.Context, payload json.RawMessage) (map[string]any, bool, error) {
	return f.m.find(f.smTool, payload)
}

func (f smFinder) Settle() time.Duration { return smSettle }

type smPayload struct {
	EffectID string         `json:"effect_id"`
	Args     map[string]any `json:"args"`
	By       string         `json:"by"` // the agentd that rendered it
}

func payloadEffect(payload json.RawMessage) string {
	var p smPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		panic(fmt.Sprintf("a payload the walk did not render: %v", err))
	}
	return p.EffectID
}

// What an attempt of a write does outside (machine.execute).
type attemptKind int

const (
	attemptOK            attemptKind = iota // the write takes effect, and the tool says so
	attemptUnknown                          // it went out, and no answer came: it takes effect now, later or never
	attemptUnknownBefore                    // the tool cannot tell what came of it, having sent nothing
	attemptNotSent                          // nothing went out (ErrNotSent)
	attemptRetryable                        // refused for now: nothing came of it
	attemptRefused                          // refused for good: nothing came of it
	attemptDenied                           // the tool refused the payload before sending anything
	attemptConflict                         // the outside world holds something else under the write's name
)

var attemptNames = map[attemptKind]string{attemptOK: "ok", attemptUnknown: "unknown", attemptUnknownBefore: "unknown before sending",
	attemptNotSent: "not sent", attemptRetryable: "refused for now", attemptRefused: "refused", attemptDenied: "denied",
	attemptConflict: "conflict"}

// errDied is what a tool of an agentd that died answers: nobody hears it.
var errDied = errors.New("agentd died")

// smTools are the walk's writes, one of each class. The idempotent one dedupes by key outside, and
// can be looked up in some walks.
func (m *machine) smTools(inc *incarnation) map[string]tools.Writer {
	lookup := &smTool{m: m, inc: inc, name: "sm.lookup", class: policy.EffectLookup}
	idem := &smTool{m: m, inc: inc, name: "sm.idempotent", class: policy.EffectIdempotent, dedupe: true}
	unsafe := &smTool{m: m, inc: inc, name: "sm.unsafe", class: policy.EffectUnsafe}
	out := map[string]tools.Writer{lookup.name: smFinder{lookup}, idem.name: idem, unsafe.name: unsafe}
	if m.idemFinds {
		out[idem.name] = smFinder{idem}
	}
	return out
}
