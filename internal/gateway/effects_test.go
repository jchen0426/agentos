package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/faults"
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
	"github.com/jchen0426/agentos/internal/policy"
	"github.com/jchen0426/agentos/internal/tools"
)

// writer is a test tool: an external write that counts what reaches the outside.
type writer struct {
	class   string
	sent    atomic.Int64
	release chan struct{} // when set, Execute waits for it before it asks to proceed
	err     error         // what Execute returns, once sent
	deny    string        // when set, Execute refuses the payload before it sends anything
	unsent  bool          // when set, Execute fails before it sends anything (ErrNotSent)
	// When set, Execute asks to proceed and then fails with it, making nothing: a write of several
	// requests whose first went out, and whose next did nothing (a retryable refusal).
	after error
}

var noArgs = tools.MustSchema(`{"type": "object"}`)
var anyResult = tools.MustSchema(`{"type": "object", "required": ["id"], "properties": {"id": {"type": "integer"}}}`)

func (w *writer) Name() string                          { return "test.write" }
func (w *writer) Class() string                         { return w.class }
func (w *writer) Args() *tools.Schema                   { return noArgs }
func (w *writer) Result() *tools.Schema                 { return anyResult }
func (w *writer) Live() bool                            { return true }
func (w *writer) DryRun(json.RawMessage) map[string]any { return map[string]any{"dry_run": true} }

func (w *writer) Prepare(_ context.Context, req tools.Request) (json.RawMessage, error) {
	return json.Marshal(req.Args)
}

func (w *writer) Execute(ctx context.Context, _ json.RawMessage, _ string, proceed tools.Proceed) (map[string]any, error) {
	if w.release != nil {
		<-w.release
	}
	if w.deny != "" {
		return nil, &tools.DeniedError{Reason: w.deny}
	}
	if w.unsent {
		return nil, tools.ErrNotSent
	}
	if err := proceed(ctx); err != nil {
		return nil, err
	}
	if w.after != nil {
		return nil, w.after
	}
	w.sent.Add(1)
	if w.err != nil {
		return nil, w.err
	}
	return map[string]any{"id": 1}, nil
}

// finder is a lookup tool: what it finds is set by the test.
type finder struct {
	writer
	found   map[string]any
	findErr error
	settle  time.Duration
}

func (f *finder) Find(context.Context, json.RawMessage) (map[string]any, bool, error) {
	return f.found, f.found != nil, f.findErr
}
func (f *finder) Settle() time.Duration { return f.settle }

type fixture struct {
	g   *Gateway
	run db.Run
}

func newFixture(t *testing.T, dryRun bool) *fixture {
	t.Helper()
	ctx := context.Background()
	l, err := ledger.Open(ctx, filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	var dry int64
	if dryRun {
		dry = 1
	}
	if err := l.Q.InsertRun(ctx, db.InsertRunParams{WorkflowID: "wf", Task: "security-audit", ParamsJson: "{}",
		BudgetLimitMicros: 1, DryRun: dry, CreatedAt: ledger.Now()}); err != nil {
		t.Fatal(err)
	}
	run, err := l.Q.GetRun(ctx, "wf")
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{run: run, g: &Gateway{Ledger: l, Tools: tools.Registry{}, Incarnation: "this-agentd",
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))}}
}

func (f *fixture) req() tools.Request {
	return tools.Request{WorkflowID: "wf", EffectID: strings.Repeat("e", 64), Args: map[string]any{"n": 1}}
}

func (f *fixture) do(t *testing.T, w tools.Writer) (string, error) {
	t.Helper()
	res, err := f.g.effect(context.Background(), f.run, w, w.Class(), 1, nil, f.req(), nil, faults.Effect{})
	return res.GetStatus(), err
}

func (f *fixture) waitClaimed(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if e, err := f.g.Ledger.Q.GetEffect(context.Background(), f.req().EffectID); err == nil && e.ClaimedBy.Valid {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the effect was never claimed")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (f *fixture) cancel(t *testing.T) {
	t.Helper()
	if _, err := f.g.Ledger.Q.MarkRunCancelled(context.Background(), db.MarkRunCancelledParams{
		CancelledAt: valid(ledger.Now()), WorkflowID: "wf"}); err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) status(t *testing.T) db.Effect {
	t.Helper()
	e, err := f.g.Ledger.Q.GetEffect(context.Background(), f.req().EffectID)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// T16, below the black-box tests: two executors bypass the in-process merge and handle the
// same effect at once; only the one that claims it sends.
func TestOnlyTheClaimantSends(t *testing.T) {
	f := newFixture(t, false)
	w := &writer{class: policy.EffectUnsafe, release: make(chan struct{})}
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Go(func() { _, errs[i] = f.do(t, w) })
	}
	f.waitClaimed(t)
	time.Sleep(200 * time.Millisecond) // give the other executor time to try
	close(w.release)
	wg.Wait()
	if got := w.sent.Load(); got != 1 {
		t.Fatalf("sent %d times, want once", got)
	}
	busy := 0
	for _, err := range errs {
		if errors.Is(err, errEffectBusy) {
			busy++
		} else if err != nil {
			t.Fatal(err)
		}
	}
	if busy != 1 || f.status(t).Status != effectSucceeded {
		t.Fatalf("errors %v, effect %s", errs, f.status(t).Status)
	}
}

func TestClaimIsCompareAndSwap(t *testing.T) {
	f := newFixture(t, false)
	w := &writer{class: policy.EffectUnsafe}
	if _, err := f.g.Ledger.Q.InsertEffect(context.Background(), db.InsertEffectParams{
		EffectID: f.req().EffectID, WorkflowID: "wf", Tool: w.Name(), EffectClass: w.class, PayloadSha256: "x",
		IdempotencyKey: "k", Status: effectPending, CreatedAt: ledger.Now(), UpdatedAt: ledger.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	e := f.status(t)
	won := 0
	for range 2 {
		if _, ok, err := f.g.claim(context.Background(), e); err != nil {
			t.Fatal(err)
		} else if ok {
			won++
		}
	}
	if won != 1 {
		t.Fatalf("%d claims of one version won", won)
	}
}

func TestACancelledRunSendsNothing(t *testing.T) {
	f := newFixture(t, false)
	f.cancel(t)
	w := &writer{class: policy.EffectUnsafe}
	status, err := f.do(t, w)
	if err != nil || status != StatusDenied || w.sent.Load() != 0 || f.status(t).Status != effectAbandoned {
		t.Fatalf("status %s, err %v, sent %d, effect %s", status, err, w.sent.Load(), f.status(t).Status)
	}
}

// Once the run is cancelled, a tool's proceed fails with ErrStopped: the tool can tell a cancel
// from agentd failing to record the request, which may be tried again. A write that has sent
// part of itself leaves the rest to a person only for the cancel.
func TestProceedStopsOnceTheRunIsCancelled(t *testing.T) {
	f := newFixture(t, false)
	ctx := context.Background()
	w := &writer{class: policy.EffectUnsafe}
	if res, err := f.g.record(ctx, f.run, w, w.Class(), 1, nil, f.req(), nil); res != nil || err != nil {
		t.Fatalf("record: %v %v", res, err)
	}
	e, ok, err := f.g.claim(ctx, f.status(t))
	if !ok || err != nil {
		t.Fatalf("claim: %v %v", ok, err)
	}
	proceed := f.g.proceed(e)
	if err := proceed(ctx); err != nil {
		t.Fatalf("before the cancel: %v", err)
	}
	f.cancel(t)
	if err := proceed(ctx); !errors.Is(err, tools.ErrStopped) || !errors.Is(err, tools.ErrNotSent) {
		t.Fatalf("after the cancel: %v, want ErrStopped", err)
	}
}

// A tool that refuses its payload while executing it, before anything went out (it does not
// match what GitHub holds, say), ends the effect: it never will be sent, and the call is denied
// with the tool's reason. It counts against the run's writes: the check may have cost requests
// of its own, and a workflow must not be able to have them made again and again.
func TestAToolsDenialEndsTheEffect(t *testing.T) {
	for _, class := range []string{policy.EffectUnsafe, policy.EffectLookup} {
		f := newFixture(t, false)
		w := &finder{writer: writer{class: class, deny: "app/db.py is not what the mirror has"}}
		res, err := f.g.effect(context.Background(), f.run, w, class, 1, nil, f.req(), nil, faults.Effect{})
		e := f.status(t)
		if err != nil || res.GetStatus() != StatusDenied || !strings.Contains(res.GetValueJson(), w.deny) ||
			e.Status != effectDenied || e.Reason.String != w.deny || w.sent.Load() != 0 {
			t.Fatalf("%s: result %v, err %v, effect %s %q, sent %d", class, res, err, e.Status, e.Reason.String, w.sent.Load())
		}
		// The same call again gets the same answer, and nothing is sent.
		if res, err := f.g.effect(context.Background(), f.run, w, class, 1, nil, f.req(), nil, faults.Effect{}); err != nil ||
			res.GetStatus() != StatusDenied || w.sent.Load() != 0 {
			t.Fatalf("%s: again: %v %v", class, res, err)
		}
		// Another payload of the same tool is one write too many.
		other := f.req()
		other.EffectID, other.Args = strings.Repeat("f", 64), map[string]any{"n": 2}
		res, err = f.g.effect(context.Background(), f.run, &writer{class: class}, class, 1, nil, other, nil, faults.Effect{})
		if err != nil || res.GetStatus() != StatusDenied || !strings.Contains(res.GetValueJson(), "already has 1") {
			t.Fatalf("%s: another payload: %v %v", class, res, err)
		}
	}
}

func TestDryRunSendsNothing(t *testing.T) {
	f := newFixture(t, true)
	w := &writer{class: policy.EffectUnsafe}
	status, err := f.do(t, w)
	if err != nil || status != StatusOK || w.sent.Load() != 0 || f.status(t).Status != effectDryRun {
		t.Fatalf("status %s, err %v, sent %d, effect %s", status, err, w.sent.Load(), f.status(t).Status)
	}
}

// claimElsewhere records the effect as claimed by an agentd process that has stopped.
func (f *fixture) claimElsewhere(t *testing.T, w tools.Writer) {
	t.Helper()
	ctx := context.Background()
	payload, _ := w.Prepare(ctx, f.req())
	ref, err := f.g.Ledger.PutBlob(ctx, payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.g.Ledger.Q.InsertEffect(ctx, db.InsertEffectParams{
		EffectID: f.req().EffectID, WorkflowID: "wf", Tool: w.Name(), EffectClass: w.Class(), PayloadSha256: sha256Hex(payload),
		PayloadBlob: valid(ref), IdempotencyKey: "k", Status: effectPending, CreatedAt: ledger.Now(), UpdatedAt: ledger.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.g.Ledger.Q.ClaimEffect(ctx, db.ClaimEffectParams{ClaimedBy: valid("gone"), ClaimedAt: valid(ledger.Now()),
		UpdatedAt: ledger.Now(), EffectID: f.req().EffectID, Version: 0}); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryByClass(t *testing.T) {
	t.Run("unsafe waits for a person", func(t *testing.T) {
		f := newFixture(t, false)
		w := &writer{class: policy.EffectUnsafe}
		f.claimElsewhere(t, w)
		status, err := f.do(t, w)
		if err != nil || status != StatusNeedsReconcile || w.sent.Load() != 0 {
			t.Fatalf("status %s, err %v, sent %d", status, err, w.sent.Load())
		}
	})
	t.Run("lookup finds it and records it", func(t *testing.T) {
		f := newFixture(t, false)
		w := &finder{writer: writer{class: policy.EffectLookup}, found: map[string]any{"id": 7}, settle: time.Hour}
		f.claimElsewhere(t, w)
		status, err := f.do(t, w)
		if err != nil || status != StatusOK || w.sent.Load() != 0 || f.status(t).Status != effectSucceeded {
			t.Fatalf("status %s, err %v, sent %d", status, err, w.sent.Load())
		}
	})
	t.Run("lookup finds nothing, waits, sends once", func(t *testing.T) {
		f := newFixture(t, false)
		w := &finder{writer: writer{class: policy.EffectLookup}, settle: 300 * time.Millisecond}
		f.claimElsewhere(t, w)
		start := time.Now()
		status, err := f.do(t, w)
		if err != nil || status != StatusOK || w.sent.Load() != 1 {
			t.Fatalf("status %s, err %v, sent %d", status, err, w.sent.Load())
		}
		if time.Since(start) < 250*time.Millisecond {
			t.Error("sent again before the old request could no longer take effect")
		}
	})
	t.Run("a cancelled run only looks", func(t *testing.T) {
		f := newFixture(t, false)
		w := &finder{writer: writer{class: policy.EffectLookup}, settle: 10 * time.Millisecond}
		f.claimElsewhere(t, w)
		f.cancel(t)
		status, err := f.do(t, w)
		if err != nil || status != StatusDenied || w.sent.Load() != 0 || f.status(t).Status != effectAbandoned {
			t.Fatalf("status %s, err %v, sent %d, effect %s", status, err, w.sent.Load(), f.status(t).Status)
		}
	})
	t.Run("an unknown outcome while sending is recovered at once", func(t *testing.T) {
		f := newFixture(t, false)
		w := &writer{class: policy.EffectUnsafe, err: errors.New("connection reset")}
		status, err := f.do(t, w)
		if err != nil || status != StatusNeedsReconcile || w.sent.Load() != 1 {
			t.Fatalf("status %s, err %v, sent %d", status, err, w.sent.Load())
		}
	})
	t.Run("a failed lookup goes to a person", func(t *testing.T) {
		f := newFixture(t, false)
		w := &finder{writer: writer{class: policy.EffectLookup}, findErr: errors.New("rate limited"), settle: time.Hour}
		f.claimElsewhere(t, w)
		status, err := f.do(t, w)
		if err != nil || status != StatusNeedsReconcile || w.sent.Load() != 0 {
			t.Fatalf("status %s, err %v, sent %d", status, err, w.sent.Load())
		}
	})
	t.Run("at start it only looks", func(t *testing.T) {
		f := newFixture(t, false)
		w := &finder{writer: writer{class: policy.EffectLookup}, settle: 10 * time.Millisecond}
		f.g.Tools.Add(w)
		f.claimElsewhere(t, w)
		_, err := f.g.recoverClaimed(context.Background(), f.req().EffectID)
		if !errors.Is(err, errEffectReleased) || w.sent.Load() != 0 {
			t.Fatalf("err %v, sent %d", err, w.sent.Load())
		}
		// The run's next call sends it.
		if e := f.status(t); e.Status != effectPending || e.ClaimedBy.Valid {
			t.Fatalf("effect %s claimed by %q", e.Status, e.ClaimedBy.String)
		}
		if status, err := f.do(t, w); err != nil || status != StatusOK || w.sent.Load() != 1 {
			t.Fatalf("the next call: %s %v, sent %d", status, err, w.sent.Load())
		}
	})
	t.Run("a cancel after the claim stops the send", func(t *testing.T) {
		f := newFixture(t, false)
		w := &writer{class: policy.EffectUnsafe, release: make(chan struct{})}
		done := make(chan string)
		go func() { status, _ := f.do(t, w); done <- status }()
		f.waitClaimed(t)
		f.cancel(t)
		close(w.release)
		if status := <-done; status != StatusDenied || w.sent.Load() != 0 || f.status(t).Status != effectAbandoned {
			t.Fatalf("status %s, sent %d, effect %s", status, w.sent.Load(), f.status(t).Status)
		}
	})
	t.Run("not sent is released for a retry", func(t *testing.T) {
		f := newFixture(t, false)
		w := &writer{class: policy.EffectUnsafe, err: tools.ErrNotSent}
		if _, err := f.do(t, w); err == nil {
			t.Fatal("no error for a request that was not sent")
		}
		if e := f.status(t); e.Status != effectPending || e.ClaimedBy.Valid {
			t.Fatalf("effect %s claimed by %q", e.Status, e.ClaimedBy.String)
		}
	})
}

func TestReconcile(t *testing.T) {
	ctx := context.Background()
	unknown := func(t *testing.T) (*fixture, *writer) {
		f := newFixture(t, false)
		w := &writer{class: policy.EffectUnsafe}
		f.g.Tools.Add(w)
		f.claimElsewhere(t, w)
		if status, err := f.do(t, w); err != nil || status != StatusNeedsReconcile {
			t.Fatalf("status %s, err %v", status, err)
		}
		return f, w
	}
	t.Run("executed", func(t *testing.T) {
		f, w := unknown(t)
		if _, _, err := f.g.Reconcile(ctx, Resolution{EffectID: f.req().EffectID, Executed: true, ResultJSON: `{"id": "x"}`, Key: "a"}); !errors.Is(err, ErrBadResult) {
			t.Fatalf("a result that breaks the tool's schema: %v", err)
		}
		status, _, err := f.g.Reconcile(ctx, Resolution{EffectID: f.req().EffectID, Executed: true, ResultJSON: `{"id": 3}`, Key: "b"})
		if err != nil || status != effectSucceeded {
			t.Fatalf("status %s, err %v", status, err)
		}
		if status, _, err := f.g.Reconcile(ctx, Resolution{EffectID: f.req().EffectID, Executed: true, ResultJSON: `{"id": 3}`, Key: "b"}); err != nil || status != effectSucceeded {
			t.Fatalf("the same resolution sent again: %s, %v", status, err)
		}
		if _, _, err := f.g.Reconcile(ctx, Resolution{EffectID: f.req().EffectID, Executed: false, Key: "c"}); !errors.Is(err, ErrNotUnknown) {
			t.Fatalf("a second, different resolution: %v", err)
		}
		res, err := f.g.effect(ctx, f.run, w, w.class, 1, nil, f.req(), nil, faults.Effect{})
		if err != nil || res.GetValueJson() != `{"id":3}` || w.sent.Load() != 0 {
			t.Fatalf("the call after: %v %v, sent %d", res, err, w.sent.Load())
		}
	})
	t.Run("not executed", func(t *testing.T) {
		f, w := unknown(t)
		status, _, err := f.g.Reconcile(ctx, Resolution{EffectID: f.req().EffectID, Executed: false, Key: "a"})
		if err != nil || status != effectPending {
			t.Fatalf("status %s, err %v", status, err)
		}
		if status, err := f.do(t, w); err != nil || status != StatusOK || w.sent.Load() != 1 {
			t.Fatalf("the call after: %s %v, sent %d", status, err, w.sent.Load())
		}
	})
	t.Run("a finding sent again applies once, even after the effect moved on", func(t *testing.T) {
		f, w := unknown(t)
		if _, _, err := f.g.Reconcile(ctx, Resolution{EffectID: f.req().EffectID, Executed: false, Key: "a"}); err != nil {
			t.Fatal(err)
		}
		w.err = errors.New("connection reset") // sent again, and again nobody knows
		if status, err := f.do(t, w); err != nil || status != StatusNeedsReconcile || w.sent.Load() != 1 {
			t.Fatalf("the call after: %s %v, sent %d", status, err, w.sent.Load())
		}
		status, _, err := f.g.Reconcile(ctx, Resolution{EffectID: f.req().EffectID, Executed: false, Key: "a"})
		if err != nil || status != effectPending || f.status(t).Status != effectUnknown {
			t.Fatalf("the replay: %s %v, effect %s", status, err, f.status(t).Status)
		}
		if _, _, err := f.g.Reconcile(ctx, Resolution{EffectID: f.req().EffectID, Executed: true, ResultJSON: `{"id": 1}`, Key: "a"}); !errors.Is(err, ErrKeyReused) {
			t.Fatalf("the key reused for another finding: %v", err)
		}
	})
	t.Run("too early to say a lookup tool's write never happened", func(t *testing.T) {
		f := newFixture(t, false)
		w := &finder{writer: writer{class: policy.EffectLookup}, findErr: errors.New("rate limited"), settle: time.Hour}
		f.g.Tools.Add(w)
		f.claimElsewhere(t, w)
		if status, err := f.do(t, w); err != nil || status != StatusNeedsReconcile {
			t.Fatalf("status %s, err %v", status, err)
		}
		if _, _, err := f.g.Reconcile(ctx, Resolution{EffectID: f.req().EffectID, Executed: false, Key: "a"}); !errors.Is(err, ErrTooEarly) {
			t.Fatalf("got %v, want too early", err)
		}
		status, _, err := f.g.Reconcile(ctx, Resolution{EffectID: f.req().EffectID, Executed: false, Key: "b", RunClosed: true})
		if err != nil || status != effectAbandoned {
			t.Fatalf("an ended run's effect: %s %v", status, err)
		}
	})
}

func TestOneWritePerRunByDefault(t *testing.T) {
	f := newFixture(t, false)
	w := &writer{class: policy.EffectUnsafe}
	if status, err := f.do(t, w); err != nil || status != StatusOK {
		t.Fatalf("first: %s %v", status, err)
	}
	other := tools.Request{WorkflowID: "wf", EffectID: strings.Repeat("f", 64), Args: map[string]any{"n": 2}}
	res, err := f.g.effect(context.Background(), f.run, w, w.class, 1, nil, other, nil, faults.Effect{})
	if err != nil || res.GetStatus() != StatusDenied || w.sent.Load() != 1 {
		t.Fatalf("a second write: %v %v, sent %d", res, err, w.sent.Load())
	}
	// The same call again is the same effect, not a second one.
	if status, err := f.do(t, w); err != nil || status != StatusOK || w.sent.Load() != 1 {
		t.Fatalf("the first again: %s %v, sent %d", status, err, w.sent.Load())
	}
	if res, err := f.g.effect(context.Background(), f.run, w, w.class, 2, nil, other, nil, faults.Effect{}); err != nil || res.GetStatus() != StatusOK {
		t.Fatalf("allowed two: %v %v", res, err)
	}
}

func TestEffectClassComesFromTheTool(t *testing.T) {
	w := &writer{class: policy.EffectLookup}
	f := &finder{writer: writer{class: policy.EffectLookup}}
	for _, c := range []struct {
		tool    tools.Tool
		stated  string
		want    string
		refused bool
	}{
		{f, "", policy.EffectLookup, false},
		{f, policy.EffectUnsafe, policy.EffectUnsafe, false}, // more careful: fine
		{f, policy.EffectIdempotent, "", true},               // weaker: refused
		{w, "", "", true},                                    // declares lookup but cannot look up
		{&writer{class: policy.EffectNone}, "", "", true},    // a write that claims to be a read
		{&tools.ShowDiff{}, policy.EffectUnsafe, "", true},   // a read cannot be recovered as a write
		{&tools.ShowDiff{}, policy.EffectNone, policy.EffectNone, false},
	} {
		got, err := effectClass(c.tool, c.stated)
		if (err != nil) != c.refused || got != c.want {
			t.Errorf("%T stated %q: got %q, %v", c.tool, c.stated, got, err)
		}
	}
}

// A write that may have taken effect (a request of it went out, and nothing since showed that it did
// not take effect: maybe_sent) is never recorded as one that did not, whatever stops the attempt at
// hand: the tool denies it, the outside world refuses it for good, agentd only dry-runs, the run was
// cancelled. An earlier attempt may have made it: a person settles it.
func TestAWriteThatMayHaveTakenEffectIsNeverRecordedAsNotSent(t *testing.T) {
	ctx := context.Background()
	call := func(f *fixture, w tools.Writer) (*agentosv1.Result, error) {
		return f.g.effect(ctx, f.run, w, w.Class(), 1, nil, f.req(), nil, faults.Effect{})
	}
	idempotent := func(w *writer) (tools.Writer, *writer) {
		w.class = policy.EffectIdempotent
		return w, w
	}
	for _, c := range []struct {
		name string
		w    func() (tools.Writer, *writer) // the tool, and what it sent
		do   func(t *testing.T, f *fixture, w tools.Writer) (*agentosv1.Result, error)
	}{
		{"the tool denies it", func() (tools.Writer, *writer) { return idempotent(&writer{deny: "not what GitHub holds"}) }, nil},
		{"the outside world refuses it for good", func() (tools.Writer, *writer) {
			return idempotent(&writer{after: &tools.RefusedError{Reason: "unprocessable"}})
		}, nil},
		{"agentd only dry-runs", func() (tools.Writer, *writer) {
			w := &offline{writer{class: policy.EffectIdempotent}}
			return w, &w.writer
		}, nil},
		{"the run was cancelled", func() (tools.Writer, *writer) { return idempotent(&writer{}) },
			func(t *testing.T, f *fixture, w tools.Writer) (*agentosv1.Result, error) {
				f.cancel(t)
				return call(f, w)
			}},
		{"the run was cancelled while the tool sent nothing", func() (tools.Writer, *writer) {
			return idempotent(&writer{release: make(chan struct{}), unsent: true})
		}, func(t *testing.T, f *fixture, w tools.Writer) (*agentosv1.Result, error) {
			type answer struct {
				res *agentosv1.Result
				err error
			}
			done := make(chan answer)
			go func() {
				res, err := call(f, w)
				done <- answer{res, err}
			}()
			f.waitClaimed(t)
			f.cancel(t)
			close(w.(*writer).release)
			a := <-done
			return a.res, a.err
		}},
		{"agentd starts after the run was cancelled", func() (tools.Writer, *writer) { return idempotent(&writer{}) },
			func(t *testing.T, f *fixture, w tools.Writer) (*agentosv1.Result, error) {
				f.cancel(t)
				return nil, f.g.RecoverEffects(ctx)
			}},
		{"a resend finds the run cancelled", func() (tools.Writer, *writer) { return idempotent(&writer{}) },
			func(t *testing.T, f *fixture, w tools.Writer) (*agentosv1.Result, error) {
				payload, err := f.g.payload(ctx, f.status(t))
				if err != nil {
					t.Fatal(err)
				}
				f.cancel(t)
				return f.g.resend(ctx, f.run, w, f.status(t), payload)
			}},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newFixture(t, false)
			w, sent := c.w()
			if res, err := f.g.record(ctx, f.run, w, w.Class(), 1, nil, f.req(), nil); res != nil || err != nil {
				t.Fatalf("record: %v %v", res, err)
			}
			f.sentThenGivenBack(t, f.req().EffectID)
			do := c.do
			if do == nil {
				do = func(t *testing.T, f *fixture, w tools.Writer) (*agentosv1.Result, error) { return call(f, w) }
			}
			res, err := do(t, f, w)
			if e := f.status(t); err != nil || res != nil && res.GetStatus() != StatusNeedsReconcile || e.Status != effectUnknown ||
				sent.sent.Load() != 0 {
				t.Fatalf("%v %v: effect %s %q, sent %d", res.GetStatus(), err, e.Status, e.Reason.String, sent.sent.Load())
			}
		})
	}
}
