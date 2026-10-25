package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/audit"
	"github.com/jchen0426/agentos/internal/budget"
	"github.com/jchen0426/agentos/internal/faults"
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
	"github.com/jchen0426/agentos/internal/models"
	"github.com/jchen0426/agentos/internal/policy"
	"github.com/jchen0426/agentos/internal/pricing"
)

// counted wraps an adapter and counts what it is asked; countErr, when set, is what counting
// answers.
type counted struct {
	models.Adapter
	mu            sync.Mutex
	counts, calls int
	countErr      error
}

func (c *counted) CountTokens(ctx context.Context, req models.Request) (int64, error) {
	c.mu.Lock()
	c.counts++
	c.mu.Unlock()
	if c.countErr != nil {
		return 0, c.countErr
	}
	return c.Adapter.CountTokens(ctx, req)
}

func (c *counted) Call(ctx context.Context, req models.Request) (models.Response, error) {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	return c.Adapter.Call(ctx, req)
}

// fallbackGateway is a gateway with two fake providers, fake and other, the second the first's
// cross-provider fallback, and a run with budget to spare.
func fallbackGateway(t *testing.T) (*Gateway, db.Run, models.Route) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	schema, err := filepath.Abs("../../config/schemas/audit_report.json")
	if err != nil {
		t.Fatal(err)
	}
	write := func(name, body string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	mods, err := models.Load(write("models.yaml", fmt.Sprintf(`
providers:
  fake:  {adapter: fake}
  other: {adapter: fake}
tasks:
  audit_v1:
    model: fake/audit-fixture
    max_tokens: 4096
    inputs: [diff_ref]
    output_schema: %s
    blobs: [patch]
    cross_provider_fallback: [other/audit-fixture]
`, schema)))
	if err != nil {
		t.Fatal(err)
	}
	prices, err := pricing.Load(write("prices.yaml", `
price_table_version: test-1
models:
  fake/audit-fixture:  {input: 1.00, output: 5.00}
  other/audit-fixture: {input: 3.00, output: 15.00}
`))
	if err != nil {
		t.Fatal(err)
	}
	l, err := ledger.Open(ctx, filepath.Join(dir, "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	if err := l.Q.InsertRun(ctx, db.InsertRunParams{WorkflowID: "wf", Task: "security-audit", ParamsJson: "{}",
		BudgetLimitMicros: 100_000_000, CreatedAt: ledger.Now()}); err != nil {
		t.Fatal(err)
	}
	route, err := mods.Route("audit_v1")
	if err != nil {
		t.Fatal(err)
	}
	g := &Gateway{Ledger: l, Models: mods, Prices: prices, Budget: &budget.Budget{Ledger: l},
		Policy: &policy.Policy{Models: map[string]policy.Model{"audit_v1": {AllowedProviders: []string{"fake", "other"}}}},
		Limits: LoopLimits{Attempts: 10}, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	return g, db.Run{WorkflowID: "wf"}, route
}

// diffCall is the security audit's model call as the Worker makes it: audit_v1, its one input the
// run's own diff.
func diffCall(t *testing.T, g *Gateway, run db.Run) (*agentosv1.Call, map[string]any, []byte) {
	t.Helper()
	ref, err := audit.PutRunBlob(context.Background(), g.Ledger, run.WorkflowID, []byte("diff --git a/app.py b/app.py\n"))
	if err != nil {
		t.Fatal(err)
	}
	args := map[string]any{"diff_ref": ref}
	canon, err := encodeCanonical(args)
	if err != nil {
		t.Fatal(err)
	}
	return &agentosv1.Call{Kind: "model", Name: "audit_v1", ArgsJson: string(canon)}, args, canon
}

// An answer that broke the schema leaves too little of the attempt for the second one: the
// next attempt sends the second generation, not the first again, and no attempt after it pays
// for anything (the call's answer is the invalid one).
func TestReaskCarriesAcrossAttempts(t *testing.T) {
	g, run, route := fallbackGateway(t)
	fake := &counted{Adapter: models.Fake{}}
	g.Adapters = map[string]models.Adapter{"fake": fake, "other": fake}
	retry := &temporal.RetryPolicy{MaximumAttempts: 5}
	fx := faults.Effect{ProviderBadOutput: true} // every attempt's first request breaks the schema
	for attempt := int32(1); attempt <= 5; attempt++ {
		// A minute left, and a request for 4096 tokens may take about two.
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		info := activity.Info{ActivityID: "1:audit_v1", Attempt: attempt, RetryPolicy: retry, ScheduledTime: time.Unix(1000, 0)}
		resp, err := g.modelCallOnce(ctx, info, run, route, fake, map[string]any{"diff": "x"}, "args", fx)
		cancel()
		switch {
		case attempt == 1 && err == nil:
			t.Fatalf("attempt 1 sent the second generation with too little time left: %+v", resp)
		case attempt > 1 && (err != nil || resp.Status != StatusInvalidOutput):
			t.Errorf("attempt %d: %+v, %v", attempt, resp, err)
		}
	}
	if fake.calls != generations {
		t.Errorf("%d requests for one call, want %d", fake.calls, generations)
	}
}

// Declined while counting: a refusal like any other, so the fallback takes the call, nothing
// is reserved for the request that never went out, and a retry does not ask again.
func TestDeclinedWhileCountingFallsBack(t *testing.T) {
	g, run, route := fallbackGateway(t)
	primary := &counted{Adapter: models.Fake{}, countErr: &models.Declined{Category: "invalid_prompt", Explanation: "flagged"}}
	fallback := &counted{Adapter: models.Fake{}}
	g.Adapters = map[string]models.Adapter{"fake": primary, "other": fallback}
	ctx := context.Background()
	retry := &temporal.RetryPolicy{MaximumAttempts: 5}
	resp, err := g.modelCallOnce(ctx, activity.Info{ActivityID: "2:audit_v1", Attempt: 1, RetryPolicy: retry},
		run, route, primary, map[string]any{"diff": "y"}, "args2", faults.Effect{})
	if err != nil || resp.Status != StatusOK || primary.calls != 0 || fallback.calls != 1 {
		t.Fatalf("%+v, %v; primary sent %d, fallback %d", resp, err, primary.calls, fallback.calls)
	}
	var others, all int
	if err := g.Ledger.DB.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(model = 'other/audit-fixture'), 0)
		FROM budget_entries WHERE workflow_id = 'wf'`).Scan(&all, &others); err != nil {
		t.Fatal(err)
	}
	if all == 0 || others != all {
		t.Errorf("%d budget entries, %d of them the fallback's: one is for the request that never went out", all, others)
	}
	// The answer's completion lost: the retry serves it, asking neither provider.
	resp, err = g.modelCallOnce(ctx, activity.Info{ActivityID: "2:audit_v1", Attempt: 2, RetryPolicy: retry},
		run, route, primary, map[string]any{"diff": "y"}, "args2", faults.Effect{})
	if err != nil || resp.Status != StatusOK || primary.counts != 1 || fallback.calls != 1 {
		t.Errorf("retry: %+v, %v; primary counted %d, fallback sent %d", resp, err, primary.counts, fallback.calls)
	}
}

// gated holds Call until released, so that a second attempt can join the first's execution.
type gated struct {
	models.Adapter
	started, release chan struct{}
	once             sync.Once
}

func (g *gated) Call(ctx context.Context, req models.Request) (models.Response, error) {
	g.once.Do(func() { close(g.started) })
	<-g.release
	return g.Adapter.Call(ctx, req)
}

// The last attempt joins an earlier attempt's execution, in which the fallback's 429 is an
// error to retry: the last attempt keeps the refusal instead.
func TestJoinedLastAttemptKeepsTheRefusal(t *testing.T) {
	g, run, route := fallbackGateway(t)
	gate := &gated{Adapter: models.Fake{}, started: make(chan struct{}), release: make(chan struct{})}
	g.Adapters = map[string]models.Adapter{"fake": gate, "other": models.Fake{}}
	fx := faults.Effect{ProviderRefuses: "fake", ProviderStatus: 429, ProviderStatusOn: "other"}
	retry := &temporal.RetryPolicy{MaximumAttempts: 2}
	args := map[string]any{"diff": "z"}
	type outcome struct {
		status string
		err    error
	}
	call := func(attempt int32, out chan<- outcome) {
		r, err := g.callModel(context.Background(), activity.Info{ActivityID: "3:audit_v1", Attempt: attempt, RetryPolicy: retry},
			run, route, args, "args3", fx)
		o := outcome{err: err}
		if r != nil {
			o.status = r.Status
		}
		out <- o
	}
	first, last := make(chan outcome, 1), make(chan outcome, 1)
	go call(1, first)
	<-gate.started
	go call(2, last)
	time.Sleep(200 * time.Millisecond) // the last attempt joins the first's execution
	close(gate.release)
	if o := <-first; o.err == nil {
		t.Errorf("attempt 1 is not the last: the fallback's 429 is retried, but got %+v", o)
	}
	if o := <-last; o.err != nil || o.status != StatusRefused {
		t.Errorf("the last attempt: %+v", o)
	}
}

func TestDetachFollowsTheActivityDeadline(t *testing.T) {
	ctx := context.Background()
	soon := time.Now().Add(2 * time.Minute)
	for name, c := range map[string]struct {
		info activity.Info
		want time.Time
	}{
		"start to close":       {activity.Info{StartToCloseTimeout: 10 * time.Minute}, time.Now().Add(10 * time.Minute)},
		"schedule to close":    {activity.Info{StartToCloseTimeout: 10 * time.Minute, Deadline: soon}, soon},
		"a deadline long past": {activity.Info{StartToCloseTimeout: 10 * time.Minute, Deadline: time.Now().Add(-time.Hour)}, time.Now().Add(10 * time.Minute)},
	} {
		exec, cancel := detach(ctx, c.info)
		got, _ := exec.Deadline()
		cancel()
		if d := got.Sub(c.want); d < -time.Second || d > time.Second {
			t.Errorf("%s: deadline %v, want about %v", name, got, c.want)
		}
	}
}

// A model call's call.started comes before its request goes to the provider: one that cannot be
// written sends nothing.
func TestNoModelRequestBeforeItsRecord(t *testing.T) {
	g, run, _ := fallbackGateway(t)
	fake := &counted{Adapter: models.Fake{}}
	g.Adapters = map[string]models.Adapter{"fake": fake, "other": fake}
	call, args, canon := diffCall(t, g, run)
	info := activity.Info{ActivityID: "1:audit_v1", Attempt: 1, RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 5},
		ScheduledTime: time.Unix(1000, 0)}
	info.WorkflowExecution.ID = run.WorkflowID
	drop := failAppends(t, g.Ledger, "call.started")
	rec := &callRecord{argsSHA256: sha256Hex(canon)}
	var app *temporal.ApplicationError
	if _, err := g.dispatch(context.Background(), info, run, call, args, canon, faults.Effect{}, rec); !errors.As(err, &app) ||
		app.Type() != "AuditUnavailable" || fake.calls != 0 || rec.startedSeq != 0 {
		t.Fatalf("got %v, %d requests sent", err, fake.calls)
	}
	drop()
	rec = &callRecord{argsSHA256: sha256Hex(canon)}
	if _, err := g.dispatch(context.Background(), info, run, call, args, canon, faults.Effect{}, rec); err != nil || fake.calls == 0 {
		t.Fatalf("%v, %d requests", err, fake.calls)
	}
	started, err := g.Ledger.Q.ListAuditRecords(context.Background(), db.ListAuditRecordsParams{Seq: rec.startedSeq, Limit: 1})
	if err != nil || len(started) != 1 || started[0].Type != "call.started" || !strings.Contains(started[0].Payload, `"kind":"model"`) {
		t.Fatalf("call.started %+v %v", started, err)
	}
	var requested string
	if err := g.Ledger.DB.QueryRow(`SELECT MIN(started_at) FROM model_calls`).Scan(&requested); err != nil || requested < started[0].At {
		t.Fatalf("the first request at %s, its record at %s: %v", requested, started[0].At, err)
	}
}
