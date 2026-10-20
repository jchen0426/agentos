package outbox

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"

	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
)

// temporal records the doorbells it gets, and fails them for the runs the test says.
type temporal struct {
	mu      sync.Mutex
	signals map[string]int
	runIDs  map[string]string
	fail    map[string]error
}

func (t *temporal) SignalWorkflow(_ context.Context, workflowID, runID, name string, arg any) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if name != SignalName || arg != nil {
		return errors.New("not a doorbell")
	}
	if err := t.fail[workflowID]; err != nil {
		return err
	}
	if t.signals == nil {
		t.signals, t.runIDs = map[string]int{}, map[string]string{}
	}
	t.signals[workflowID]++
	t.runIDs[workflowID] = runID
	return nil
}

func (t *temporal) count(workflowID string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.signals[workflowID]
}

func (t *temporal) failing(workflowID string, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.fail == nil {
		t.fail = map[string]error{}
	}
	t.fail[workflowID] = err
}

func open(t *testing.T, path string) *ledger.Ledger {
	t.Helper()
	l, err := ledger.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func newLedger(t *testing.T, runs ...string) (*ledger.Ledger, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ledger.db")
	l := open(t, path)
	for _, wf := range runs {
		if err := l.Q.InsertRun(context.Background(), db.InsertRunParams{WorkflowID: wf, Task: "t", ParamsJson: "{}",
			BudgetLimitMicros: 1, CreatedAt: ledger.Now()}); err != nil {
			t.Fatal(err)
		}
		if _, err := l.Q.SetRunID(context.Background(), db.SetRunIDParams{RunID: sql.NullString{String: "run-" + wf, Valid: true},
			WorkflowID: wf}); err != nil {
			t.Fatal(err)
		}
	}
	return l, path
}

func ring(t *testing.T, l *ledger.Ledger, workflowID, why string) {
	t.Helper()
	if err := l.Tx(context.Background(), func(q *db.Queries) error { return Ring(context.Background(), q, workflowID, why) }); err != nil {
		t.Fatal(err)
	}
}

func rows(t *testing.T, l *ledger.Ledger, workflowID string) []db.Outbox {
	t.Helper()
	r, err := l.DB.Query(`SELECT seq, workflow_id, kind, why, created_at, attempts, next_attempt_at, delivered_at, outcome
		FROM outbox WHERE workflow_id = ? ORDER BY seq`, workflowID)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var out []db.Outbox
	for r.Next() {
		var o db.Outbox
		if err := r.Scan(&o.Seq, &o.WorkflowID, &o.Kind, &o.Why, &o.CreatedAt, &o.Attempts, &o.NextAttemptAt, &o.DeliveredAt, &o.Outcome); err != nil {
			t.Fatal(err)
		}
		out = append(out, o)
	}
	return out
}

func only(t *testing.T, r []db.Outbox) db.Outbox {
	t.Helper()
	if len(r) != 1 {
		t.Fatalf("%d rows, want one", len(r))
	}
	return r[0]
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// Every doorbell rung for a run until it is delivered goes out as one signal, to the execution
// agentd started; once delivered, none again.
func TestOneSignalPerRun(t *testing.T) {
	l, _ := newLedger(t, "a", "b")
	tc := &temporal{}
	o := &Outbox{Ledger: l, Temporal: tc, Log: quiet()}
	ring(t, l, "a", WhyApproval)
	ring(t, l, "a", WhyBudget)
	ring(t, l, "b", WhyReconciliation)
	o.Deliver(context.Background())
	if tc.count("a") != 1 || tc.count("b") != 1 || tc.runIDs["a"] != "run-a" {
		t.Fatalf("signals %v to %v", tc.signals, tc.runIDs)
	}
	for _, r := range append(rows(t, l, "a"), rows(t, l, "b")...) {
		if !r.DeliveredAt.Valid || r.Outcome.String != "delivered" {
			t.Fatalf("row %+v", r)
		}
	}
	o.Deliver(context.Background())
	if tc.count("a") != 1 {
		t.Fatal("a delivered doorbell went out again")
	}
}

// A doorbell Temporal does not take is tried again later, until it does; meanwhile others go out.
func TestAFailedDoorbellIsTriedAgain(t *testing.T) {
	l, _ := newLedger(t, "a", "b")
	tc := &temporal{}
	tc.failing("a", errors.New("unavailable"))
	o := &Outbox{Ledger: l, Temporal: tc, Log: quiet()}
	ring(t, l, "a", WhyApproval)
	ring(t, l, "b", WhyApproval)
	wait := o.Deliver(context.Background())
	if tc.count("b") != 1 {
		t.Fatal("one run's failure held up another's doorbell")
	}
	r := only(t, rows(t, l, "a"))
	if r.DeliveredAt.Valid || r.Attempts != 1 || r.Outcome.String != "unavailable" {
		t.Fatalf("row %+v", r)
	}
	if wait < 500*time.Millisecond || wait > firstRetry {
		t.Fatalf("looks again in %v", wait)
	}
	o.Deliver(context.Background()) // not due yet
	tc.failing("a", nil)
	if tc.count("a") != 0 {
		t.Fatal("tried before it was due")
	}
	time.Sleep(wait + 50*time.Millisecond)
	o.Deliver(context.Background())
	if tc.count("a") != 1 {
		t.Fatal("not delivered once Temporal took it")
	}
}

// A run that has ended waits for nothing: its doorbell is dropped, not tried forever.
func TestARunThatEndedTakesNoDoorbell(t *testing.T) {
	l, _ := newLedger(t, "a")
	tc := &temporal{}
	tc.failing("a", serviceerror.NewNotFound("workflow execution already completed"))
	o := &Outbox{Ledger: l, Temporal: tc, Log: quiet()}
	ring(t, l, "a", WhyApproval)
	o.Deliver(context.Background())
	if r := only(t, rows(t, l, "a")); !r.DeliveredAt.Valid || r.Outcome.String != "run_ended" {
		t.Fatalf("row %+v", r)
	}
}

// T14 below the black-box test: a doorbell agentd wrote and never delivered (it stopped first)
// is delivered by the next agentd on the ledger.
func TestDoorbellsOutliveTheProcess(t *testing.T) {
	l, path := newLedger(t, "a")
	tc := &temporal{}
	held := true
	o := &Outbox{Ledger: l, Temporal: tc, Hold: func() bool { return held }, Log: quiet()}
	ring(t, l, "a", WhyApproval)
	o.Deliver(context.Background())
	if tc.count("a") != 0 {
		t.Fatal("delivered while held")
	}
	l.Close()
	next := &Outbox{Ledger: open(t, path), Temporal: tc, Log: quiet()}
	next.Deliver(context.Background())
	if tc.count("a") != 1 {
		t.Fatal("the next agentd did not deliver it")
	}
}

// Run delivers a doorbell as soon as it is rung and poked, not at its next look.
func TestPokeWakesTheOutbox(t *testing.T) {
	l, _ := newLedger(t, "a")
	tc := &temporal{}
	o := &Outbox{Ledger: l, Temporal: tc, Log: quiet()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go o.Run(ctx)
	time.Sleep(100 * time.Millisecond) // Run is idle now, for a minute
	ring(t, l, "a", WhyApproval)
	o.Poke()
	deadline := time.Now().Add(2 * time.Second)
	for tc.count("a") == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if tc.count("a") != 1 {
		t.Fatal("the poke did not wake the outbox")
	}
}
