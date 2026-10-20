// Package outbox delivers doorbells (design doc, section 10): the signal that wakes a run waiting
// for an approval, a reconciliation or budget. A doorbell is written in the same transaction as
// the change it announces, then signalled until Temporal has it or the run has ended: agentd
// stopping in between delays a doorbell, and never loses it. A doorbell only wakes the run, which
// asks again and gets its answer from agentd's own tables, so one delivered twice does no harm.
package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.temporal.io/api/serviceerror"

	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
)

// SignalName is the workflows' doorbell signal.
const SignalName = "doorbell"

// Why a doorbell rings.
const (
	WhyApproval       = "approval"
	WhyReconciliation = "reconciliation"
	WhyBudget         = "budget"
)

// Ring records a doorbell for the run through q, in the transaction of the change it announces.
// Poke the Outbox once that has committed, to have it delivered at once.
func Ring(ctx context.Context, q *db.Queries, workflowID, why string) error {
	return q.InsertDoorbell(ctx, db.InsertDoorbellParams{
		WorkflowID: workflowID, Why: why, CreatedAt: ledger.Now(), NextAttemptAt: ledger.Stamp(time.Now()),
	})
}

// Signaler is the part of Temporal's client the outbox uses.
type Signaler interface {
	SignalWorkflow(ctx context.Context, workflowID, runID, signalName string, arg any) error
}

// Outbox delivers the doorbells in the ledger.
type Outbox struct {
	Ledger   *ledger.Ledger
	Temporal Signaler
	// Hold is for tests only (agentd's fault script): while it reports true, nothing is delivered.
	Hold func() bool
	Log  *slog.Logger

	once sync.Once
	wake chan struct{}
}

// Bounds of a delivery: one signal's wait for Temporal, the wait before the first retry and the
// longest wait between retries. Nothing is due: the outbox still looks again after idle.
const (
	signalTimeout = 10 * time.Second
	firstRetry    = time.Second
	maxRetry      = time.Minute
	idle          = time.Minute
)

func (o *Outbox) init() { o.once.Do(func() { o.wake = make(chan struct{}, 1) }) }

// Poke has the outbox look at once: a doorbell was just rung.
func (o *Outbox) Poke() {
	o.init()
	select {
	case o.wake <- struct{}{}:
	default:
	}
}

// Run delivers doorbells as they come due, until ctx ends.
func (o *Outbox) Run(ctx context.Context) {
	o.init()
	for {
		wait := o.Deliver(ctx)
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-o.wake:
			t.Stop()
		case <-t.C:
		}
	}
}

// Deliver signals every run with a doorbell due, once for all of its doorbells, and returns how
// long until the next one is due.
func (o *Outbox) Deliver(ctx context.Context) time.Duration {
	if o.Hold != nil && o.Hold() {
		return 100 * time.Millisecond
	}
	rows, err := o.Ledger.Q.ListDueDoorbells(ctx, ledger.Stamp(time.Now()))
	if err != nil {
		o.Log.Error("doorbells: cannot read the outbox", "error", err)
		return firstRetry
	}
	type due struct {
		last     int64 // the newest doorbell: the signal covers it and every older one
		attempts int64
		why      []string
	}
	runs := map[string]*due{}
	var order []string
	for _, r := range rows {
		d, ok := runs[r.WorkflowID]
		if !ok {
			d = &due{}
			runs[r.WorkflowID] = d
			order = append(order, r.WorkflowID)
		}
		d.last, d.attempts, d.why = max(d.last, r.Seq), max(d.attempts, r.Attempts), append(d.why, r.Why)
	}
	for _, wf := range order {
		if ctx.Err() != nil {
			return 0
		}
		o.deliverRun(ctx, wf, runs[wf].last, runs[wf].attempts, runs[wf].why)
	}
	// Not at once even when a doorbell is due: one whose failed delivery could not be recorded
	// stays due, and the outbox must not spin on it.
	return max(o.untilNext(ctx), 20*time.Millisecond)
}

func (o *Outbox) deliverRun(ctx context.Context, workflowID string, last, attempts int64, why []string) {
	log := o.Log.With("workflow_id", workflowID, "why", why)
	err := o.signal(ctx, workflowID)
	var notFound *serviceerror.NotFound
	outcome := "delivered"
	switch {
	case errors.As(err, &notFound), errors.Is(err, errNoRun):
		// The run has ended (or is past Temporal's retention): nothing waits for its doorbell.
		outcome = "run_ended"
	case err != nil:
		wait := min(firstRetry<<min(attempts, 16), maxRetry)
		log.Warn("doorbell not delivered; trying again", "attempts", attempts+1, "retry_in", wait.String(), "error", err)
		if _, rerr := o.Ledger.Q.RetryDoorbells(ctx, db.RetryDoorbellsParams{
			NextAttemptAt: ledger.Stamp(time.Now().Add(wait)), Outcome: sql.NullString{String: err.Error(), Valid: true},
			WorkflowID: workflowID, Seq: last,
		}); rerr != nil {
			log.Error("doorbells: cannot record a failed delivery", "error", rerr)
		}
		return
	}
	if _, err := o.Ledger.Q.MarkDoorbellsDelivered(ctx, db.MarkDoorbellsDeliveredParams{
		DeliveredAt: sql.NullString{String: ledger.Now(), Valid: true}, Outcome: sql.NullString{String: outcome, Valid: true},
		WorkflowID: workflowID, Seq: last,
	}); err != nil {
		// Delivered again later: a doorbell twice only wakes the run twice.
		log.Error("doorbells: cannot record a delivery", "error", err)
		return
	}
	log.Info("doorbell "+outcome, "attempts", attempts+1)
}

// errNoRun: the ledger has no such run, so nothing can wait for its doorbell.
var errNoRun = errors.New("no such run")

func (o *Outbox) signal(ctx context.Context, workflowID string) error {
	r, err := o.Ledger.Q.GetRun(ctx, workflowID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return errNoRun
	case err != nil:
		return fmt.Errorf("read the run: %w", err)
	case !r.RunID.Valid:
		return errors.New("the run is still starting")
	}
	ctx, cancel := context.WithTimeout(ctx, signalTimeout)
	defer cancel()
	// The execution agentd started: a later one under the same workflow ID is not this run.
	return o.Temporal.SignalWorkflow(ctx, workflowID, r.RunID.String, SignalName, nil)
}

func (o *Outbox) untilNext(ctx context.Context) time.Duration {
	next, err := o.Ledger.Q.NextDoorbellDue(ctx)
	if err != nil || next == "" {
		return idle
	}
	at, err := time.Parse(time.RFC3339Nano, next)
	if err != nil {
		return idle
	}
	return max(time.Until(at), 0)
}
