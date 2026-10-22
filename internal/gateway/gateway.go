// Package gateway implements Invoke, the only activity agentd registers on the "gateway"
// task queue. Who is calling, whether the call is allowed and whether it is affordable are
// all decided here (design doc, section 7).
package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"golang.org/x/sync/singleflight"
	"golang.org/x/time/rate"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/audit"
	"github.com/jchen0426/agentos/internal/budget"
	"github.com/jchen0426/agentos/internal/faults"
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
	"github.com/jchen0426/agentos/internal/models"
	"github.com/jchen0426/agentos/internal/policy"
	"github.com/jchen0426/agentos/internal/pricing"
	"github.com/jchen0426/agentos/internal/tasks"
	"github.com/jchen0426/agentos/internal/tools"
)

// ActivityName is the activity type the workflows schedule.
const ActivityName = "Invoke"

// Result statuses (proto Result.status).
const (
	StatusOK              = "ok"
	StatusNeedsApproval   = "needs_approval"
	StatusNeedsReconcile  = "needs_reconcile"
	StatusBudgetExhausted = "budget_exhausted"
	StatusDenied          = "denied"
	StatusInvalidOutput   = "invalid_output" // the model's answer broke the task's output schema, twice
	StatusRefused         = "refused"        // the model, and every cross-provider fallback, declined to answer
)

type Gateway struct {
	Ledger   *ledger.Ledger
	Policy   *policy.Policy
	Tasks    tasks.Registry // for each run's scope
	Models   *models.Config
	Prices   *pricing.Table
	Budget   *budget.Budget
	Adapters map[string]models.Adapter // by provider name in models.yaml
	Limiters map[string]*rate.Limiter  // by provider name; none for a provider without a limit
	Limits   LoopLimits
	Tools    tools.Registry
	Faults   *faults.Script // nil unless agentd runs with --fault-script
	Log      *slog.Logger
	// Incarnation names this agentd process in the effects it claims. One data directory has
	// one agentd at a time, so an effect another incarnation claimed has nobody sending it.
	Incarnation string

	// Doorbells delivers the doorbells the gateway rings (an approval's expiry, a decision). With
	// none, they wait in the ledger for the outbox to look.
	Doorbells interface{ Poke() }

	runIDWait  time.Duration      // how long a call waits for its run's ID; 0 is defaultRunIDWait
	modelCalls singleflight.Group // in-flight model calls, by workflow and activity ID
	effects    singleflight.Group // in-flight effects and their recoveries, by effect ID
	executions sync.WaitGroup     // detached executions, drained on shutdown

	expiryOnce sync.Once
	expiryCh   chan struct{} // wakes ExpireApprovals: an approval was made
	recording  keyedLock     // a run's new effects of a write that needs approval, one at a time (record)

	sendMu  sync.Mutex      // makes claiming an effect and marking it sent one step
	sending map[string]bool // effect IDs this process has claimed and is sending

	attempts attempts // executing now, and those whose record a fault lost (Executing, Faulted)

	// wait is how recovery waits for a request that may still take effect (sleep); nil is the
	// system's clock. A test that moves ledger.Clock sets it.
	wait func(ctx context.Context, d time.Duration) error
}

// LoopLimits cap the logical calls of one run, and the attempts of one call; 0 is no limit.
type LoopLimits struct {
	ModelCalls    int64
	GatewayCalls  int64
	SameToolCalls int64
	Attempts      int64
}

// Drain waits up to timeout for detached executions to finish recording their results.
// It reports whether they all did.
func (g *Gateway) Drain(timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		g.executions.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// track runs one detached execution so that Drain waits for it.
func (g *Gateway) track(fn func() (any, error)) (any, error) {
	g.executions.Add(1)
	defer g.executions.Done()
	return fn()
}

// Invoke is the only activity agentd registers on the "gateway" task queue. Each attempt ends with
// its record in the audit log, and the result it returns carries that record (recordCall). From
// before its call.started to after its record, the attempt is executing (Executing): verify takes
// an open call.started of one for one in flight, whatever its run's state.
func (g *Gateway) Invoke(ctx context.Context, c *agentosv1.Call) (*agentosv1.Result, error) {
	info := activity.GetInfo(ctx)
	log := g.Log.With("workflow_id", info.WorkflowExecution.ID, "activity_id", info.ActivityID,
		"attempt", info.Attempt, "kind", c.GetKind(), "name", c.GetName())
	key := attemptKey{info.WorkflowExecution.ID, info.ActivityID, int64(info.Attempt)}
	g.attempts.begin(key)
	defer g.attempts.end(key)
	rec := &callRecord{}
	recording := false
	defer func() {
		// A panic in a tool or an adapter: the SDK recovers it and fails the attempt, and the call's
		// record would never be written, its call.started left open. It is recorded here, with the
		// panic as its error, and the panic goes on to the SDK.
		if p := recover(); p != nil {
			if !recording {
				log.Error("the call panicked", "panic", fmt.Sprint(p))
				_, _ = g.recordCall(ctx, info, c, log, rec, nil, fmt.Errorf("panic: %v", p))
			}
			panic(p)
		}
	}()
	res, err := g.invoke(ctx, info, c, log, rec)
	recording = true
	return g.recordCall(ctx, info, c, log, rec, res, err)
}

// attemptKey names an attempt of a call: its run, its activity, which attempt.
type attemptKey struct {
	workflowID, activityID string
	attempt                int64
}

// attempts are the gateway's attempts as verify asks of them: those executing in this agentd now,
// and those whose call record a fault lost after their call.started (the write failed after its
// tries; agentd logs it), which verify reports apart from mismatches. At most maxFaulted of the
// latter: past them, verify finds a mismatch, which says no less.
type attempts struct {
	mu        sync.Mutex
	executing map[attemptKey]int
	faulted   map[attemptKey]string
}

const maxFaulted = 1000

func (a *attempts) begin(k attemptKey) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.executing == nil {
		a.executing = map[attemptKey]int{}
	}
	a.executing[k]++
}

func (a *attempts) end(k attemptKey) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.executing[k]--; a.executing[k] <= 0 {
		delete(a.executing, k)
	}
}

func (a *attempts) fault(k attemptKey, why string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.faulted == nil {
		a.faulted = map[attemptKey]string{}
	}
	if len(a.faulted) < maxFaulted {
		a.faulted[k] = why
	}
}

// Executing reports whether the attempt is executing in this agentd now: from before its
// call.started to after its call record.
func (g *Gateway) Executing(workflowID, activityID string, attempt int64) bool {
	g.attempts.mu.Lock()
	defer g.attempts.mu.Unlock()
	return g.attempts.executing[attemptKey{workflowID, activityID, attempt}] > 0
}

// Faulted is why the attempt's call record could not be written, after its call.started, by this
// agentd; false for an attempt it has no such fault of.
func (g *Gateway) Faulted(workflowID, activityID string, attempt int64) (string, bool) {
	g.attempts.mu.Lock()
	defer g.attempts.mu.Unlock()
	why, ok := g.attempts.faulted[attemptKey{workflowID, activityID, attempt}]
	return why, ok
}

func (g *Gateway) invoke(ctx context.Context, info activity.Info, c *agentosv1.Call, log *slog.Logger,
	rec *callRecord) (*agentosv1.Result, error) {
	// Decoded before anything else for the call's record; refused below, in its turn.
	args, canon, argsErr := canonicalArgs(c.GetArgsJson())
	if argsErr == nil {
		rec.argsSHA256 = sha256Hex(canon)
	} else {
		rec.rawArgsSHA256 = sha256Hex([]byte(c.GetArgsJson()))
	}

	// Only executions started through Control: the caller's identity comes from Temporal's
	// activity info, never from fields the workflow fills in.
	run, err := g.Ledger.LookupRun(ctx, info.WorkflowExecution.ID, info.WorkflowExecution.RunID)
	var waited time.Duration
	if errors.Is(err, ledger.ErrRunStarting) {
		// StartRun records the run ID once Temporal has started the run, and a Worker already
		// polling can send the run's first call before that. Refused, the call would lose one
		// of its attempts and wait out Temporal's backoff: it waits for the run ID instead.
		start := time.Now()
		stop := heartbeat(ctx, false)
		run, err = g.awaitRunID(ctx, info.WorkflowExecution.ID, info.WorkflowExecution.RunID)
		stop()
		waited = time.Since(start)
		if err == nil {
			log.Info("invoke waited for the run ID", "waited_ms", waited.Milliseconds())
		}
	}
	switch {
	case errors.Is(err, ledger.ErrRunStarting):
		log.Warn("invoke refused: the run ID is still not recorded", "run_id", info.WorkflowExecution.RunID,
			"waited_ms", waited.Milliseconds())
		rec.temporalRunID, rec.unknown = info.WorkflowExecution.RunID, true
		return nil, err // retried: StartRun, or RecoverStarts when agentd starts, may record it yet
	case errors.Is(err, ledger.ErrUnknownRun):
		log.Warn("invoke from an unknown run", "run_id", info.WorkflowExecution.RunID)
		rec.temporalRunID, rec.unknown = info.WorkflowExecution.RunID, true
		return nil, temporal.NewNonRetryableApplicationError("unknown run", "UnknownRun", err)
	case err != nil:
		return nil, err
	}
	if run.CancelledAt.Valid {
		// CancelRun recorded this before asking Temporal: whatever the workflow does with the
		// cancel request, nothing more of this run goes out.
		log.Info("invoke refused: run cancelled", "cancelled_at", run.CancelledAt.String)
		return result(StatusDenied, map[string]any{"reason": "run cancelled"})
	}
	if argsErr != nil {
		return nil, temporal.NewNonRetryableApplicationError("args_json must be a JSON object", "BadArguments", argsErr)
	}
	if limit := g.Limits.Attempts; limit > 0 && int64(info.Attempt) > limit {
		// The Worker writes the retry policy; a call retried without end stops here.
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("more than %d attempts of one call", limit), "TooManyAttempts", nil)
	}
	if reason, err := g.guard(ctx, run, info, c, canon, rec); err != nil {
		return nil, err
	} else if reason != "" {
		log.Warn("invoke refused: loop guard", "reason", reason)
		return result(StatusDenied, map[string]any{"reason": reason})
	}

	fx := g.Faults.Match(run.WorkflowID, c.GetName(), info.Attempt)
	stop := heartbeat(ctx, fx.NoHeartbeat)
	defer stop()

	log.Info("invoke started", "no_heartbeat", fx.NoHeartbeat)
	rec.dispatched = true
	res, err := g.dispatch(ctx, info, run, c, args, canon, fx, rec)
	if err != nil {
		log.Info("invoke failed", "error", err)
		return nil, err
	}
	log.Info("invoke finished", "status", res.GetStatus())
	return res, nil
}

// callRecord is what the audit log records of one attempt of a call, gathered as Invoke goes.
type callRecord struct {
	argsSHA256    string          // of the canonical arguments
	rawArgsSHA256 string          // of args_json as it came, when it is not a JSON object
	temporalRunID string          // the execution's, when it is not one agentd started
	unknown       bool            // nobody vouches for the caller: the record is by unknown-run
	policy        *policy.Verdict // the tool's or the model's, once the policy was asked
	dispatched    bool            // the loop guard counted the call: its outcome goes with its record
	head          *audit.Head     // the record, when the loop guard wrote it with its refusal
	startedSeq    int64           // its call.started, once the policy let it past
	scheduledAt   string          // its logical call's, once the loop guard counted it (calls)
}

// agentdStatuses are the statuses whose reason agentd wrote: the reason an ok result carries is a
// tool's or the outside world's, and stays out of the record.
var agentdStatuses = map[string]bool{
	StatusDenied: true, StatusRejected: true, StatusExpired: true, StatusNeedsApproval: true, StatusNeedsReconcile: true,
	StatusBudgetExhausted: true, StatusRefused: true, StatusEdited: true, StatusInvalidOutput: true,
}

// callFields are what call.started and call say of an attempt of c: which attempt, of what, with
// what arguments, and the policy's decision of it once it was asked.
func callFields(info activity.Info, c *agentosv1.Call, rec *callRecord) map[string]any {
	p := map[string]any{"activity_id": audit.Text(info.ActivityID), "attempt": info.Attempt,
		"kind": audit.Text(c.GetKind()), "name": audit.Text(c.GetName())}
	if rec.argsSHA256 != "" {
		p["args_sha256"] = rec.argsSHA256
	} else {
		p["raw_args_sha256"] = rec.rawArgsSHA256
	}
	if rec.scheduledAt != "" {
		p["scheduled_at"] = rec.scheduledAt
	}
	if v := rec.policy; v != nil {
		verdict := map[string]any{"decision": v.Decision.String()}
		if v.Reason != "" {
			verdict["reason"] = audit.Text(v.Reason)
		}
		p["policy"] = verdict
	}
	return p
}

// callEvent is the record of one attempt of c that ended with status and value (a result), or with
// callErr. A result's record holds the sha256 of its value (audit.ValueSHA256), which the anchor in
// Temporal's history lets a verifier check against the value the workflow got.
func callEvent(info activity.Info, c *agentosv1.Call, rec *callRecord, status string, value map[string]any,
	callErr error) (audit.Event, error) {
	p := callFields(info, c, rec)
	p["status"] = status
	if rec.temporalRunID != "" {
		p["temporal_run_id"] = audit.Text(rec.temporalRunID)
	}
	if rec.startedSeq != 0 {
		p["started_seq"] = rec.startedSeq
	}
	if value != nil && rec.dispatched {
		p["outcome"] = status // what its record's transaction sets as the call's outcome (calls), which the loop guard reads
	}
	if value != nil {
		sum, err := audit.ValueSHA256(value)
		if err != nil {
			return audit.Event{}, err
		}
		p["result_sha256"] = sum
		if reason, ok := value["reason"].(string); ok && agentdStatuses[status] {
			p["reason"] = audit.Text(reason)
		}
	} else {
		p["error"] = audit.Text(callErr.Error())
		var failed *temporal.ApplicationError
		if errors.As(callErr, &failed) && failed.NonRetryable() {
			p["error_type"] = failed.Type()
		}
	}
	actor := audit.Run
	if rec.unknown {
		actor = audit.UnknownRun
	}
	return audit.Event{WorkflowID: info.WorkflowExecution.ID, Type: "call", Actor: actor, Payload: p}, nil
}

// recordCall appends the attempt's record to the audit log once it has ended, whatever ended it: in
// one transaction with the call's outcome, which the loop guard reads, when there is one. The
// result returns with that record, the chain's head after it, in _audit: Temporal's history keeps
// it, out of reach of whoever can write agentd's ledger, so a chain rewritten after the call no
// longer matches its anchors (agentctl audit verify). An attempt whose record cannot be written,
// tried a few times, ends with a retryable error (AuditUnavailable) when it has a result, or a
// retryable error, to give: the call's next attempt answers from the ledger, and records it. One
// refused for good (UnknownRun, BadArguments, TooManyAttempts) ends with its own error all the
// same, its record lost and logged: a retry would be refused again, and the cap on attempts must
// hold.
func (g *Gateway) recordCall(ctx context.Context, info activity.Info, c *agentosv1.Call, log *slog.Logger,
	rec *callRecord, res *agentosv1.Result, callErr error) (*agentosv1.Result, error) {
	var value map[string]any
	if res != nil {
		var err error
		if value, err = ledger.DecodeObject([]byte(res.GetValueJson())); err != nil {
			return nil, fmt.Errorf("the call's result: %w", err)
		}
		delete(value, "_audit")
	}
	head := rec.head
	if head == nil {
		status := "error"
		if res != nil {
			status = res.GetStatus()
		}
		ev, err := callEvent(info, c, rec, status, value, callErr)
		if err != nil {
			return nil, err
		}
		h, err := g.appendCall(ctx, info, rec, res, ev)
		if err != nil {
			log.Error("could not record the call in the audit log", "error", err, "status", status)
			if rec.startedSeq != 0 { // its call.started stays open: a fault, which verify reports apart
				g.attempts.fault(attemptKey{info.WorkflowExecution.ID, info.ActivityID, int64(info.Attempt)}, err.Error())
			}
			var refused *temporal.ApplicationError
			if res == nil && errors.As(callErr, &refused) && refused.NonRetryable() {
				return nil, callErr
			}
			return nil, temporal.NewApplicationError("agentd could not record the call; retry", "AuditUnavailable", err)
		}
		head = &h
	}
	if res == nil {
		return nil, callErr
	}
	value["_audit"] = *head // a new result: res may be another attempt's, which joined the same execution
	return result(res.GetStatus(), value)
}

// appendTries bounds how often appendCall tries before the attempt fails for want of its record,
// and appendTurnWait how long each try waits for the write turn.
const appendTries = 5

var appendTurnWait = 10 * time.Second

// appendCall appends a call's record, with its outcome when the loop guard counted the call, trying
// again a few times, each try waiting for the write turn at most appendTurnWait, and heartbeating
// meanwhile: a write that fails once (the disk, a lock) need not cost the attempt, and one that
// waits does not leave Temporal thinking the attempt is gone.
func (g *Gateway) appendCall(ctx context.Context, info activity.Info, rec *callRecord, res *agentosv1.Result,
	ev audit.Event) (audit.Head, error) {
	var head audit.Head
	err := g.appendTried(ctx, func(tx context.Context, q *db.Queries) error {
		if res != nil && rec.dispatched {
			// The loop guard counts a call that only waited (for an approval, a reconciliation or
			// budget) as a step, not as a repeat or a model call.
			if err := q.SetCallOutcome(tx, db.SetCallOutcomeParams{
				Outcome: sql.NullString{String: res.GetStatus(), Valid: true}, WorkflowID: info.WorkflowExecution.ID,
				ActivityID: info.ActivityID, ScheduledAt: info.ScheduledTime.UTC().Format(time.RFC3339Nano),
			}); err != nil {
				return err
			}
		}
		var err error
		head, err = audit.Append(tx, q, ev)
		return err
	})
	return head, err
}

// appendTried runs write, a transaction of an attempt's records, as appendCall describes: on the
// write turn, waiting at most appendTurnWait for it each try, tried appendTries times, with the
// attempt kept alive meanwhile, and whether or not the attempt has ended.
func (g *Gateway) appendTried(ctx context.Context, write func(tx context.Context, q *db.Queries) error) error {
	base := context.WithoutCancel(ctx) // the attempt may have ended: its record is written all the same
	if activity.IsActivity(ctx) {
		// The record waits for its turn, and is tried again, with the attempt kept alive meanwhile.
		stop := keepAlive(ctx)
		defer stop()
	}
	for try, pause := 1, 50*time.Millisecond; ; try, pause = try+1, min(2*pause, 2*time.Second) {
		tx, cancel := context.WithTimeout(base, appendTurnWait)
		err := g.Ledger.Tx(tx, func(q *db.Queries) error { return write(tx, q) })
		cancel()
		if err == nil || try == appendTries {
			return err
		}
		if activity.IsActivity(ctx) {
			activity.RecordHeartbeat(ctx)
		}
		time.Sleep(pause)
	}
}

// callStarted appends call.started: the attempt's record before anything of it leaves agentd, once
// the policy has let it past (design doc, section 7: no entry, no call). It says what the attempt
// is and what the policy decided; the call's record, at its end, names it. An attempt cut short (a
// crash) leaves it behind, closed by the next agentd's agentd.started. When it cannot be written,
// nothing is sent: the attempt fails, retryably.
func (g *Gateway) callStarted(ctx context.Context, info activity.Info, c *agentosv1.Call, rec *callRecord) error {
	p := callFields(info, c, rec)
	ev := audit.Event{WorkflowID: info.WorkflowExecution.ID, Type: "call.started", Actor: audit.Run, Payload: p}
	var head audit.Head
	if err := g.appendTried(ctx, func(tx context.Context, q *db.Queries) error {
		var err error
		head, err = audit.Append(tx, q, ev)
		return err
	}); err != nil {
		return temporal.NewApplicationError("agentd could not record the call before making it; retry", "AuditUnavailable", err)
	}
	rec.startedSeq = head.Seq
	return nil
}

// keepAliveEvery is how often keepAlive heartbeats: within any heartbeat timeout a Worker sets for
// a gateway call (Temporal's SDK sends at most one heartbeat per interval of its own).
var keepAliveEvery = time.Second

// keepAlive heartbeats the attempt until stop, whatever heartbeat timeout it has: heartbeat, which
// the call's work uses, does nothing without one.
func keepAlive(ctx context.Context) (stop func()) {
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		t := time.NewTicker(keepAliveEvery)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				activity.RecordHeartbeat(ctx)
			}
		}
	}()
	return func() { close(done); <-finished }
}

// defaultRunIDWait bounds how long a run's calls wait for StartRun to record its run ID: usually
// milliseconds, the time it takes StartRun to learn the run ID from Temporal and record it.
const defaultRunIDWait = 5 * time.Second

// awaitRunID reads the run again, while StartRun has not recorded its run ID, until it has, the
// run is gone or the wait is over, and answers as LookupRun does: a run ID recorded for another
// execution is still ErrUnknownRun, and a run still starting is still ErrRunStarting.
//
// The wait counts from when StartRun recorded the run, not from this attempt: the calls of a run
// that keeps no run ID (StartRun could neither record nor stop it, or the execution under its
// workflow ID is not the one agentd started) are refused at once, attempt after attempt.
func (g *Gateway) awaitRunID(ctx context.Context, workflowID, runID string) (db.Run, error) {
	wait := g.runIDWait
	if wait <= 0 {
		wait = defaultRunIDWait
	}
	// None for a run that is gone or unreadable (LookupRun answers), or recorded after now on the
	// wall clock, which was set back since: how long it has been starting is unknown.
	var remaining time.Duration
	if r, err := g.Ledger.Q.GetRun(ctx, workflowID); err == nil {
		if created, err := time.Parse(time.RFC3339Nano, r.CreatedAt); err == nil {
			if starting := time.Since(created); starting >= 0 {
				remaining = wait - starting
			}
		}
	}
	// From here on this process's monotonic clock: setting the wall clock back stretches nothing.
	deadline := time.Now().Add(remaining)
	for pause := 10 * time.Millisecond; ; pause = min(2*pause, 100*time.Millisecond) {
		run, err := g.Ledger.LookupRun(ctx, workflowID, runID)
		if !errors.Is(err, ledger.ErrRunStarting) {
			return run, err
		}
		left := time.Until(deadline)
		if left <= 0 {
			return db.Run{}, err
		}
		select {
		case <-ctx.Done():
			return db.Run{}, ctx.Err()
		case <-time.After(min(pause, left)):
		}
	}
}

// CheckModels reports, at start, a cross-provider fallback the policy would never let a
// task's data reach (D11): a fallback that cannot happen is a mistake in models.yaml.
func CheckModels(p *policy.Policy, m *models.Config) error {
	for task := range m.Tasks {
		r, err := m.Route(task)
		if err != nil {
			return err
		}
		for _, next := range r.Next {
			if v := p.EvaluateModel(task, next.Provider); v.Decision != policy.Allow {
				return fmt.Errorf("models: task %s: cross_provider_fallback %s: %s", task, next.Name(), v.Reason)
			}
		}
	}
	return nil
}

// dispatch runs the call, and notes the policy's verdict in its record (rec).
func (g *Gateway) dispatch(ctx context.Context, info activity.Info, run db.Run, c *agentosv1.Call,
	args map[string]any, canon []byte, fx faults.Effect, rec *callRecord) (*agentosv1.Result, error) {
	switch c.GetKind() {
	case "model":
		route, err := g.Models.Route(c.GetName())
		if err != nil {
			return nil, temporal.NewNonRetryableApplicationError(err.Error(), "UnknownModelTask", err)
		}
		v := g.Policy.EvaluateModel(c.GetName(), route.Provider)
		rec.policy = &v
		if v.Decision != policy.Allow {
			return result(StatusDenied, map[string]any{"reason": v.Reason})
		}
		if err := g.callStarted(ctx, info, c, rec); err != nil {
			return nil, err
		}
		if fx.Status != "" {
			return result(fx.Status, nil)
		}
		return g.callModel(ctx, info, run, route, args, sha256Hex(canon), fx)
	case "tool":
		t, ok := g.Tools[c.GetName()]
		if !ok {
			return result(StatusDenied, map[string]any{"reason": "no such tool"})
		}
		scope, err := g.scope(run)
		if err != nil {
			return result(StatusDenied, map[string]any{"reason": err.Error()})
		}
		// A call that leaves part of its run's scope out gets the run's own value, never the
		// tool's default (the default branch, the latest commit). One that names another
		// value is denied by the policy, and one that passes an argument the tool does not
		// declare is refused by its schema.
		args, canon, err = fillScope(args, scope, t.Args().Props())
		if err != nil {
			return nil, err
		}
		if err := t.Args().Validate(args); err != nil {
			return nil, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("arguments of %s: %v", c.GetName(), err), "BadArguments", nil)
		}
		v := g.Policy.EvaluateTool(c.GetName(), args, scope)
		rec.policy = &v
		if v.Decision == policy.Deny {
			return result(StatusDenied, map[string]any{"reason": v.Reason})
		}
		if err := g.callStarted(ctx, info, c, rec); err != nil {
			return nil, err
		}
		var rule *approvalRule
		switch v.Decision {
		case policy.RequireApproval:
			// A person approves each effect of the write before it is sent (section 10): none for a
			// dry run, which sends nothing, or for a tool that cannot reach its system, which only
			// dry-runs. A read has no effect to approve; CheckTools refuses such a policy at start.
			w, ok := t.(tools.Writer)
			if !ok {
				return result(StatusDenied, map[string]any{"reason": "approval applies to writes only"})
			}
			if run.DryRun == 0 && w.Live() {
				rule = &approvalRule{ttl: v.ApprovalTTL}
			}
		}
		class, err := effectClass(t, v.Effect)
		if err != nil {
			return nil, temporal.NewNonRetryableApplicationError(err.Error(), "BadPolicy", err)
		}
		if fx.Status != "" {
			return result(fx.Status, nil)
		}
		// A write may have one effect per run unless the policy allows more: a workflow that
		// varies its arguments gets new effects, and each would reach the outside world.
		maxPerRun := v.MaxPerRun
		if maxPerRun == 0 {
			maxPerRun = 1
		}
		return g.callTool(ctx, info, run, t, class, maxPerRun, rule, args, canon, fx)
	default:
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("unknown call kind %q", c.GetKind()), "BadCall", nil)
	}
}

// scope returns the tool argument values run is confined to: its task's Scope, filled in
// from the parameters agentd resolved when it started the run.
func (g *Gateway) scope(run db.Run) (map[string]string, error) {
	t, ok := g.Tasks[run.Task]
	if !ok {
		return nil, fmt.Errorf("task %q is not registered", run.Task)
	}
	var params map[string]string
	if err := json.Unmarshal([]byte(run.ParamsJson), &params); err != nil {
		return nil, fmt.Errorf("run parameters: %w", err)
	}
	scope := make(map[string]string, len(t.Scope))
	for arg, param := range t.Scope {
		v, ok := params[param]
		if !ok {
			return nil, fmt.Errorf("the run has no %s", param)
		}
		scope[arg] = v
	}
	return scope, nil
}

// fillScope adds each scoped argument the tool declares and the call left out, with the
// run's value, and encodes the result again: the canonical form, and so the effect ID, covers
// what the tool receives.
func fillScope(args map[string]any, scope map[string]string, declared []string) (map[string]any, []byte, error) {
	filled := maps.Clone(args)
	for arg, v := range scope {
		if _, ok := filled[arg]; !ok && slices.Contains(declared, arg) {
			filled[arg] = v
		}
	}
	canon, err := encodeCanonical(filled)
	if err != nil {
		return nil, nil, err
	}
	return filled, canon, nil
}

func (g *Gateway) callModel(ctx context.Context, info activity.Info, run db.Run, route models.Route,
	args map[string]any, argsHash string, fx faults.Effect) (*agentosv1.Result, error) {
	adapter, ok := g.Adapters[route.Provider]
	if !ok {
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("provider %q has no adapter %q in this build", route.Provider, route.Adapter), "AdapterUnavailable", nil)
	}
	// Activities run at least once: a new attempt can start while the old one is still in
	// flight. Both wait on the same provider call.
	v, err, _ := g.modelCalls.Do(run.WorkflowID+"\x00"+info.ActivityID, func() (any, error) {
		return g.track(func() (any, error) {
			exec, cancel := detach(ctx, info)
			defer cancel()
			return g.modelCallOnce(exec, info, run, route, adapter, args, argsHash, fx)
		})
	})
	var failed *temporal.ApplicationError
	var unrecorded recordError
	if err != nil && g.lastAttempt(info) && !(errors.As(err, &failed) && failed.NonRetryable()) && !errors.As(err, &unrecorded) {
		// The execution was an earlier attempt's, which this one joined: for it, a fallback's
		// failure could still pass. On the last attempt, what the call already got stands.
		hops := append([]models.Route{route}, route.Next...)
		if p, perr := g.resume(context.WithoutCancel(ctx), info, run, argsHash, hops); perr == nil {
			switch {
			case p.answer != nil:
				return result(p.answer.Status, p.answer.Value)
			case p.refusal != nil:
				g.Log.Warn("the last attempt keeps the refusal", "workflow_id", run.WorkflowID, "error", err)
				return result(p.refusal.Status, p.refusal.Value)
			}
		}
	}
	if err != nil {
		return nil, err
	}
	resp := v.(models.Response)
	return result(resp.Status, resp.Value)
}

// modelCallOnce asks the task's model and, while the answer is a refusal, each of the task's
// cross-provider fallbacks in turn: each a request of its own, reserved for alone, sent only
// if the policy lets the task's data go to its provider (section 9).
func (g *Gateway) modelCallOnce(ctx context.Context, info activity.Info, run db.Run, route models.Route,
	adapter models.Adapter, args map[string]any, argsHash string, fx faults.Effect) (models.Response, error) {
	hops := append([]models.Route{route}, route.Next...)
	p, err := g.resume(ctx, info, run, argsHash, hops)
	if err != nil {
		return models.Response{}, err
	}
	if p.answer != nil {
		// An earlier attempt got an answer Temporal never recorded (its completion was
		// lost). Serve that answer instead of paying for a second call.
		return *p.answer, nil
	}
	refusal := p.refusal
	seq := 0
	for hop, r := range hops {
		if p.declined[hop] {
			continue
		}
		if hop > 0 {
			if v := g.Policy.EvaluateModel(r.Task, r.Provider); v.Decision != policy.Allow {
				g.Log.Warn("not falling back: "+v.Reason, "workflow_id", run.WorkflowID, "to", r.Name())
				continue
			}
			next, ok := g.Adapters[r.Provider]
			if !ok {
				g.Log.Error("not falling back: no adapter", "workflow_id", run.WorkflowID, "to", r.Name(), "adapter", r.Adapter)
				continue
			}
			adapter = next
			g.Log.Warn("the model declined; falling back to another provider", "workflow_id", run.WorkflowID,
				"activity_id", info.ActivityID, "from", refusal.Model, "to", r.Name())
		}
		// Answers of this hop that broke the output schema count against its generations,
		// whichever attempt got them: one that had them all, or was cut off at max_tokens,
		// has had its answer.
		if last := p.lastInvalid[hop]; last != nil && (p.invalid[hop] >= generations || last.StopReason == "max_tokens") {
			return *last, nil
		}
		resp, err := g.ask(ctx, info, run, r, adapter, args, fx, &seq, p.invalid[hop]+1)
		var failed *temporal.ApplicationError
		var unrecorded recordError
		switch {
		case err != nil && hop > 0 && !errors.As(err, &unrecorded) && (g.lastAttempt(info) ||
			errors.As(err, &failed) && failed.NonRetryable() && slices.Contains(hopFailures, failed.Type())):
			// The refusal stands if none of the later providers answers. On the last attempt, a
			// failure that might pass is no different: nothing would ask this provider again.
			// An answer the ledger failed to record is: it may have been paid for.
			g.Log.Warn("not falling back: the request failed", "workflow_id", run.WorkflowID, "to", r.Name(), "error", err)
			continue
		case err != nil:
			return models.Response{}, err
		case resp.Status != StatusRefused:
			return resp, nil
		}
		refusal = &resp
	}
	if refusal == nil {
		return models.Response{}, errors.New("model call: no provider left to ask") // a declined hop comes with its refusal
	}
	return *refusal, nil
}

// ask sends one route's request, from generation gen on: once more when the answer breaks the
// output schema, up to generations in all.
func (g *Gateway) ask(ctx context.Context, info activity.Info, run db.Run, r models.Route,
	adapter models.Adapter, args map[string]any, fx faults.Effect, seq *int, gen int) (models.Response, error) {
	for ; ; gen++ {
		*seq++
		resp, err := g.modelRequest(ctx, info, run, r, adapter, args, fx, *seq, gen)
		if err != nil || !asksAgain(resp, gen) {
			return resp, err
		}
		g.Log.Warn("the answer broke the output schema; asking again", "workflow_id", run.WorkflowID,
			"activity_id", info.ActivityID, "request_seq", *seq, "reason", resp.Value["reason"])
	}
}

// past is what a model call's earlier requests left, hop by hop (resume).
type past struct {
	answer      *models.Response   // an answer one of the call's attempts got: served
	refusal     *models.Response   // the latest refusal: returned if no other hop answers
	declined    []bool             // the hops that declined: never asked, and paid, again
	invalid     []int              // the hops' answers that broke the output schema
	lastInvalid []*models.Response // the latest of those, per hop
}

// resume finds what a model call got before: in its own earlier attempts, and in the
// activities that sent it, with the same arguments, before it waited for budget (the workflow
// sends a call that waited again, under a new activity). A request counts for a hop of the
// same provider, model, prompt and effort: one under a prompt edited since, or by a model no
// longer listed, does not.
func (g *Gateway) resume(ctx context.Context, info activity.Info, run db.Run, argsHash string,
	hops []models.Route) (past, error) {
	activities, err := g.pausedBefore(ctx, info, run, hops[0].Task, argsHash)
	if err != nil {
		return past{}, err
	}
	p := past{declined: make([]bool, len(hops)), invalid: make([]int, len(hops)), lastInvalid: make([]*models.Response, len(hops))}
	for _, id := range append(activities, info.ActivityID) {
		rows, err := g.Ledger.Q.ListFinishedModelCalls(ctx, db.ListFinishedModelCallsParams{WorkflowID: run.WorkflowID, ActivityID: id})
		if err != nil {
			return past{}, err
		}
		for _, row := range rows {
			resp, err := g.loadResponse(ctx, row.ResponseBlob.String)
			if err != nil {
				return past{}, err
			}
			if row.StopReason.String != StatusInvalidOutput && resp.Status != StatusRefused {
				if id == info.ActivityID {
					return past{answer: &resp}, nil
				}
				continue // a call that waited for budget got no answer
			}
			for i, r := range hops {
				if r.Provider != row.Provider || r.Model != row.Model || r.PromptVersion != row.PromptVersion ||
					r.Effort != row.Effort.String {
					continue
				}
				if resp.Status == StatusRefused {
					p.declined[i], p.refusal = true, &resp
				} else {
					p.invalid[i]++
					p.lastInvalid[i] = &resp
				}
			}
		}
	}
	return p, nil
}

// lastAttempt reports whether Temporal will not retry this attempt: the workflow's retry
// policy, or agentd's own cap, allows no more.
func (g *Gateway) lastAttempt(info activity.Info) bool {
	n := int64(info.Attempt)
	return (info.RetryPolicy != nil && info.RetryPolicy.MaximumAttempts > 0 && n >= int64(info.RetryPolicy.MaximumAttempts)) ||
		(g.Limits.Attempts > 0 && n >= g.Limits.Attempts)
}

// tooLate keeps a request out of an attempt that may end before its answer: any request but
// the attempt's first, when less time is left than the request may take
// (models.ExpectedDuration). Sent, it would be cut off at the deadline, charged in full and
// asked again anyway. The next attempt sends it, with all its time; on the last, it fails for
// good, and a fallback is skipped.
func (g *Gateway) tooLate(ctx context.Context, info activity.Info, route models.Route, seq int) error {
	deadline, ok := ctx.Deadline()
	need := models.ExpectedDuration(route.MaxTokens)
	if seq <= 1 || !ok || time.Until(deadline) >= need {
		return nil
	}
	err := fmt.Errorf("%s may take %v and this attempt has %v left", route.Name(), need.Round(time.Second),
		time.Until(deadline).Round(time.Second))
	if g.lastAttempt(info) {
		return temporal.NewNonRetryableApplicationError(err.Error(), "NoTimeLeft", err)
	}
	return fmt.Errorf("%w: the next attempt sends it", err)
}

// pausedBefore lists, oldest first, the activities that sent the same model call just before
// this one and returned budget_exhausted.
func (g *Gateway) pausedBefore(ctx context.Context, info activity.Info, run db.Run, task, argsHash string) ([]string, error) {
	calls, err := g.Ledger.Q.ListSameCalls(ctx, db.ListSameCallsParams{
		WorkflowID: run.WorkflowID, Kind: "model", Name: task, ArgsSha256: argsHash})
	if err != nil {
		return nil, err
	}
	// The scheduled times are RFC 3339 with trailing zeros cut, which does not sort as text.
	at := func(c db.Call) time.Time { t, _ := time.Parse(time.RFC3339Nano, c.ScheduledAt); return t }
	slices.SortStableFunc(calls, func(a, b db.Call) int {
		if c := at(a).Compare(at(b)); c != 0 {
			return c
		}
		return strings.Compare(a.CreatedAt, b.CreatedAt)
	})
	self := slices.IndexFunc(calls, func(c db.Call) bool {
		return c.ActivityID == info.ActivityID && c.ScheduledAt == info.ScheduledTime.UTC().Format(time.RFC3339Nano)
	})
	var paused []string
	for i := self - 1; i >= 0 && calls[i].Outcome.String == StatusBudgetExhausted; i-- {
		paused = append([]string{calls[i].ActivityID}, paused...)
	}
	return paused, nil
}

// guard records a logical call and returns why it is refused when it takes the run past a
// loop limit (section 9): too many model calls, too many calls, or one tool called once too
// often with the same arguments. A logical call is its activity ID and the time Temporal first
// scheduled it, which the server sets and retries share: retries count once, and a new call
// under a reused activity ID counts anew. The same arguments sent to a model again are not a
// loop: a call paused for budget or approval is sent again, with the same arguments, once it
// is let through.
func (g *Gateway) guard(ctx context.Context, run db.Run, info activity.Info, c *agentosv1.Call, canon []byte,
	rec *callRecord) (string, error) {
	var reason string
	var head *audit.Head
	scheduled := info.ScheduledTime.UTC().Format(time.RFC3339Nano)
	err := g.Ledger.Tx(ctx, func(q *db.Queries) error {
		reason, head = "", nil
		args := sha256Hex(canon)
		inserted, err := q.InsertCall(ctx, db.InsertCallParams{
			WorkflowID: run.WorkflowID, ActivityID: info.ActivityID,
			ScheduledAt: scheduled, Kind: c.GetKind(), Name: c.GetName(),
			ArgsSha256: args, CreatedAt: ledger.Now(),
		})
		if err != nil {
			return err
		}
		if inserted == 0 { // counted already, by an earlier attempt: the row is there, or the write was dropped
			there, err := q.CallCounted(ctx, db.CallCountedParams{WorkflowID: run.WorkflowID, ActivityID: info.ActivityID,
				ScheduledAt: scheduled})
			if err != nil {
				return err
			}
			if there == 0 {
				return fmt.Errorf("%w: the count of call %q", ledger.ErrWriteDropped, info.ActivityID)
			}
		}
		if inserted == 1 {
			// The call counts from here (calls, which the loop guard reads), and its count is recorded
			// with it: a row deleted would let the run past its limits, and is found missing.
			if _, err := audit.Append(ctx, q, audit.Event{WorkflowID: run.WorkflowID, Type: "call.counted", Actor: audit.Agentd,
				Payload: map[string]any{"activity_id": audit.Text(info.ActivityID), "scheduled_at": scheduled,
					"kind": audit.Text(c.GetKind()), "name": audit.Text(c.GetName()), "args_sha256": args}}); err != nil {
				return err
			}
		}
		n, err := q.GetCallCounts(ctx, db.GetCallCountsParams{Kind: c.GetKind(), Name: c.GetName(), ArgsSha256: args, WorkflowID: run.WorkflowID})
		if err != nil {
			return err
		}
		over := func(count, limit int64) bool { return limit > 0 && count > limit }
		switch {
		case over(n.GatewayCalls, g.Limits.GatewayCalls):
			reason = fmt.Sprintf("loop guard: more than %d calls in one run", g.Limits.GatewayCalls)
		case c.GetKind() == "model" && over(n.ModelCalls, g.Limits.ModelCalls):
			reason = fmt.Sprintf("loop guard: more than %d model calls in one run", g.Limits.ModelCalls)
		case c.GetKind() == "tool" && over(n.SameCalls, g.Limits.SameToolCalls):
			reason = fmt.Sprintf("loop guard: %s called more than %d times with the same arguments", c.GetName(), g.Limits.SameToolCalls)
		}
		if reason == "" {
			return nil
		}
		// The refused call's record goes with the count that refused it: one write, not two, for a
		// flood of calls past the limit.
		rec.scheduledAt = scheduled
		ev, err := callEvent(info, c, rec, StatusDenied, map[string]any{"reason": reason}, nil)
		if err != nil {
			return err
		}
		h, err := audit.Append(ctx, q, ev)
		head = &h
		return err
	})
	if err != nil {
		// Rolled back, the record with it: recordCall records the attempt's error instead, of a call
		// that counted for nothing (no scheduled_at: its record names no count).
		rec.scheduledAt = ""
		return "", err
	}
	rec.head, rec.scheduledAt = head, scheduled
	return reason, nil
}

// waitTurn waits for the provider's rate limit, at most a minute. A request that gets no turn
// is not sent: its reservation is released, and Temporal retries the call later.
func (g *Gateway) waitTurn(ctx context.Context, provider string) error {
	l := g.Limiters[provider]
	if l == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if err := l.Wait(ctx); err != nil {
		return fmt.Errorf("rate limit of %s: %w", provider, err)
	}
	return nil
}

// generations is how many requests a model call makes within one attempt: an answer that
// breaks the task's output schema is asked for once more, and paid for like any request.
const generations = 2

// asksAgain reports whether ask sends another request after resp, the answer of generation
// gen. An answer cut off at max_tokens would be cut off again: only a larger max_tokens helps.
func asksAgain(resp models.Response, gen int) bool {
	return resp.Status == StatusInvalidOutput && gen < generations && resp.StopReason != "max_tokens"
}

// modelRequest is one network request to the provider, of generation gen: reserve the most it
// can cost, record it, send it, check the answer, then record the answer together with its
// charge.
func (g *Gateway) modelRequest(ctx context.Context, info activity.Info, run db.Run, route models.Route,
	adapter models.Adapter, args map[string]any, fx faults.Effect, seq, gen int) (models.Response, error) {
	if err := g.tooLate(ctx, info, route, seq); err != nil {
		return models.Response{}, err
	}
	if fx.HoldRecordOn != "" && fx.HoldRecordOn != route.Provider {
		fx.HoldBeforeRecordWhileExists, fx.HoldAfterRecordWhileExists = "", ""
	}
	req := models.Request{
		WorkflowID: run.WorkflowID, Route: route, Args: args,
		// A retry, and a second generation, is a new request with its own reservation.
		RequestID: fmt.Sprintf("%s/%s/%d/%d", run.WorkflowID, info.ActivityID, info.Attempt, seq),
		Fault: models.FakeFault{Status: fx.ProviderStatus, StatusOn: fx.ProviderStatusOn, LoseAnswer: fx.ProviderLoseAnswer,
			Unreachable: fx.ProviderUnreachable, ServedBy: fx.ProviderServedBy,
			InvalidOutput: fx.ProviderBadOutput && seq == 1, RefusedBy: fx.ProviderRefuses},
	}
	// A run that cannot afford even the output side stops here, before counting: counting is
	// a request too, free but rate-limited by the provider.
	floor, err := g.Prices.Bound(route.Candidates, 0, route.MaxTokens)
	if err != nil {
		return models.Response{}, temporal.NewNonRetryableApplicationError(err.Error(), "Unpriced", err)
	}
	if totals, err := g.Budget.Totals(ctx, run.WorkflowID); err != nil {
		return models.Response{}, err
	} else if totals.Available() < floor {
		return models.Response{Status: StatusBudgetExhausted, Value: map[string]any{
			"needed_micros": floor, "available_micros": totals.Available(), "limit_micros": totals.Limit,
		}}, nil
	}
	// Reserve the most the request can cost before it goes out (section 9).
	tokens, err := adapter.CountTokens(ctx, req)
	var declined *models.Declined
	switch {
	case errors.As(err, &declined):
		return g.recordDeclined(ctx, info, run, route, args, seq, declined)
	case err != nil:
		if failed := nonRetryable(err); failed != nil {
			return models.Response{}, failed
		}
		return models.Response{}, fmt.Errorf("count input tokens: %w", err)
	}
	reserved, err := g.Prices.Bound(route.Candidates, tokens, route.MaxTokens)
	if err != nil {
		return models.Response{}, temporal.NewNonRetryableApplicationError(err.Error(), "Unpriced", err)
	}
	breq := budget.Request{ID: req.RequestID, WorkflowID: run.WorkflowID, ActivityID: info.ActivityID,
		Attempt: info.Attempt, Model: route.Name()}
	totals, err := g.Budget.Reserve(ctx, breq, reserved, g.Prices.Version)
	if errors.Is(err, budget.ErrExhausted) {
		// Not an error: the workflow waits in PAUSED_BUDGET for an extension, then asks again.
		return models.Response{Status: StatusBudgetExhausted, Value: map[string]any{
			"needed_micros": reserved, "available_micros": totals.Available(), "limit_micros": totals.Limit,
		}}, nil
	}
	if err != nil {
		return models.Response{}, err
	}

	if err := g.waitTurn(ctx, route.Provider); err != nil {
		g.closeUnanswered(ctx, breq, reserved, fmt.Errorf("%w: %v", models.ErrNotSent, err))
		return models.Response{}, err
	}
	// Counting and the wait for a turn took time too.
	if err := g.tooLate(ctx, info, route, seq); err != nil {
		g.closeUnanswered(ctx, breq, reserved, fmt.Errorf("%w: %v", models.ErrNotSent, err))
		return models.Response{}, err
	}
	if err := g.recordRequest(ctx, info, run, route, args, seq); err != nil {
		g.closeUnanswered(ctx, breq, reserved, fmt.Errorf("%w: %v", models.ErrNotSent, err))
		return models.Response{}, err
	}
	fx.Stall(ctx.Done())
	resp, err := adapter.Call(ctx, req)
	if err != nil {
		g.closeUnanswered(ctx, breq, reserved, err)
		if failed := nonRetryable(err); failed != nil {
			return models.Response{}, failed
		}
		return models.Response{}, err
	}
	if resp.Model == "" {
		resp.Model = route.Name()
	}
	g.Log.Info("model answered", "workflow_id", run.WorkflowID, "request_id", req.RequestID,
		"model", resp.Model, "status", resp.Status, "stop_reason", resp.StopReason)
	fx.StallBeforeRecord(ctx.Done())
	cost := g.charge(req.RequestID, resp, reserved)
	stop := resp.StopReason // as recorded: invalid_output keeps the answer from being reused
	if resp.Status == StatusOK {
		if resp.Value, err = g.accept(ctx, run.WorkflowID, route, resp.Output); err != nil {
			var invalid invalidOutputError
			if !errors.As(err, &invalid) {
				return models.Response{}, recordError{err}
			}
			resp.Status, stop = StatusInvalidOutput, StatusInvalidOutput
			resp.Value = map[string]any{"reason": invalid.Error(), "stop_reason": resp.StopReason}
		}
	}
	respJSON, err := json.Marshal(resp)
	if err != nil {
		return models.Response{}, recordError{err}
	}
	respRef, err := g.Ledger.PutBlob(ctx, respJSON)
	if err != nil {
		return models.Response{}, recordError{err}
	}
	billed, err := json.Marshal(resp.Billed)
	if err != nil {
		return models.Response{}, recordError{err}
	}
	// The answer and its charge are recorded together. A crash or a failed write before
	// this leaves the reservation open: it stays held, and agentd charges it in full when
	// it starts again.
	if err := g.Ledger.Tx(ctx, func(q *db.Queries) error {
		if err := q.FinishModelCall(ctx, db.FinishModelCallParams{
			ResponseBlob: sql.NullString{String: respRef, Valid: true},
			StopReason:   sql.NullString{String: stop, Valid: stop != ""},
			UsageJson:    sql.NullString{String: string(billed), Valid: true},
			FinishedAt:   sql.NullString{String: ledger.Now(), Valid: true},
			WorkflowID:   run.WorkflowID, ActivityID: info.ActivityID, Attempt: int64(info.Attempt), RequestSeq: int64(seq),
		}); err != nil {
			return err
		}
		if err := noteAnswered(ctx, q, run, info, seq, respRef, stop, billed); err != nil {
			return err
		}
		_, err := budget.Settle(ctx, q, req.RequestID, cost, resp.Model, g.Prices.Version, budget.ReasonUsage)
		return err
	}); err != nil {
		return models.Response{}, recordError{err}
	}
	// Once the answer the call returns is recorded: not one it asks for again.
	if !asksAgain(resp, gen) {
		fx.StallAfterRecord(ctx.Done())
	}
	return resp, nil
}

// recordError is a failure to record an answer the provider gave and may have billed: its
// reservation stays open, to be charged in full when agentd starts again, and the call cannot
// carry on as if there had been no answer.
type recordError struct{ error }

func (e recordError) Unwrap() error { return e.error }

// recordDeclined records a provider's refusal while counting a request: a refusal like any
// other, in the ledger so that no later attempt asks again, with nothing reserved or charged.
func (g *Gateway) recordDeclined(ctx context.Context, info activity.Info, run db.Run, route models.Route,
	args map[string]any, seq int, d *models.Declined) (models.Response, error) {
	resp := models.Response{Status: StatusRefused, Model: route.Name(), StopReason: d.Category,
		Value: map[string]any{"category": d.Category, "explanation": d.Explanation}, Billed: []models.Billed{{Model: route.Name()}}}
	g.Log.Warn("the provider declined the request while counting it", "workflow_id", run.WorkflowID,
		"model", route.Name(), "category", d.Category)
	if err := g.recordRequest(ctx, info, run, route, args, seq); err != nil {
		return models.Response{}, err
	}
	respJSON, err := json.Marshal(resp)
	if err != nil {
		return models.Response{}, err
	}
	respRef, err := g.Ledger.PutBlob(ctx, respJSON)
	if err != nil {
		return models.Response{}, err
	}
	billed, err := json.Marshal(resp.Billed)
	if err != nil {
		return models.Response{}, err
	}
	return resp, g.Ledger.Tx(ctx, func(q *db.Queries) error {
		if err := q.FinishModelCall(ctx, db.FinishModelCallParams{
			ResponseBlob: sql.NullString{String: respRef, Valid: true},
			StopReason:   sql.NullString{String: d.Category, Valid: true},
			UsageJson:    sql.NullString{String: string(billed), Valid: true},
			FinishedAt:   sql.NullString{String: ledger.Now(), Valid: true},
			WorkflowID:   run.WorkflowID, ActivityID: info.ActivityID, Attempt: int64(info.Attempt), RequestSeq: int64(seq),
		}); err != nil {
			return err
		}
		return noteAnswered(ctx, q, run, info, seq, respRef, d.Category, billed)
	})
}

// nonRetryable is the error Temporal must not retry for an adapter's err, or nil: what a request
// cannot become by being sent again.
func nonRetryable(err error) error {
	var rejected *models.RejectedError
	kind := ""
	switch {
	case errors.Is(err, models.ErrBadArguments):
		kind = "BadArguments"
	case errors.Is(err, models.ErrTooLong):
		kind = "InputTooLong"
	case errors.Is(err, models.ErrCloudModel):
		kind = "CloudModel"
	case errors.Is(err, models.ErrUnverified):
		kind = "Unverified"
	case errors.As(err, &rejected) && !rejected.Retryable():
		kind = "ProviderRejected"
	default:
		return nil
	}
	return temporal.NewNonRetryableApplicationError(err.Error(), kind, err)
}

// hopFailures are the errors of one provider's request that leave the next provider to ask:
// what one fallback cannot take, another may.
var hopFailures = []string{"Unpriced", "InputTooLong", "CloudModel", "Unverified", "ProviderRejected", "NoTimeLeft"}

// charge is what resp was billed: each attempt within the request at its own model's price,
// so that a server-side fallback is charged at the fallback's rates. An answer the price
// table cannot price is charged its whole reservation.
func (g *Gateway) charge(requestID string, resp models.Response, reserved int64) int64 {
	if len(resp.Billed) == 0 {
		g.Log.Error("an answer came without usage; charging its whole reservation", "request_id", requestID)
		return reserved
	}
	var total int64
	for _, b := range resp.Billed {
		c, err := g.Prices.Cost(b.Model, b.Usage)
		if err != nil {
			g.Log.Error("cannot price an answer; charging its whole reservation", "request_id", requestID,
				"model", b.Model, "error", err)
			return reserved
		}
		total += c
	}
	if total > reserved {
		// The ledger records what was billed even so: the bound in section 9 failed.
		g.Log.Error("a request cost more than its reservation", "request_id", requestID,
			"cost_micros", total, "reserved_micros", reserved)
	}
	return total
}

// invalidOutputError is an answer that breaks the task's output schema.
type invalidOutputError struct{ error }

// accept checks output against the task's schema and builds what the workflow gets: the whole
// output as report_ref, each of the task's blob fields as <field>_ref (absent when null), and
// the other fields inline. Large parts never travel through Temporal's history.
func (g *Gateway) accept(ctx context.Context, workflowID string, route models.Route, output map[string]any) (map[string]any, error) {
	if output == nil {
		return nil, invalidOutputError{errors.New("no structured output")}
	}
	if err := route.Check(output); err != nil {
		return nil, invalidOutputError{err}
	}
	whole, err := json.Marshal(output)
	if err != nil {
		return nil, err
	}
	ref, err := audit.PutRunBlob(ctx, g.Ledger, workflowID, whole)
	if err != nil {
		return nil, err
	}
	value := map[string]any{"report_ref": ref}
	for field, v := range output {
		if !slices.Contains(route.Blobs, field) {
			value[field] = v
			continue
		}
		var b []byte
		switch v := v.(type) {
		case nil:
			continue
		case string:
			b = []byte(v)
		default:
			if b, err = json.Marshal(v); err != nil {
				return nil, err
			}
		}
		if value[field+"_ref"], err = audit.PutRunBlob(ctx, g.Ledger, workflowID, b); err != nil {
			return nil, err
		}
	}
	return value, nil
}

// recordRequest writes the exact request of a model call before it is sent.
func (g *Gateway) recordRequest(ctx context.Context, info activity.Info, run db.Run, route models.Route,
	args map[string]any, seq int) error {
	// The task's effort, and the one the provider is sent for it (they differ for a provider
	// that lists its efforts).
	reqJSON, err := json.Marshal(map[string]any{
		"task": route.Task, "provider": route.Provider, "model": route.Model, "effort": route.Effort,
		"effort_sent": g.Models.EffortSent(route), "args": args,
	})
	if err != nil {
		return err
	}
	reqRef, err := g.Ledger.PutBlob(ctx, reqJSON)
	if err != nil {
		return err
	}
	return g.Ledger.Tx(ctx, func(q *db.Queries) error {
		if err := q.InsertModelCall(ctx, db.InsertModelCallParams{
			WorkflowID: run.WorkflowID, ActivityID: info.ActivityID, Attempt: int64(info.Attempt), RequestSeq: int64(seq),
			RequestBlob: reqRef, Provider: route.Provider, Model: route.Model, PromptVersion: route.PromptVersion,
			Effort: sql.NullString{String: route.Effort, Valid: route.Effort != ""}, StartedAt: ledger.Now(),
		}); err != nil {
			return err
		}
		return note(ctx, q, run.WorkflowID, "model.requested", audit.Agentd, map[string]any{
			"activity_id": audit.Text(info.ActivityID), "attempt": info.Attempt, "request_seq": seq,
			"request_sha256": strings.TrimPrefix(reqRef, "sha256:"), "provider": route.Provider, "model": route.Model,
			"prompt_version": route.PromptVersion, "effort": route.Effort})
	})
}

// noteAnswered is model.answered: what a model request got, recorded with the row of model_calls
// it finishes (its response, stop reason and usage, which the budget's charge follows).
func noteAnswered(ctx context.Context, q *db.Queries, run db.Run, info activity.Info, seq int, respRef, stop string,
	billed []byte) error {
	return note(ctx, q, run.WorkflowID, "model.answered", audit.Agentd, map[string]any{
		"activity_id": audit.Text(info.ActivityID), "attempt": info.Attempt, "request_seq": seq,
		"response_sha256": strings.TrimPrefix(respRef, "sha256:"), "stop_reason": stop, "usage_sha256": sha256Hex(billed)})
}

// closeUnanswered closes the reservation of a request that got no answer. One that never
// left agentd is released; one the provider certainly did not serve (429, 529, other 4xx) is
// settled at zero; after any other failure, a 500 or 504 included, nobody knows whether the
// provider billed it, so the whole reservation is charged.
func (g *Gateway) closeUnanswered(ctx context.Context, r budget.Request, reserved int64, callErr error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	var rejected *models.RejectedError
	err := g.Ledger.Tx(ctx, func(q *db.Queries) error {
		var err error
		switch {
		case errors.Is(callErr, models.ErrNotSent):
			err = budget.Release(ctx, q, r.ID)
		case errors.As(callErr, &rejected) && rejected.Unbilled():
			_, err = budget.Settle(ctx, q, r.ID, 0, r.Model, g.Prices.Version, budget.ReasonRejected)
		default:
			_, err = budget.Settle(ctx, q, r.ID, reserved, r.Model, g.Prices.Version, budget.ReasonOutcomeUnknown)
		}
		return err
	})
	if err != nil {
		// Left open, it stays held until agentd charges it in full at its next start.
		g.Log.Error("could not close a reservation", "request_id", r.ID, "error", err)
	}
}

func (g *Gateway) loadResponse(ctx context.Context, ref string) (models.Response, error) {
	b, err := g.Ledger.GetBlob(ctx, ref)
	if err != nil {
		return models.Response{}, err
	}
	var resp models.Response
	err = json.Unmarshal(b, &resp)
	return resp, err
}

func result(status string, value any) (*agentosv1.Result, error) {
	if value == nil {
		value = map[string]any{}
	}
	b, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return &agentosv1.Result{Status: status, ValueJson: string(b)}, nil
}

// canonicalArgs decodes args_json, which must be one JSON object, and re-encodes it with
// sorted keys and no insignificant whitespace: the form agentd hashes.
func canonicalArgs(s string) (map[string]any, []byte, error) {
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil, nil, err
	}
	if m == nil {
		return nil, nil, errors.New("not an object")
	}
	if err := dec.Decode(new(json.RawMessage)); err != io.EOF {
		return nil, nil, errors.New("trailing data after the object")
	}
	canon, err := encodeCanonical(m)
	if err != nil {
		return nil, nil, err
	}
	return m, canon, nil
}

// encodeCanonical encodes args with sorted keys and no insignificant whitespace.
func encodeCanonical(args map[string]any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(args); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

func sha256Hex(parts ...[]byte) string {
	h := sha256.New()
	for i, p := range parts {
		if i > 0 {
			h.Write([]byte{0})
		}
		h.Write(p)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// detach gives an execution that several attempts may wait on a context of its own: it
// keeps the activity's values but not the attempt's cancellation, and it ends at the
// start-to-close timeout, or at the activity's deadline if that is sooner.
func detach(ctx context.Context, info activity.Info) (context.Context, context.CancelFunc) {
	timeout := info.StartToCloseTimeout
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	deadline := time.Now().Add(timeout)
	// The activity's own deadline when it is sooner: a schedule-to-close timeout ends the
	// activity early. One already past is another clock's: it is not followed.
	if d := info.Deadline; !d.IsZero() && d.After(time.Now()) && d.Before(deadline) {
		deadline = d
	}
	return context.WithDeadline(context.WithoutCancel(ctx), deadline)
}

// heartbeatInterval is a third of the heartbeat timeout, but never under 100ms. The
// workflow, which agentd does not trust, picks the timeout: a 2ns timeout would make the
// ticker panic and take agentd down.
func heartbeatInterval(timeout time.Duration) time.Duration {
	return max(timeout/3, 100*time.Millisecond)
}

// heartbeat keeps the attempt alive while it runs. Temporal re-dispatches an attempt whose
// heartbeats stop, for example because the gateway process died.
func heartbeat(ctx context.Context, skip bool) (stop func()) {
	timeout := activity.GetInfo(ctx).HeartbeatTimeout
	if skip || timeout <= 0 {
		return func() {}
	}
	done, cancel := context.WithCancel(ctx)
	go func() {
		t := time.NewTicker(heartbeatInterval(timeout))
		defer t.Stop()
		for {
			select {
			case <-done.Done():
				return
			case <-t.C:
				// With the attempt's own context: Temporal's SDK sends a heartbeat it held back
				// later with the context it came with, and done ends with stop, maybe before.
				activity.RecordHeartbeat(ctx)
			}
		}
	}()
	return cancel
}
