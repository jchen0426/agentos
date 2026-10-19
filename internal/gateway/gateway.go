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

	runIDWait  time.Duration      // how long a call waits for its run's ID; 0 is defaultRunIDWait
	modelCalls singleflight.Group // in-flight model calls, by workflow and activity ID
	effects    singleflight.Group // in-flight effects and their recoveries, by effect ID
	executions sync.WaitGroup     // detached executions, drained on shutdown

	sendMu  sync.Mutex      // makes claiming an effect and marking it sent one step
	sending map[string]bool // effect IDs this process has claimed and is sending
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

// Invoke is the only activity agentd registers on the "gateway" task queue.
func (g *Gateway) Invoke(ctx context.Context, c *agentosv1.Call) (*agentosv1.Result, error) {
	info := activity.GetInfo(ctx)
	log := g.Log.With("workflow_id", info.WorkflowExecution.ID, "activity_id", info.ActivityID,
		"attempt", info.Attempt, "kind", c.GetKind(), "name", c.GetName())

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
		return nil, err // retried: StartRun, or RecoverStarts when agentd starts, may record it yet
	case errors.Is(err, ledger.ErrUnknownRun):
		log.Warn("invoke from an unknown run", "run_id", info.WorkflowExecution.RunID)
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
	args, canon, err := canonicalArgs(c.GetArgsJson())
	if err != nil {
		return nil, temporal.NewNonRetryableApplicationError("args_json must be a JSON object", "BadArguments", err)
	}
	if limit := g.Limits.Attempts; limit > 0 && int64(info.Attempt) > limit {
		// The Worker writes the retry policy; a call retried without end stops here.
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("more than %d attempts of one call", limit), "TooManyAttempts", nil)
	}
	if reason, err := g.guard(ctx, run, info, c, canon); err != nil {
		return nil, err
	} else if reason != "" {
		log.Warn("invoke refused: loop guard", "reason", reason)
		return result(StatusDenied, map[string]any{"reason": reason})
	}
	// M5: append the hash-chained audit log entry here, before anything else happens.

	fx := g.Faults.Match(run.WorkflowID, c.GetName(), info.Attempt)
	stop := heartbeat(ctx, fx.NoHeartbeat)
	defer stop()

	log.Info("invoke started", "no_heartbeat", fx.NoHeartbeat)
	res, err := g.dispatch(ctx, info, run, c, args, canon, fx)
	if err != nil {
		log.Info("invoke failed", "error", err)
		return nil, err
	}
	// The loop guard counts a call that only waited (for an approval, a reconciliation or
	// budget) as a step, not as a repeat or a model call.
	if err := g.Ledger.Q.SetCallOutcome(context.WithoutCancel(ctx), db.SetCallOutcomeParams{
		Outcome: sql.NullString{String: res.GetStatus(), Valid: true}, WorkflowID: run.WorkflowID,
		ActivityID: info.ActivityID, ScheduledAt: info.ScheduledTime.UTC().Format(time.RFC3339Nano),
	}); err != nil {
		log.Error("could not record the call's outcome", "error", err)
	}
	log.Info("invoke finished", "status", res.GetStatus())
	return res, nil
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
	var remaining time.Duration // none for a run that is gone or unreadable: LookupRun answers
	if r, err := g.Ledger.Q.GetRun(ctx, workflowID); err == nil {
		if created, err := time.Parse(time.RFC3339Nano, r.CreatedAt); err == nil {
			remaining = min(wait, time.Until(created.Add(wait)))
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

func (g *Gateway) dispatch(ctx context.Context, info activity.Info, run db.Run, c *agentosv1.Call,
	args map[string]any, canon []byte, fx faults.Effect) (*agentosv1.Result, error) {
	switch c.GetKind() {
	case "model":
		route, err := g.Models.Route(c.GetName())
		if err != nil {
			return nil, temporal.NewNonRetryableApplicationError(err.Error(), "UnknownModelTask", err)
		}
		if v := g.Policy.EvaluateModel(c.GetName(), route.Provider); v.Decision != policy.Allow {
			return result(StatusDenied, map[string]any{"reason": v.Reason})
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
		switch v.Decision {
		case policy.Deny:
			return result(StatusDenied, map[string]any{"reason": v.Reason})
		case policy.RequireApproval:
			// M4 replaces this with the approvals table; until then nobody can approve.
			return result(StatusNeedsApproval, nil)
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
		return g.callTool(ctx, info, run, t, class, maxPerRun, args, canon, fx)
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
		resp, err := g.modelRequest(ctx, info, run, r, adapter, args, fx, *seq)
		// An answer cut off at max_tokens would be cut off again: only a larger max_tokens helps.
		if err != nil || resp.Status != StatusInvalidOutput || gen >= generations || resp.StopReason == "max_tokens" {
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
func (g *Gateway) guard(ctx context.Context, run db.Run, info activity.Info, c *agentosv1.Call, canon []byte) (string, error) {
	var reason string
	err := g.Ledger.Tx(ctx, func(q *db.Queries) error {
		args := sha256Hex(canon)
		if err := q.InsertCall(ctx, db.InsertCallParams{
			WorkflowID: run.WorkflowID, ActivityID: info.ActivityID,
			ScheduledAt: info.ScheduledTime.UTC().Format(time.RFC3339Nano), Kind: c.GetKind(), Name: c.GetName(),
			ArgsSha256: args, CreatedAt: ledger.Now(),
		}); err != nil {
			return err
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
		return nil
	})
	return reason, err
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

// modelRequest is one network request to the provider: reserve the most it can cost, record
// it, send it, check the answer, then record the answer together with its charge.
func (g *Gateway) modelRequest(ctx context.Context, info activity.Info, run db.Run, route models.Route,
	adapter models.Adapter, args map[string]any, fx faults.Effect, seq int) (models.Response, error) {
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
		_, err := budget.Settle(ctx, q, req.RequestID, cost, resp.Model, g.Prices.Version, budget.ReasonUsage)
		return err
	}); err != nil {
		return models.Response{}, recordError{err}
	}
	fx.StallAfterRecord(ctx.Done())
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
	return resp, g.Ledger.Q.FinishModelCall(ctx, db.FinishModelCallParams{
		ResponseBlob: sql.NullString{String: respRef, Valid: true},
		StopReason:   sql.NullString{String: d.Category, Valid: true},
		UsageJson:    sql.NullString{String: string(billed), Valid: true},
		FinishedAt:   sql.NullString{String: ledger.Now(), Valid: true},
		WorkflowID:   run.WorkflowID, ActivityID: info.ActivityID, Attempt: int64(info.Attempt), RequestSeq: int64(seq),
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
	ref, err := g.Ledger.PutRunBlob(ctx, workflowID, whole)
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
		if value[field+"_ref"], err = g.Ledger.PutRunBlob(ctx, workflowID, b); err != nil {
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
	return g.Ledger.Q.InsertModelCall(ctx, db.InsertModelCallParams{
		WorkflowID: run.WorkflowID, ActivityID: info.ActivityID, Attempt: int64(info.Attempt), RequestSeq: int64(seq),
		RequestBlob: reqRef, Provider: route.Provider, Model: route.Model, PromptVersion: route.PromptVersion,
		Effort: sql.NullString{String: route.Effort, Valid: route.Effort != ""}, StartedAt: ledger.Now(),
	})
}

// closeUnanswered closes the reservation of a request that got no answer. One that never
// left agentd is released; one the provider certainly did not serve (429, 529, other 4xx) is
// settled at zero; after any other failure, a 500 or 504 included, nobody knows whether the
// provider billed it, so the whole reservation is charged.
func (g *Gateway) closeUnanswered(ctx context.Context, r budget.Request, reserved int64, callErr error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	var rejected *models.RejectedError
	var err error
	switch {
	case errors.Is(callErr, models.ErrNotSent):
		err = budget.Release(ctx, g.Ledger.Q, r.ID)
	case errors.As(callErr, &rejected) && rejected.Unbilled():
		_, err = budget.Settle(ctx, g.Ledger.Q, r.ID, 0, r.Model, g.Prices.Version, budget.ReasonRejected)
	default:
		_, err = budget.Settle(ctx, g.Ledger.Q, r.ID, reserved, r.Model, g.Prices.Version, budget.ReasonOutcomeUnknown)
	}
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
