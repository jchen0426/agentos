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
	Tools    *tools.Stubs
	Faults   *faults.Script // nil unless agentd runs with --fault-script
	Log      *slog.Logger

	modelCalls singleflight.Group // in-flight model calls, by workflow and activity ID
	effects    singleflight.Group // in-flight external effects, by effect ID
	executions sync.WaitGroup     // detached executions, drained on shutdown
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
	switch {
	case errors.Is(err, ledger.ErrRunStarting):
		return nil, err // retried until StartRun has recorded the run ID
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
	log.Info("invoke finished", "status", res.GetStatus())
	return res, nil
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
		return g.callModel(ctx, info, run, route, args, fx)
	case "tool":
		scope, err := g.scope(run)
		if err != nil {
			return result(StatusDenied, map[string]any{"reason": err.Error()})
		}
		// A call that leaves part of its run's scope out gets the run's own value, never the
		// tool's default (the default branch, the latest commit). One that names another
		// value is denied by the policy.
		args, canon, err = fillScope(args, scope)
		if err != nil {
			return nil, err
		}
		v := g.Policy.EvaluateTool(c.GetName(), args, scope)
		switch v.Decision {
		case policy.Deny:
			return result(StatusDenied, map[string]any{"reason": v.Reason})
		case policy.RequireApproval:
			// M4 replaces this with the approvals table; until then nobody can approve.
			return result(StatusNeedsApproval, nil)
		}
		if fx.Status != "" {
			return result(fx.Status, nil)
		}
		return g.callTool(ctx, info, run, c.GetName(), v.Effect, args, canon, fx)
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

// fillScope adds each scoped argument the call left out, with the run's value, and encodes
// the result again: the canonical form, and so the effect ID, covers what the tool receives.
func fillScope(args map[string]any, scope map[string]string) (map[string]any, []byte, error) {
	filled := maps.Clone(args)
	for arg, v := range scope {
		if _, ok := filled[arg]; !ok {
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
	args map[string]any, fx faults.Effect) (*agentosv1.Result, error) {
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
			return g.modelCallOnce(exec, info, run, route, adapter, args, fx)
		})
	})
	if err != nil {
		return nil, err
	}
	resp := v.(models.Response)
	return result(resp.Status, resp.Value)
}

func (g *Gateway) modelCallOnce(ctx context.Context, info activity.Info, run db.Run, route models.Route,
	adapter models.Adapter, args map[string]any, fx faults.Effect) (models.Response, error) {
	prev, err := g.Ledger.Q.GetFinishedModelCall(ctx, db.GetFinishedModelCallParams{WorkflowID: run.WorkflowID, ActivityID: info.ActivityID})
	switch {
	case err == nil:
		// An earlier attempt got an answer Temporal never recorded (its completion was
		// lost). Serve that answer instead of paying for a second call.
		return g.loadResponse(ctx, prev.ResponseBlob.String)
	case !errors.Is(err, sql.ErrNoRows):
		return models.Response{}, err
	}
	for seq := 1; ; seq++ {
		resp, err := g.modelRequest(ctx, info, run, route, adapter, args, fx, seq)
		// An answer cut off at max_tokens would be cut off again: only a larger max_tokens helps.
		if err != nil || resp.Status != StatusInvalidOutput || seq == generations || resp.StopReason == "max_tokens" {
			return resp, err
		}
		g.Log.Warn("the answer broke the output schema; asking again", "workflow_id", run.WorkflowID,
			"activity_id", info.ActivityID, "request_seq", seq, "reason", resp.Value["reason"])
	}
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
	req := models.Request{
		WorkflowID: run.WorkflowID, Route: route, Args: args,
		// A retry, and a second generation, is a new request with its own reservation.
		RequestID: fmt.Sprintf("%s/%s/%d/%d", run.WorkflowID, info.ActivityID, info.Attempt, seq),
		Fault: models.FakeFault{Status: fx.ProviderStatus, LoseAnswer: fx.ProviderLoseAnswer,
			Unreachable: fx.ProviderUnreachable, ServedBy: fx.ProviderServedBy,
			InvalidOutput: fx.ProviderBadOutput && seq == 1},
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
	if errors.Is(err, models.ErrBadArguments) {
		return models.Response{}, temporal.NewNonRetryableApplicationError(err.Error(), "BadArguments", err)
	}
	if err != nil {
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
	if err := g.recordRequest(ctx, info, run, route, args, seq); err != nil {
		g.closeUnanswered(ctx, breq, reserved, fmt.Errorf("%w: %v", models.ErrNotSent, err))
		return models.Response{}, err
	}
	fx.Stall(ctx.Done())
	resp, err := adapter.Call(ctx, req)
	if err != nil {
		g.closeUnanswered(ctx, breq, reserved, err)
		var rejected *models.RejectedError
		if errors.As(err, &rejected) && !rejected.Retryable() {
			return models.Response{}, temporal.NewNonRetryableApplicationError(err.Error(), "ProviderRejected", err)
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
				return models.Response{}, err
			}
			resp.Status, stop = StatusInvalidOutput, StatusInvalidOutput
			resp.Value = map[string]any{"reason": invalid.Error(), "stop_reason": resp.StopReason}
		}
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
		return models.Response{}, err
	}
	fx.StallAfterRecord(ctx.Done())
	return resp, nil
}

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
	reqJSON, err := json.Marshal(map[string]any{
		"task": route.Task, "provider": route.Provider, "model": route.Model, "effort": route.Effort, "args": args,
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
// start-to-close timeout.
func detach(ctx context.Context, info activity.Info) (context.Context, context.CancelFunc) {
	timeout := info.StartToCloseTimeout
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	return context.WithTimeout(context.WithoutCancel(ctx), timeout)
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
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		t := time.NewTicker(heartbeatInterval(timeout))
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				activity.RecordHeartbeat(ctx)
			}
		}
	}()
	return cancel
}
