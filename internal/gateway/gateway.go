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
	"strings"
	"sync"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"golang.org/x/sync/singleflight"

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
)

type Gateway struct {
	Ledger   *ledger.Ledger
	Policy   *policy.Policy
	Tasks    tasks.Registry // for each run's scope
	Models   *models.Config
	Prices   *pricing.Table
	Budget   *budget.Budget
	Adapters map[string]models.Adapter // by adapter name in models.yaml
	Tools    *tools.Stubs
	Faults   *faults.Script // nil unless agentd runs with --fault-script
	Log      *slog.Logger

	modelCalls singleflight.Group // in-flight model calls, by workflow and activity ID
	effects    singleflight.Group // in-flight external effects, by effect ID
	executions sync.WaitGroup     // detached executions, drained on shutdown
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
	adapter, ok := g.Adapters[route.Adapter]
	if !ok {
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("adapter %q is not in this build (arrives in M2)", route.Adapter), "AdapterUnavailable", nil)
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
	q := g.Ledger.Q
	prev, err := q.GetFinishedModelCall(ctx, db.GetFinishedModelCallParams{WorkflowID: run.WorkflowID, ActivityID: info.ActivityID})
	switch {
	case err == nil:
		// An earlier attempt got an answer Temporal never recorded (its completion was
		// lost). Serve that answer instead of paying for a second call.
		return g.loadResponse(ctx, prev.ResponseBlob.String)
	case !errors.Is(err, sql.ErrNoRows):
		return models.Response{}, err
	}

	req := models.Request{
		WorkflowID: run.WorkflowID, Route: route, Args: args,
		// One network request per attempt: a retry is a new request with its own reservation.
		RequestID: fmt.Sprintf("%s/%s/%d", run.WorkflowID, info.ActivityID, info.Attempt),
		Fault: models.FakeFault{Status: fx.ProviderStatus, LoseAnswer: fx.ProviderLoseAnswer,
			Unreachable: fx.ProviderUnreachable, ServedBy: fx.ProviderServedBy},
	}
	// Reserve the most the request can cost before it goes out (section 9).
	tokens, err := adapter.CountTokens(ctx, req)
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

	if err := g.recordRequest(ctx, info, run, route, args); err != nil {
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
	fx.StallBeforeRecord(ctx.Done())
	if resp.Model == "" {
		resp.Model = route.Name()
	}
	cost, err := g.Prices.Cost(resp.Model, resp.Usage)
	if err != nil {
		g.Log.Error("cannot price an answer; charging its whole reservation", "request_id", req.RequestID,
			"model", resp.Model, "error", err)
		cost = reserved
	}
	if cost > reserved {
		// The ledger records what was billed even so: the bound in section 9 failed.
		g.Log.Error("a request cost more than its reservation", "request_id", req.RequestID,
			"cost_micros", cost, "reserved_micros", reserved)
	}
	respJSON, err := json.Marshal(resp)
	if err != nil {
		return models.Response{}, err
	}
	respRef, err := g.Ledger.PutBlob(ctx, respJSON)
	if err != nil {
		return models.Response{}, err
	}
	usage, err := json.Marshal(resp.Usage)
	if err != nil {
		return models.Response{}, err
	}
	// The answer and its charge are recorded together. A crash or a failed write before
	// this leaves the reservation open: it stays held, and agentd charges it in full when
	// it starts again.
	if err := g.Ledger.Tx(ctx, func(q *db.Queries) error {
		if err := q.FinishModelCall(ctx, db.FinishModelCallParams{
			ResponseBlob: sql.NullString{String: respRef, Valid: true},
			StopReason:   sql.NullString{String: resp.StopReason, Valid: resp.StopReason != ""},
			UsageJson:    sql.NullString{String: string(usage), Valid: true},
			FinishedAt:   sql.NullString{String: ledger.Now(), Valid: true},
			WorkflowID:   run.WorkflowID, ActivityID: info.ActivityID, Attempt: int64(info.Attempt),
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

// recordRequest writes the exact request of a model call before it is sent.
func (g *Gateway) recordRequest(ctx context.Context, info activity.Info, run db.Run, route models.Route, args map[string]any) error {
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
		WorkflowID: run.WorkflowID, ActivityID: info.ActivityID, Attempt: int64(info.Attempt),
		RequestBlob: reqRef, Provider: route.Provider, Model: route.Model, PromptVersion: route.PromptVersion,
		Effort: sql.NullString{String: route.Effort, Valid: route.Effort != ""}, StartedAt: ledger.Now(),
	})
}

// closeUnanswered closes the reservation of a request that got no answer. One that never
// left agentd is released; one the provider turned away (429, 5xx) is settled at zero; after
// any other failure nobody knows whether the provider billed it, so the whole reservation
// is charged.
func (g *Gateway) closeUnanswered(ctx context.Context, r budget.Request, reserved int64, callErr error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	var rejected *models.RejectedError
	var err error
	switch {
	case errors.Is(callErr, models.ErrNotSent):
		err = budget.Release(ctx, g.Ledger.Q, r.ID)
	case errors.As(callErr, &rejected):
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
