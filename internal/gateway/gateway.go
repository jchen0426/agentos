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
	"strings"
	"sync"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"golang.org/x/sync/singleflight"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/faults"
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
	"github.com/jchen0426/agentos/internal/models"
	"github.com/jchen0426/agentos/internal/policy"
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
	Models   *models.Config
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
		// M2: reserve budget here; a refused reservation returns budget_exhausted.
		return g.callModel(ctx, info, run, route, args, fx)
	case "tool":
		v := g.Policy.EvaluateTool(c.GetName(), args)
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

	reqJSON, err := json.Marshal(map[string]any{
		"task": route.Task, "provider": route.Provider, "model": route.Model, "effort": route.Effort, "args": args,
	})
	if err != nil {
		return models.Response{}, err
	}
	reqRef, err := g.Ledger.PutBlob(ctx, reqJSON)
	if err != nil {
		return models.Response{}, err
	}
	err = q.InsertModelCall(ctx, db.InsertModelCallParams{
		WorkflowID: run.WorkflowID, ActivityID: info.ActivityID, Attempt: int64(info.Attempt),
		RequestBlob: reqRef, Provider: route.Provider, Model: route.Model, PromptVersion: route.PromptVersion,
		Effort: sql.NullString{String: route.Effort, Valid: route.Effort != ""}, StartedAt: ledger.Now(),
	})
	if err != nil {
		return models.Response{}, err
	}

	fx.Stall(ctx.Done())
	resp, err := adapter.Call(ctx, models.Request{WorkflowID: run.WorkflowID, Route: route, Args: args})
	if err != nil {
		return models.Response{}, err // M2 maps provider errors to retryable and non-retryable
	}
	fx.StallBeforeRecord(ctx.Done())
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
	if err := q.FinishModelCall(ctx, db.FinishModelCallParams{
		ResponseBlob: sql.NullString{String: respRef, Valid: true},
		StopReason:   sql.NullString{String: resp.StopReason, Valid: resp.StopReason != ""},
		UsageJson:    sql.NullString{String: string(usage), Valid: true},
		FinishedAt:   sql.NullString{String: ledger.Now(), Valid: true},
		WorkflowID:   run.WorkflowID, ActivityID: info.ActivityID, Attempt: int64(info.Attempt),
	}); err != nil {
		return models.Response{}, err
	}
	fx.StallAfterRecord(ctx.Done())
	return resp, nil
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
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(m); err != nil {
		return nil, nil, err
	}
	return m, bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
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
