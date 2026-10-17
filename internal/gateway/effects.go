package gateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/faults"
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
	"github.com/jchen0426/agentos/internal/policy"
	"github.com/jchen0426/agentos/internal/tools"
)

// effectOutcome is what an effect's execution produced; it is stored as the effect's result.
type effectOutcome struct {
	Status string         `json:"status"` // a Result status, or "failed"
	Value  map[string]any `json:"value"`
}

// callTool runs one tool call. Writes follow the effect protocol of section 6: record the
// intent, execute, record the result. M1 implements the part T8 needs; recovery by effect
// class after an agentd crash arrives in M3.
func (g *Gateway) callTool(ctx context.Context, info activity.Info, run db.Run, tool, class string,
	args map[string]any, canon []byte, fx faults.Effect) (*agentosv1.Result, error) {
	if class == policy.EffectNone {
		// Pure reads: no effect record; a retry simply reads again.
		v, err := g.track(func() (any, error) {
			exec, cancel := detach(ctx, info)
			defer cancel()
			fx.Stall(exec.Done())
			return g.Tools.Call(exec, tools.Request{WorkflowID: run.WorkflowID, Tool: tool, Args: args})
		})
		if err != nil {
			return nil, err
		}
		return result(StatusOK, v.(map[string]any))
	}

	// The same payload in the same run is the same effect, on every attempt.
	effectID := sha256Hex([]byte(run.WorkflowID), []byte(tool), canon)
	v, err, _ := g.effects.Do(effectID, func() (any, error) {
		return g.track(func() (any, error) {
			exec, cancel := detach(ctx, info)
			defer cancel()
			return g.effectOnce(exec, run, tool, class, effectID, canon, args, fx)
		})
	})
	if err != nil {
		return nil, err
	}
	out := v.(effectOutcome)
	if out.Status == "failed" {
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("tool %s failed: %v", tool, out.Value["error"]), "ToolFailed", nil)
	}
	return result(out.Status, out.Value)
}

func (g *Gateway) effectOnce(ctx context.Context, run db.Run, tool, class, effectID string, canon []byte,
	args map[string]any, fx faults.Effect) (effectOutcome, error) {
	q := g.Ledger.Q
	now := ledger.Now()
	idempotencyKey := "agentos-" + effectID[:32]
	inserted, err := q.InsertEffect(ctx, db.InsertEffectParams{
		EffectID: effectID, WorkflowID: run.WorkflowID, Tool: tool, EffectClass: class,
		PayloadSha256: sha256Hex(canon), IdempotencyKey: idempotencyKey, Status: "PENDING",
		CreatedAt: now, UpdatedAt: now,
	})
	if err != nil {
		return effectOutcome{}, err
	}
	if inserted == 0 {
		// An earlier attempt recorded this effect and is no longer running here (a
		// concurrent attempt would have joined it through singleflight instead).
		e, err := q.GetEffect(ctx, effectID)
		if err != nil {
			return effectOutcome{}, err
		}
		if e.Status == "SUCCEEDED" || e.Status == "FAILED" {
			return g.loadEffect(ctx, e)
		}
		// PENDING with no execution behind it (agentd restarted mid-call), UNKNOWN or
		// AWAITING_APPROVAL. M3 recovers by effect class; until then a person decides.
		return effectOutcome{Status: StatusNeedsReconcile,
			Value: map[string]any{"effect_id": effectID, "effect_status": e.Status}}, nil
	}

	// The intent is committed; only now does the request go out.
	fx.Stall(ctx.Done())
	value, callErr := g.Tools.Call(ctx, tools.Request{
		WorkflowID: run.WorkflowID, Tool: tool, Args: args, IdempotencyKey: idempotencyKey,
	})
	fx.StallBeforeRecord(ctx.Done())
	out, status := effectOutcome{Status: StatusOK, Value: value}, "SUCCEEDED"
	switch {
	case callErr == nil:
	case errors.Is(callErr, tools.ErrNotSent):
		// The request never left: nothing happened outside, a definite failure.
		out, status = effectOutcome{Status: "failed", Value: map[string]any{"error": callErr.Error()}}, "FAILED"
	default:
		// The write may or may not have happened (a timeout after sending, a broken
		// connection): a person has to check. M3 adds lookups by effect class first.
		out, status = effectOutcome{Status: StatusNeedsReconcile,
			Value: map[string]any{"effect_id": effectID, "error": callErr.Error()}}, "UNKNOWN"
	}
	body, err := json.Marshal(out)
	if err != nil {
		return effectOutcome{}, err
	}
	ref, err := g.Ledger.PutBlob(ctx, body)
	if err != nil {
		return effectOutcome{}, err
	}
	// A failure here leaves the effect PENDING: the next attempt then asks a person.
	if _, err := q.FinishEffect(ctx, db.FinishEffectParams{
		Status: status, ResultBlob: sql.NullString{String: ref, Valid: true},
		UpdatedAt: ledger.Now(), EffectID: effectID,
	}); err != nil {
		return effectOutcome{}, err
	}
	fx.StallAfterRecord(ctx.Done())
	return out, nil
}

func (g *Gateway) loadEffect(ctx context.Context, e db.Effect) (effectOutcome, error) {
	b, err := g.Ledger.GetBlob(ctx, e.ResultBlob.String)
	if err != nil {
		return effectOutcome{}, err
	}
	var out effectOutcome
	err = json.Unmarshal(b, &out)
	return out, err
}
