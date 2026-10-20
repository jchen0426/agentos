package gateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/faults"
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
	"github.com/jchen0426/agentos/internal/outbox"
	"github.com/jchen0426/agentos/internal/policy"
	"github.com/jchen0426/agentos/internal/tools"
)

// Effect statuses in the effects ledger (design doc, section 6).
const (
	effectAwaitingApproval = "AWAITING_APPROVAL" // M4
	effectPending          = "PENDING"           // recorded; maybe claimed, maybe sent
	effectSucceeded        = "SUCCEEDED"
	effectFailed           = "FAILED"    // the external system refused it: nothing happened
	effectUnknown          = "UNKNOWN"   // nobody can tell whether it happened: a person reconciles it
	effectAbandoned        = "ABANDONED" // never sent, and never will be
	effectDenied           = "DENIED"    // the tool refused the payload before sending anything: counts against the run
	effectDryRun           = "DRY_RUN"   // rendered and recorded, not sent
)

// errEffectBusy is retryable: another execution in this process holds the effect, or changed
// it while this one looked. The next attempt finds it settled.
var errEffectBusy = temporal.NewApplicationError("the effect is being executed or changed; retry", "EffectBusy")

// CheckTools matches the policy's tools against the tools agentd has, at start: each tool the
// policy lists exists, a class the policy states is never weaker than the tool's own, and only a
// write asks for approval: an approval gates an effect, and a read has none.
func CheckTools(p *policy.Policy, reg tools.Registry) error {
	for name, rule := range p.Tools {
		t, ok := reg[name]
		if !ok {
			return fmt.Errorf("policy: tool %s is not a tool agentd has", name)
		}
		if _, err := effectClass(t, rule.Effect); err != nil {
			return fmt.Errorf("policy: tool %s: %w", name, err)
		}
		if _, isWriter := t.(tools.Writer); rule.Decision == "require_approval" && !isWriter {
			return fmt.Errorf("policy: tool %s: require_approval, but the tool only reads: approvals gate writes", name)
		}
	}
	return nil
}

// effectClass is the class a call of t runs under: the tool's own, or unsafe when the policy
// says so for a write. A policy cannot make a write look safer than its tool declares.
func effectClass(t tools.Tool, stated string) (string, error) {
	declared := t.Class()
	_, isWriter := t.(tools.Writer)
	_, isFinder := t.(tools.Finder)
	switch {
	case isWriter == (declared == policy.EffectNone), declared == policy.EffectLookup && !isFinder:
		return "", fmt.Errorf("the tool declares class %q but does not implement it", declared)
	case stated == "" || stated == declared:
		return declared, nil
	case stated == policy.EffectUnsafe && isWriter:
		return stated, nil
	}
	return "", fmt.Errorf("effect %q, but the tool declares %q; a policy may state the tool's own class, or unsafe for a write", stated, declared)
}

// callTool runs one tool call. A read just runs. A write follows the effect protocol of
// section 6: record the payload, claim it, send it, record the result; and when nobody knows
// what happened, recover by the tool's class. maxPerRun caps the run's effects of a write. A
// write the policy wants approved (rule) waits for a person's approval before it is claimed.
func (g *Gateway) callTool(ctx context.Context, info activity.Info, run db.Run, t tools.Tool, class string, maxPerRun int,
	rule *approvalRule, args map[string]any, canon []byte, fx faults.Effect) (*agentosv1.Result, error) {
	created, err := time.Parse(time.RFC3339Nano, run.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("run %s: created_at: %w", run.WorkflowID, err)
	}
	req := tools.Request{WorkflowID: run.WorkflowID, RunCreatedAt: created, Args: args}
	if r, ok := t.(tools.Reader); ok {
		v, err := g.track(func() (any, error) {
			exec, cancel := detach(ctx, info)
			defer cancel()
			fx.Stall(exec.Done())
			return r.Read(exec, req)
		})
		if err != nil {
			return toolError(t.Name(), err)
		}
		return result(StatusOK, v.(map[string]any))
	}
	w := t.(tools.Writer)
	// The same payload in the same run is the same effect, on every attempt.
	req.EffectID = sha256Hex([]byte(run.WorkflowID), []byte(t.Name()), canon)
	v, err, _ := g.effects.Do(req.EffectID, func() (any, error) {
		return g.track(func() (any, error) {
			exec, cancel := detach(ctx, info)
			defer cancel()
			return g.effect(exec, run, w, class, maxPerRun, rule, req, canon, fx)
		})
	})
	if err != nil {
		return nil, err
	}
	return v.(*agentosv1.Result), nil
}

// toolError turns a tool's error into the call's outcome: a denial is a result, a refusal
// fails the call for good, and anything else is retried by Temporal.
func toolError(tool string, err error) (*agentosv1.Result, error) {
	var denied *tools.DeniedError
	var refused *tools.RefusedError
	switch {
	case errors.As(err, &denied):
		return result(StatusDenied, map[string]any{"reason": denied.Reason})
	case errors.As(err, &refused) && !refused.Retryable:
		return nil, temporal.NewNonRetryableApplicationError(fmt.Sprintf("tool %s: %s", tool, refused.Reason), "ToolFailed", nil)
	}
	return nil, err
}

// effect takes one write through the protocol, from wherever the ledger says it is. The
// effect's first call renders and records its payload; every later call goes on with the
// recorded payload, which is what went out or will, whatever has changed since. An effect that
// awaits approval answers from its approval until a person approves it.
func (g *Gateway) effect(ctx context.Context, run db.Run, t tools.Writer, class string, maxPerRun int,
	rule *approvalRule, req tools.Request, canon []byte, fx faults.Effect) (*agentosv1.Result, error) {
	e, err := g.Ledger.Q.GetEffect(ctx, req.EffectID)
	if errors.Is(err, sql.ErrNoRows) {
		if res, err := g.record(ctx, run, t, class, maxPerRun, rule, req, canon); res != nil || err != nil {
			return res, err
		}
		e, err = g.Ledger.Q.GetEffect(ctx, req.EffectID)
	}
	if err != nil {
		return nil, err
	}
	switch e.Status {
	case effectAwaitingApproval:
		if res, err := g.awaitApproval(ctx, run, t, e); res != nil || err != nil {
			return res, err
		}
		if e, err = g.Ledger.Q.GetEffect(ctx, e.EffectID); err != nil { // approved: PENDING now
			return nil, err
		}
	case effectAbandoned:
		if res, err := g.decidedResult(ctx, t, e); res != nil || err != nil {
			return res, err
		}
	}
	if e.Status != effectPending {
		return g.effectResult(ctx, e)
	}
	payload, err := g.payload(ctx, e)
	if err != nil {
		return nil, err
	}
	fx.Stall(ctx.Done()) // recorded, not yet claimed
	for {
		switch {
		case e.Status != effectPending:
			return g.effectResult(ctx, e)
		case !e.ClaimedBy.Valid:
			if run.DryRun != 0 || !t.Live() {
				return g.settle(ctx, e, effectDryRun, t.DryRun(payload), "")
			}
			// A write the policy wants approved goes only with an approval in its time: recorded
			// before the policy asked, or while the tool could only dry-run, it waits for one now.
			if rule != nil {
				if res, err := g.gate(ctx, run, t, e, rule, req, canon, payload); res != nil || err != nil {
					return res, err
				}
			}
			claimed, ok, err := g.claimToSend(ctx, e)
			if err != nil {
				return nil, err
			}
			if ok {
				return g.send(ctx, run, t, claimed, payload, fx)
			}
			if cancelled, err := g.runCancelled(ctx, run.WorkflowID); err != nil {
				return nil, err
			} else if cancelled {
				if e, err = g.Ledger.Q.GetEffect(ctx, e.EffectID); err != nil {
					return nil, err
				}
				if e.Status == effectPending && !e.ClaimedBy.Valid {
					return g.settle(ctx, e, effectAbandoned, nil, "the run was cancelled before the request was sent")
				}
			}
		case e.ClaimedBy.String == g.Incarnation && g.isSending(e.EffectID):
			return nil, errEffectBusy
		default:
			// Claimed by an agentd process that is gone, or by this one with nothing in
			// flight: the request may or may not have gone out. Sent again only as the policy
			// allows: a write it wants approved, without an approval in its time, is only looked
			// up, and if given back meets gate on the next call.
			resend, err := g.mayResend(ctx, e, rule)
			if err != nil {
				return nil, err
			}
			return g.recoverEffect(ctx, run, t, e, payload, "agentd lost track of the request after claiming it", resend)
		}
		// Another change came first: look again.
		if e, err = g.Ledger.Q.GetEffect(ctx, e.EffectID); err != nil {
			return nil, err
		}
	}
}

// record renders a new effect's payload and records it, unless the run already has as many
// effects of the tool as the policy allows: those that went out or may have. A write the policy
// wants approved (rule) is recorded AWAITING_APPROVAL, with its approval, in the same
// transaction; when the call carries a reviewer's edit, the approval it replaces is superseded
// there too. It returns a result only when the call ends here.
func (g *Gateway) record(ctx context.Context, run db.Run, t tools.Writer, class string, maxPerRun int,
	rule *approvalRule, req tools.Request, canon []byte) (*agentosv1.Result, error) {
	payload, err := t.Prepare(ctx, req)
	if err != nil {
		return toolError(t.Name(), err)
	}
	ref, err := g.Ledger.PutBlob(ctx, payload)
	if err != nil {
		return nil, err
	}
	status := effectPending
	var approval *db.InsertApprovalParams
	if rule != nil {
		// What the reviewer is shown is gathered (requests to GitHub among it) only for a call the
		// run has room for, which the transaction below counts again. One at a time for the run
		// and tool, from that count to the record: calls with other arguments at once would each
		// gather it first.
		unlock, err := g.recording.lock(ctx, run.WorkflowID+"\x00"+t.Name())
		if err != nil {
			return nil, err
		}
		defer unlock()
		if n, err := g.writesBefore(ctx, run, t, req.EffectID, canon); err != nil {
			return nil, err
		} else if maxPerRun > 0 && n >= int64(maxPerRun) {
			return result(StatusDenied, map[string]any{"reason": fmt.Sprintf(
				"the run already has %d %s write(s); the policy allows %d per run", n, t.Name(), maxPerRun)})
		}
		card, err := g.describe(ctx, run, t, req, payload)
		if err != nil {
			return nil, err
		}
		a := newApproval(run, t.Name(), req.EffectID, canon, sha256Hex(payload), card, rule.ttl)
		status, approval = effectAwaitingApproval, &a
	}
	var already int64
	err = g.Ledger.Tx(ctx, func(q *db.Queries) error {
		if approval != nil {
			if err := supersedeEdited(ctx, q, approval, sha256Hex(canon)); err != nil {
				return err
			}
		}
		n, err := q.CountRunEffects(ctx, db.CountRunEffectsParams{WorkflowID: run.WorkflowID, Tool: t.Name(), EffectID: req.EffectID})
		if err != nil {
			return err
		}
		if maxPerRun > 0 && n >= int64(maxPerRun) {
			already = n
			return errAtLimit // superseding nothing either
		}
		now := ledger.Now()
		inserted, err := q.InsertEffect(ctx, db.InsertEffectParams{
			EffectID: req.EffectID, WorkflowID: run.WorkflowID, Tool: t.Name(), EffectClass: class,
			PayloadSha256: sha256Hex(payload), PayloadBlob: valid(ref), IdempotencyKey: "agentos-" + req.EffectID[:32],
			Status: status, CreatedAt: now, UpdatedAt: now,
		})
		switch {
		case err != nil:
			return err
		case inserted == 0:
			return errChanged // recorded meanwhile: the caller reads it
		case approval != nil:
			return q.InsertApproval(ctx, *approval)
		}
		return nil
	})
	switch {
	case errors.Is(err, errAtLimit):
		return result(StatusDenied, map[string]any{"reason": fmt.Sprintf(
			"the run already has %d %s write(s); the policy allows %d per run", already, t.Name(), maxPerRun)})
	case errors.Is(err, errChanged):
		return nil, nil
	case err != nil:
		return nil, err
	}
	if approval != nil {
		g.Log.Info("approval requested", "workflow_id", run.WorkflowID, "approval_id", approval.ApprovalID,
			"effect_id", req.EffectID, "tool", t.Name(), "expires_at", approval.ExpiresAt, "supersedes", approval.Supersedes.String)
		g.wakeExpiry()
	}
	return nil, nil
}

// errAtLimit rolls back the recording of an effect the run has no room for.
var errAtLimit = errors.New("the run has as many effects of the tool as the policy allows")

// payload is an effect's recorded payload.
func (g *Gateway) payload(ctx context.Context, e db.Effect) (json.RawMessage, error) {
	if !e.PayloadBlob.Valid {
		return nil, fmt.Errorf("effect %s has no recorded payload", e.EffectID)
	}
	b, err := g.Ledger.GetBlob(ctx, e.PayloadBlob.String)
	if err != nil {
		return nil, err
	}
	if sha256Hex(b) != e.PayloadSha256 {
		return nil, fmt.Errorf("effect %s: the payload does not match its hash", e.EffectID)
	}
	return b, nil
}

// claim lets this process send e, if nobody changed e since it was read and its run was not
// cancelled. It returns e as claimed.
func (g *Gateway) claim(ctx context.Context, e db.Effect) (db.Effect, bool, error) {
	now := ledger.Now()
	n, err := g.Ledger.Q.ClaimEffect(ctx, db.ClaimEffectParams{
		ClaimedBy: valid(g.Incarnation), ClaimedAt: valid(now), UpdatedAt: now, EffectID: e.EffectID, Version: e.Version,
	})
	if err != nil || n == 0 {
		return e, false, err
	}
	e.ClaimedBy, e.ClaimedAt, e.Version = valid(g.Incarnation), valid(now), e.Version+1
	return e, true, nil
}

// claimToSend claims e and marks it as being sent, as one step for this process: whoever here
// sees the claim also sees the send, and does not take the effect for one nobody is sending.
func (g *Gateway) claimToSend(ctx context.Context, e db.Effect) (db.Effect, bool, error) {
	g.sendMu.Lock()
	defer g.sendMu.Unlock()
	claimed, ok, err := g.claim(ctx, e)
	if ok {
		if g.sending == nil {
			g.sending = map[string]bool{}
		}
		g.sending[e.EffectID] = true
	}
	return claimed, ok, err
}

func (g *Gateway) isSending(effectID string) bool {
	g.sendMu.Lock()
	defer g.sendMu.Unlock()
	return g.sending[effectID]
}

// proceed is what a tool asks before each visible request of e: the run must not have been
// cancelled since, and the request's time is recorded, for a later lookup's wait.
func (g *Gateway) proceed(e db.Effect) tools.Proceed {
	return func(ctx context.Context) error {
		now := ledger.Now()
		n, err := g.Ledger.Q.TouchEffect(ctx, db.TouchEffectParams{
			ClaimedAt: valid(now), LastRequestAt: valid(now), UpdatedAt: now, EffectID: e.EffectID, Version: e.Version,
			ClaimedBy: valid(g.Incarnation),
		})
		switch {
		case err != nil:
			return fmt.Errorf("%w: %v", tools.ErrNotSent, err)
		case n == 0:
			return tools.ErrStopped
		}
		return nil
	}
}

// send sends an effect claimed with claimToSend and records what came of it.
func (g *Gateway) send(ctx context.Context, run db.Run, t tools.Writer, e db.Effect, payload json.RawMessage,
	fx faults.Effect) (*agentosv1.Result, error) {
	defer func() {
		g.sendMu.Lock()
		delete(g.sending, e.EffectID)
		g.sendMu.Unlock()
	}()
	fx.StallAfterClaim(ctx.Done())
	value, err := t.Execute(ctx, payload, e.IdempotencyKey, g.proceed(e))
	fx.StallBeforeRecord(ctx.Done())
	var refused *tools.RefusedError
	var conflict *tools.ConflictError
	var denied *tools.DeniedError
	switch {
	case err == nil:
		res, err := g.settle(ctx, e, effectSucceeded, value, "")
		fx.StallAfterRecord(ctx.Done())
		return res, err
	case errors.As(err, &denied):
		// The tool refused the recorded payload before sending anything: it never will be.
		return g.settle(ctx, e, effectDenied, nil, denied.Reason)
	case errors.Is(err, tools.ErrNotSent) || errors.As(err, &refused) && refused.Retryable:
		// This attempt did nothing outside. A cancelled run's effect is done with; anyone else's goes
		// back for a later attempt: as one that never took effect, unless an earlier attempt may
		// have (e is as it was when this one began).
		if cancelled, cerr := g.runCancelled(ctx, run.WorkflowID); cerr == nil && cancelled {
			return g.settle(ctx, e, effectAbandoned, nil, "the run was cancelled before the request was sent")
		}
		if _, rerr := g.giveBack(ctx, e, e.MaybeSent == 0); rerr != nil {
			g.Log.Error("could not release an unsent effect", "effect_id", e.EffectID, "error", rerr)
		}
		return nil, temporal.NewApplicationError(fmt.Sprintf("tool %s: %v", t.Name(), err), "ToolNotSent")
	case errors.As(err, &refused):
		return g.settle(ctx, e, effectFailed, nil, refused.Reason)
	case errors.As(err, &conflict):
		return g.settle(ctx, e, effectUnknown, nil, conflict.Reason)
	}
	g.Log.Warn("effect outcome unknown", "workflow_id", run.WorkflowID, "effect_id", e.EffectID, "tool", t.Name(), "error", err)
	// The attempt may have taken effect if it made a visible request, which proceed marked in the
	// ledger: then whatever a later attempt does, the effect stays in flight.
	cur, rerr := g.Ledger.Q.GetEffect(ctx, e.EffectID)
	if rerr != nil {
		return nil, rerr
	}
	e.MaybeSent = cur.MaybeSent
	return g.recoverEffect(ctx, run, t, e, payload, err.Error(), true)
}

// errEffectReleased is retryable: the effect was given back, and the run's next call takes it up.
var errEffectReleased = temporal.NewApplicationError("the effect was given back for the run's next call", "EffectReleased")

// recoverEffect settles an effect whose request may have gone out, by the class it was
// recorded under (section 6): an idempotent tool sends again under the same key; a lookup
// tool looks, and sends again only once the old request can no longer take effect; anything
// else waits for a person. A cancelled run sends nothing more: it only looks. Without
// mayResend (a recovery no call asked for, as at start) nothing is sent either: an effect that
// never took effect goes back for the run's next call, which comes only if the run goes on.
func (g *Gateway) recoverEffect(ctx context.Context, run db.Run, t tools.Writer, e db.Effect, payload json.RawMessage,
	why string, mayResend bool) (*agentosv1.Result, error) {
	cancelled, err := g.runCancelled(ctx, run.WorkflowID)
	if err != nil {
		return nil, err
	}
	finder, canFind := t.(tools.Finder)
	switch class := e.EffectClass; {
	case class == policy.EffectIdempotent && !cancelled && mayResend && e.MaybeSent != 0:
		return g.resend(ctx, run, t, e, payload) // in flight, whatever its approval's time
	case class == policy.EffectIdempotent && !cancelled && mayResend:
		return g.resendNew(ctx, run, t, e, payload) // nothing of it went out
	case class == policy.EffectIdempotent && !cancelled:
		return g.release(ctx, e, false) // not looked up: it may have taken effect
	case class == policy.EffectUnsafe || !canFind:
		return g.settle(ctx, e, effectUnknown, nil, "the outcome is unknown: "+why)
	}
	v, found, err := finder.Find(ctx, payload)
	if err != nil {
		return g.settle(ctx, e, effectUnknown, nil, "the outcome is unknown, and looking it up failed: "+err.Error())
	}
	if !found {
		// The request may still be on its way. Look again once it no longer can be: the wait
		// counts from its last visible request, which the tool may have sent after the claim.
		if e, err = g.Ledger.Q.GetEffect(ctx, e.EffectID); err != nil {
			return nil, err
		}
		if e.Status != effectPending {
			return g.effectResult(ctx, e)
		}
		sentAt, err := time.Parse(time.RFC3339Nano, e.ClaimedAt.String)
		if err != nil {
			return nil, fmt.Errorf("effect %s: claimed_at: %w", e.EffectID, err)
		}
		if wait := time.Until(sentAt.Add(finder.Settle())); wait > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(wait):
			}
			if v, found, err = finder.Find(ctx, payload); err != nil {
				return g.settle(ctx, e, effectUnknown, nil, "the outcome is unknown, and looking it up failed: "+err.Error())
			}
		}
		if cancelled, err = g.runCancelled(ctx, run.WorkflowID); err != nil {
			return nil, err
		}
	}
	switch {
	case found:
		return g.settle(ctx, e, effectSucceeded, v, "")
	case cancelled:
		return g.settle(ctx, e, effectAbandoned, nil, "the run was cancelled, and its request never took effect")
	case !mayResend:
		return g.release(ctx, e, true)
	}
	return g.resendNew(ctx, run, t, e, payload)
}

// resendNew sends again an effect none of whose requests took effect, as far as agentd knows. It is
// a new request, which an approval allows only in its time: past it, the effect goes back to gate,
// which voids the approval.
func (g *Gateway) resendNew(ctx context.Context, run db.Run, t tools.Writer, e db.Effect, payload json.RawMessage) (*agentosv1.Result, error) {
	if lapsed, err := g.lapsed(ctx, e.EffectID); err != nil {
		return nil, err
	} else if lapsed {
		return g.release(ctx, e, true)
	}
	e.MaybeSent = 0 // no earlier request took effect: only the next one can
	return g.resend(ctx, run, t, e, payload)
}

// release gives back a claimed effect for the run's next call: one a lookup found never took
// effect (unsent), or an idempotent one that may have, which the next call sends again under its
// key, or a person settles.
func (g *Gateway) release(ctx context.Context, e db.Effect, unsent bool) (*agentosv1.Result, error) {
	n, err := g.giveBack(ctx, e, unsent)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, errEffectBusy
	}
	if unsent {
		g.Log.Info("the request never took effect; the run's next call sends it", "workflow_id", e.WorkflowID, "effect_id", e.EffectID)
	} else {
		g.Log.Info("the request may have taken effect; the run's next call takes it up", "workflow_id", e.WorkflowID, "effect_id", e.EffectID)
	}
	return nil, errEffectReleased
}

// giveBack releases a claimed effect. Unsent, it is one that never took effect, as far as agentd
// knows: no attempt so far did. Otherwise an attempt may have, and the effect stays in flight.
func (g *Gateway) giveBack(ctx context.Context, e db.Effect, unsent bool) (int64, error) {
	p := db.ReleaseEffectParams{UpdatedAt: ledger.Now(), EffectID: e.EffectID, Version: e.Version}
	if unsent {
		return g.Ledger.Q.ReleaseUnsentEffect(ctx, db.ReleaseUnsentEffectParams(p))
	}
	return g.Ledger.Q.ReleaseEffect(ctx, p)
}

// resend claims e again and sends it under its original key.
func (g *Gateway) resend(ctx context.Context, run db.Run, t tools.Writer, e db.Effect, payload json.RawMessage) (*agentosv1.Result, error) {
	claimed, ok, err := g.claimToSend(ctx, e)
	if err != nil {
		return nil, err
	}
	if !ok {
		// Cancelled meanwhile, or changed: a cancelled run's effect is done with.
		if cancelled, err := g.runCancelled(ctx, run.WorkflowID); err == nil && cancelled {
			if cur, err := g.Ledger.Q.GetEffect(ctx, e.EffectID); err == nil && cur.Status == effectPending {
				return g.settle(ctx, cur, effectAbandoned, nil, "the run was cancelled, and its request never took effect")
			}
		}
		return nil, errEffectBusy
	}
	g.Log.Info("sending an effect again", "workflow_id", run.WorkflowID, "effect_id", e.EffectID, "tool", t.Name())
	return g.send(ctx, run, t, claimed, payload, faults.Effect{})
}

// settle records how an effect ended, if nobody changed it since e was read, and returns
// what the call gets. When another change came first, it returns what that one recorded.
func (g *Gateway) settle(ctx context.Context, e db.Effect, status string, value map[string]any, reason string) (*agentosv1.Result, error) {
	var blob sql.NullString
	if value != nil {
		b, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		ref, err := g.Ledger.PutBlob(ctx, b)
		if err != nil {
			return nil, err
		}
		blob = valid(ref)
	}
	n, err := g.Ledger.Q.SetEffectStatus(ctx, db.SetEffectStatusParams{
		Status: status, ResultBlob: blob, Reason: sql.NullString{String: reason, Valid: reason != ""},
		UpdatedAt: ledger.Now(), EffectID: e.EffectID, Version: e.Version,
	})
	if err != nil {
		return nil, err
	}
	if n == 0 {
		cur, err := g.Ledger.Q.GetEffect(ctx, e.EffectID)
		if err != nil {
			return nil, err
		}
		if cur.Status == effectPending {
			return nil, errEffectBusy
		}
		return g.effectResult(ctx, cur)
	}
	if status != effectSucceeded && status != effectDryRun {
		g.Log.Warn("effect settled", "workflow_id", e.WorkflowID, "effect_id", e.EffectID, "status", status, "reason", reason)
	}
	e.Status, e.ResultBlob, e.Reason = status, blob, sql.NullString{String: reason, Valid: reason != ""}
	return g.effectResult(ctx, e)
}

// effectResult is what a call gets for an effect in a settled state.
func (g *Gateway) effectResult(ctx context.Context, e db.Effect) (*agentosv1.Result, error) {
	switch e.Status {
	case effectSucceeded, effectDryRun:
		b, err := g.Ledger.GetBlob(ctx, e.ResultBlob.String)
		if err != nil {
			return nil, err
		}
		v, err := ledger.DecodeObject(b)
		if err != nil {
			return nil, err
		}
		return result(StatusOK, v)
	case effectFailed:
		return nil, temporal.NewNonRetryableApplicationError(fmt.Sprintf("tool %s failed: %s", e.Tool, e.Reason.String), "ToolFailed", nil)
	case effectUnknown:
		return result(StatusNeedsReconcile, map[string]any{"effect_id": e.EffectID, "reason": e.Reason.String})
	case effectAbandoned, effectDenied:
		return result(StatusDenied, map[string]any{"effect_id": e.EffectID, "reason": e.Reason.String})
	case effectAwaitingApproval:
		return result(StatusNeedsApproval, map[string]any{"effect_id": e.EffectID})
	}
	return nil, fmt.Errorf("effect %s is %s", e.EffectID, e.Status)
}

func (g *Gateway) runCancelled(ctx context.Context, workflowID string) (bool, error) {
	r, err := g.Ledger.Q.GetRun(ctx, workflowID)
	if err != nil {
		return false, err
	}
	return r.CancelledAt.Valid, nil
}

// RecoverEffects settles, as agentd starts, the effects the previous agentd process left:
// those of cancelled runs it never sent are abandoned; those it claimed are looked up in the
// background, under the key a retry of the call joins. Nothing is sent from here: an effect
// that never took effect waits for its run's next call.
func (g *Gateway) RecoverEffects(ctx context.Context) error {
	if n, err := g.Ledger.Q.AbandonUnsentEffects(ctx, ledger.Now()); err != nil {
		return err
	} else if n > 0 {
		g.Log.Info("abandoned the unsent effects of cancelled runs", "count", n)
	}
	claimed, err := g.Ledger.Q.ListClaimedEffects(ctx, valid(g.Incarnation))
	if err != nil {
		return err
	}
	for _, e := range claimed {
		g.Log.Warn("recovering an effect agentd claimed before it stopped", "workflow_id", e.WorkflowID,
			"effect_id", e.EffectID, "tool", e.Tool, "class", e.EffectClass)
		go func() {
			v, err, _ := g.effects.Do(e.EffectID, func() (any, error) {
				return g.track(func() (any, error) { return g.recoverClaimed(ctx, e.EffectID) })
			})
			switch {
			case errors.Is(err, errEffectReleased):
			case err != nil:
				g.Log.Warn("effect recovery did not finish; the next call or start tries again", "effect_id", e.EffectID, "error", err)
			default:
				g.Log.Info("effect recovered", "effect_id", e.EffectID, "status", v.(*agentosv1.Result).GetStatus())
			}
		}()
	}
	return nil
}

func (g *Gateway) recoverClaimed(ctx context.Context, effectID string) (*agentosv1.Result, error) {
	e, err := g.Ledger.Q.GetEffect(ctx, effectID)
	if err != nil {
		return nil, err
	}
	if e.Status != effectPending || !e.ClaimedBy.Valid || e.ClaimedBy.String == g.Incarnation {
		return g.effectResult(ctx, e) // a call got there first
	}
	run, err := g.Ledger.Q.GetRun(ctx, e.WorkflowID)
	if err != nil {
		return nil, err
	}
	t, ok := g.Tools[e.Tool].(tools.Writer)
	if !ok {
		return g.settle(ctx, e, effectUnknown, nil, "agentd no longer has the tool to look up or resend this effect")
	}
	payload, err := g.payload(ctx, e)
	if err != nil {
		return g.settle(ctx, e, effectUnknown, nil, "agentd cannot look up or resend this effect: "+err.Error())
	}
	return g.recoverEffect(ctx, run, t, e, payload, "agentd stopped while the request was in flight", false)
}

// Resolution is a person's finding about an effect whose outcome nobody knew.
type Resolution struct {
	EffectID   string
	Executed   bool
	ResultJSON string // when executed: what the effect produced
	Lookup     bool   // when executed: let agentd find the result itself
	Key        string // idempotency key: the same finding sent again applies once
	RunClosed  bool   // the run's workflow has ended: nobody will send the effect again
}

// Resolution errors, for the Control service to map.
var (
	ErrNoSuchEffect = errors.New("no such effect")
	ErrNotUnknown   = errors.New("the effect is not waiting for reconciliation")
	ErrBadResult    = errors.New("bad result")
	ErrTooEarly     = errors.New("too early")
	ErrKeyReused    = errors.New("the idempotency key was used for a different finding")
)

// Reconcile records a person's finding about an effect in UNKNOWN (section 6). Executed, with
// its result (given, or looked up by agentd), makes it SUCCEEDED: the next call returns that
// result. Not executed puts it back to PENDING, sent again under its original key by the run's
// next call; or ABANDONED when the run was cancelled or has ended. A finding sent again under
// the same key applies once, however the effect has moved on since. It returns the status the
// finding gave the effect, and the effect's run.
func (g *Gateway) Reconcile(ctx context.Context, r Resolution) (string, string, error) {
	e, err := g.Ledger.Q.GetEffect(ctx, r.EffectID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", ErrNoSuchEffect
	}
	if err != nil {
		return "", "", err
	}
	if prev, err := g.Ledger.Q.GetResolution(ctx, db.GetResolutionParams{EffectID: r.EffectID, ResolveKey: r.Key}); err == nil {
		if (prev.Executed != 0) != r.Executed {
			return "", "", ErrKeyReused
		}
		return prev.Status, e.WorkflowID, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return "", "", err
	}
	if e.Status != effectUnknown {
		return "", "", fmt.Errorf("%w: it is %s", ErrNotUnknown, e.Status)
	}
	t, _ := g.Tools[e.Tool].(tools.Writer)
	status := effectPending
	var blob sql.NullString
	if r.Executed {
		status = effectSucceeded
		value, err := g.reportedResult(ctx, e, t, r)
		if err != nil {
			return "", "", err
		}
		b, err := json.Marshal(value)
		if err != nil {
			return "", "", err
		}
		ref, err := g.Ledger.PutBlob(ctx, b)
		if err != nil {
			return "", "", err
		}
		blob = valid(ref)
	} else {
		cancelled, err := g.runCancelled(ctx, e.WorkflowID)
		if err != nil {
			return "", "", err
		}
		if cancelled || r.RunClosed {
			status = effectAbandoned
		} else if f, ok := t.(tools.Finder); ok && e.ClaimedAt.Valid {
			// The request may still land while GitHub works on it: the finding can wait.
			if sentAt, err := time.Parse(time.RFC3339Nano, e.ClaimedAt.String); err == nil {
				if until := sentAt.Add(f.Settle()); time.Now().Before(until) {
					return "", "", fmt.Errorf("%w: the request may still take effect until %s; look again then",
						ErrTooEarly, until.UTC().Format(time.RFC3339))
				}
			}
		}
	}
	now := ledger.Now()
	err = g.Ledger.Tx(ctx, func(q *db.Queries) error {
		n, err := q.ResolveEffect(ctx, db.ResolveEffectParams{
			Status: status, ResultBlob: blob, UpdatedAt: now, EffectID: e.EffectID, Version: e.Version,
		})
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("%w: it changed meanwhile; look again", ErrNotUnknown)
		}
		var executed int64
		if r.Executed {
			executed = 1
		}
		if err := q.InsertResolution(ctx, db.InsertResolutionParams{
			EffectID: e.EffectID, ResolveKey: r.Key, Executed: executed, Status: status, ResolvedAt: now,
		}); err != nil {
			return err
		}
		// The call waiting in WAITING_RECONCILE asks again: it gets the result, or sends the effect.
		return outbox.Ring(ctx, q, e.WorkflowID, outbox.WhyReconciliation)
	})
	if err != nil {
		return "", "", err
	}
	g.Log.Info("effect reconciled", "workflow_id", e.WorkflowID, "effect_id", e.EffectID, "executed", r.Executed, "status", status)
	g.ring()
	return status, e.WorkflowID, nil
}

// reportedResult is the result of an effect a person found executed: the one they give,
// checked against the tool's result schema, or the one agentd looks up.
func (g *Gateway) reportedResult(ctx context.Context, e db.Effect, t tools.Writer, r Resolution) (map[string]any, error) {
	if r.Lookup {
		f, ok := t.(tools.Finder)
		if !ok {
			return nil, fmt.Errorf("%w: %s cannot be looked up; give its result instead", ErrBadResult, e.Tool)
		}
		payload, err := g.payload(ctx, e)
		if err != nil {
			return nil, err
		}
		v, found, err := f.Find(ctx, payload)
		if err != nil {
			return nil, fmt.Errorf("look up effect %s: %w", e.EffectID, err)
		}
		if !found {
			return nil, fmt.Errorf("%w: agentd looked and found nothing; check by hand and give the result", ErrBadResult)
		}
		return v, nil
	}
	if r.ResultJSON == "" {
		return nil, fmt.Errorf("%w: an executed effect needs its result, or a lookup", ErrBadResult)
	}
	v, err := ledger.DecodeObject([]byte(r.ResultJSON))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadResult, err)
	}
	if t != nil {
		if err := t.Result().Validate(v); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrBadResult, err)
		}
	}
	return v, nil
}

func valid(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }
