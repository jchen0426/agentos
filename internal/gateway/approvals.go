package gateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
	"go.temporal.io/sdk/temporal"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
	"github.com/jchen0426/agentos/internal/outbox"
	"github.com/jchen0426/agentos/internal/patch"
	"github.com/jchen0426/agentos/internal/tools"
)

// Approvals (design doc, section 10). A write the policy wants approved is recorded as an effect
// in AWAITING_APPROVAL, with an approval that holds the hash of the effect's recorded payload:
// exactly what will be sent. Until a person decides, every call of the write answers
// needs_approval. People decide through the Control socket only: approve, reject (with a reason)
// or edit; the first valid decision wins, and is written in one transaction with the doorbell
// that wakes the run. agentd expires an approval nobody decided in time, which counts as a
// rejection. An approved effect goes on to PENDING, and is then sent like any other, in the same
// transaction that checks that it is still approved, within its time, and that the recorded
// payload is still the one approved, to the byte.

// Approval statuses (design doc, section 7).
const (
	approvalPending    = "PENDING"
	approvalApproved   = "APPROVED"
	approvalRejected   = "REJECTED"
	approvalExpired    = "EXPIRED"
	approvalEdited     = "EDITED"     // the reviewer's edit went back to the workflow
	approvalSuperseded = "SUPERSEDED" // the approval of the edited call replaced this one
)

// Statuses of a call whose approval was decided against it, or edited (proto Result.status).
const (
	StatusRejected = "rejected"
	StatusExpired  = "expired"
	StatusEdited   = "edited"
)

// approvalRule is the policy's requirement of a write: a person approves each effect of it, within
// ttl, before it is sent.
type approvalRule struct{ ttl time.Duration }

// errChanged rolls back a transaction that found the approval or its effect changed since they
// were read.
var errChanged = errors.New("changed meanwhile")

// supersedeEdited is part of the transaction that records a new effect awaiting approval: when
// the call carries a reviewer's edit (its arguments are the edited call's), the approval it was
// edited from is SUPERSEDED by the new one, and that approval's effect ABANDONED, before the run's
// writes of the tool are counted. It sets a.Supersedes.
func supersedeEdited(ctx context.Context, q *db.Queries, a *db.InsertApprovalParams, argsSHA256 string) error {
	edited, err := q.GetEditedApproval(ctx, db.GetEditedApprovalParams{
		WorkflowID: a.WorkflowID, Tool: a.Tool, EditedArgsSha256: valid(argsSHA256)})
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return err
	}
	if n, err := q.SupersedeApproval(ctx, db.SupersedeApprovalParams{
		SupersededBy: valid(a.ApprovalID), ApprovalID: edited.ApprovalID}); err != nil {
		return err
	} else if n == 0 {
		return errChanged
	}
	if _, err := q.AbandonAwaitingEffect(ctx, db.AbandonAwaitingEffectParams{
		Reason: valid(fmt.Sprintf("the reviewer edited it: approval %s of the edited call replaced approval %s",
			a.ApprovalID, edited.ApprovalID)),
		UpdatedAt: ledger.Now(), EffectID: edited.EffectID,
	}); err != nil {
		return err
	}
	a.Supersedes = valid(edited.ApprovalID)
	return nil
}

// recordingLock is the lock that makes a run's new effects of a tool that needs approval one at a
// time (a stripe: another run's may share it, for as long as one record takes).
func (g *Gateway) recordingLock(workflowID, tool string) *sync.Mutex {
	h := fnv.New32a()
	h.Write([]byte(workflowID + "\x00" + tool))
	return &g.recording[h.Sum32()%uint32(len(g.recording))]
}

// writesBefore counts the run's writes of the tool that count against its limit, but for the
// effect asked about and for the one whose edit this call carries, which recording it abandons.
// The transaction that records the effect counts again.
func (g *Gateway) writesBefore(ctx context.Context, run db.Run, t tools.Writer, effectID string, canon []byte) (int64, error) {
	n, err := g.Ledger.Q.CountRunEffects(ctx, db.CountRunEffectsParams{WorkflowID: run.WorkflowID, Tool: t.Name(), EffectID: effectID})
	if err != nil {
		return 0, err
	}
	switch _, err := g.Ledger.Q.GetEditedApproval(ctx, db.GetEditedApprovalParams{
		WorkflowID: run.WorkflowID, Tool: t.Name(), EditedArgsSha256: valid(sha256Hex(canon))}); {
	case err == nil:
		n--
	case !errors.Is(err, sql.ErrNoRows):
		return 0, err
	}
	return n, nil
}

// describe renders what the reviewer is shown besides the payload, as agentd observes it while it
// makes the approval: the write's own account of itself, and the task's context. It is stored as
// a blob.
func (g *Gateway) describe(ctx context.Context, run db.Run, t tools.Writer, req tools.Request, payload json.RawMessage) (string, error) {
	card := map[string]any{}
	if d, ok := t.(tools.Describer); ok {
		card["write"] = d.Describe(ctx, req, payload)
	}
	if task, ok := g.Tasks[run.Task]; ok && task.ApprovalContext != nil {
		card["task"] = task.ApprovalContext(ctx, runOutputs{g.Ledger, run.WorkflowID}, req.Args)
	}
	b, err := json.Marshal(card)
	if err != nil {
		return "", err
	}
	return g.Ledger.PutBlob(ctx, b)
}

// runOutputs is tasks.RunOutputs for one run, from the ledger.
type runOutputs struct {
	l          *ledger.Ledger
	workflowID string
}

func (r runOutputs) ModelOutputs(ctx context.Context, task string) ([]map[string]any, error) {
	return r.l.ModelOutputs(ctx, r.workflowID, task)
}

// newApproval is the approval of a new effect, open for ttl.
func newApproval(run db.Run, tool, effectID string, canon []byte, payloadSHA256, card string, ttl time.Duration) db.InsertApprovalParams {
	now := time.Now()
	return db.InsertApprovalParams{
		ApprovalID: uuid.NewString(), EffectID: effectID, WorkflowID: run.WorkflowID, Tool: tool,
		ArgsJson: string(canon), PayloadSha256: payloadSHA256, ContextBlob: card,
		CreatedAt: ledger.Stamp(now), ExpiresAt: ledger.Stamp(now.Add(ttl)),
	}
}

// expired reports whether an approval's time is up. One whose expiry agentd cannot read is.
func expired(a db.Approval) bool {
	at, err := time.Parse(time.RFC3339Nano, a.ExpiresAt)
	return err != nil || !time.Now().Before(at)
}

// awaitApproval answers a call of an effect in AWAITING_APPROVAL from its approval. It returns no
// result when the approval lets the effect go on: the effect is PENDING then, for the caller to
// send.
func (g *Gateway) awaitApproval(ctx context.Context, run db.Run, t tools.Writer, e db.Effect) (*agentosv1.Result, error) {
	a, err := g.Ledger.Q.GetApprovalByEffect(ctx, e.EffectID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, fmt.Errorf("effect %s awaits an approval agentd has no record of", e.EffectID)
	case err != nil:
		return nil, err
	}
	if a.Status == approvalPending {
		if !expired(a) {
			return result(StatusNeedsApproval, map[string]any{"approval_id": a.ApprovalID, "expires_at": a.ExpiresAt})
		}
		if _, err := g.expire(ctx, a); err != nil {
			return nil, err
		}
		// Expired now, or decided just before it was: read which.
		if a, err = g.Ledger.Q.GetApproval(ctx, a.ApprovalID); err != nil {
			return nil, err
		}
		if e, err = g.Ledger.Q.GetEffect(ctx, e.EffectID); err != nil {
			return nil, err
		}
	}
	if a.Status == approvalApproved && e.Status == effectAwaitingApproval {
		return g.approved(ctx, e, a)
	}
	return approvalResult(t, a, e)
}

// approved moves an approved effect to PENDING, in one transaction that checks the approval is
// still APPROVED and unused, its time is not up, the run was not cancelled, and the payload
// recorded for the effect still hashes to the one approved. It returns a result only when the
// effect may not go on: then it is ABANDONED (the approval expired, the payload changed, the run
// was cancelled).
func (g *Gateway) approved(ctx context.Context, e db.Effect, a db.Approval) (*agentosv1.Result, error) {
	var res *agentosv1.Result
	err := g.Ledger.Tx(ctx, func(q *db.Queries) error {
		res = nil
		cur, err := q.GetApproval(ctx, a.ApprovalID)
		if err != nil {
			return err
		}
		ce, err := q.GetEffect(ctx, e.EffectID)
		if err != nil {
			return err
		}
		if cur.Status != approvalApproved || cur.UsedAt.Valid || ce.Status != effectAwaitingApproval {
			return errChanged
		}
		now := ledger.Now()
		abandon := func(status string, value map[string]any, reason string) error {
			if _, err := q.AbandonAwaitingEffect(ctx, db.AbandonAwaitingEffectParams{
				Reason: valid(reason), UpdatedAt: now, EffectID: ce.EffectID}); err != nil {
				return err
			}
			value["approval_id"], value["reason"] = cur.ApprovalID, reason
			r, err := result(status, value)
			res = r
			return err
		}
		if expired(cur) {
			reason := fmt.Sprintf("approved by %s at %s, but the run did not act on it before the approval expired at %s",
				cur.DecidedBy.String, cur.DecidedAt.String, cur.ExpiresAt)
			if _, err := q.ExpireUnusedApproval(ctx, db.ExpireUnusedApprovalParams{
				DecidedAt: valid(now), Reason: valid(reason), ApprovalID: cur.ApprovalID}); err != nil {
				return err
			}
			return abandon(StatusExpired, map[string]any{"expires_at": cur.ExpiresAt}, reason)
		}
		// What goes out is the payload recorded for the effect, never one rendered again: it must
		// be the one the reviewer approved, to the byte.
		b, err := q.GetBlob(ctx, strings.TrimPrefix(ce.PayloadBlob.String, "sha256:"))
		if err != nil {
			return fmt.Errorf("effect %s: the recorded payload: %w", ce.EffectID, err)
		}
		if got := sha256Hex(b); got != cur.PayloadSha256 || ce.PayloadSha256 != cur.PayloadSha256 {
			reason := fmt.Sprintf("the payload recorded for it (sha256 %s) is not the payload approved (sha256 %s): agentd does not send it",
				got, cur.PayloadSha256)
			g.Log.Error("an approved payload changed; not sending it", "workflow_id", ce.WorkflowID, "effect_id", ce.EffectID,
				"approval_id", cur.ApprovalID, "recorded_sha256", got, "approved_sha256", cur.PayloadSha256)
			return abandon(StatusDenied, map[string]any{}, reason)
		}
		n, err := q.ApproveEffect(ctx, db.ApproveEffectParams{UpdatedAt: now, EffectID: ce.EffectID, Version: ce.Version})
		if err != nil {
			return err
		}
		if n == 0 { // the run was cancelled: the cancel is checked in the transaction that lets it go
			return abandon(StatusDenied, map[string]any{}, "the run was cancelled before the approved write was sent")
		}
		_, err = q.UseApproval(ctx, db.UseApprovalParams{UsedAt: valid(now), ApprovalID: cur.ApprovalID})
		return err
	})
	switch {
	case errors.Is(err, errChanged):
		return nil, errEffectBusy
	case err != nil:
		return nil, err
	}
	if res == nil {
		g.Log.Info("approved write goes on", "workflow_id", e.WorkflowID, "effect_id", e.EffectID, "approval_id", a.ApprovalID)
	}
	return res, nil
}

// holdUnapproved holds back a PENDING effect the policy wants approved and nobody approved: one
// recorded before the policy asked (a policy changed across a restart), or while its tool could
// only dry-run, never claimed or given back unsent. In one transaction it goes to
// AWAITING_APPROVAL with an approval of its own, and the call answers needs_approval. An effect
// that became PENDING through its approval goes on: it returns no result then.
func (g *Gateway) holdUnapproved(ctx context.Context, run db.Run, t tools.Writer, e db.Effect, rule *approvalRule,
	req tools.Request, canon []byte, payload json.RawMessage) (*agentosv1.Result, error) {
	switch a, err := g.Ledger.Q.GetApprovalByEffect(ctx, e.EffectID); {
	case err == nil && a.Status == approvalApproved && a.UsedAt.Valid:
		return nil, nil
	case err == nil:
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("effect %s is PENDING, but its approval %s is %s", e.EffectID, a.ApprovalID, a.Status), "BadApproval", nil)
	case !errors.Is(err, sql.ErrNoRows):
		return nil, err
	}
	card, err := g.describe(ctx, run, t, req, payload)
	if err != nil {
		return nil, err
	}
	a := newApproval(run, t.Name(), e.EffectID, canon, e.PayloadSha256, card, rule.ttl)
	err = g.Ledger.Tx(ctx, func(q *db.Queries) error {
		n, err := q.HoldForApproval(ctx, db.HoldForApprovalParams{UpdatedAt: ledger.Now(), EffectID: e.EffectID, Version: e.Version})
		switch {
		case err != nil:
			return err
		case n == 0:
			return errChanged
		}
		return q.InsertApproval(ctx, a)
	})
	switch {
	case errors.Is(err, errChanged):
		return nil, errEffectBusy
	case err != nil:
		return nil, err
	}
	g.Log.Warn("approval requested for a write recorded without one", "workflow_id", run.WorkflowID,
		"approval_id", a.ApprovalID, "effect_id", e.EffectID, "tool", t.Name(), "expires_at", a.ExpiresAt)
	g.wakeExpiry()
	return result(StatusNeedsApproval, map[string]any{"approval_id": a.ApprovalID, "expires_at": a.ExpiresAt})
}

// decidedResult answers a call of an ABANDONED effect from its approval, if it has one: the
// reviewer's rejection, the expiry, or the edit that replaced it. No result when it has none.
func (g *Gateway) decidedResult(ctx context.Context, t tools.Writer, e db.Effect) (*agentosv1.Result, error) {
	a, err := g.Ledger.Q.GetApprovalByEffect(ctx, e.EffectID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, err
	}
	return approvalResult(t, a, e)
}

// approvalResult is what a call gets for an approval that is decided and does not let it go on.
func approvalResult(t tools.Writer, a db.Approval, e db.Effect) (*agentosv1.Result, error) {
	switch a.Status {
	case approvalRejected:
		return result(StatusRejected, map[string]any{"approval_id": a.ApprovalID, "reason": a.Reason.String,
			"decided_by": a.DecidedBy.String, "decided_at": a.DecidedAt.String})
	case approvalExpired:
		return result(StatusExpired, map[string]any{"approval_id": a.ApprovalID, "reason": a.Reason.String,
			"expires_at": a.ExpiresAt})
	case approvalEdited, approvalSuperseded:
		// The edited call comes next, with the reviewer's value: the workflow checks it again first.
		if ed, ok := t.(tools.Editable); ok {
			return result(StatusEdited, map[string]any{"approval_id": a.ApprovalID, ed.EditArg(): a.EditedPatchBlob.String})
		}
		return result(StatusDenied, map[string]any{"approval_id": a.ApprovalID, "reason": t.Name() + " takes no edits"})
	}
	return result(StatusDenied, map[string]any{"approval_id": a.ApprovalID, "effect_id": e.EffectID, "reason": e.Reason.String})
}

// expire ends a PENDING approval whose time is up, as a rejection (design doc, section 10): its
// effect is never sent, and its run's doorbell rings. It reports whether it expired it: a
// decision may have come first.
func (g *Gateway) expire(ctx context.Context, a db.Approval) (bool, error) {
	reason := "nobody decided before the approval expired at " + a.ExpiresAt
	var done bool
	err := g.Ledger.Tx(ctx, func(q *db.Queries) error {
		now := ledger.Now()
		n, err := q.ExpirePendingApproval(ctx, db.ExpirePendingApprovalParams{
			DecidedAt: valid(now), Reason: valid(reason), ApprovalID: a.ApprovalID})
		if err != nil || n == 0 {
			done = false
			return err
		}
		if _, err := q.AbandonAwaitingEffect(ctx, db.AbandonAwaitingEffectParams{
			Reason: valid("the approval expired: " + reason), UpdatedAt: now, EffectID: a.EffectID}); err != nil {
			return err
		}
		done = true
		return outbox.Ring(ctx, q, a.WorkflowID, outbox.WhyApproval)
	})
	if err != nil || !done {
		return false, err
	}
	g.Log.Warn("approval expired", "workflow_id", a.WorkflowID, "approval_id", a.ApprovalID, "effect_id", a.EffectID,
		"expires_at", a.ExpiresAt)
	g.ring()
	return true, nil
}

// ExpireApprovals expires each approval nobody decided in time, when its time comes, until ctx
// ends; at start, those whose time came while agentd was not running. A new approval wakes it.
func (g *Gateway) ExpireApprovals(ctx context.Context) {
	for {
		pause := 50 * time.Millisecond
		due, err := g.Ledger.Q.ListExpiredApprovals(ctx, ledger.Stamp(time.Now()))
		if err != nil && ctx.Err() == nil {
			g.Log.Error("approvals: cannot list the expired ones", "error", err)
			pause = time.Second
		}
		for _, a := range due {
			if _, err := g.expire(ctx, a); err != nil && ctx.Err() == nil {
				g.Log.Error("approvals: cannot expire one", "approval_id", a.ApprovalID, "error", err)
				pause = time.Second
			}
		}
		wait := time.Minute
		if next, err := g.Ledger.Q.NextApprovalExpiry(ctx); err == nil && next != "" {
			if at, err := time.Parse(time.RFC3339Nano, next); err == nil {
				wait = min(wait, time.Until(at))
			}
		}
		t := time.NewTimer(max(wait, pause))
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-g.expiryWake():
			t.Stop()
		case <-t.C:
		}
	}
}

// expiryWake is the channel that wakes ExpireApprovals: an approval was made.
func (g *Gateway) expiryWake() chan struct{} {
	g.expiryOnce.Do(func() { g.expiryCh = make(chan struct{}, 1) })
	return g.expiryCh
}

func (g *Gateway) wakeExpiry() {
	select {
	case g.expiryWake() <- struct{}{}:
	default:
	}
}

// ring has the outbox deliver the doorbells just rung.
func (g *Gateway) ring() {
	if g.Doorbells != nil {
		g.Doorbells.Poke()
	}
}

// Decision kinds.
const (
	DecisionApprove = "approve"
	DecisionReject  = "reject"
	DecisionEdit    = "edit"
)

// Decision is a person's decision on an approval, sent through the Control socket.
type Decision struct {
	ApprovalID    string
	PayloadSHA256 string // of the payload the reviewer saw: it must be the approval's
	Key           string // the decision sent again under the same key applies once
	By            string // who decides, as agentctl names them
	Kind          string // DecisionApprove, DecisionReject or DecisionEdit
	Reason        string // a rejection's, required
	Patch         []byte // an edit's: the reviewer's value for the tool's EditArg
	RunClosed     bool   // the run's workflow has ended: nothing would act on the decision
}

// Decision errors, for the Control service to map.
var (
	ErrNoSuchApproval = errors.New("no such approval")
	ErrBadDecision    = errors.New("bad decision")
	ErrStaleView      = errors.New("the payload named is not the one waiting for approval")
	ErrRunOver        = errors.New("the run is over")
)

// DecidedError answers a decision that came after another: who decided what, and when.
type DecidedError struct{ Approval db.Approval }

func (e *DecidedError) Error() string {
	a := e.Approval
	return fmt.Sprintf("approval %s was already decided: %s by %s at %s", a.ApprovalID, a.Status, a.DecidedBy.String, a.DecidedAt.String)
}

// Limits on what a decision carries.
const (
	maxReviewer = 100  // runes of the reviewer's name
	maxReason   = 2000 // bytes of a rejection's reason
)

// Decide records a person's decision on an approval (design doc, section 10), unless another came
// first: the first valid decision wins, in one SQLite transaction with the doorbell of its run.
// Approved, the run's next call of the write sends it; rejected, its effect is abandoned and the
// reason goes into the run's report; edited, the workflow gets the reviewer's value, checks it
// and calls again, and that call needs an approval of its own. A decision sent again under the
// same key returns what it recorded. It returns the approval as decided.
func (g *Gateway) Decide(ctx context.Context, d Decision) (db.Approval, error) {
	if err := checkDecision(d); err != nil {
		return db.Approval{}, err
	}
	a, err := g.Ledger.Q.GetApproval(ctx, d.ApprovalID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return db.Approval{}, ErrNoSuchApproval
	case err != nil:
		return db.Approval{}, err
	}
	var editedRef, editedArgs string
	if d.Kind == DecisionEdit {
		if editedRef, editedArgs, err = g.edit(ctx, a, d.Patch); err != nil {
			return db.Approval{}, err
		}
	}
	status, reason := map[string]string{DecisionApprove: approvalApproved, DecisionReject: approvalRejected,
		DecisionEdit: approvalEdited}[d.Kind], strings.TrimSpace(d.Reason)
	var late, replay bool
	err = g.Ledger.Tx(ctx, func(q *db.Queries) error {
		late, replay = false, false
		cur, err := q.GetApproval(ctx, d.ApprovalID)
		if err != nil {
			return err
		}
		if cur.DecisionKey.Valid && cur.DecisionKey.String == d.Key {
			// The same decision sent again: what it recorded stands, however the approval moved on.
			if !sameDecision(cur, d.Kind, reason, editedRef) {
				return fmt.Errorf("%w: the idempotency key was used for another decision", ErrBadDecision)
			}
			if cur.Status == approvalExpired {
				return &DecidedError{cur} // approved, and void since: the run did not act on it in time
			}
			replay = true
			return nil
		}
		if cur.Status != approvalPending {
			return &DecidedError{cur}
		}
		run, err := q.GetRun(ctx, cur.WorkflowID)
		if err != nil {
			return err
		}
		switch {
		case run.CancelledAt.Valid:
			return fmt.Errorf("%w: the run was cancelled; nothing it asks for goes out", ErrRunOver)
		case d.RunClosed:
			return fmt.Errorf("%w: the run has ended; nothing would act on the decision", ErrRunOver)
		case expired(cur):
			late = true // expired first: agentd's decision, which is recorded below
			return nil
		case d.PayloadSHA256 != cur.PayloadSha256:
			return fmt.Errorf("%w: approval %s holds payload sha256 %s, not %s; look at it again (agentctl approvals show)",
				ErrStaleView, cur.ApprovalID, cur.PayloadSha256, d.PayloadSHA256)
		}
		e, err := q.GetEffect(ctx, cur.EffectID)
		if err != nil {
			return err
		}
		b, err := q.GetBlob(ctx, strings.TrimPrefix(e.PayloadBlob.String, "sha256:"))
		if err != nil {
			return err
		}
		if sha256Hex(b) != cur.PayloadSha256 {
			return fmt.Errorf("%w: the payload recorded for approval %s no longer hashes to %s", ErrStaleView, cur.ApprovalID, cur.PayloadSha256)
		}
		if d.Kind == DecisionEdit {
			// The workflow's checks and the edited call read it as the run's own.
			if err := q.PutBlob(ctx, db.PutBlobParams{Sha256: strings.TrimPrefix(editedRef, "sha256:"), Bytes: d.Patch}); err != nil {
				return err
			}
			if err := q.AddBlobOwner(ctx, db.AddBlobOwnerParams{WorkflowID: cur.WorkflowID,
				Sha256: strings.TrimPrefix(editedRef, "sha256:")}); err != nil {
				return err
			}
		}
		n, err := q.DecideApproval(ctx, db.DecideApprovalParams{
			Status: status, DecidedBy: valid(d.By), DecidedAt: valid(ledger.Now()), DecisionKey: valid(d.Key),
			Reason:           sql.NullString{String: reason, Valid: d.Kind == DecisionReject},
			EditedPatchBlob:  sql.NullString{String: editedRef, Valid: d.Kind == DecisionEdit},
			EditedArgsSha256: sql.NullString{String: editedArgs, Valid: d.Kind == DecisionEdit},
			ApprovalID:       cur.ApprovalID,
		})
		if err != nil {
			return err
		}
		if n == 0 {
			return errChanged
		}
		if d.Kind == DecisionReject {
			if _, err := q.AbandonAwaitingEffect(ctx, db.AbandonAwaitingEffectParams{
				Reason:    valid(fmt.Sprintf("rejected by %s: %s", d.By, reason)),
				UpdatedAt: ledger.Now(), EffectID: cur.EffectID,
			}); err != nil {
				return err
			}
		}
		return outbox.Ring(ctx, q, cur.WorkflowID, outbox.WhyApproval)
	})
	if late {
		if _, err := g.expire(ctx, a); err != nil {
			return db.Approval{}, err
		}
	}
	if err != nil {
		return db.Approval{}, err
	}
	cur, err := g.Ledger.Q.GetApproval(ctx, d.ApprovalID)
	if err != nil {
		return db.Approval{}, err
	}
	switch {
	case late, cur.DecisionKey.String != d.Key:
		return db.Approval{}, &DecidedError{cur}
	case !replay:
		g.Log.Info("approval decided", "workflow_id", cur.WorkflowID, "approval_id", cur.ApprovalID, "status", cur.Status,
			"decided_by", cur.DecidedBy.String)
		g.ring()
	}
	return cur, nil
}

// checkDecision checks the shape of a decision, before anything is read.
func checkDecision(d Decision) error {
	bad := func(format string, args ...any) error {
		return fmt.Errorf("%w: "+format, append([]any{ErrBadDecision}, args...)...)
	}
	by := strings.TrimSpace(d.By)
	switch {
	case d.Key == "":
		return bad("an idempotency key is required")
	case by == "" || by != d.By || utf8.RuneCountInString(by) > maxReviewer || !printable(by, false):
		return bad("the reviewer's name must be printable text of at most %d characters, without spaces around it", maxReviewer)
	case strings.EqualFold(by, "agentd"):
		return bad("agentd is the name of agentd's own decisions (an expiry); name yourself")
	case d.PayloadSHA256 == "":
		return bad("the sha256 of the payload you saw is required")
	}
	switch d.Kind {
	case DecisionApprove:
		if d.Reason != "" || d.Patch != nil {
			return bad("an approval carries neither a reason nor a patch")
		}
	case DecisionReject:
		if r := strings.TrimSpace(d.Reason); r == "" || len(r) > maxReason || !printable(r, true) {
			return bad("a rejection needs its reason: printable text of at most %d bytes", maxReason)
		}
		if d.Patch != nil {
			return bad("a rejection carries no patch")
		}
	case DecisionEdit:
		if d.Reason != "" {
			return bad("an edit carries no reason")
		}
	default:
		return bad("unknown decision %q", d.Kind)
	}
	return nil
}

// printable reports whether s is all printable runes; with lines, newlines and tabs too.
func printable(s string, lines bool) bool {
	for _, r := range s {
		if !unicode.IsPrint(r) && !(lines && (r == '\n' || r == '\t')) {
			return false
		}
	}
	return utf8.ValidString(s)
}

// edit stores a reviewer's patch as a blob of the run and returns its reference, with the hash of
// the arguments of the call that will carry it: the edited call, whose approval supersedes this one.
func (g *Gateway) edit(ctx context.Context, a db.Approval, value []byte) (string, string, error) {
	t, ok := g.Tools[a.Tool].(tools.Editable)
	if !ok {
		return "", "", fmt.Errorf("%w: %s takes no edits", ErrBadDecision, a.Tool)
	}
	switch {
	case len(value) == 0:
		return "", "", fmt.Errorf("%w: the edit is empty", ErrBadDecision)
	case len(value) > patch.MaxBytes:
		return "", "", fmt.Errorf("%w: the edit is %d bytes, over the limit of %d", ErrBadDecision, len(value), patch.MaxBytes)
	}
	if _, err := patch.Parse(value); err != nil {
		return "", "", fmt.Errorf("%w: the edit is not a patch agentd takes: %v", ErrBadDecision, err)
	}
	args, err := ledger.DecodeObject([]byte(a.ArgsJson))
	if err != nil {
		return "", "", fmt.Errorf("approval %s: the call's arguments: %w", a.ApprovalID, err)
	}
	ref := "sha256:" + sha256Hex(value)
	args[t.EditArg()] = ref
	canon, err := encodeCanonical(args)
	if err != nil {
		return "", "", err
	}
	// The edited call must be a new one. The approval's own call (an edit that changes nothing),
	// or another the run made already (an edit back to an earlier patch), answers edited again,
	// and the workflow goes round between the two.
	switch _, err := g.Ledger.Q.GetEffect(ctx, sha256Hex([]byte(a.WorkflowID), []byte(a.Tool), canon)); {
	case err == nil:
		return "", "", fmt.Errorf("%w: the run has asked for %s with that %s already; approve or reject instead",
			ErrBadDecision, a.Tool, t.EditArg())
	case !errors.Is(err, sql.ErrNoRows):
		return "", "", err
	}
	return ref, sha256Hex(canon), nil
}

// sameDecision reports whether an approval holds the decision described. A person's decision is
// the one its key was recorded with, whatever became of the approval since: approved, it may have
// expired unused (only an approved one expires with a decision key); edited, superseded.
func sameDecision(a db.Approval, kind, reason, editedRef string) bool {
	switch a.Status {
	case approvalApproved, approvalExpired:
		return kind == DecisionApprove
	case approvalRejected:
		return kind == DecisionReject && a.Reason.String == reason
	case approvalEdited, approvalSuperseded:
		return kind == DecisionEdit && a.EditedPatchBlob.String == editedRef
	}
	return false
}
