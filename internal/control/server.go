// Package control serves the Control service on the control socket, the only way to start,
// inspect or cancel runs. The socket lives in a 0700 directory the sandbox cannot see.
package control

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/budget"
	"github.com/jchen0426/agentos/internal/faults"
	"github.com/jchen0426/agentos/internal/gateway"
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
	"github.com/jchen0426/agentos/internal/safetext"
	"github.com/jchen0426/agentos/internal/tasks"
)

// AgentState is the custom search attribute the workflows set while they wait.
var AgentState = temporal.NewSearchAttributeKeyKeyword("AgentState")

// runningStates are the AgentState values a running workflow may report. The Worker sets
// AgentState and is not trusted: any other value is ignored rather than shown.
var runningStates = map[string]bool{
	"RUNNING": true, "WAITING_APPROVAL": true, "WAITING_RECONCILE": true, "PAUSED_BUDGET": true,
}

type Server struct {
	agentosv1.UnimplementedControlServer

	Ledger      *ledger.Ledger
	Budget      *budget.Budget
	Effects     Reconciler
	Approvals   Approver
	Doorbells   interface{ Poke() } // the outbox: rings the doorbells the server's changes wrote
	Temporal    client.Client
	Tasks       tasks.Registry
	TaskQueue   string         // where the Python Worker polls
	CancelGrace time.Duration  // how long a cancelled run may keep running before it is terminated
	Faults      *faults.Script // nil unless agentd runs with --fault-script
	Log         *slog.Logger
}

// Reconciler records a person's finding about an effect whose outcome nobody knew: the
// gateway, which knows the tools.
type Reconciler interface {
	Reconcile(ctx context.Context, r gateway.Resolution) (status, workflowID string, err error)
}

// Approver records a person's decision on an approval: the gateway, which knows the tools.
type Approver interface {
	Decide(ctx context.Context, d gateway.Decision) (db.Approval, error)
}

func (s *Server) StartRun(ctx context.Context, req *agentosv1.StartRunRequest) (*agentosv1.Run, error) {
	t, ok := s.Tasks[req.GetTask()]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "unknown task %q", req.GetTask())
	}
	input, params, err := t.Resolve(ctx, req.GetParams())
	var unavailable *tasks.Unavailable
	switch {
	case errors.As(err, &unavailable):
		return nil, status.Error(codes.Unavailable, err.Error()) // the start may work later
	case err != nil:
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	budget := req.GetBudgetLimitMicros()
	switch {
	case budget < 0:
		return nil, status.Error(codes.InvalidArgument, "budget must not be negative")
	case budget == 0:
		budget = t.DefaultBudget
	}
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}

	// Record the run before starting it, so the gateway recognizes its first call; the
	// run ID follows once Temporal has assigned one.
	workflowID, createdAt := uuid.NewString(), ledger.Now()
	var dryRun int64
	if req.GetDryRun() {
		dryRun = 1
	}
	if err := s.Ledger.Q.InsertRun(ctx, db.InsertRunParams{
		WorkflowID: workflowID, Task: t.Name, ParamsJson: string(paramsJSON),
		BudgetLimitMicros: budget, DryRun: dryRun, CreatedAt: createdAt,
	}); err != nil {
		return nil, err
	}
	we, err := s.Temporal.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       workflowID,
		TaskQueue:                s.TaskQueue,
		WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
		TypedSearchAttributes:    temporal.NewSearchAttributes(AgentState.ValueSet("RUNNING")),
	}, t.WorkflowType, input)
	if err != nil {
		if derr := s.Ledger.Q.DeleteRun(context.WithoutCancel(ctx), workflowID); derr != nil {
			s.Log.Error("could not remove the run that failed to start", "workflow_id", workflowID, "error", derr)
		}
		return nil, status.Errorf(codes.Unavailable, "start workflow: %v", err)
	}
	// The run exists in Temporal now: finish the bookkeeping even if agentctl goes away.
	ctx = context.WithoutCancel(ctx)
	s.Faults.DelayRunID()
	if _, err := s.Ledger.Q.SetRunID(ctx, db.SetRunIDParams{
		RunID: sql.NullString{String: we.GetRunID(), Valid: true}, WorkflowID: workflowID,
	}); err != nil {
		// Without its run ID the gateway refuses every call of this run: stop it now
		// rather than let it fail step by step.
		if terr := s.Temporal.TerminateWorkflow(ctx, workflowID, we.GetRunID(), "agentd could not record the run"); terr != nil {
			s.Log.Error("could not terminate a run agentd failed to record", "workflow_id", workflowID, "error", terr)
		}
		return nil, status.Errorf(codes.Internal, "record run: %v", err)
	}
	s.Log.Info("run started", "workflow_id", workflowID, "run_id", we.GetRunID(), "task", t.Name, "dry_run", req.GetDryRun())
	// Answer from what agentd knows: an error here would invite a retry, and a retry
	// starts a second run.
	return &agentosv1.Run{
		RunId: workflowID, TemporalRunId: we.GetRunID(), Task: t.Name, State: "RUNNING",
		Params: params, BudgetLimitMicros: budget, CreatedAt: createdAt, DryRun: req.GetDryRun(),
	}, nil
}

// RecoverStarts settles runs whose start was interrupted: agentd stopped after starting the
// workflow but before recording its run ID, or the start never reached Temporal.
func (s *Server) RecoverStarts(ctx context.Context) error {
	rows, err := s.Ledger.Q.ListStartingRuns(ctx)
	if err != nil {
		return err
	}
	for _, r := range rows {
		d, err := s.Temporal.DescribeWorkflowExecution(ctx, r.WorkflowID, "")
		var notFound *serviceerror.NotFound
		switch {
		case errors.As(err, &notFound):
			s.Log.Warn("dropping a run that never started", "workflow_id", r.WorkflowID)
			if err := s.Ledger.Q.DeleteRun(ctx, r.WorkflowID); err != nil {
				return err
			}
		case err != nil:
			return err
		case d.GetWorkflowExecutionInfo().GetParentExecution() != nil:
			// agentd starts runs without a parent; a child under this ID is not ours.
			s.Log.Warn("run ID left unset: the execution under this workflow ID has a parent", "workflow_id", r.WorkflowID)
		default:
			runID := d.GetWorkflowExecutionInfo().GetExecution().GetRunId()
			s.Log.Warn("recording the run ID of an interrupted start", "workflow_id", r.WorkflowID, "run_id", runID)
			if _, err := s.Ledger.Q.SetRunID(ctx, db.SetRunIDParams{
				RunID: sql.NullString{String: runID, Valid: true}, WorkflowID: r.WorkflowID,
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Server) GetRun(ctx context.Context, req *agentosv1.GetRunRequest) (*agentosv1.Run, error) {
	return s.getRun(ctx, req.GetRunId())
}

func (s *Server) ListRuns(ctx context.Context, req *agentosv1.ListRunsRequest) (*agentosv1.ListRunsResponse, error) {
	limit := int64(req.GetLimit())
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.Ledger.Q.ListRuns(ctx, limit)
	if err != nil {
		return nil, err
	}
	resp := &agentosv1.ListRunsResponse{}
	for _, r := range rows {
		run, err := s.describe(ctx, r)
		if err != nil {
			return nil, err
		}
		resp.Runs = append(resp.Runs, run)
	}
	return resp, nil
}

// CancelRun stops a run. It records the cancel first, so the gateway refuses the run's calls
// from then on, then asks Temporal to cancel. Temporal's cancel is a request the workflow may
// ignore, so a run still going after CancelGrace is terminated. Requests already sent are not
// undone; they finish or go to reconciliation.
func (s *Server) CancelRun(ctx context.Context, req *agentosv1.CancelRunRequest) (*agentosv1.Run, error) {
	r, err := s.lookup(ctx, req.GetRunId())
	if err != nil {
		return nil, err
	}
	if !r.RunID.Valid {
		return nil, status.Error(codes.FailedPrecondition, "the run is still starting")
	}
	current, err := s.describe(ctx, r)
	if err != nil {
		return nil, err
	}
	if closed(current.GetState()) {
		return nil, status.Errorf(codes.FailedPrecondition, "the run has already ended: %s", current.GetState())
	}
	ctx = context.WithoutCancel(ctx) // once recorded, the cancel must reach Temporal
	at := time.Now().UTC()
	if _, err := s.Ledger.Q.MarkRunCancelled(ctx, db.MarkRunCancelledParams{
		CancelledAt: sql.NullString{String: at.Format(time.RFC3339Nano), Valid: true}, WorkflowID: r.WorkflowID,
	}); err != nil {
		return nil, err
	}
	if err := s.Temporal.CancelWorkflow(ctx, r.WorkflowID, r.RunID.String); err != nil {
		// The gateway already refuses the run; the deadline below still ends it.
		s.Log.Warn("cancel request to Temporal failed", "workflow_id", r.WorkflowID, "error", err)
	}
	s.Log.Info("run cancelled", "workflow_id", r.WorkflowID, "terminate_after", s.CancelGrace)
	s.enforceCancel(r.WorkflowID, r.RunID.String, at.Add(s.CancelGrace))
	return s.getRun(ctx, r.WorkflowID)
}

// RecoverCancels re-arms the deadline of every cancelled run that is still going, after an
// agentd restart.
func (s *Server) RecoverCancels(ctx context.Context) error {
	rows, err := s.Ledger.Q.ListCancelledRuns(ctx)
	if err != nil {
		return err
	}
	for _, r := range rows {
		run, err := s.describe(ctx, r)
		if err != nil {
			return err
		}
		if closed(run.GetState()) {
			continue
		}
		at, err := time.Parse(time.RFC3339Nano, r.CancelledAt.String)
		if err != nil {
			return fmt.Errorf("run %s: cancelled_at: %w", r.WorkflowID, err)
		}
		s.enforceCancel(r.WorkflowID, r.RunID.String, at.Add(s.CancelGrace))
	}
	return nil
}

// enforceCancel terminates the execution at deadline unless it has ended by then.
// Termination is done by the Temporal server and cannot be refused by the workflow.
func (s *Server) enforceCancel(workflowID, runID string, deadline time.Time) {
	time.AfterFunc(time.Until(deadline), func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		d, err := s.Temporal.DescribeWorkflowExecution(ctx, workflowID, runID)
		if err != nil {
			s.Log.Error("cancel deadline: describe failed", "workflow_id", workflowID, "error", err)
			return
		}
		if d.GetWorkflowExecutionInfo().GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			return
		}
		reason := fmt.Sprintf("cancel not honoured within %s", s.CancelGrace)
		if err := s.Temporal.TerminateWorkflow(ctx, workflowID, runID, reason); err != nil {
			s.Log.Error("cancel deadline: terminate failed", "workflow_id", workflowID, "error", err)
			return
		}
		s.Log.Warn("run terminated: it did not honour the cancel", "workflow_id", workflowID, "grace", s.CancelGrace)
	})
}

func closed(state string) bool {
	switch state {
	case "SUCCEEDED", "FAILED", "CANCELLED", "UNKNOWN":
		return true
	}
	return false
}

// ListPending lists what waits for a person: approvals nobody has decided yet (but those of
// cancelled runs, whose writes never go out), and reconciliations, effects whose outcome nobody
// knows. What the external system was to receive and the reason come from tools and the outside
// world: neither leaves with raw control characters.
func (s *Server) ListPending(ctx context.Context, _ *agentosv1.ListPendingRequest) (*agentosv1.ListPendingResponse, error) {
	resp := &agentosv1.ListPendingResponse{}
	approvals, err := s.Ledger.Q.ListPendingApprovals(ctx)
	if err != nil {
		return nil, err
	}
	for _, a := range approvals {
		if at, err := time.Parse(time.RFC3339Nano, a.ExpiresAt); err == nil && !time.Now().Before(at) {
			continue // expired: agentd records it as such any moment
		}
		// Without the payload, which GetApproval shows and checks: a pull request's runs to 20 files
		// of 1 MiB, and one large approval would make the whole list too large to receive.
		resp.Items = append(resp.Items, &agentosv1.PendingItem{Kind: "approval", Id: a.ApprovalID, RunId: a.WorkflowID,
			ExpiresAt: a.ExpiresAt, Tool: a.Tool, Since: a.CreatedAt, PayloadSha256: a.PayloadSha256})
	}
	rows, err := s.Ledger.Q.ListEffectsByStatus(ctx, "UNKNOWN")
	if err != nil {
		return nil, err
	}
	for _, e := range rows {
		item := &agentosv1.PendingItem{Kind: "reconciliation", Id: e.EffectID, RunId: e.WorkflowID, Tool: e.Tool,
			Reason: safetext.Line(e.Reason.String), Since: e.UpdatedAt}
		if e.PayloadBlob.Valid {
			b, err := s.Ledger.GetBlob(ctx, e.PayloadBlob.String)
			if err != nil {
				return nil, err
			}
			item.PayloadJson = string(safetext.JSON(b))
		}
		resp.Items = append(resp.Items, item)
	}
	return resp, nil
}

// errPayloadChanged: the payload recorded for an approval no longer hashes to the one it holds.
var errPayloadChanged = errors.New("changed since it was recorded")

// approvalPayload is the payload an approval holds the hash of: its effect's, as recorded.
func (s *Server) approvalPayload(ctx context.Context, a db.Approval) ([]byte, error) {
	e, err := s.Ledger.Q.GetEffect(ctx, a.EffectID)
	if err != nil {
		return nil, err
	}
	b, err := s.Ledger.GetBlob(ctx, e.PayloadBlob.String)
	if err != nil {
		return nil, err
	}
	if sum := sha256.Sum256(b); hex.EncodeToString(sum[:]) != a.PayloadSha256 {
		return nil, fmt.Errorf("the payload recorded for approval %s does not hash to %s: it %w; do not approve it",
			a.ApprovalID, a.PayloadSha256, errPayloadChanged)
	}
	return b, nil
}

// GetApproval shows one approval (design doc, section 10): exactly what will be sent and its hash,
// what agentd observed when it made the approval, how it was decided, and what the run has spent.
func (s *Server) GetApproval(ctx context.Context, req *agentosv1.GetApprovalRequest) (*agentosv1.Approval, error) {
	a, err := s.Ledger.Q.GetApproval(ctx, req.GetApprovalId())
	if errors.Is(err, sql.ErrNoRows) {
		return nil, status.Errorf(codes.NotFound, "no approval %q", req.GetApprovalId())
	}
	if err != nil {
		return nil, err
	}
	payload, err := s.approvalPayload(ctx, a)
	switch {
	case errors.Is(err, errPayloadChanged):
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	case err != nil:
		return nil, err
	}
	card, err := s.Ledger.GetBlob(ctx, a.ContextBlob)
	if err != nil {
		return nil, err
	}
	out := &agentosv1.Approval{
		ApprovalId: a.ApprovalID, RunId: a.WorkflowID, Tool: a.Tool, Status: a.Status, CreatedAt: a.CreatedAt,
		ExpiresAt: a.ExpiresAt, PayloadSha256: a.PayloadSha256, PayloadJson: string(safetext.JSON(payload)),
		ContextJson: string(safetext.JSON(card)), DecidedBy: safetext.Line(a.DecidedBy.String),
		DecidedAt: a.DecidedAt.String, Reason: safetext.Text(a.Reason.String), Supersedes: a.Supersedes.String,
		SupersededBy: a.SupersededBy.String,
	}
	if a.EditedPatchBlob.Valid {
		b, err := s.Ledger.GetBlob(ctx, a.EditedPatchBlob.String)
		if err != nil {
			return nil, err
		}
		out.EditedPatch = safetext.Text(string(b))
	}
	t, err := s.Budget.Totals(ctx, a.WorkflowID)
	if err != nil {
		return nil, err
	}
	out.BudgetSpentMicros, out.BudgetReservedMicros, out.BudgetLimitMicros = t.Spent, t.Held, t.Limit
	return out, nil
}

// ResolveApproval records a person's decision on an approval (design doc, section 10): approve,
// approve with edits, or reject with a reason. The first valid decision wins; one that comes
// after another is told who decided what, and when. The decision and the run's doorbell are
// written together, and the outbox rings it.
func (s *Server) ResolveApproval(ctx context.Context, req *agentosv1.ResolveApprovalRequest) (*agentosv1.ResolveApprovalResponse, error) {
	d := gateway.Decision{ApprovalID: req.GetApprovalId(), PayloadSHA256: req.GetPayloadSha256(),
		Key: req.GetIdempotencyKey(), By: req.GetDecidedBy()}
	switch x := req.GetDecision().(type) {
	case *agentosv1.ResolveApprovalRequest_Approve:
		d.Kind = gateway.DecisionApprove
	case *agentosv1.ResolveApprovalRequest_Reject:
		d.Kind, d.Reason = gateway.DecisionReject, x.Reject.GetReason()
	case *agentosv1.ResolveApprovalRequest_ApproveWithEdits:
		d.Kind, d.Patch = gateway.DecisionEdit, []byte(x.ApproveWithEdits.GetEditedPatch())
	default:
		return nil, status.Error(codes.InvalidArgument, "a decision is required: approve, approve_with_edits or reject")
	}
	a, err := s.Ledger.Q.GetApproval(ctx, d.ApprovalID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, status.Errorf(codes.NotFound, "no approval %q", d.ApprovalID)
	}
	if err != nil {
		return nil, err
	}
	r, err := s.lookup(ctx, a.WorkflowID)
	if err != nil {
		return nil, err
	}
	// Once the run has ended, nothing acts on a decision: it is refused rather than recorded.
	current, err := s.describe(ctx, r)
	if err != nil {
		return nil, err
	}
	d.RunClosed = closed(current.GetState())
	got, err := s.Approvals.Decide(context.WithoutCancel(ctx), d)
	var decided *gateway.DecidedError
	switch {
	case errors.Is(err, gateway.ErrNoSuchApproval):
		return nil, status.Errorf(codes.NotFound, "no approval %q", d.ApprovalID)
	case errors.Is(err, gateway.ErrBadDecision):
		return nil, status.Error(codes.InvalidArgument, safetext.Line(err.Error()))
	case errors.As(err, &decided), errors.Is(err, gateway.ErrStaleView), errors.Is(err, gateway.ErrRunOver):
		return nil, status.Error(codes.FailedPrecondition, safetext.Line(err.Error()))
	case err != nil:
		return nil, err
	}
	return &agentosv1.ResolveApprovalResponse{Status: got.Status, DecidedBy: safetext.Line(got.DecidedBy.String),
		DecidedAt: got.DecidedAt.String}, nil
}

// ResolveReconciliation records a person's finding about an effect whose outcome nobody knew
// (design doc, section 6), with the run's doorbell: its call waiting in WAITING_RECONCILE asks
// again and gets the result, or sends the effect again.
func (s *Server) ResolveReconciliation(ctx context.Context, req *agentosv1.ResolveReconciliationRequest) (*agentosv1.ResolveReconciliationResponse, error) {
	if req.GetIdempotencyKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key is required")
	}
	if !req.GetExecuted() && (req.GetLookup() || req.GetResultJson() != "") {
		return nil, status.Error(codes.InvalidArgument, "a result or a lookup only goes with executed")
	}
	e, err := s.Ledger.Q.GetEffect(ctx, req.GetEffectId())
	if errors.Is(err, sql.ErrNoRows) {
		return nil, status.Errorf(codes.NotFound, "no effect %q", req.GetEffectId())
	}
	if err != nil {
		return nil, err
	}
	r, err := s.lookup(ctx, e.WorkflowID)
	if err != nil {
		return nil, err
	}
	// Once the run has ended, nothing will send the effect again: not executed means dropped.
	current, err := s.describe(ctx, r)
	if err != nil {
		return nil, err
	}
	ctx = context.WithoutCancel(ctx) // the finding and its doorbell are written together
	st, _, err := s.Effects.Reconcile(ctx, gateway.Resolution{
		EffectID: req.GetEffectId(), Executed: req.GetExecuted(), ResultJSON: req.GetResultJson(),
		Lookup: req.GetLookup(), Key: req.GetIdempotencyKey(), RunClosed: closed(current.GetState()),
	})
	switch {
	case errors.Is(err, gateway.ErrNoSuchEffect):
		return nil, status.Errorf(codes.NotFound, "no effect %q", req.GetEffectId())
	case errors.Is(err, gateway.ErrNotUnknown), errors.Is(err, gateway.ErrTooEarly), errors.Is(err, gateway.ErrKeyReused):
		return nil, status.Error(codes.FailedPrecondition, safetext.Line(err.Error()))
	case errors.Is(err, gateway.ErrBadResult):
		return nil, status.Error(codes.InvalidArgument, safetext.Line(err.Error()))
	case err != nil:
		return nil, err
	}
	return &agentosv1.ResolveReconciliationResponse{Status: st}, nil
}

// ExtendBudget raises a run's budget, with its doorbell: calls paused in PAUSED_BUDGET ask again.
// The outbox rings it until Temporal has it.
func (s *Server) ExtendBudget(ctx context.Context, req *agentosv1.ExtendBudgetRequest) (*agentosv1.Run, error) {
	if req.GetAmountMicros() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "amount must be positive")
	}
	if req.GetIdempotencyKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key is required")
	}
	r, err := s.lookup(ctx, req.GetRunId())
	if err != nil {
		return nil, err
	}
	if !r.RunID.Valid {
		return nil, status.Error(codes.FailedPrecondition, "the run is still starting")
	}
	current, err := s.describe(ctx, r)
	if err != nil {
		return nil, err
	}
	if closed(current.GetState()) {
		return nil, status.Errorf(codes.FailedPrecondition, "the run has already ended: %s", current.GetState())
	}
	ctx = context.WithoutCancel(ctx) // the extension and its doorbell are written together
	added, err := s.Budget.Extend(ctx, r.WorkflowID, req.GetAmountMicros(), req.GetIdempotencyKey())
	if err != nil {
		return nil, err
	}
	if added {
		s.Log.Info("budget extended", "workflow_id", r.WorkflowID, "amount_micros", req.GetAmountMicros())
		if s.Doorbells != nil {
			s.Doorbells.Poke()
		}
	}
	return s.getRun(ctx, r.WorkflowID)
}

func (s *Server) ExplainRun(context.Context, *agentosv1.ExplainRunRequest) (*agentosv1.Explanation, error) {
	return nil, status.Error(codes.Unimplemented, "runs explain arrives in M5")
}

func (s *Server) lookup(ctx context.Context, workflowID string) (db.Run, error) {
	r, err := s.Ledger.Q.GetRun(ctx, workflowID)
	if errors.Is(err, sql.ErrNoRows) {
		return db.Run{}, status.Errorf(codes.NotFound, "no run %q", workflowID)
	}
	return r, err
}

func (s *Server) getRun(ctx context.Context, workflowID string) (*agentosv1.Run, error) {
	r, err := s.lookup(ctx, workflowID)
	if err != nil {
		return nil, err
	}
	return s.describe(ctx, r)
}

// describe combines the ledger row with the execution's state in Temporal. The result and
// the failure message come from the Worker: neither leaves here with raw control characters.
func (s *Server) describe(ctx context.Context, r db.Run) (*agentosv1.Run, error) {
	out := &agentosv1.Run{
		RunId: r.WorkflowID, TemporalRunId: r.RunID.String, Task: r.Task, CreatedAt: r.CreatedAt, DryRun: r.DryRun != 0,
	}
	if err := json.Unmarshal([]byte(r.ParamsJson), &out.Params); err != nil {
		return nil, err
	}
	b, err := s.Budget.Totals(ctx, r.WorkflowID)
	if err != nil {
		return nil, err
	}
	out.BudgetLimitMicros, out.BudgetSpentMicros, out.BudgetReservedMicros = b.Limit, b.Spent, b.Held
	if !r.RunID.Valid {
		out.State = "CREATED"
		return out, nil
	}
	d, err := s.Temporal.DescribeWorkflowExecution(ctx, r.WorkflowID, r.RunID.String)
	var notFound *serviceerror.NotFound
	switch {
	case errors.As(err, &notFound):
		// Past the namespace's retention, or a dev server that lost its state.
		out.State = "UNKNOWN"
		out.Failure = "the execution is no longer in Temporal"
		return out, nil
	case err != nil:
		return nil, status.Errorf(codes.Unavailable, "describe: %v", err)
	}
	info := d.GetWorkflowExecutionInfo()
	switch info.GetStatus() {
	case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING:
		out.State = "RUNNING"
		if p := info.GetSearchAttributes().GetIndexedFields()[AgentState.GetName()]; p != nil {
			var state string
			if err := converter.GetDefaultDataConverter().FromPayload(p, &state); err == nil && runningStates[state] {
				out.State = state
			}
		}
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		out.State = "SUCCEEDED"
		var v any
		if err := s.Temporal.GetWorkflow(ctx, r.WorkflowID, r.RunID.String).Get(ctx, &v); err != nil {
			out.Failure = safetext.Line("result unavailable: " + err.Error())
			return out, nil
		}
		b, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		out.ResultJson = string(safetext.JSON(b))
	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		out.State = "CANCELLED"
	default: // FAILED, TERMINATED, TIMED_OUT
		out.State = "FAILED"
		if r.CancelledAt.Valid {
			// Ended after a cancel, whether it gave up, was refused or was terminated.
			out.State = "CANCELLED"
		}
		if err := s.Temporal.GetWorkflow(ctx, r.WorkflowID, r.RunID.String).Get(ctx, nil); err != nil {
			out.Failure = safetext.Line(err.Error())
		}
	}
	return out, nil
}
