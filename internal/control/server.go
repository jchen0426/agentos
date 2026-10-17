// Package control serves the Control service on the control socket, the only way to start,
// inspect or cancel runs. The socket lives in a 0700 directory the sandbox cannot see.
package control

import (
	"context"
	"database/sql"
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
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
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
	Temporal    client.Client
	Tasks       tasks.Registry
	TaskQueue   string        // where the Python Worker polls
	CancelGrace time.Duration // how long a cancelled run may keep running before it is terminated
	Log         *slog.Logger
}

func (s *Server) StartRun(ctx context.Context, req *agentosv1.StartRunRequest) (*agentosv1.Run, error) {
	t, ok := s.Tasks[req.GetTask()]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "unknown task %q", req.GetTask())
	}
	input, params, err := t.Resolve(ctx, req.GetParams())
	if err != nil {
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
	if err := s.Ledger.Q.InsertRun(ctx, db.InsertRunParams{
		WorkflowID: workflowID, Task: t.Name, ParamsJson: string(paramsJSON),
		BudgetLimitMicros: budget, CreatedAt: createdAt,
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
	s.Log.Info("run started", "workflow_id", workflowID, "run_id", we.GetRunID(), "task", t.Name)
	// Answer from what agentd knows: an error here would invite a retry, and a retry
	// starts a second run.
	return &agentosv1.Run{
		RunId: workflowID, TemporalRunId: we.GetRunID(), Task: t.Name, State: "RUNNING",
		Params: params, BudgetLimitMicros: budget, CreatedAt: createdAt,
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

func (s *Server) ListPending(context.Context, *agentosv1.ListPendingRequest) (*agentosv1.ListPendingResponse, error) {
	return nil, status.Error(codes.Unimplemented, "approvals and reconciliations arrive in M3 and M4")
}

func (s *Server) ResolveApproval(context.Context, *agentosv1.ResolveApprovalRequest) (*agentosv1.ResolveApprovalResponse, error) {
	return nil, status.Error(codes.Unimplemented, "approvals arrive in M4")
}

func (s *Server) ResolveReconciliation(context.Context, *agentosv1.ResolveReconciliationRequest) (*agentosv1.ResolveReconciliationResponse, error) {
	return nil, status.Error(codes.Unimplemented, "reconciliation arrives in M3")
}

func (s *Server) ExtendBudget(context.Context, *agentosv1.ExtendBudgetRequest) (*agentosv1.Run, error) {
	return nil, status.Error(codes.Unimplemented, "budgets arrive in M2")
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

// describe combines the ledger row with the execution's state in Temporal.
func (s *Server) describe(ctx context.Context, r db.Run) (*agentosv1.Run, error) {
	out := &agentosv1.Run{
		RunId: r.WorkflowID, TemporalRunId: r.RunID.String, Task: r.Task,
		BudgetLimitMicros: r.BudgetLimitMicros, CreatedAt: r.CreatedAt,
	}
	if err := json.Unmarshal([]byte(r.ParamsJson), &out.Params); err != nil {
		return nil, err
	}
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
			out.Failure = "result unavailable: " + err.Error()
			return out, nil
		}
		b, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		out.ResultJson = string(b)
	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		out.State = "CANCELLED"
	default: // FAILED, TERMINATED, TIMED_OUT
		out.State = "FAILED"
		if r.CancelledAt.Valid {
			// Ended after a cancel, whether it gave up, was refused or was terminated.
			out.State = "CANCELLED"
		}
		if err := s.Temporal.GetWorkflow(ctx, r.WorkflowID, r.RunID.String).Get(ctx, nil); err != nil {
			out.Failure = err.Error()
		}
	}
	return out, nil
}
