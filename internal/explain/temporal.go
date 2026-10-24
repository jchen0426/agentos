package explain

import (
	"context"
	"errors"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Unreachable reports whether err says agentd did not get Temporal's answer, as against an answer of
// Temporal's own: unavailable (a connection refused or dropped, the server going away), out of time,
// a call ended by its caller, or Temporal too busy to answer now. Asked again later, Temporal may
// answer; an answer of its own (no such execution, an argument refused) would stand.
func Unreachable(err error) bool {
	var unavailable *serviceerror.Unavailable
	var deadline *serviceerror.DeadlineExceeded
	var busy *serviceerror.ResourceExhausted
	var canceled *serviceerror.Canceled
	switch {
	case err == nil:
		return false
	case errors.As(err, &unavailable), errors.As(err, &deadline), errors.As(err, &busy), errors.As(err, &canceled),
		errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return true
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Canceled:
		return true
	}
	return false
}

// Temporal is what explain reads of a run in Temporal; Temporal's client is one.
type Temporal interface {
	DescribeWorkflowExecution(ctx context.Context, workflowID, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error)
	GetWorkflowHistory(ctx context.Context, workflowID, runID string, isLongPoll bool,
		filterType enumspb.HistoryEventFilterType) client.HistoryEventIterator
}

// ReadTemporal sets what Temporal has of the run's execution (in.Run's): its description now, and its
// history, which Explain and Report read a page at a time. A run whose execution agentd has not
// recorded yet has neither, and neither has one Temporal no longer has (in.Describe stays nil). Any
// other failure is returned: Control fails the explanation with it; a report waits for a Temporal it
// could not reach (Unreachable), and says it could not read the run otherwise.
func ReadTemporal(ctx context.Context, t Temporal, in *Input) error {
	if !in.Run.RunID.Valid {
		return nil
	}
	d, err := t.DescribeWorkflowExecution(ctx, in.Run.WorkflowID, in.Run.RunID.String)
	var notFound *serviceerror.NotFound
	switch {
	case errors.As(err, &notFound):
		return nil
	case err != nil:
		return err
	}
	in.Describe = d
	in.History = historyOf{t.GetWorkflowHistory(ctx, in.Run.WorkflowID, in.Run.RunID.String, false,
		enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)}
	return nil
}

// historyOf is Temporal's history iterator as Explain reads it: an execution Temporal no longer has
// is ErrHistoryGone.
type historyOf struct{ it client.HistoryEventIterator }

func (h historyOf) HasNext() bool { return h.it.HasNext() }

func (h historyOf) Next() (*historypb.HistoryEvent, error) {
	e, err := h.it.Next()
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		return nil, ErrHistoryGone
	}
	return e, err
}
