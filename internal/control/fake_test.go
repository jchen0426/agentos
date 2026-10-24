package control

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/jchen0426/agentos/internal/audit"
)

// fakeTemporal is the part of Temporal's client the Control service uses: starts, describes,
// cancels and terminations; lists, histories and the namespace's retention. Anything else it is
// asked panics.
type fakeTemporal struct {
	client.Client
	mu         sync.Mutex
	startErr   error
	started    []string // workflow IDs started
	cancelled  []string
	terminated []string
	missing    bool // DescribeWorkflowExecution finds nothing

	executions map[string][]string                        // what each listing query lists: workflowID/runID
	kinds      map[string][2]string                       // by workflowID/runID: its type and task queue, when not the usual
	running    map[string]bool                            // by workflowID/runID: listed as running
	histories  map[string][]*historypb.HistoryEvent       // by workflowID/runID
	historyErr map[string]error                           // by workflowID/runID
	pageSize   int                                        // listings in pages of this many; 0 for one page
	queries    []string                                   // the listings asked for
	retention  time.Duration                              // the namespace's
	status     enumspb.WorkflowExecutionStatus            // what DescribeWorkflowExecution says
	statuses   map[string]enumspb.WorkflowExecutionStatus // by workflowID/runID: listed with this status
	// onTerminate runs as TerminateWorkflow is asked, with the fake's lock held.
	onTerminate  func()
	terminateErr error // what TerminateWorkflow answers
}

type fakeRun struct {
	client.WorkflowRun
	id string
}

func (r fakeRun) GetID() string    { return r.id }
func (r fakeRun) GetRunID() string { return "run-of-" + r.id }

func (f *fakeTemporal) ExecuteWorkflow(_ context.Context, o client.StartWorkflowOptions, _ any, _ ...any) (client.WorkflowRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.startErr != nil {
		return nil, f.startErr
	}
	f.started = append(f.started, o.ID)
	return fakeRun{id: o.ID}, nil
}

func (f *fakeTemporal) DescribeWorkflowExecution(_ context.Context, workflowID, _ string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.missing {
		return nil, serviceerror.NewNotFound("no such execution")
	}
	st := f.status
	if st == enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED {
		st = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	}
	return &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
		Status: st, Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: "run-of-" + workflowID}}}, nil
}

func (f *fakeTemporal) CancelWorkflow(_ context.Context, workflowID, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelled = append(f.cancelled, workflowID)
	return nil
}

func (f *fakeTemporal) TerminateWorkflow(_ context.Context, workflowID, _, _ string, _ ...any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.terminated = append(f.terminated, workflowID)
	if f.onTerminate != nil {
		f.onTerminate()
	}
	return f.terminateErr
}

func (f *fakeTemporal) ListWorkflow(_ context.Context, req *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queries = append(f.queries, req.GetQuery())
	all := f.executions[req.GetQuery()]
	from := 0
	if len(req.GetNextPageToken()) > 0 {
		from, _ = strconv.Atoi(string(req.GetNextPageToken()))
	}
	to := len(all)
	if f.pageSize > 0 {
		to = min(from+f.pageSize, len(all))
	}
	resp := &workflowservice.ListWorkflowExecutionsResponse{}
	for _, e := range all[from:to] {
		wf, run, _ := strings.Cut(e, "/")
		kind, ok := f.kinds[e]
		switch {
		case ok:
		case strings.HasPrefix(wf, audit.AnchorIDPrefix):
			kind = [2]string{audit.AnchorWorkflowType, audit.AnchorTaskQueue}
		default:
			kind = [2]string{"t", "agentos-workflows"}
		}
		st := enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
		if f.running[e] {
			st = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
		}
		if s, ok := f.statuses[e]; ok {
			st = s
		}
		resp.Executions = append(resp.Executions, &workflowpb.WorkflowExecutionInfo{
			Execution: &commonpb.WorkflowExecution{WorkflowId: wf, RunId: run}, Type: &commonpb.WorkflowType{Name: kind[0]},
			TaskQueue: kind[1], Status: st})
	}
	if to < len(all) {
		resp.NextPageToken = []byte(strconv.Itoa(to))
	}
	return resp, nil
}

func (f *fakeTemporal) GetWorkflowHistory(_ context.Context, workflowID, runID string, _ bool, _ enumspb.HistoryEventFilterType) client.HistoryEventIterator {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := workflowID + "/" + runID
	if err := f.historyErr[key]; err != nil {
		return &fakeIterator{err: err}
	}
	events, ok := f.histories[key]
	if !ok {
		return &fakeIterator{err: serviceerror.NewNotFound(fmt.Sprintf("workflow execution %s not found", key))}
	}
	return &fakeIterator{events: events}
}

func (f *fakeTemporal) WorkflowService() workflowservice.WorkflowServiceClient {
	return fakeService{retention: f.retention}
}

type fakeIterator struct {
	events []*historypb.HistoryEvent
	err    error
	i      int
}

func (it *fakeIterator) HasNext() bool { return it.err != nil || it.i < len(it.events) }

func (it *fakeIterator) Next() (*historypb.HistoryEvent, error) {
	if it.err != nil {
		err := it.err
		it.err = nil
		return nil, err
	}
	it.i++
	return it.events[it.i-1], nil
}

type fakeService struct {
	workflowservice.WorkflowServiceClient
	retention time.Duration
}

func (s fakeService) DescribeNamespace(context.Context, *workflowservice.DescribeNamespaceRequest, ...grpc.CallOption) (*workflowservice.DescribeNamespaceResponse, error) {
	return &workflowservice.DescribeNamespaceResponse{Config: &namespacepb.NamespaceConfig{
		WorkflowExecutionRetentionTtl: durationpb.New(s.retention)}}, nil
}
