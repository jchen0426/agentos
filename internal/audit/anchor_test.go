package audit

import (
	"cmp"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
)

type signal struct {
	workflowID, name string
	arg              any
	options          client.StartWorkflowOptions
	workflow         any
}

// signaler is a Temporal that keeps the signals it gets, or fails them with err; its workflows are
// of typ on queue, agentd's unless a test says otherwise.
type signaler struct {
	mu          sync.Mutex
	signals     []signal
	err         error
	typ, queue  string
	describeErr error
	described   []string
}

func (s *signaler) DescribeWorkflowExecution(_ context.Context, workflowID, _ string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.described = append(s.described, workflowID)
	if s.describeErr != nil {
		return nil, s.describeErr
	}
	typ, queue := cmp.Or(s.typ, AnchorWorkflowType), cmp.Or(s.queue, AnchorTaskQueue)
	return &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
		Type: &commonpb.WorkflowType{Name: typ}, TaskQueue: queue}}, nil
}

func (s *signaler) SignalWithStartWorkflow(_ context.Context, workflowID, signalName string, signalArg any,
	options client.StartWorkflowOptions, workflow any, _ ...any) (client.WorkflowRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	s.signals = append(s.signals, signal{workflowID, signalName, signalArg, options, workflow})
	return nil, nil
}

func (s *signaler) got() []signal {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]signal(nil), s.signals...)
}

// The chain's head goes to the day's anchor workflow, started by the signal, whenever it has moved.
func TestAnchorsPushTheHeadWhenItMoved(t *testing.T) {
	l := open(t)
	tc := &signaler{}
	var failed []error
	a := &Anchors{Ledger: l, Temporal: tc, Every: time.Hour, Identity: "this-agentd",
		OnError: func(err error) { failed = append(failed, err) }}
	ctx := context.Background()
	a.Push(ctx)
	if len(tc.got()) != 0 {
		t.Fatal("pushed an empty chain")
	}
	heads := chain(t, l, 2)
	a.Push(ctx)
	a.Push(ctx) // the head has not moved
	got := tc.got()
	if len(got) != 1 {
		t.Fatalf("%d pushes, want one", len(got))
	}
	s := got[0]
	key := records(t, l)[0].Sha256[:16]
	if s.workflowID != "agentos-audit-anchor-"+key+"-"+time.Now().UTC().Format("2006-01-02") || s.name != AnchorSignal || s.arg != signed(heads[1]) || s.workflow != AnchorWorkflowType ||
		s.options.ID != s.workflowID || s.options.TaskQueue != AnchorTaskQueue || s.options.WorkflowExecutionTimeout != AnchorTimeout {
		t.Fatalf("pushed %+v, want head %v", s, heads[1])
	}
	// A push that fails is tried again by the next.
	tc.err = errors.New("the frontend is down")
	more := chain(t, l, 1)
	a.Push(ctx)
	if len(failed) != 1 || len(tc.got()) != 1 {
		t.Fatalf("errors %v", failed)
	}
	tc.err = nil
	a.Push(ctx)
	if got := tc.got(); len(got) != 2 || got[1].arg != signed(more[0]) {
		t.Fatalf("pushes %+v", got)
	}
}

// signed is h as this-agentd pushes it.
func signed(h Head) AnchorHead {
	return AnchorHead{Seq: h.Seq, Sha256: h.Sha256, Agentd: "this-agentd"}
}

// Run pushes every Every, until its context ends.
func TestAnchorsRunPushesEveryEvery(t *testing.T) {
	l := open(t)
	tc := &signaler{}
	a := &Anchors{Ledger: l, Temporal: tc, Every: 10 * time.Millisecond, Identity: "this-agentd"}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { a.Run(ctx); close(done) }()
	heads := chain(t, l, 1)
	deadline := time.Now().Add(5 * time.Second)
	for len(tc.got()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
	if got := tc.got(); len(got) != 1 || got[0].arg != signed(heads[0]) {
		t.Fatalf("pushes %+v", got)
	}
}

// An anchor workflow is the chain's, and the UTC day's.
func TestTheAnchorWorkflowIsTheChainsAndTheUTCDays(t *testing.T) {
	at := time.Date(2026, 10, 22, 1, 30, 0, 0, time.FixedZone("AEDT", 11*3600)) // 21 October, 14:30 UTC
	chain := AnchorChain("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	if got := AnchorWorkflowID(chain, at); got != "agentos-audit-anchor-0123456789abcdef-2026-10-21" ||
		AnchorPrefix(chain) != "agentos-audit-anchor-0123456789abcdef-" {
		t.Fatal(got)
	}
}

// The first push of a day looks at what the day's workflow is: one that is not agentd's is said,
// once; one that cannot be looked at is looked at again with the next push.
func TestTheDaysAnchorWorkflowIsLookedAt(t *testing.T) {
	l := open(t)
	tc := &signaler{describeErr: errors.New("the frontend is down")}
	var failed, misplaced []error
	a := &Anchors{Ledger: l, Temporal: tc, Every: time.Hour, OnError: func(err error) { failed = append(failed, err) },
		OnMisplaced: func(err error) { misplaced = append(misplaced, err) }}
	ctx := context.Background()
	chain(t, l, 1)
	a.Push(ctx)
	if len(failed) != 1 || len(misplaced) != 0 || len(tc.signals) != 1 {
		t.Fatalf("failed %v, misplaced %v", failed, misplaced)
	}
	tc.describeErr, tc.typ = nil, "someone-elses"
	chain(t, l, 1)
	a.Push(ctx)
	chain(t, l, 1)
	a.Push(ctx) // the same day: not looked at again
	if len(tc.described) != 2 || len(misplaced) != 1 || !strings.Contains(misplaced[0].Error(), `is a "someone-elses"`) {
		t.Fatalf("described %v, misplaced %v", tc.described, misplaced)
	}
}
