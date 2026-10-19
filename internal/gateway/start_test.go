package gateway

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/interceptor"
	tlog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
)

// The execution Temporal's test environments run activities for.
const (
	testWorkflowID = "default-test-workflow-id"
	testRunID      = "default-test-run-id"
)

// A call to a tool that does not exist: one that gets past the run lookup is denied by the
// tool registry, which needs nothing set up.
var noSuchTool = &agentosv1.Call{Kind: "tool", Name: "no.such.tool", ArgsJson: "{}"}

// startingRun returns a gateway whose ledger holds the test execution's run as StartRun leaves
// it between starting the workflow and recording the run ID Temporal assigned; StartRun
// recorded the run at created.
func startingRun(t *testing.T, created time.Time) (*Gateway, *ledger.Ledger) {
	t.Helper()
	ctx := context.Background()
	l, err := ledger.Open(ctx, filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	if err := l.Q.InsertRun(ctx, db.InsertRunParams{WorkflowID: testWorkflowID, Task: "security-audit",
		ParamsJson: "{}", BudgetLimitMicros: 1, CreatedAt: created.UTC().Format(time.RFC3339Nano)}); err != nil {
		t.Fatal(err)
	}
	return &Gateway{Ledger: l, Log: slog.New(slog.DiscardHandler)}, l
}

// after runs fn once d has passed, as StartRun goes on while a call waits; the test waits for it.
func after(t *testing.T, d time.Duration, fn func(ctx context.Context) error) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		time.Sleep(d)
		done <- fn(context.Background())
	}()
	t.Cleanup(func() {
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
}

func recordRunID(l *ledger.Ledger, runID string) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		_, err := l.Q.SetRunID(ctx, db.SetRunIDParams{RunID: sql.NullString{String: runID, Valid: true}, WorkflowID: testWorkflowID})
		return err
	}
}

// quietSuite is a test suite whose environments log nothing: refusals are what these tests expect.
func quietSuite() *testsuite.WorkflowTestSuite {
	s := &testsuite.WorkflowTestSuite{}
	s.SetLogger(tlog.NewStructuredLogger(slog.New(slog.DiscardHandler)))
	return s
}

// invoke delivers c to g.Invoke as Temporal would, for the test execution.
func invoke(g *Gateway, c *agentosv1.Call) (*agentosv1.Result, error) {
	env := quietSuite().NewTestActivityEnvironment()
	env.RegisterActivityWithOptions(g.Invoke, activity.RegisterOptions{Name: ActivityName})
	v, err := env.ExecuteActivity(ActivityName, c)
	if err != nil {
		return nil, err
	}
	res := &agentosv1.Result{}
	return res, v.Get(res)
}

func TestInvokeWaitsForTheRunIDOfARunStillStarting(t *testing.T) {
	t.Parallel()
	g, l := startingRun(t, time.Now())
	start := time.Now()
	after(t, 300*time.Millisecond, recordRunID(l, testRunID))

	res, err := invoke(g, noSuchTool)
	if err != nil {
		t.Fatalf("the call was refused: %v", err)
	}
	// It went on as the run's call, after the run ID was recorded.
	if res.GetStatus() != StatusDenied || !strings.Contains(res.GetValueJson(), "no such tool") {
		t.Errorf("result: %v", res)
	}
	if waited := time.Since(start); waited < 300*time.Millisecond {
		t.Errorf("answered after %v, before the run ID was recorded", waited)
	}
	if n, err := l.Q.GetCallCounts(context.Background(), db.GetCallCountsParams{Kind: noSuchTool.Kind,
		Name: noSuchTool.Name, ArgsSha256: sha256Hex([]byte("{}")), WorkflowID: testWorkflowID}); err != nil || n.GatewayCalls != 1 {
		t.Errorf("the run's calls: %+v, %v", n, err)
	}
}

// unknownRun reports whether err is the refusal of a caller that is not the run agentd started.
func unknownRun(err error) bool {
	var app *temporal.ApplicationError
	return errors.As(err, &app) && app.Type() == "UnknownRun" && app.NonRetryable()
}

// stillStarting reports whether err is the refusal of a call whose run has no run ID yet:
// retryable, since StartRun, or agentd's next start, may still record it.
func stillStarting(err error) bool {
	var app *temporal.ApplicationError
	return errors.As(err, &app) && !app.NonRetryable() && strings.Contains(app.Error(), ledger.ErrRunStarting.Error())
}

func TestInvokeWaitingForAnotherExecutionsRunIDRefusesTheCall(t *testing.T) {
	t.Parallel()
	g, l := startingRun(t, time.Now())
	start := time.Now()
	// The run ID StartRun records is not the caller's: another execution took the workflow ID.
	after(t, 200*time.Millisecond, recordRunID(l, "another-run-id"))

	_, err := invoke(g, noSuchTool)
	if !unknownRun(err) {
		t.Fatalf("got %v, want the unknown-run refusal", err)
	}
	if waited := time.Since(start); waited > 2*time.Second {
		t.Errorf("refused after %v: the wait went on past the recorded run ID", waited)
	}
}

func TestInvokeWaitingForARunWhoseStartFailedRefusesTheCall(t *testing.T) {
	t.Parallel()
	g, l := startingRun(t, time.Now())
	start := time.Now()
	// Temporal did not start the run, as far as StartRun can tell: it removes the run.
	after(t, 200*time.Millisecond, func(ctx context.Context) error { return l.Q.DeleteRun(ctx, testWorkflowID) })

	_, err := invoke(g, noSuchTool)
	if !unknownRun(err) {
		t.Fatalf("got %v, want the unknown-run refusal", err)
	}
	if waited := time.Since(start); waited > 2*time.Second {
		t.Errorf("refused after %v: the wait went on past the removed run", waited)
	}
}

func TestInvokeRefusesARunStillStartingOnceTheWaitIsOver(t *testing.T) {
	t.Parallel()
	start := time.Now()
	g, _ := startingRun(t, start)
	g.runIDWait = 300 * time.Millisecond

	_, err := invoke(g, noSuchTool)
	waited := time.Since(start)
	if !stillStarting(err) {
		t.Fatalf("got %v, want a retryable %q", err, ledger.ErrRunStarting)
	}
	// The wait ends 300ms after the run's created_at, a wall-clock time: allow for the clocks.
	if waited < 250*time.Millisecond || waited > 3*time.Second {
		t.Errorf("refused after %v, want about 300ms", waited)
	}
}

func TestInvokeWaitsNoLongerForARunStartingSinceLongAgo(t *testing.T) {
	t.Parallel()
	// A run that keeps no run ID: StartRun could neither record nor stop it, or RecoverStarts
	// left it because the execution under its workflow ID has a parent. The wait counts from
	// the run's start, so none of its attempts waits again.
	g, _ := startingRun(t, time.Now().Add(-time.Minute))
	start := time.Now()

	_, err := invoke(g, noSuchTool)
	if !stillStarting(err) {
		t.Fatalf("got %v, want a retryable %q", err, ledger.ErrRunStarting)
	}
	if waited := time.Since(start); waited > time.Second {
		t.Errorf("refused after %v: the attempt waited anew", waited)
	}
}

func TestInvokeWaitsNotForARunRecordedAfterNowOnTheWallClock(t *testing.T) {
	t.Parallel()
	// The wall clock was set back since StartRun recorded the run: how long the run has been
	// starting is unknown, and every attempt waiting the full bound would be the per-attempt
	// wait again.
	g, _ := startingRun(t, time.Now().Add(time.Hour))
	start := time.Now()

	_, err := invoke(g, noSuchTool)
	if !stillStarting(err) {
		t.Fatalf("got %v, want a retryable %q", err, ledger.ErrRunStarting)
	}
	if waited := time.Since(start); waited > time.Second {
		t.Errorf("refused after %v: the attempt waited", waited)
	}
}

func TestInvokeHeartbeatsWhileItWaits(t *testing.T) {
	t.Parallel()
	g, l := startingRun(t, time.Now())
	g.runIDWait = time.Minute
	// The workflow picks the heartbeat timeout; here the wait is twice as long. Temporal's SDK
	// sends heartbeats at most every 80% of the timeout, so an attempt that heartbeats keeps a
	// margin of 400ms, and one that does not times out once 2s pass without one (the test
	// environment checks every second).
	const heartbeatTimeout = 2 * time.Second
	after(t, 2*heartbeatTimeout, recordRunID(l, testRunID))

	var mu sync.Mutex
	var sent []context.Context // the contexts the gateway heartbeated with
	env := quietSuite().NewTestWorkflowEnvironment()
	env.SetTestTimeout(time.Minute)
	env.SetWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{&heartbeatSpy{seen: func(ctx context.Context) {
		mu.Lock()
		defer mu.Unlock()
		sent = append(sent, ctx)
	}}}})
	env.RegisterActivityWithOptions(func(ctx context.Context, c *agentosv1.Call) (*agentosv1.Result, error) {
		res, err := g.Invoke(ctx, c)
		// Temporal's SDK sends a heartbeat it held back later, with the context it came with: one
		// that ended with the wait would leave the rest of the attempt without heartbeats.
		mu.Lock()
		defer mu.Unlock()
		if len(sent) == 0 {
			t.Error("no heartbeat while the call waited")
		}
		for _, hctx := range sent {
			if hctx.Err() != nil {
				t.Error("a heartbeat went out with a context that ended before the attempt")
				break
			}
		}
		return res, err
	}, activity.RegisterOptions{Name: ActivityName})
	env.RegisterWorkflowWithOptions(func(ctx workflow.Context) (*agentosv1.Result, error) {
		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: time.Minute,
			HeartbeatTimeout:    heartbeatTimeout,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		})
		res := &agentosv1.Result{}
		err := workflow.ExecuteActivity(ctx, ActivityName, noSuchTool).Get(ctx, res)
		return res, err
	}, workflow.RegisterOptions{Name: "invoke-once"})
	env.ExecuteWorkflow("invoke-once")

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("the attempt did not outlast the wait: %v", err)
	}
	res := &agentosv1.Result{}
	if err := env.GetWorkflowResult(res); err != nil || res.GetStatus() != StatusDenied {
		t.Errorf("result: %v, %v", res, err)
	}
}

// heartbeatSpy shows seen each context an activity heartbeats with.
type heartbeatSpy struct {
	interceptor.WorkerInterceptorBase
	seen func(context.Context)
}

func (s *heartbeatSpy) InterceptActivity(ctx context.Context, next interceptor.ActivityInboundInterceptor) interceptor.ActivityInboundInterceptor {
	return &heartbeatSpyInbound{ActivityInboundInterceptorBase: interceptor.ActivityInboundInterceptorBase{Next: next}, seen: s.seen}
}

type heartbeatSpyInbound struct {
	interceptor.ActivityInboundInterceptorBase
	seen func(context.Context)
}

func (i *heartbeatSpyInbound) Init(outbound interceptor.ActivityOutboundInterceptor) error {
	return i.Next.Init(&heartbeatSpyOutbound{ActivityOutboundInterceptorBase: interceptor.ActivityOutboundInterceptorBase{Next: outbound}, seen: i.seen})
}

type heartbeatSpyOutbound struct {
	interceptor.ActivityOutboundInterceptorBase
	seen func(context.Context)
}

func (o *heartbeatSpyOutbound) RecordHeartbeat(ctx context.Context, details ...any) {
	o.seen(ctx)
	o.Next.RecordHeartbeat(ctx, details...)
}

func TestAwaitRunIDStopsWithTheAttempt(t *testing.T) {
	t.Parallel()
	g, _ := startingRun(t, time.Now())
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()

	_, err := g.awaitRunID(ctx, testWorkflowID, testRunID)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("got %v, want the attempt's deadline", err)
	}
	if waited := time.Since(start); waited > 2*time.Second {
		t.Errorf("returned after %v, long after the attempt ended", waited)
	}
}
