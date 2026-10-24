package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/faults"
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/tools"
)

// testActivityID is the activity ID of a call in the test activity environment.
const testActivityID = "0"

// An attempt is executing in this agentd from before its call.started to after its call record:
// verify takes an open call.started of one for one in flight, whatever its run's state.
// The same attempt begun twice in this agentd (one call handled twice over) executes until both end.
func TestAnAttemptBegunTwiceExecutesUntilBothEnd(t *testing.T) {
	g := &Gateway{}
	k := attemptKey{"wf", "1:x", 1}
	g.attempts.begin(k)
	g.attempts.begin(k)
	g.attempts.end(k)
	if !g.Executing("wf", "1:x", 1) {
		t.Fatal("one of two ended, and the attempt is not executing")
	}
	g.attempts.end(k)
	if g.Executing("wf", "1:x", 1) || len(g.attempts.executing) != 0 {
		t.Fatalf("both ended, and the attempt is executing (%v)", g.attempts.executing)
	}
}

func TestAnAttemptIsExecutingUntilItsRecord(t *testing.T) {
	g, l, _ := invokable(t)
	w := &writer{class: "idempotent", release: make(chan struct{})}
	g.Tools = tools.Registry{w.Name(): w}
	done := make(chan error, 1)
	go func() {
		_, err := invoke(g, &agentosv1.Call{Kind: "tool", Name: w.Name(), ArgsJson: `{}`})
		done <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for { // past its call.started, in the tool
		if r, err := l.Q.LastAuditRecord(context.Background()); err == nil && r.Type == "effect.claimed" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the call never reached its tool")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !g.Executing(testWorkflowID, testActivityID, 1) {
		t.Fatal("an attempt in its tool is not executing")
	}
	close(w.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if g.Executing(testWorkflowID, testActivityID, 1) || lastRecord(t, l).Type != "call" {
		t.Fatalf("executing after its record, which is %s", lastRecord(t, l).Type)
	}
}

// panicker is a tool that panics as it executes.
type panicker struct{ writer }

func (*panicker) Execute(context.Context, json.RawMessage, string, tools.Proceed) (map[string]any, error) {
	panic("the tool fell over")
}

// A panic in a tool after the attempt's call.started: the SDK recovers it and fails the attempt,
// and the call's record would never be written. It is, with the panic as its error, naming the
// call.started; the attempt fails as a panic does.
func TestAPanickingCallHasItsRecord(t *testing.T) {
	g, l, _ := invokable(t)
	p := &panicker{writer{class: "idempotent"}}
	g.Tools = tools.Registry{p.Name(): p}
	_, err := invoke(g, &agentosv1.Call{Kind: "tool", Name: p.Name(), ArgsJson: `{}`})
	if err == nil || !strings.Contains(err.Error(), "the tool fell over") {
		t.Fatalf("err %v, want the panic", err)
	}
	last := lastRecord(t, l)
	pl := payloadOf(t, last)
	if last.Type != "call" || pl["status"] != "error" || !strings.Contains(pl["error"].(string), "panic: the tool fell over") ||
		pl["started_seq"] == nil {
		t.Fatalf("the last record %s %v", last.Type, pl)
	}
	// Its error_type is the failure's Temporal records of the attempt (the SDK's PanicError), which runs
	// explain holds the failure to: a panic of agentd's is agentd's failure, not one it did not send.
	var panicked *temporal.PanicError
	if !errors.As(err, &panicked) || pl["error_type"] != temporal.GetDefaultFailureConverter().ErrorToFailure(panicked).GetApplicationFailureInfo().GetType() {
		t.Fatalf("error_type %v, the attempt's failure %v", pl["error_type"], err)
	}
	if g.Executing(testWorkflowID, testActivityID, 1) {
		t.Fatal("still executing")
	}
}

// An attempt whose call record cannot be written, after its call.started: a fault, which the
// gateway keeps for verify to report apart from mismatches.
func TestAFaultThatLostACallRecordIsKept(t *testing.T) {
	g, l, w := invokable(t)
	drop := failAppends(t, l, "call")
	defer drop()
	if _, err := invoke(g, &agentosv1.Call{Kind: "tool", Name: w.Name(), ArgsJson: `{}`}); err == nil {
		t.Fatal("no error for a call whose record could not be written")
	}
	why, ok := g.Faulted(testWorkflowID, testActivityID, 1)
	if !ok || !strings.Contains(why, "no room for the record") {
		t.Fatalf("faulted %v %q", ok, why)
	}
	if _, ok := g.Faulted(testWorkflowID, testActivityID, 2); ok {
		t.Fatal("another attempt faulted")
	}
}

// A status a fault script answers with (tests only, faults) is an answer like any other: the
// attempt's call.started comes first, and the call's record names it. For a tool, and for a model.
func TestAFaultedStatusIsAnsweredAfterTheCallStarted(t *testing.T) {
	named := func(t *testing.T, l *ledger.Ledger, rec *callRecord) {
		t.Helper()
		if rec.startedSeq == 0 || recordAt(t, l, rec.startedSeq).Type != "call.started" {
			t.Fatalf("the attempt has no call.started: %+v", rec)
		}
	}
	t.Run("a tool", func(t *testing.T) {
		g, l, w := invokable(t)
		path := filepath.Join(t.TempDir(), "faults.json")
		if err := os.WriteFile(path, []byte(`{"rules": [{"name": "`+w.Name()+`", "status": "retry_later"}]}`), 0o600); err != nil {
			t.Fatal(err)
		}
		script, err := faults.Load(path)
		if err != nil {
			t.Fatal(err)
		}
		g.Faults = script
		res, err := invoke(g, &agentosv1.Call{Kind: "tool", Name: w.Name(), ArgsJson: `{}`})
		if err != nil || res.GetStatus() != "retry_later" || w.sent.Load() != 0 {
			t.Fatalf("%v %v, sent %d", res, err, w.sent.Load())
		}
		last := lastRecord(t, l)
		p := payloadOf(t, last)
		if seq, ok := p["started_seq"].(float64); last.Type != "call" || !ok || recordAt(t, l, int64(seq)).Type != "call.started" {
			t.Fatalf("the call's record %s %v", last.Type, p)
		}
	})
	t.Run("a model", func(t *testing.T) {
		g, run, _ := fallbackGateway(t)
		call := &agentosv1.Call{Kind: "model", Name: "audit_v1", ArgsJson: `{"diff":"x"}`}
		args := map[string]any{"diff": "x"}
		canon, _ := encodeCanonical(args)
		info := activity.Info{ActivityID: "1:audit_v1", Attempt: 1, RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 5},
			ScheduledTime: time.Unix(1000, 0)}
		info.WorkflowExecution.ID = run.WorkflowID
		rec := &callRecord{argsSHA256: sha256Hex(canon)}
		res, err := g.dispatch(context.Background(), info, run, call, args, canon, faults.Effect{Status: "retry_later"}, rec)
		if err != nil || res.GetStatus() != "retry_later" {
			t.Fatalf("%v %v", res, err)
		}
		named(t, g.Ledger, rec)
	})
}
