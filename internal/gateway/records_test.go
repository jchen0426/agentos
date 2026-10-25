package gateway

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/audit"
	"github.com/jchen0426/agentos/internal/explain"
	"github.com/jchen0426/agentos/internal/faults"
	"github.com/jchen0426/agentos/internal/githubtool"
	"github.com/jchen0426/agentos/internal/gitmcp"
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
	"github.com/jchen0426/agentos/internal/models"
	"github.com/jchen0426/agentos/internal/policy"
	"github.com/jchen0426/agentos/internal/tasks"
	"github.com/jchen0426/agentos/internal/tools"
)

// offline is a write whose tool cannot reach its system: it only dry-runs.
type offline struct{ writer }

func (*offline) Live() bool { return false }

// The report shows an approved write that did not go out as approved as not sent: also one that an
// agentd which could not reach the tool's system took up, and only dry-ran (the approval was decided
// under one that could). Approved alone, it would read as if it went out. A request an earlier
// attempt made of it, which did not take effect, the report names: what it made may remain.
func TestTheReportOfAnApprovedWriteThatOnlyDryRan(t *testing.T) {
	for _, earlier := range []bool{false, true} {
		t.Run(fmt.Sprintf("a request went out before %v", earlier), func(t *testing.T) {
			f := newFixture(t, false)
			ctx := context.Background()
			args := map[string]any{"n": 1}
			if _, err := f.ask(t, &writer{class: policy.EffectLookup}, args, 1, time.Hour); err != nil {
				t.Fatal(err)
			}
			if _, err := f.decide(t, f.one(t), Decision{Kind: DecisionApprove}); err != nil {
				t.Fatal(err)
			}
			if earlier {
				// A request of a write of two (a branch) went out, and the next was refused for now.
				w := &writer{class: policy.EffectLookup, after: &tools.RefusedError{Reason: "rate limited", Retryable: true}}
				if _, err := f.ask(t, w, args, 1, time.Hour); err == nil {
					t.Fatal("the refused write did not fail the call")
				}
			}
			// A dry run asks for no approval (dispatch): the call comes without a rule.
			w := &offline{writer{class: policy.EffectLookup}}
			req, canon := call(t, w, args)
			if res, err := f.g.effect(ctx, f.run, w, w.Class(), 1, nil, req, canon, faults.Effect{}); err != nil || res.GetStatus() != StatusOK {
				t.Fatalf("%v %v", res, err)
			}
			if e := effectOf(t, f, f.one(t)); e.Status != effectDryRun || w.sent.Load() != 0 {
				t.Fatalf("effect %s, sent %d", e.Status, w.sent.Load())
			}
			got, err := RunRecords{f.g.Ledger}.RunApprovals(ctx, "wf")
			if err != nil || len(got) != 1 || got[0].Status != approvalApproved || got[0].Write != "not sent" ||
				!strings.Contains(got[0].WriteReason, "only dry-ran") ||
				strings.Contains(got[0].WriteReason, "A request of it went out at") != earlier {
				t.Fatalf("%+v %v", got, err)
			}
		})
	}
}

// fakeHistory is Temporal as a report reads it: one execution, its history, or an error. describeErrs
// and historyErrs fail the reads in turn, one each, before they succeed; err fails every one.
type fakeHistory struct {
	events       []*historypb.HistoryEvent
	err          error
	delay        time.Duration // before err: as long as the SDK's own retries take, scaled down
	describeErrs []error
	historyErrs  []error
	describes    int
}

func (h *fakeHistory) DescribeWorkflowExecution(context.Context, string, string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	h.describes++
	if h.err != nil {
		time.Sleep(h.delay)
		return nil, h.err
	}
	if len(h.describeErrs) > 0 {
		err := h.describeErrs[0]
		h.describeErrs = h.describeErrs[1:]
		return nil, err
	}
	return &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
		Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}}, nil
}

func (h *fakeHistory) GetWorkflowHistory(context.Context, string, string, bool, enumspb.HistoryEventFilterType) client.HistoryEventIterator {
	l := &eventList{events: h.events}
	if len(h.historyErrs) > 0 {
		l.err, h.historyErrs = h.historyErrs[0], h.historyErrs[1:]
	}
	return l
}

type eventList struct {
	events []*historypb.HistoryEvent
	i      int
	err    error // what reading the history fails with, after its first event
}

func (e *eventList) HasNext() bool { return e.i < len(e.events) }

func (e *eventList) Next() (*historypb.HistoryEvent, error) {
	if e.err != nil && e.i == 1 {
		return nil, e.err
	}
	e.i++
	return e.events[e.i-1], nil
}

// called is c's call made through the gateway, as its history has it: scheduled, started and
// completed with the result the gateway returned, under the activity ID agentd recorded.
func (h *fakeHistory) called(t *testing.T, g *Gateway, l *ledger.Ledger, c *agentosv1.Call) {
	t.Helper()
	res, err := invoke(g, c)
	if err != nil {
		t.Fatal(err)
	}
	p := payloadOf(t, lastRecord(t, l))
	input, _ := converter.GetDefaultDataConverter().ToPayloads(c)
	result, _ := converter.GetDefaultDataConverter().ToPayloads(res)
	n := int64(len(h.events))
	if n == 0 {
		h.events = append(h.events, &historypb.HistoryEvent{EventId: 1, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			EventTime: timestamppb.Now()})
		n = 1
	}
	h.events = append(h.events,
		&historypb.HistoryEvent{EventId: n + 1, EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, EventTime: timestamppb.Now(),
			Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
				ActivityId: p["activity_id"].(string), Input: input}}},
		&historypb.HistoryEvent{EventId: n + 2, EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED, EventTime: timestamppb.Now(),
			Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
				ScheduledEventId: n + 1, Attempt: 1}}},
		&historypb.HistoryEvent{EventId: n + 3, EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED, EventTime: timestamppb.Now(),
			Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
				ScheduledEventId: n + 1, StartedEventId: n + 2, Result: result}}})
}

// The report's verdict is runs explain's, over the run's history and agentd's records: the step it
// names is the run's task's, by name only when the task names it; without the history it is unknown.
func TestTheReportsVerdictIsRunsExplains(t *testing.T) {
	g, l, w := invokable(t)
	task := tasks.Task{Name: "security-audit", Steps: map[tasks.Call]string{{Kind: "tool", Name: w.Name()}: "write_it"}}
	registry := tasks.Registry{"security-audit": task}
	g.Policy.Tools[w.Name()] = policy.Tool{Decision: "deny"}
	var h fakeHistory
	h.called(t, g, l, &agentosv1.Call{Kind: "tool", Name: w.Name(), ArgsJson: `{}`})
	ctx := context.Background()
	v, err := ReportVerdicts{Ledger: l, Temporal: &h, Tasks: registry}.ReportVerdict(ctx, testWorkflowID, "done", "")
	if err != nil || v.Verdict != "step_failed" || v.Kind != "policy_denied" || v.Step != "write_it" || len(v.Cut) != 0 {
		t.Fatalf("%+v %v", v, err)
	}
	// A step the task does not name is not named at all: not its kind and name, which the workflow
	// chose, nor agentd's reason, which could quote them.
	h = fakeHistory{}
	h.called(t, g, l, noSuchTool)
	v, err = ReportVerdicts{Ledger: l, Temporal: &h, Tasks: registry}.ReportVerdict(ctx, testWorkflowID, "done", "")
	if err != nil || v.Kind != "denied" || v.Step != "" || v.Cause != "agentd refused a call the task does not name" {
		t.Fatalf("%+v %v", v, err)
	}
	// A history longer than agentd reads: what the run did past it is not known.
	long := h
	for n := int64(len(long.events)); n <= 100_001; n++ {
		long.events = append(long.events, &historypb.HistoryEvent{EventId: n + 1, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			EventTime: timestamppb.Now()})
	}
	v, err = ReportVerdicts{Ledger: l, Temporal: &long, Tasks: registry}.ReportVerdict(ctx, testWorkflowID, "done", "")
	if err != nil || v.Verdict != "unknown" || !strings.Contains(v.Cause, "longer than agentd reads") {
		t.Fatalf("%+v %v", v, err)
	}
}

// The report is rendered once, and what it says stands: while Temporal cannot be reached (as it is
// described, or as its history is read), the report's call fails, for Temporal to try it again once
// it answers, and not "unknown" for good. Each read takes as long as the SDK's own retries do (delay
// stands in for its 10 s): the call fails after one. Only Temporal's own answer makes the verdict
// unknown: no execution of the run, or an error of its own, which agentd's log has, not the report.
func TestTheReportWaitsForTemporal(t *testing.T) {
	g, l, w := invokable(t)
	registry := tasks.Registry{"security-audit": {Name: "security-audit"}}
	g.Policy.Tools[w.Name()] = policy.Tool{Decision: "deny"}
	var h fakeHistory
	h.called(t, g, l, &agentosv1.Call{Kind: "tool", Name: w.Name(), ArgsJson: `{}`})
	ctx := context.Background()
	var logged bytes.Buffer
	verdicts := ReportVerdicts{Ledger: l, Temporal: &h, Tasks: registry, Log: slog.New(slog.NewTextHandler(&logged, nil))}
	down := serviceerror.NewUnavailable("dial tcp temporal.internal:7233: connect: connection refused")
	for name, fail := range map[string]func(){
		"as it describes the run":    func() { h.err, h.delay = down, 100*time.Millisecond },
		"as it gives the history":    func() { h.historyErrs = []error{down} },
		"out of time":                func() { h.err = serviceerror.NewDeadlineExceeded("context deadline exceeded") },
		"too busy":                   func() { h.err = serviceerror.NewResourceExhausted(0, "rate limited") },
		"its connection gone":        func() { h.err = status.Error(codes.Unavailable, "transport is closing") },
		"the call's own time is out": func() { h.err = context.DeadlineExceeded },
	} {
		h.describes = 0
		fail()
		start := time.Now()
		v, err := verdicts.ReportVerdict(ctx, testWorkflowID, "done", "")
		if !explain.Unreachable(err) || v.Verdict != "" || h.describes != 1 || time.Since(start) > 5*time.Second {
			t.Fatalf("%s: %+v %v, %d reads in %s", name, v, err, h.describes, time.Since(start))
		}
		h.err, h.delay = nil, 0
		if v, err := verdicts.ReportVerdict(ctx, testWorkflowID, "done", ""); err != nil || v.Verdict != "step_failed" || v.Kind != "policy_denied" {
			t.Fatalf("%s, then back: %+v %v", name, v, err)
		}
	}
	if !strings.Contains(logged.String(), "waits for Temporal") || !strings.Contains(logged.String(), "temporal.internal:7233") {
		t.Fatalf("log %s", logged.String())
	}

	// Temporal's own answers.
	for name, answer := range map[string]error{
		"an error of its own": serviceerror.NewPermissionDenied("namespace agentos: request unauthorized", ""),
		"no such execution":   serviceerror.NewNotFound("workflow execution not found"),
	} {
		logged.Reset()
		h.err = answer
		v, err := verdicts.ReportVerdict(ctx, testWorkflowID, "done", "")
		want := "agentd could not read the run in Temporal; agentd's log has the details"
		if name == "no such execution" {
			want = "Temporal has no history of the run"
		}
		if err != nil || v.Verdict != "unknown" || v.Kind != "history_unavailable" || v.Cause != want {
			t.Fatalf("%s: %+v %v", name, v, err)
		}
		if name == "an error of its own" && !strings.Contains(logged.String(), "request unauthorized") {
			t.Fatalf("log %s", logged.String())
		}
	}
	h.err = nil
}

// Through the gateway: the report's call while Temporal cannot be reached fails, retryable, and
// records nothing of the write (no effect, no payload); the next attempt, once Temporal answers,
// renders the report with the run's verdict.
func TestAReportWaitsForTemporalBeforeItIsRecorded(t *testing.T) {
	g, l, w := invokable(t)
	registry := tasks.Registry{"security-audit": {Name: "security-audit"}}
	g.Policy.Tools[w.Name()] = policy.Tool{Decision: "deny"}
	var h fakeHistory
	h.called(t, g, l, &agentosv1.Call{Kind: "tool", Name: w.Name(), ArgsJson: `{}`})
	cc := &githubtool.CommentCommit{Repos: map[string]githubtool.Repo{"o/r": {Owner: "o", Name: "r"}}, Blobs: audit.RunBlobs{Ledger: l},
		Effects: RunRecords{l}, Verdicts: ReportVerdicts{Ledger: l, Temporal: &h, Tasks: registry}, Task: "audit_v1"}
	run, err := l.Q.GetRun(context.Background(), testWorkflowID)
	if err != nil {
		t.Fatal(err)
	}
	args := map[string]any{"repo": "o/r", "commit": strings.Repeat("a", 40), "reason": "done"}
	canon, err := encodeCanonical(args)
	if err != nil {
		t.Fatal(err)
	}
	req := tools.Request{WorkflowID: testWorkflowID, EffectID: sha256Hex([]byte(testWorkflowID), []byte(cc.Name()), canon), Args: args}
	ctx := context.Background()
	h.err = serviceerror.NewUnavailable("connection refused")
	res, err := g.effect(ctx, run, cc, cc.Class(), 1, nil, req, canon, faults.Effect{})
	var app *temporal.ApplicationError
	if err == nil || errors.As(err, &app) && app.NonRetryable() || res != nil {
		t.Fatalf("while Temporal is out of reach: %v %v", res, err)
	}
	if _, err := l.Q.GetEffect(ctx, req.EffectID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("an effect was recorded: %v", err)
	}
	h.err = nil
	if res, err := g.effect(ctx, run, cc, cc.Class(), 1, nil, req, canon, faults.Effect{}); err != nil || res.GetStatus() != StatusOK {
		t.Fatalf("once Temporal answers: %v %v", res, err)
	}
	e, err := l.Q.GetEffect(ctx, req.EffectID)
	if err != nil {
		t.Fatal(err)
	}
	b, err := l.GetBlob(ctx, e.PayloadBlob.String)
	if err != nil || !strings.Contains(string(b), "Outcome: step_failed") || strings.Contains(string(b), "unknown") {
		t.Fatalf("payload %s %v", b, err)
	}
}

// Every error record says the type of the failure Temporal has of the attempt, as the SDK names it:
// runs explain holds a failure in the history to it.
func TestTheRecordHasTheFailuresType(t *testing.T) {
	g, l, _ := invokable(t)
	// An application error wrapped is typed as its wrapper is, as the SDK's converter does (it does not
	// unwrap): the record holds what Temporal will.
	for _, err := range []error{errors.New("plain"), fmt.Errorf("wrapped: %w", errors.New("x")),
		&gitmcp.ServerFailure{Tool: "show_diff"}, temporal.NewApplicationError("typed", "Typed"),
		temporal.NewNonRetryableApplicationError("for good", "ForGood", nil),
		fmt.Errorf("wrapped: %w", temporal.NewApplicationError("typed", "Typed"))} {
		info := activity.Info{ActivityID: "9:x", Attempt: 1}
		info.WorkflowExecution.ID, info.WorkflowExecution.RunID = testWorkflowID, testRunID
		if _, got := g.recordCall(context.Background(), info, noSuchTool, g.Log, &callRecord{argsSHA256: "x"}, nil, err); got != err {
			t.Fatalf("%v: got %v", err, got)
		}
		p := payloadOf(t, lastRecord(t, l))
		// What the SDK makes of the error, as an activity's: the failure Temporal records.
		var app *temporal.ApplicationError
		f := temporal.GetDefaultFailureConverter().ErrorToFailure(err)
		if !errors.As(temporal.GetDefaultFailureConverter().FailureToError(f), &app) || p["error_type"] != app.Type() {
			t.Fatalf("%v: record says %v, Temporal %v", err, p["error_type"], f.GetApplicationFailureInfo().GetType())
		}
	}
}

// agentd refuses to start when a task requires a step on a condition no call of its configuration
// meets: the field is not one the call's result holds, or the call is not one agentd makes.
func TestCheckRequired(t *testing.T) {
	dir := t.TempDir()
	schema, err := filepath.Abs("../../config/schemas/audit_report.json")
	if err != nil {
		t.Fatal(err)
	}
	load := func(task string) *models.Config {
		path := filepath.Join(dir, "models.yaml")
		if err := os.WriteFile(path, []byte(fmt.Sprintf("providers:\n  fake: {adapter: fake}\ntasks:\n%s", fmt.Sprintf(task, schema))),
			0o600); err != nil {
			t.Fatal(err)
		}
		m, err := models.Load(path)
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	toolset := tools.Registry{}
	for _, name := range []string{"git.show_diff", "git.apply_check"} {
		toolset[name] = stubTool{name: name}
	}
	toolset.Add(&writer{class: policy.EffectIdempotent})
	audit := tasks.SecurityAudit(nil, nil, nil)
	audit.Required = []tasks.Requirement{{Call: tasks.Call{Kind: "tool", Name: "git.show_diff"}},
		{Call: tasks.Call{Kind: "model", Name: "audit_v1"}},
		{Call: tasks.Call{Kind: "tool", Name: "git.apply_check"}, When: &tasks.Holds{Call: tasks.Call{Kind: "model", Name: "audit_v1"},
			Field: "patch_ref"}}}
	reg := tasks.Registry{"security-audit": audit}
	const with = "  audit_v1: {model: fake/audit-fixture, max_tokens: 4096, output_schema: %s, blobs: [patch]}\n"
	if err := CheckRequired(reg, load(with), toolset); err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct {
		task string
		says string
	}{
		"the patch not a blob": {"  audit_v1: {model: fake/audit-fixture, max_tokens: 4096, output_schema: %s}\n",
			"task security-audit requires validate_patch (tool git.apply_check) once analyze (model audit_v1)'s result holds " +
				"patch_ref, which model task audit_v1 does not produce: its output's patch is not one of its blobs"},
		"no such model task": {"  other_v1: {model: fake/audit-fixture, max_tokens: 4096, output_schema: %s}\n",
			"task security-audit requires analyze (model audit_v1): models.yaml has no model task audit_v1"},
	} {
		if err := CheckRequired(reg, load(tc.task), toolset); err == nil || !strings.Contains(err.Error(), tc.says) {
			t.Errorf("%s: got %v", name, err)
		}
	}
	m := load(with)
	for _, tc := range []struct {
		when tasks.Holds
		says string
	}{
		{tasks.Holds{Call: tasks.Call{Kind: "model", Name: "audit_v1"}, Field: "summary"}, ""},
		{tasks.Holds{Call: tasks.Call{Kind: "model", Name: "audit_v1"}, Field: "report_ref"}, ""},
		{tasks.Holds{Call: tasks.Call{Kind: "model", Name: "audit_v1"}, Field: "patch"}, "does not produce"},
		{tasks.Holds{Call: tasks.Call{Kind: "model", Name: "audit_v1"}, Field: "verdict"}, "nor a field of its output schema"},
		{tasks.Holds{Call: tasks.Call{Kind: "tool", Name: "git.show_diff"}, Field: "blob"}, "which only reads"},
		{tasks.Holds{Call: tasks.Call{Kind: "tool", Name: "test.write"}, Field: "id"}, ""},
		{tasks.Holds{Call: tasks.Call{Kind: "tool", Name: "test.write"}, Field: "url"}, "result schema does not have"},
		{tasks.Holds{Call: tasks.Call{Kind: "tool", Name: "nope"}, Field: "x"}, "agentd has no tool nope"},
		{tasks.Holds{Call: tasks.Call{Kind: "mcp", Name: "x"}, Field: "x"}, `kind "mcp"`},
	} {
		when := tc.when
		r := tasks.Registry{"t": {Name: "t", Required: []tasks.Requirement{{Call: tasks.Call{Kind: "tool", Name: "git.show_diff"},
			When: &when}}}}
		if err := CheckRequired(r, m, toolset); (tc.says == "") != (err == nil) || err != nil && !strings.Contains(err.Error(), tc.says) {
			t.Errorf("%+v: got %v", tc.when, err)
		}
	}
}

// stubTool is a read tool by its name only.
type stubTool struct{ name string }

func (s stubTool) Name() string      { return s.name }
func (stubTool) Class() string       { return policy.EffectNone }
func (stubTool) Args() *tools.Schema { return noArgs }
func (stubTool) Read(context.Context, tools.Request) (map[string]any, error) {
	return map[string]any{}, nil
}

// A fault script's status comes with its value, numbers as written (tests only).
func TestAFaultsStatusValue(t *testing.T) {
	g, _, w := invokable(t)
	path := filepath.Join(t.TempDir(), "faults.json")
	digits := strings.Repeat("9", 400)
	if err := os.WriteFile(path, []byte(`{"rules": [{"name": "`+w.Name()+`", "status": "ok", "value": {"id": `+digits+`}}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	script, err := faults.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	g.Faults = script
	res, err := invoke(g, &agentosv1.Call{Kind: "tool", Name: w.Name(), ArgsJson: `{}`})
	if err != nil || res.GetStatus() != StatusOK || !strings.Contains(res.GetValueJson(), `"id":`+digits) {
		t.Fatalf("%v %v", res, err)
	}
	if w.sent.Load() != 0 {
		t.Fatal("the write was sent")
	}
}

// refusedBy is who a result says refused its call, before recordCall takes it into the call's record.
func refusedBy(t *testing.T, res *agentosv1.Result) any {
	t.Helper()
	var v map[string]any
	if err := json.Unmarshal([]byte(res.GetValueJson()), &v); err != nil {
		t.Fatal(err)
	}
	return v[refusedByKey]
}

// Every place that refuses a call says who did, for its record: runs explain picks the kind by it.
func TestEveryRefusalSaysWhoRefused(t *testing.T) {
	ctx := context.Background()
	t.Run("the policy's limit on a run's writes, unapproved", func(t *testing.T) {
		f := newFixture(t, false)
		w := &writer{class: policy.EffectIdempotent}
		for n, want := range []any{nil, audit.RefusedByPolicy} {
			req, canon := call(t, w, map[string]any{"n": n})
			res, err := f.g.effect(ctx, f.run, w, w.Class(), 1, nil, req, canon, faults.Effect{})
			if err != nil || refusedBy(t, res) != want {
				t.Fatalf("write %d: %v %v", n, res, err)
			}
		}
	})
	t.Run("the policy's limit on a run's writes, to approve", func(t *testing.T) {
		f := newFixture(t, false)
		w := &writer{class: policy.EffectIdempotent}
		if _, err := f.ask(t, w, map[string]any{"n": 1}, 1, time.Hour); err != nil {
			t.Fatal(err)
		}
		res, err := f.ask(t, w, map[string]any{"n": 2}, 1, time.Hour)
		if err != nil || res.GetStatus() != StatusDenied || refusedBy(t, res) != audit.RefusedByPolicy {
			t.Fatalf("%v %v", res, err)
		}
	})
	t.Run("a write the tool refused when it was sent", func(t *testing.T) {
		f := newFixture(t, false)
		w := &writer{class: policy.EffectIdempotent, deny: "GitHub said 422"}
		for range 2 { // and the call again, which the settled effect answers
			res, err := f.g.effect(ctx, f.run, w, w.Class(), 1, nil, f.req(), nil, faults.Effect{})
			if err != nil || res.GetStatus() != StatusDenied || refusedBy(t, res) != audit.RefusedByTool {
				t.Fatalf("%v %v", res, err)
			}
		}
	})
	t.Run("an approved write of a run cancelled before it went out", func(t *testing.T) {
		f := newFixture(t, false)
		w := &writer{class: policy.EffectIdempotent}
		if _, err := f.ask(t, w, map[string]any{"n": 1}, 1, time.Hour); err != nil {
			t.Fatal(err)
		}
		if _, err := f.decide(t, f.one(t), Decision{Kind: DecisionApprove}); err != nil {
			t.Fatal(err)
		}
		f.cancel(t)
		res, err := f.ask(t, w, map[string]any{"n": 1}, 1, time.Hour)
		if err != nil || res.GetStatus() != StatusDenied || refusedBy(t, res) != audit.RefusedByCancel {
			t.Fatalf("%v %v", res, err)
		}
	})
	// The cancel lands after Invoke read the run, and before the write went out: a cancel's
	// refusal, whichever step of the write found it (effect is called with the run as Invoke read it).
	t.Run("a write of a run cancelled before it was claimed", func(t *testing.T) {
		f := newFixture(t, false)
		w := &writer{class: policy.EffectIdempotent}
		f.cancel(t)
		for range 2 { // and the call again, which the settled effect answers
			res, err := f.g.effect(ctx, f.run, w, w.Class(), 1, nil, f.req(), nil, faults.Effect{})
			if err != nil || res.GetStatus() != StatusDenied || refusedBy(t, res) != audit.RefusedByCancel || w.sent.Load() != 0 {
				t.Fatalf("%v %v, sent %d", res, err, w.sent.Load())
			}
		}
	})
	t.Run("a write to be held for approval, of a run cancelled meanwhile", func(t *testing.T) {
		f := newFixture(t, false)
		w := &writer{class: policy.EffectIdempotent}
		args := map[string]any{"n": 1}
		req, canon := call(t, w, args)
		if res, err := f.g.record(ctx, f.run, w, w.Class(), 1, nil, req, canon); res != nil || err != nil {
			t.Fatalf("record: %v %v", res, err)
		}
		f.cancel(t)
		res, err := f.ask(t, w, args, 1, time.Hour)
		if err != nil || res.GetStatus() != StatusDenied || refusedBy(t, res) != audit.RefusedByCancel {
			t.Fatalf("%v %v", res, err)
		}
	})
	t.Run("an approved write agentd would not send, of a run not cancelled", func(t *testing.T) {
		f := newFixture(t, false)
		w := &writer{class: policy.EffectIdempotent}
		args := map[string]any{"title": "fix"}
		if _, err := f.ask(t, w, args, 1, time.Hour); err != nil {
			t.Fatal(err)
		}
		a := f.one(t)
		if _, err := f.decide(t, a, Decision{Kind: DecisionApprove}); err != nil {
			t.Fatal(err)
		}
		if _, err := f.g.Ledger.DB.Exec(`UPDATE blobs SET bytes = CAST(replace(CAST(bytes AS TEXT), 'fix', 'fiy') AS BLOB) WHERE sha256 = ?`,
			a.PayloadSha256); err != nil {
			t.Fatal(err)
		}
		for range 2 { // and the call again, which the abandoned effect answers
			res, err := f.ask(t, w, args, 1, time.Hour)
			if err != nil || res.GetStatus() != StatusDenied || refusedBy(t, res) != audit.RefusedByAgentd {
				t.Fatalf("%v %v", res, err)
			}
		}
	})
	// What a call gets again for a write never sent: the cancel's refusal once the run is cancelled,
	// agentd's otherwise; and a run agentd cannot read fails the call.
	t.Run("an abandoned write, as agentd answers it again", func(t *testing.T) {
		f := newFixture(t, false)
		e := db.Effect{EffectID: strings.Repeat("a", 64), WorkflowID: "wf", Status: effectAbandoned, Reason: valid("why")}
		for _, want := range []string{audit.RefusedByAgentd, audit.RefusedByCancel} {
			if want == audit.RefusedByCancel {
				f.cancel(t)
			}
			res, err := f.g.effectResult(ctx, e)
			if err != nil || res.GetStatus() != StatusDenied || refusedBy(t, res) != want {
				t.Fatalf("%v %v, want %s", res, err, want)
			}
		}
		e.WorkflowID = "no such run"
		if res, err := f.g.effectResult(ctx, e); err == nil {
			t.Fatalf("%v", res)
		}
	})
	t.Run("the policy's denial of a model's provider", func(t *testing.T) {
		g, run, _ := fallbackGateway(t)
		g.Policy = &policy.Policy{Models: map[string]policy.Model{"audit_v1": {AllowedProviders: []string{"other"}}}}
		res, err := g.dispatch(ctx, activity.Info{ActivityID: "2:audit_v1", Attempt: 1}, run,
			&agentosv1.Call{Kind: "model", Name: "audit_v1"}, map[string]any{"diff": "x"}, []byte(`{"diff":"x"}`), faults.Effect{},
			&callRecord{})
		if err != nil || res.GetStatus() != StatusDenied || refusedBy(t, res) != audit.RefusedByPolicy {
			t.Fatalf("%v %v", res, err)
		}
	})
}

// runs explain's rule for an attempt agentd stopped during, on the records Invoke writes: the attempt's
// call.started (written once the policy lets the call by), no call record of it, and agentd.started
// after it. agentd stopped during the attempt, by the records' order, whatever the clocks say; the
// next attempt's record names the call.started it began with (started_seq).
func TestExplainTellsTheAttemptAgentdStoppedDuring(t *testing.T) {
	g, l, w := invokable(t)
	ctx := context.Background()
	c := &agentosv1.Call{Kind: "tool", Name: w.Name(), ArgsJson: `{"n":1}`}
	info := activity.Info{ActivityID: "1:" + w.Name(), Attempt: 1}
	info.WorkflowExecution.ID, info.WorkflowExecution.RunID = testWorkflowID, testRunID
	if err := g.callStarted(ctx, info, c, &callRecord{argsSHA256: "a"}); err != nil {
		t.Fatal(err)
	}
	began := lastRecord(t, l)
	if _, err := audit.Record(ctx, l, audit.Event{Type: "agentd.started", Actor: audit.Agentd, Payload: map[string]any{}}); err != nil {
		t.Fatal(err)
	}
	restarted := lastRecord(t, l)
	info.Attempt = 2
	rec := &callRecord{argsSHA256: "a"}
	if err := g.callStarted(ctx, info, c, rec); err != nil {
		t.Fatal(err)
	}
	ok, _ := result(StatusOK, map[string]any{"id": 1})
	res, err := g.recordCall(ctx, info, c, g.Log, rec, ok, nil)
	if err != nil {
		t.Fatal(err)
	}
	if p := payloadOf(t, lastRecord(t, l)); fmt.Sprint(p["started_seq"]) != fmt.Sprint(rec.startedSeq) || rec.startedSeq <= restarted.Seq {
		t.Fatalf("record %v, started %d", p, rec.startedSeq)
	}
	input, _ := converter.GetDefaultDataConverter().ToPayloads(c)
	value, _ := converter.GetDefaultDataConverter().ToPayloads(res)
	h := fakeHistory{events: []*historypb.HistoryEvent{
		{EventId: 1, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, EventTime: timestamppb.Now()},
		{EventId: 2, EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, EventTime: timestamppb.Now(),
			Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
				ActivityId: info.ActivityID, Input: input}}},
		{EventId: 3, EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED, EventTime: timestamppb.Now(),
			Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
				ScheduledEventId: 2, Attempt: 2}}},
		{EventId: 4, EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED, EventTime: timestamppb.Now(),
			Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
				ScheduledEventId: 2, StartedEventId: 3, Result: value}}},
	}}
	run, err := l.Q.GetRun(ctx, testWorkflowID)
	if err != nil {
		t.Fatal(err)
	}
	in := explain.Input{RunID: testWorkflowID, Task: tasks.Task{Name: "security-audit"}, Run: run, State: "RUNNING", Now: time.Now()}
	if err := explain.ReadTemporal(ctx, &h, &in); err != nil {
		t.Fatal(err)
	}
	e, err := explain.Explain(in, func(want explain.Wanted) (explain.Ledger, error) {
		var got explain.Ledger
		err := l.Read(ctx, func(q *db.Queries) error {
			var err error
			got, err = explain.ReadLedger(ctx, q, run, want)
			return err
		})
		return got, err
	})
	if err != nil {
		t.Fatal(err)
	}
	var stopped *agentosv1.Incident
	for _, inc := range e.GetIncidents() {
		if inc.GetKind() == explain.KindAgentdStopped && inc.GetAttempt() == 1 {
			stopped = inc
		}
	}
	if stopped == nil || !strings.Contains(stopped.GetCause(), "began and never ended: agentd stopped during it") {
		t.Fatalf("incidents %v", e.GetIncidents())
	}
	seqs := map[int64]bool{}
	for _, ev := range stopped.GetEvidence() {
		seqs[ev.GetSeq()] = true
	}
	if !seqs[began.Seq] || !seqs[restarted.Seq] {
		t.Fatalf("evidence %v, want seqs %d and %d", stopped.GetEvidence(), began.Seq, restarted.Seq)
	}
}

// A ledger whose schema is not its migrations' is read by nothing a report says: the report refuses
// (its call fails), before it asks Temporal of the run whose row it read, and judges nothing. The run's
// row names the execution and the task the report judges: it is read as the rest is, through a read
// transaction, which checks the schema first.
func TestTheReportRefusesALedgerWhoseSchemaChanged(t *testing.T) {
	g, l, w := invokable(t)
	registry := tasks.Registry{"security-audit": {Name: "security-audit"}}
	g.Policy.Tools[w.Name()] = policy.Tool{Decision: "deny"}
	var h fakeHistory
	h.called(t, g, l, &agentosv1.Call{Kind: "tool", Name: w.Name(), ArgsJson: `{}`})
	if _, err := l.DB.Exec(`CREATE VIEW v AS SELECT 1`); err != nil {
		t.Fatal(err)
	}
	v, err := ReportVerdicts{Ledger: l, Temporal: &h, Tasks: registry}.ReportVerdict(context.Background(), testWorkflowID, "done", "")
	if !errors.Is(err, ledger.ErrSchemaMismatch) || v.Verdict != "" || h.describes != 0 {
		t.Fatalf("%+v %v, %d descriptions", v, err, h.describes)
	}
}
