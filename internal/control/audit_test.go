package control

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/timestamppb"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/audit"
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
)

func row(runID string) *db.Run {
	return &db.Run{WorkflowID: "wf", RunID: sql.NullString{String: runID, Valid: runID != ""}}
}

// The runs table must say of each run what Temporal and the audit log say: a run either has that
// the table has otherwise, or not at all, is a mismatch, said once.
func TestTheRunsTableMustAgreeWithTemporalAndTheLog(t *testing.T) {
	for name, tc := range map[string]struct {
		c    runCheck
		want []string // what each mismatch says, in order; none for a run that agrees
	}{
		"all three agree":                          {runCheck{listed: []string{"r1"}, row: row("r1"), created: 3, started: "r1", startedSeq: 4}, nil},
		"from before the audit log":                {runCheck{listed: []string{"r1"}, row: row("r1")}, nil},
		"past Temporal's retention":                {runCheck{row: row("r1"), created: 3, started: "r1", startedSeq: 4}, nil},
		"discarded":                                {runCheck{created: 3, discarded: 5}, nil},
		"starting, Temporal not yet":               {runCheck{row: row(""), created: 3}, nil},
		"its row deleted":                          {runCheck{listed: []string{"r1"}, created: 3, started: "r1", startedSeq: 4}, []string{"Temporal has execution r1 of the run, and the ledger has no row", "the audit log records the run (seq 4), and the ledger has no row"}},
		"its row and its records gone":             {runCheck{listed: []string{"r1"}}, []string{"Temporal has execution r1 of the run, and the ledger has no row"}},
		"its row deleted, Temporal past retention": {runCheck{created: 3, started: "r1", startedSeq: 4}, []string{"the audit log records the run (seq 4), and the ledger has no row"}},
		"its execution removed":                    {runCheck{listed: []string{"r1"}, row: row(""), created: 3, started: "r1", startedSeq: 4}, []string{"the audit log records its execution as r1 (seq 4), and the ledger as none"}},
		"no execution recorded anywhere":           {runCheck{listed: []string{"r1"}, row: row(""), created: 3}, []string{"Temporal has execution r1 of the run, and the ledger records no execution of it"}},
		"another execution recorded":               {runCheck{listed: []string{"r1"}, row: row("r2"), created: 3, started: "r1", startedSeq: 4}, []string{"Temporal has execution r1 under the run's ID, and the ledger records execution r2", "the audit log records its execution as r1 (seq 4), and the ledger as r2"}},
		"another execution under its ID":           {runCheck{listed: []string{"r1", "rX"}, row: row("r1"), created: 3, started: "r1", startedSeq: 4}, []string{"Temporal has execution rX under the run's ID, and the ledger records execution r1"}},
		"a discarded run in Temporal":              {runCheck{listed: []string{"r1"}, created: 3, discarded: 5}, []string{"Temporal has execution r1 of the run, which the audit log records as discarded (seq 5)"}},
		"a discarded run with a row":               {runCheck{row: row("r1"), created: 3, discarded: 5}, []string{"the audit log records the run as discarded (seq 5), and the ledger has a row"}},
	} {
		t.Run(name, func(t *testing.T) {
			got := tc.c.mismatches()
			if len(got) != len(tc.want) {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
			for i := range got {
				if !strings.Contains(got[i], tc.want[i]) {
					t.Errorf("mismatch %d: %q, want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
	// Each execution any of them names is read, once: those Temporal lists, the row's and the one
	// the audit log says started.
	c := runCheck{listed: []string{"r1", "rX"}, row: row("r2"), started: "r3"}
	if got := c.executions(); strings.Join(got, ",") != "r1,r2,r3,rX" {
		t.Errorf("executions: %q", got)
	}
}

// completed is a history's ActivityTaskScheduled, ActivityTaskStarted (attempt) and
// ActivityTaskCompleted of a gateway call that returned res, from event id on.
func completed(t *testing.T, id int64, activityID string, attempt int32, res *agentosv1.Result) []*historypb.HistoryEvent {
	t.Helper()
	payloads, err := converter.GetDefaultDataConverter().ToPayloads(res)
	if err != nil {
		t.Fatal(err)
	}
	return []*historypb.HistoryEvent{
		{EventId: id, EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
			ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{ActivityId: activityID}}},
		{EventId: id + 1, EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED, Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{
			ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{ScheduledEventId: id, Attempt: attempt}}},
		{EventId: id + 2, EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED, Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{
			ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{ScheduledEventId: id, StartedEventId: id + 1,
				Result: payloads}}},
	}
}

func TestEachResultOfAHistoryIsAnAnchor(t *testing.T) {
	events := []*historypb.HistoryEvent{{EventId: 1, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		EventTime: timestamppb.New(time.Now())}}
	events = append(events, completed(t, 5, "1:git.show_diff", 2, &agentosv1.Result{Status: "ok",
		ValueJson: `{"blob":"sha256:ab","n":1.50,"_audit":{"seq":7,"sha256":"cd"}}`})...)
	events = append(events, completed(t, 9, "2:audit_v1", 1, &agentosv1.Result{Status: "denied", ValueJson: `{"reason":"no"}`})...)
	events = append(events, completed(t, 13, "3:x", 1, &agentosv1.Result{Status: "ok", ValueJson: `{"_audit":{"seq":"7"}}`})...)
	// A completion that is not a gateway result at all.
	events = append(events, &historypb.HistoryEvent{EventId: 20, EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
			Result: &commonpb.Payloads{Payloads: []*commonpb.Payload{{Metadata: map[string][]byte{"encoding": []byte("binary/plain")}, Data: []byte("x")}}}}}})
	got := anchorsOf(events)
	if len(got) != 4 {
		t.Fatalf("%d anchors: %+v", len(got), got)
	}
	// The value's sha256 is of the value without its anchor, its numbers as written: what the
	// call's record holds.
	want, _ := audit.ValueSHA256(map[string]any{"blob": "sha256:ab", "n": json.Number("1.50")})
	if a := got[0]; a.activityID != "1:git.show_diff" || a.head == nil || *a.head != (audit.Head{Seq: 7, Sha256: "cd"}) || a.unreadable != "" ||
		a.attempt != 2 || a.status != "ok" || a.resultSHA != want {
		t.Errorf("an anchored result: %+v, want sha256 %s", a, want)
	}
	if a := got[1]; a.activityID != "2:audit_v1" || a.head != nil || a.unreadable != "" || a.status != "denied" {
		t.Errorf("a result without one: %+v", a)
	}
	if a := got[2]; a.unreadable == "" {
		t.Errorf("an anchor that is not one: %+v", a)
	}
	if a := got[3]; a.unreadable == "" || !strings.Contains(a.unreadable, "not a gateway result") {
		t.Errorf("not a gateway result: %+v", a)
	}
}

// Each anchor must name the run's own record of that call, as the ledger holds it: of its activity,
// its attempt, its status and its value; and no other result of the execution names it too.
func TestAnchorsMustNameTheCallsRecord(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(ctx, filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	value := map[string]any{"blob": "sha256:ab"}
	valueSHA, _ := audit.ValueSHA256(value)
	record := func(wf, typ string, p map[string]any) audit.Head {
		h, err := audit.Record(ctx, l, audit.Event{WorkflowID: wf, Type: typ, Actor: audit.Run, Payload: p})
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	call := func(wf, activityID, status string, attempt int) audit.Head {
		p := map[string]any{"activity_id": activityID, "status": status, "attempt": attempt}
		if status != "error" {
			p["result_sha256"] = valueSHA
		}
		return record(wf, "call", p)
	}
	first := call("other", "1:x", "ok", 1) // the log began here
	began, _ := time.Parse(time.RFC3339Nano, ledger.Stamp(ledger.Clock().Add(-time.Second)))
	show, model := call("wf", "1:git.show_diff", "ok", 1), call("wf", "2:audit_v1", "ok", 1)
	settled := record("wf", "effect.settled", map[string]any{"effect_id": "e"})
	// Not a call's record, though it says the call's activity: only its type tells.
	impostor := record("wf", "effect.settled", map[string]any{"activity_id": "1:git.show_diff", "status": "ok", "attempt": 1,
		"result_sha256": valueSHA})
	failed := call("wf", "3:git.apply_check", "error", 1)
	denied := call("wf", "4:github.open_draft_pr", "denied", 1)
	second := call("wf", "5:github.comment_commit", "ok", 2)
	records, err := l.Q.ListRunAuditRecords(ctx, "wf")
	if err != nil {
		t.Fatal(err)
	}
	head := func(h audit.Head) *audit.Head { return &h }
	ok := func(activityID string, h audit.Head) anchor {
		return anchor{activityID: activityID, attempt: 1, status: "ok", resultSHA: valueSHA, head: head(h)}
	}
	after, before := began.Add(time.Hour), began.Add(-time.Hour) // when the execution started
	for name, tc := range map[string]struct {
		anchors []anchor
		started time.Time
		checked int64
		why     string
	}{
		"its own record":           {[]anchor{ok("1:git.show_diff", show)}, after, 1, ""},
		"another call's record":    {[]anchor{ok("1:git.show_diff", model)}, after, 1, `the call "2:audit_v1"'s`},
		"a record that is no call": {[]anchor{ok("1:git.show_diff", settled)}, after, 1, "is a effect.settled, not a call's"},
		"a record that says it is": {[]anchor{ok("1:git.show_diff", impostor)}, after, 1, "is a effect.settled, not a call's"},
		"another run's record":     {[]anchor{ok("1:x", first)}, after, 1, "no record of this run at seq 1"},
		"a record changed since":   {[]anchor{ok("2:audit_v1", audit.Head{Seq: model.Seq, Sha256: strings.Repeat("0", 64)})}, after, 1, "the result names 0000"},
		"a record gone":            {[]anchor{ok("2:audit_v1", audit.Head{Seq: 99, Sha256: model.Sha256})}, after, 1, "no record of this run at seq 99"},
		"an error's record":        {[]anchor{ok("3:git.apply_check", failed)}, after, 1, `ended "error"; the result is "ok"`},
		// A result made up to match an error's record: no result of agentd's says "error".
		"an error's record, an error's result": {[]anchor{{activityID: "3:git.apply_check", attempt: 1, status: "error",
			head: head(failed)}}, after, 1, `ended "error"; the result is "error"`},
		"another status":  {[]anchor{ok("4:github.open_draft_pr", denied)}, after, 1, `ended "denied"; the result is "ok"`},
		"another attempt": {[]anchor{ok("5:github.comment_commit", second)}, after, 1, "attempt 2's; attempt 1 completed"},
		"another value": {[]anchor{{activityID: "1:git.show_diff", attempt: 1, status: "ok", resultSHA: strings.Repeat("f", 64),
			head: head(show)}}, after, 1, "holds a result with sha256 " + valueSHA},
		"one record, two results": {[]anchor{ok("1:git.show_diff", show), ok("1:git.show_diff", show)}, after, 2,
			`named by the result of "1:git.show_diff" too`},
		"no anchor":              {[]anchor{{activityID: "2:audit_v1"}}, after, 0, "names no record"},
		"no anchor, from before": {[]anchor{{activityID: "2:audit_v1"}}, before, 0, ""},
		"unreadable":             {[]anchor{{activityID: "2:audit_v1", unreadable: "garbled"}}, before, 0, "garbled"},
	} {
		t.Run(name, func(t *testing.T) {
			n, bad := checkAnchors("wf", tc.anchors, records, began, tc.started)
			switch {
			case n != tc.checked:
				t.Errorf("%d checked, want %d", n, tc.checked)
			case tc.why == "" && len(bad) > 0:
				t.Errorf("mismatches: %+v", bad)
			case tc.why != "" && (len(bad) != 1 || !strings.Contains(bad[0].GetWhy(), tc.why) || bad[0].GetRunId() != "wf" ||
				bad[0].GetActivityId() != tc.anchors[len(tc.anchors)-1].activityID):
				t.Errorf("mismatches %+v, want one that says %q", bad, tc.why)
			}
		})
	}
	// No audit log at all: nothing says when it began, so nothing came before it, and a result
	// without an anchor is a mismatch as any other.
	if n, bad := checkAnchors("wf", []anchor{{activityID: "2:audit_v1"}}, nil, time.Time{}, before); n != 0 || len(bad) != 1 ||
		!strings.Contains(bad[0].GetWhy(), "names no record") {
		t.Errorf("%d, %+v", n, bad)
	}
	b, _ := json.Marshal(show)
	if string(b) != `{"seq":`+fmt.Sprint(show.Seq)+`,"sha256":"`+show.Sha256+`"}` {
		t.Errorf("an anchor's JSON: %s", b) // what Invoke puts in _audit, and anchorsOf reads
	}
}
