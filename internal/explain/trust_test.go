package explain

import (
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/converter"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/audit"
)

// incidentsOf are the kind@step of each incident whose cause says what.
func incidentsOf(e *agentosv1.Explanation, kind, says string) []*agentosv1.Incident {
	var out []*agentosv1.Incident
	for _, inc := range e.GetIncidents() {
		if inc.GetKind() == kind && strings.Contains(inc.GetCause(), says) {
			out = append(out, inc)
		}
	}
	return out
}

// A result in the history counts as agentd's only when it names agentd's record of it, by the
// anchor rules: anyone with access to Temporal's namespace can complete a call with a result of
// their own. One that does not is judged by agentd's record of the attempt, and listed as an
// incident, the result quoted as their claim.
func TestAResultAgentdDidNotSend(t *testing.T) {
	type forgery func(w *world, c *call, refused int64) string // the forged result's value_json
	for name, tc := range map[string]struct {
		forge forgery
		why   string
	}{
		"no anchor": {func(w *world, c *call, refused int64) string { return valueOf(w.t, nil, nil) },
			"names no record"},
		"another call's record": {func(w *world, c *call, refused int64) string {
			return valueOf(w.t, nil, &audit.Head{Seq: refused - 1, Sha256: sha(refused - 1)})
		}, "another call's record"},
		"a sha256 the record does not have": {func(w *world, c *call, refused int64) string {
			return valueOf(w.t, nil, &audit.Head{Seq: refused, Sha256: sha(refused + 1)})
		}, "sha256 the record there does not have"},
		"a record that is not a call's": {func(w *world, c *call, refused int64) string {
			return valueOf(w.t, nil, &audit.Head{Seq: 2, Sha256: sha(2)}) // run.created
		}, "a run.created record"},
		"a record of an execution agentd did not start": {func(w *world, c *call, refused int64) string {
			seq := w.rec("call", audit.UnknownRun, map[string]any{"activity_id": c.id, "attempt": 1, "kind": "model",
				"name": "audit_v1", "status": statusOK, "result_sha256": resultSHA(w.t, nil)})
			return valueOf(w.t, nil, &audit.Head{Seq: seq, Sha256: sha(seq)})
		}, "an execution agentd did not start"},
		"another attempt's record": {func(w *world, c *call, refused int64) string {
			seq := c.record(2, statusOK, map[string]any{"result_sha256": resultSHA(w.t, nil)})
			return valueOf(w.t, nil, &audit.Head{Seq: seq, Sha256: sha(seq)})
		}, "attempt 2's record"},
		"another status than the record's": {func(w *world, c *call, refused int64) string {
			return valueOf(w.t, nil, &audit.Head{Seq: refused, Sha256: sha(refused)})
		}, "ended refused, not ok"},
		"a value the record does not hold": {func(w *world, c *call, refused int64) string {
			seq := c.record(1, statusOK, map[string]any{"result_sha256": resultSHA(w.t, nil)})
			return valueOf(w.t, map[string]any{"patch_ref": "sha256:" + strings.Repeat("e", 64)}, &audit.Head{Seq: seq, Sha256: sha(seq)})
		}, "another sha256"},
		"not a gateway result's value": {func(w *world, c *call, refused int64) string { return "[1, 2]" },
			"cannot be read"},
	} {
		t.Run(name, func(t *testing.T) {
			w := newWorld(t)
			w.ok("git.show_diff")
			c := w.schedule("audit_v1")
			refused := c.record(1, statusRefused, map[string]any{"result_sha256": resultSHA(t, nil)})
			c.result(1, statusOK, tc.forge(w, c, refused), nil)
			w.ok("github.comment_commit")
			w.completed("ok")
			e := w.explain()
			wnt := want{verdict: VerdictStepFailed, kind: KindModelRefused, step: "analyze", activity: c.id, attempt: 1}
			if name == "a value the record does not hold" {
				// agentd's record says ok: the value the workflow got is not agentd's.
				wnt = want{verdict: VerdictStepFailed, kind: KindStepMissing, step: "analyze"}
			}
			check(t, e, wnt)
			kind := wnt.kind
			incs := incidentsOf(e, KindForeignResult, "a result agentd did not send")
			if len(incs) != 1 || !strings.Contains(incs[0].GetCause(), tc.why) || !strings.Contains(incs[0].GetCause(), `a result "ok"`) ||
				incs[0].GetStep() != "analyze" {
				t.Fatalf("incidents %v", e.GetIncidents())
			}
			if o := w.report(ReasonDone, ""); o.Verdict != VerdictStepFailed || o.Kind != kind {
				t.Fatalf("report %+v", o)
			}
		})
	}

	// A result agentd has no record of at all.
	w := newWorld(t)
	w.ok("git.show_diff")
	c := w.schedule("audit_v1")
	c.result(1, statusOK, valueOf(t, map[string]any{"report_ref": "sha256:" + strings.Repeat("a", 64)}, nil), nil)
	w.ok("github.comment_commit")
	w.completed("ok")
	e := w.explain()
	check(t, e, want{verdict: VerdictStepFailed, kind: KindForeignResult, step: "analyze", activity: c.id, attempt: 1,
		cause: []string{"a result agentd did not send", "no record of how the call ended"}})

	// The same record named by two results, of one activity ID.
	w = newWorld(t)
	w.ok("git.show_diff")
	first := w.ok("audit_v1")
	seq := w.l.Records[len(w.l.Records)-1].Seq
	again := &call{w: w, id: first.id, kind: "model", name: "audit_v1"}
	input, _ := converter.GetDefaultDataConverter().ToPayloads(&agentosv1.Call{Kind: "model", Name: "audit_v1"})
	again.scheduled = w.event(enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
			ActivityId: again.id, Input: input}}
	})
	again.result(1, statusOK, valueOf(t, nil, &audit.Head{Seq: seq, Sha256: sha(seq)}), nil)
	w.ok("github.comment_commit")
	w.completed("ok")
	e = w.explain()
	check(t, e, want{verdict: VerdictStepFailed, kind: KindForeignResult, step: "analyze", activity: first.id, attempt: 1,
		cause: []string{"names too"}})

	// A result from before the audit log names no record, and counts.
	w = newWorld(t)
	w.ok("git.show_diff")
	w.ok("audit_v1")
	w.ok("github.comment_commit")
	w.completed("ok")
	w.stripAnchors()
	w.l.Records, w.l.LogBegan = nil, w.clock.Add(1)
	if e := w.explain(); e.GetVerdict() != VerdictOK || len(incidentsOf(e, KindForeignResult, "")) != 0 {
		t.Fatalf("%v", e)
	}
}

// The record a result names is read on its own, whatever the bound on the run's records: one past
// it is checked as any other; a seq where the audit log has no record, or another run's record,
// makes the result one agentd did not send; a read of it that fails leaves the verdict unknown, never
// ok, the report saying only where the error is.
func TestAResultsRecordIsReadOnItsOwn(t *testing.T) {
	cut := []string{"ledger: the run has more than 4 audit records; the explanation reads the first 4"}
	w := newWorld(t)
	w.ok("git.show_diff")
	w.ok("audit_v1")
	w.ok("github.comment_commit")
	w.completed("ok")
	w.l.Records = w.l.Records[:len(w.l.Records)-2] // audit_v1's record and the report's are past the bound
	w.l.RecordsCut, w.l.Cut = true, cut
	if e := w.explain(); e.GetVerdict() != VerdictOK || len(incidentsOf(e, KindForeignResult, "")) != 0 ||
		!slices.Contains(e.GetMissing(), cut[0]) {
		t.Fatalf("%v", e)
	}

	for name, forge := range map[string]func(w *world, c *call) (*audit.Head, string){
		"past the chain's head": func(w *world, c *call) (*audit.Head, string) {
			return &audit.Head{Seq: 9999, Sha256: sha(9999)}, "where the audit log has no record"
		},
		"another run's record": func(w *world, c *call) (*audit.Head, string) {
			seq := w.other("run-2", "call", map[string]any{"activity_id": c.id, "attempt": 1, "kind": "model",
				"name": "audit_v1", "status": statusOK, "result_sha256": resultSHA(w.t, nil)})
			return &audit.Head{Seq: seq, Sha256: sha(seq)}, "another run's record"
		},
	} {
		t.Run(name, func(t *testing.T) {
			w := newWorld(t)
			w.ok("git.show_diff")
			c := w.schedule("audit_v1")
			c.record(1, statusRefused, map[string]any{"result_sha256": resultSHA(t, nil)})
			anchor, why := forge(w, c)
			c.result(1, statusOK, valueOf(t, nil, anchor), nil)
			w.ok("github.comment_commit")
			w.completed("ok")
			e := w.explain()
			check(t, e, want{verdict: VerdictStepFailed, kind: KindModelRefused, step: "analyze", activity: c.id, attempt: 1})
			if incs := incidentsOf(e, KindForeignResult, why); len(incs) != 1 {
				t.Fatalf("%v", e.GetIncidents())
			}
		})
	}

	// The read fails: how the call ended is not known, and nor is how the run went.
	w = newWorld(t)
	w.ok("git.show_diff")
	c := w.ok("audit_v1")
	seq := w.l.Records[len(w.l.Records)-1].Seq
	r := w.schedule("github.comment_commit")
	w.l.Records = w.l.Records[:len(w.l.Records)-1]
	w.l.RecordsCut, w.l.Cut = true, cut
	w.namedErr[seq] = "disk I/O error at /var/agentos/ledger.db"
	o := w.report(ReasonDone, "")
	if o.Verdict != VerdictUnknown || o.Kind != KindRecordUnreadable || o.Step != "analyze" ||
		strings.Contains(o.Cause, "/var/agentos") || !strings.Contains(o.Cause, "agentd's log has the details") {
		t.Fatalf("report %+v", o)
	}
	r.completes(1, statusOK, nil, nil, nil)
	w.completed("ok")
	check(t, w.explain(), want{verdict: VerdictUnknown, kind: KindRecordUnreadable, step: "analyze", activity: c.id, attempt: 1,
		cause: []string{"could not read its own record of how analyze ended", "/var/agentos/ledger.db"}})

	// A step that changed the run's course before it decides the verdict: what came after did not.
	w = newWorld(t)
	w.ok("git.show_diff")
	refused := w.returns("audit_v1", statusRefused, nil, nil)
	w.ok("github.comment_commit")
	w.completed("refused")
	seq = w.l.Records[len(w.l.Records)-1].Seq
	w.l.Records = w.l.Records[:len(w.l.Records)-1]
	w.l.RecordsCut, w.l.Cut = true, cut
	w.namedErr[seq] = "disk I/O error"
	check(t, w.explain(), want{verdict: VerdictStepFailed, kind: KindModelRefused, step: "analyze", activity: refused.id, attempt: 1})
}

// The security re-review's flood: executions agentd did not start, under the run's workflow ID, write
// a record each until the run's records are cut; then someone in Temporal's namespace completes the
// model's call with ok, naming a seq the chain never had. The result is not agentd's, the records cut
// or not, and neither runs explain nor the report says the run ended well.
func TestAForgedAnchorPastTheCut(t *testing.T) {
	forged := func(t *testing.T, agentdRecord bool) *world {
		w := newWorld(t)
		w.ok("git.show_diff")
		m := w.schedule("audit_v1")
		if agentdRecord {
			m.record(1, statusRefused, map[string]any{"reason": "the model declined", "result_sha256": resultSHA(t, nil)})
		}
		m.result(1, statusOK, valueOf(t, map[string]any{"report_ref": "sha256:" + sha(1)}, &audit.Head{Seq: 1 << 40, Sha256: "forged"}),
			nil)
		return w
	}
	for _, cut := range []bool{false, true} {
		w := forged(t, true)
		w.ok("github.comment_commit")
		w.completed("ok")
		if cut {
			w.l.RecordsCut, w.l.Cut = true, []string{"ledger: the run has more than 5000 audit records; the explanation reads the first 5000"}
		}
		e := w.explain()
		check(t, e, want{verdict: VerdictStepFailed, kind: KindModelRefused, step: "analyze", activity: "2:audit_v1", attempt: 1})
		if len(incidentsOf(e, KindForeignResult, "where the audit log has no record")) != 1 {
			t.Fatalf("cut %v: %v", cut, e.GetIncidents())
		}
	}
	// The report, on an analysis agentd never ran.
	w := forged(t, false)
	w.schedule("github.comment_commit")
	w.l.RecordsCut, w.l.Cut = true, []string{"ledger: the run has more than 5000 audit records; the explanation reads the first 5000"}
	if o := w.report(ReasonDone, ""); o.Verdict == VerdictOK || o.Kind != KindForeignResult || o.Step != "analyze" {
		t.Fatalf("%+v", o)
	}
}

// A report shows no internal error's text: an error of agentd's own, its model adapter's or the git
// tool server's says only that agentd's log has it, and so does a timeout beyond Temporal's word for
// it; a tool's own words, refusing the call for good, stay, fenced. runs explain, for the operator,
// says it all.
func TestTheReportShowsNoInternalError(t *testing.T) {
	internal := "Post \"https://api.anthropic.com/v1/messages\": dial tcp 10.1.2.3:443: connect: connection refused"
	for _, tc := range []struct {
		name   string
		end    func(w *world) *call
		report []string // in the report's cause
		hidden string   // not in it, but in runs explain's
	}{
		{"a model adapter's error", func(w *world) *call {
			c := w.schedule("audit_v1")
			c.failsForGood(1, internal, "", nil)
			return c
		}, []string{"analyze failed; agentd's log has the details"}, "10.1.2.3"},
		{"the git tool server's failure", func(w *world) *call {
			c := w.schedule("git.show_diff")
			c.record(1, statusError, map[string]any{"error": serverError, "error_cause": audit.ErrorCauseToolServer, "error_type": ""})
			c.end(1, nil, func(e *historypb.HistoryEvent, started int64) {
				e.EventType = enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED
				e.Attributes = &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{ActivityTaskFailedEventAttributes: &historypb.ActivityTaskFailedEventAttributes{
					ScheduledEventId: c.scheduled, StartedEventId: started, Failure: appFailure(serverError, "", false)}}
			})
			return c
		}, []string{"the git tool server failed; agentd's log has the details"}, "show_diff; agentd's log"},
		{"a tool's own words", func(w *world) *call {
			c := w.schedule("git.show_diff")
			c.failsForGood(1, "tool git.show_diff: fatal: bad object deadbeef", "ToolFailed", nil)
			return c
		}, []string{`fetch_diff failed: "tool git.show_diff: fatal: bad object deadbeef"`}, ""},
		{"a tool's own words, as Temporal has them", func(w *world) *call {
			c := w.schedule("git.show_diff")
			c.end(1, nil, func(e *historypb.HistoryEvent, started int64) {
				e.EventType = enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED
				e.Attributes = &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{ActivityTaskFailedEventAttributes: &historypb.ActivityTaskFailedEventAttributes{
					ScheduledEventId: c.scheduled, StartedEventId: started,
					Failure: appFailure("tool git.show_diff: fatal: bad object deadbeef", "ToolFailed", true)}}
			})
			// agentd stopped during the attempt, before its record of it.
			failedAt := w.failedAt(w.t)
			w.start("agentd.started", audit.Agentd, nil)
			w.l.Starts[len(w.l.Starts)-1].At = failedAt.Add(-500 * time.Millisecond).Format(time.RFC3339Nano)
			return c
		}, []string{"fetch_diff failed: ", "bad object deadbeef"}, ""},
		{"a failure with no record of agentd's", func(w *world) *call {
			c := w.schedule("audit_v1")
			c.end(5, appFailure("x", "PanicError", false), func(e *historypb.HistoryEvent, started int64) {
				e.EventType = enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED
				e.Attributes = &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{ActivityTaskFailedEventAttributes: &historypb.ActivityTaskFailedEventAttributes{
					ScheduledEventId: c.scheduled, StartedEventId: started, Failure: appFailure(internal, "PanicError", false)}}
			})
			return c
		}, []string{`analyze failed after 5 attempts, by a failure with no record of agentd's; it says "PanicError"`}, "10.1.2.3"},
		{"a timeout", func(w *world) *call {
			c := w.schedule("git.show_diff")
			c.end(1, nil, func(e *historypb.HistoryEvent, started int64) {
				e.EventType = enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT
				e.Attributes = &historypb.HistoryEvent_ActivityTaskTimedOutEventAttributes{ActivityTaskTimedOutEventAttributes: &historypb.ActivityTaskTimedOutEventAttributes{
					ScheduledEventId: c.scheduled, StartedEventId: started, Failure: timeoutFailure(enumspb.TIMEOUT_TYPE_HEARTBEAT)}}
			})
			return c
		}, []string{"fetch_diff timed out (Temporal's Heartbeat timeout)"}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			tc.end(w)
			r := w.schedule("github.comment_commit")
			o := w.report(ReasonDone, "")
			r.completes(1, statusOK, nil, nil, nil)
			w.completed("failed")
			for _, says := range tc.report {
				if !strings.Contains(o.Cause, says) {
					t.Errorf("report cause %q, without %q", o.Cause, says)
				}
			}
			if tc.hidden != "" && strings.Contains(o.Cause, tc.hidden) {
				t.Errorf("report cause %q", o.Cause)
			}
			if e := w.explain(); tc.hidden != "" && !strings.Contains(e.GetCause(), tc.hidden) {
				t.Errorf("explain cause %q", e.GetCause())
			}
		})
	}
	// Temporal that cannot be read: the report says only where the error is, and keeps it for the log.
	w := newWorld(t)
	w.ok("git.show_diff")
	w.schedule("github.comment_commit")
	w.unread = "rpc error: code = Unavailable desc = dial tcp temporal.internal:7233: i/o timeout"
	if o := w.report(ReasonDone, ""); o.Cause != "agentd could not read the run in Temporal; agentd's log has the details" ||
		o.Detail != w.unread {
		t.Fatalf("%+v", o)
	}
}

// A history longer than agentd reads leaves how the run went unknown, never ok: the calls past it may
// be where its course changed. Other evidence past agentd's bounds does not change the verdict, and
// the report names it.
func TestWhatABoundLeavesOut(t *testing.T) {
	w := newWorld(t)
	w.ok("git.show_diff")
	w.ok("audit_v1")
	w.schedule("github.comment_commit")
	w.l.CutWhat = []string{"model requests past the first 2000"}
	if o := w.report(ReasonDone, ""); o.Verdict != VerdictOK || len(o.Cut) != 1 || o.Cut[0] != "model requests past the first 2000" {
		t.Fatalf("%+v", o)
	}
	w = newWorld(t)
	w.ok("git.show_diff")
	w.ok("audit_v1")
	for len(w.events) <= maxEvents {
		w.task()
	}
	w.schedule("github.comment_commit")
	if o := w.report(ReasonDone, ""); o.Verdict != VerdictUnknown || o.Kind != KindHistoryUnavailable ||
		!strings.Contains(o.Cause, "longer than agentd reads") {
		t.Fatalf("%+v", o)
	}
}

// A failure in the history is agentd's only when agentd's record of the attempt says it ended with
// an error of the failure's type: anyone with access to Temporal's namespace can fail a call. One it
// does not bear out is judged by the record, and listed as an incident, the failure quoted as their
// claim. With no record, agentd stopping during the attempt, or an execution older than the audit
// log, explains it.
func TestAFailureAgentdDidNotSend(t *testing.T) {
	fail := func(c *call, attempt int32, message, typ string) {
		c.end(attempt, nil, func(e *historypb.HistoryEvent, started int64) {
			e.EventType = enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED
			e.Attributes = &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{ActivityTaskFailedEventAttributes: &historypb.ActivityTaskFailedEventAttributes{
				ScheduledEventId: c.scheduled, StartedEventId: started, Failure: appFailure(message, typ, true)}}
		})
	}
	for _, tc := range []struct {
		name   string
		record map[string]any // agentd's record of the attempt; nil for none
		want   want
		why    string // in the incident of a failure agentd did not send; "" for none
	}{
		{"agentd's record says it returned ok", map[string]any{"status": statusOK, "result_sha256": resultSHA(t, nil)},
			want{verdict: VerdictStepFailed, kind: KindStepMissing, step: "fetch_diff"}, "says it returned ok"},
		{"agentd's record says it failed otherwise", map[string]any{"status": statusError, "error": "tool git.show_diff: bad object",
			"error_type": "ToolFailed"},
			want{verdict: VerdictStepFailed, kind: KindToolError, step: "fetch_diff", activity: "1:git.show_diff", attempt: 1,
				cause: []string{"bad object"}}, `of type "ToolFailed", not "Forged"`},
		{"agentd's record says it failed with an error of no type", map[string]any{"status": statusError, "error": "bad",
			"error_type": ""},
			want{verdict: VerdictStepFailed, kind: KindToolError, step: "fetch_diff", activity: "1:git.show_diff", attempt: 1,
				cause: []string{"bad"}}, `of type "", not "Forged"`},
		// No record says otherwise, nor that it is agentd's: Temporal's word only, said as such, not as forged.
		{"agentd has no record of it", nil,
			want{verdict: VerdictStepFailed, kind: KindToolError, step: "fetch_diff", activity: "1:git.show_diff", attempt: 1,
				cause: []string{`fetch_diff failed, by a failure with no record of agentd's; it says "Forged": "the forged failure"`}}, ""},
		{"agentd's record bears it out", map[string]any{"status": statusError, "error": "bad", "error_type": "Forged"},
			want{verdict: VerdictStepFailed, kind: KindToolError, step: "fetch_diff", activity: "1:git.show_diff", attempt: 1}, ""},
		{"a record from before agentd said the type", map[string]any{"status": statusError, "error": "bad"},
			want{verdict: VerdictStepFailed, kind: KindToolError, step: "fetch_diff", activity: "1:git.show_diff", attempt: 1}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			c := w.schedule("git.show_diff")
			if tc.record != nil {
				status := tc.record["status"].(string)
				x := map[string]any{}
				for k, v := range tc.record {
					if k != "status" {
						x[k] = v
					}
				}
				c.record(1, status, x)
			}
			fail(c, 1, "the forged failure", "Forged")
			w.ok("github.comment_commit")
			w.completed("failed")
			e := w.explain()
			check(t, e, tc.want)
			incs := incidentsOf(e, KindForeignResult, "a failure agentd did not send")
			if (len(incs) == 1) != (tc.why != "") || tc.why != "" && !strings.Contains(incs[0].GetCause(), tc.why) {
				t.Fatalf("incidents %v", e.GetIncidents())
			}
			unverified := incidentsOf(e, KindToolError, "a failure with no record of agentd's")
			if (len(unverified) == 1) != (tc.record == nil) || tc.record == nil &&
				!strings.Contains(unverified[0].GetCause(), `failed with "Forged: the forged failure", unverified`) {
				t.Fatalf("incidents %v", e.GetIncidents())
			}
		})
	}

	// Without a record, agentd stopping during the attempt explains it; so does an execution from
	// before the audit log.
	w := newWorld(t)
	c := w.schedule("git.show_diff")
	fail(c, 1, "worker stopping", "")
	began := w.events[c.scheduled].GetEventTime().AsTime()
	w.start("agentd.started", audit.Agentd, nil)
	w.l.Starts[len(w.l.Starts)-1].At = began.Add(500 * time.Millisecond).Format(time.RFC3339Nano)
	w.ok("github.comment_commit")
	w.completed("failed")
	if e := w.explain(); len(incidentsOf(e, KindForeignResult, "")) != 0 || e.GetKind() != KindToolError {
		t.Fatalf("%v", e)
	}
	// So does a start a moment after the failure (agentd's clock is not Temporal's); one well after does
	// not, and the failure is Temporal's word only.
	for _, tc := range []struct {
		after      time.Duration
		unverified bool
	}{{500 * time.Millisecond, false}, {1500 * time.Millisecond, true}} {
		w := newWorld(t)
		c := w.schedule("git.show_diff")
		fail(c, 1, "worker stopping", "")
		failedAt := w.failedAt(t)
		w.start("agentd.started", audit.Agentd, nil)
		w.l.Starts[len(w.l.Starts)-1].At = failedAt.Add(tc.after).Format(time.RFC3339Nano)
		w.ok("github.comment_commit")
		w.completed("failed")
		e := w.explain()
		if len(incidentsOf(e, KindForeignResult, "")) != 0 || strings.Contains(e.GetCause(), "no record of agentd's") != tc.unverified {
			t.Fatalf("a start %s after the failure: %v", tc.after, e)
		}
	}
	w = newWorld(t)
	c = w.schedule("git.show_diff")
	fail(c, 1, "boom", "")
	w.ok("github.comment_commit")
	w.completed("failed")
	w.stripAnchors()
	w.l.Records, w.l.LogBegan = nil, w.clock.Add(1)
	if e := w.explain(); len(incidentsOf(e, KindForeignResult, "")) != 0 || e.GetKind() != KindToolError {
		t.Fatalf("%v", e)
	}

	// Past the bound on the run's records, agentd's record of the attempt is read on its own; a read of
	// it that fails leaves the verdict unknown.
	w = newWorld(t)
	c = w.schedule("git.show_diff")
	c.record(1, statusError, map[string]any{"error": "bad", "error_type": "Forged"})
	fail(c, 1, "bad", "Forged")
	w.ok("github.comment_commit")
	w.completed("failed")
	w.l.Records, w.l.RecordsCut = w.l.Records[:2], true
	if e := w.explain(); len(incidentsOf(e, KindForeignResult, "")) != 0 || e.GetKind() != KindToolError {
		t.Fatalf("%v", e)
	}
	w.attemptErr[Attempt{ActivityID: c.id, Attempt: 1}] = "database is locked"
	check(t, w.explain(), want{verdict: VerdictUnknown, kind: KindRecordUnreadable, step: "fetch_diff", activity: c.id, attempt: 1,
		cause: []string{"database is locked"}})

	// Its record past the bound on the call records read after the run's own: not known either, and
	// for a bound, not an error, which runs explain and the report both name.
	w.attemptErr[Attempt{ActivityID: c.id, Attempt: 1}] = "past agentd's bound on the run's records it reads"
	w.l.AttemptsCut = true
	check(t, w.explain(), want{verdict: VerdictUnknown, kind: KindRecordUnreadable, step: "fetch_diff", activity: c.id, attempt: 1,
		cause: []string{"own record of how fetch_diff ended is past its bound on the run's records it reads"}})
	w = newWorld(t)
	c = w.schedule("git.show_diff")
	c.record(1, statusError, map[string]any{"error": "bad", "error_type": "Forged"})
	fail(c, 1, "bad", "Forged")
	w.schedule("github.comment_commit")
	w.l.Records, w.l.RecordsCut, w.l.AttemptsCut = w.l.Records[:2], true, true
	w.attemptErr[Attempt{ActivityID: c.id, Attempt: 1}] = "past agentd's bound on the run's records it reads"
	if o := w.report(ReasonDone, ""); o.Verdict != VerdictUnknown || o.Kind != KindRecordUnreadable ||
		!strings.Contains(o.Cause, "past its bound") || strings.Contains(o.Cause, inTheLog) {
		t.Fatalf("report %+v", o)
	}
}

// agentd refuses a call from an execution it does not know yet as the run's (StartRun has not
// recorded its ID): that record is by unknown-run, with the execution's ID. One of the run's own
// execution bears out its failure; one of another execution under the run's workflow ID is not the
// run's record, and the failure has none.
func TestAFailureAgentdSentBeforeItKnewTheExecution(t *testing.T) {
	for _, tc := range []struct {
		execution  string
		unverified bool
	}{{"exec-1", false}, {"exec-2", true}, {"", true}} {
		t.Run(tc.execution, func(t *testing.T) {
			w := newWorld(t)
			c := w.schedule("git.show_diff")
			p := map[string]any{"activity_id": c.id, "attempt": 1, "kind": c.kind, "name": c.name, "status": statusError,
				"error": "run registration in progress", "error_type": ""}
			if tc.execution != "" {
				p["temporal_run_id"] = tc.execution
			}
			w.rec("call", audit.UnknownRun, p)
			c.end(1, nil, func(e *historypb.HistoryEvent, started int64) {
				e.EventType = enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED
				e.Attributes = &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{ActivityTaskFailedEventAttributes: &historypb.ActivityTaskFailedEventAttributes{
					ScheduledEventId: c.scheduled, StartedEventId: started, Failure: appFailure("run registration in progress", "", true)}}
			})
			w.ok("github.comment_commit")
			w.completed("failed")
			e := w.explain()
			if tc.unverified {
				check(t, e, want{verdict: VerdictStepFailed, kind: KindToolError, step: "fetch_diff", activity: c.id, attempt: 1,
					cause: []string{"by a failure with no record of agentd's; it says no type"}})
				return
			}
			check(t, e, want{verdict: VerdictStepFailed, kind: KindToolError, step: "fetch_diff", activity: c.id, attempt: 1,
				cause: []string{"run registration in progress"}})
			if incs := incidentsOf(e, KindForeignResult, ""); len(incs) != 0 {
				t.Fatalf("incidents %v", incs)
			}
		})
	}
}

// A run that ends without making its report call has not reported, whatever it returned.
func TestARunThatNeverReports(t *testing.T) {
	w := newWorld(t)
	w.ok("git.show_diff")
	c := w.returns("audit_v1", statusRefused, nil, nil)
	w.completed("ok")
	e := w.explain()
	check(t, e, want{verdict: VerdictStepFailed, kind: KindReportNotPosted, step: "post_report",
		cause: []string{"made no call of post_report"}, incidents: []string{"model_refused@analyze"}})
	if inc := e.GetIncidents()[0]; inc.GetActivityId() != c.id || !strings.Contains(inc.GetCause(), "before that") {
		t.Fatalf("%v", inc)
	}
}

// A report that did not go out, sent again: the last is the run's report, the first an incident.
func TestAReportSentAgain(t *testing.T) {
	w := newWorld(t)
	w.ok("git.show_diff")
	c := w.ok("audit_v1")
	first := w.returns("github.comment_commit", statusDenied, map[string]any{"reason": "GitHub said no",
		"refused_by": audit.RefusedByTool}, nil)
	w.ok("github.comment_commit")
	w.failed("the workflow raised KeyError", "KeyError")
	e := w.explain()
	check(t, e, want{verdict: VerdictFailed, kind: KindWorkflowError, step: "analyze", activity: c.id, attempt: 1,
		cause: []string{"own code failed after analyze returned ok", "post_report went out"}})
	if incs := incidentsOf(e, KindReportNotPosted, "sent again"); len(incs) != 1 || incs[0].GetActivityId() != first.id {
		t.Fatalf("%v", e.GetIncidents())
	}
}

// Who refused a call picks its kind, as agentd's record says (refused_by), never the words of the
// refusal, which a tool may write; a record from before agentd said so says only the policy's
// decision.
func TestWhoRefusedPicksTheKind(t *testing.T) {
	for _, tc := range []struct {
		name   string
		record map[string]any
		kind   string
		says   string
	}{
		{"a tool's words of the loop guard", map[string]any{"reason": "loop guard: more than 100 calls in one run",
			"refused_by": audit.RefusedByTool}, KindToolDenied, `validate_patch refused the call as it stood: "loop guard`},
		{"a tool's words of a cancel", map[string]any{"reason": "run cancelled", "refused_by": audit.RefusedByTool},
			KindToolDenied, `"run cancelled"`},
		{"the loop guard", map[string]any{"reason": "loop guard: 101 calls", "refused_by": audit.RefusedByLoopGuard},
			KindLoopGuard, "agentd's loop guard refused validate_patch"},
		{"a cancel", map[string]any{"reason": "run cancelled", "refused_by": audit.RefusedByCancel}, KindCancelled,
			"the run was cancelled"},
		{"the policy", map[string]any{"reason": "denied by deny_paths", "refused_by": audit.RefusedByPolicy,
			"policy": map[string]any{"decision": "deny", "reason": "denied by deny_paths"}}, KindPolicyDenied,
			`the policy denied validate_patch: "denied by deny_paths"`},
		{"the policy's limit on a run's writes", map[string]any{"reason": "more than 1 effects of git.apply_check in one run",
			"refused_by": audit.RefusedByPolicy, "policy": map[string]any{"decision": "allow"}}, KindPolicyDenied,
			`the policy denied validate_patch: "more than 1 effects`},
		{"agentd", map[string]any{"reason": "no such tool", "refused_by": audit.RefusedByAgentd}, KindDenied,
			`agentd refused validate_patch: "no such tool"`},
		{"an older record, the policy's decision", map[string]any{"reason": "no",
			"policy": map[string]any{"decision": "deny", "reason": "not in the policy"}}, KindPolicyDenied, `"not in the policy"`},
		{"an older record, nothing said", map[string]any{"reason": "loop guard: 101 calls", "policy": map[string]any{}},
			KindDenied, "does not say who refused it"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			w.patched()
			c := w.returns("git.apply_check", statusDenied, tc.record, nil)
			w.ok("github.comment_commit")
			w.completed("patch rejected")
			check(t, w.explain(), want{verdict: VerdictStepFailed, kind: tc.kind, step: "validate_patch", activity: c.id,
				attempt: 1, cause: []string{tc.says}})
		})
	}
}

// Where an error came from is agentd's word for it (error_cause, as its git client types the
// error), not the words of the error.
func TestWhereAnErrorCameFrom(t *testing.T) {
	for _, tc := range []struct {
		name  string
		extra map[string]any
		kind  string
	}{
		{"the git tool server", map[string]any{"error_cause": audit.ErrorCauseToolServer}, KindToolServerFailed},
		{"words that say so", nil, KindToolError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			c := w.schedule("git.show_diff")
			x := map[string]any{"error": serverError}
			for k, v := range tc.extra {
				x[k] = v
			}
			c.record(1, statusError, x)
			c.completes(2, statusOK, nil, nil, appFailure(serverError, "", false))
			w.ok("audit_v1")
			w.ok("github.comment_commit")
			w.completed("ok")
			check(t, w.explain(), want{verdict: VerdictOK, incidents: []string{tc.kind + "@fetch_diff"}})
		})
	}
}

// A step is the task's by its call's kind and name (D12), never by its activity ID, which the
// workflow chooses: any other call shows as its kind and name, quoted, and the report does not name
// it outside its fence.
func TestStepsAreTheTasksByKindAndName(t *testing.T) {
	for _, tc := range []struct{ id, kind, name, step string }{
		{"analyze", "tool", "", `"tool"`},
		{"post_report", "tool", "", `"tool"`},
		{"2:audit_v1", "tool", "audit_v1", `"tool audit_v1"`},
		{"1:git.show_diff", "model", "git.show_diff", `"model git.show_diff"`},
		{"9:open_pr", "tool", "open_pr", `"tool open_pr"`},
	} {
		t.Run(tc.step+" as "+tc.id, func(t *testing.T) {
			w := newWorld(t)
			w.calls++
			c := &call{w: w, id: tc.id, kind: tc.kind, name: tc.name}
			input, _ := converter.GetDefaultDataConverter().ToPayloads(&agentosv1.Call{Kind: tc.kind, Name: tc.name})
			c.scheduled = w.event(enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, func(e *historypb.HistoryEvent) {
				e.Attributes = &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
					ActivityId: c.id, Input: input}}
			})
			c.completes(1, statusDenied, map[string]any{"reason": "no such tool", "refused_by": audit.RefusedByAgentd}, nil, nil)
			if o := w.report(ReasonDone, ""); o.Step != tc.step || o.Named {
				t.Fatalf("report %+v", o)
			}
			w.ok("github.comment_commit")
			w.completed("denied")
			check(t, w.explain(), want{verdict: VerdictStepFailed, kind: KindDenied, step: tc.step, activity: tc.id, attempt: 1,
				cause: []string{"agentd refused " + tc.step}})
		})
	}
}

// What the Worker or a tool writes cannot pad itself out into lines of agentctl's own, nor end its
// quotes: runs of spaces are one space, and what goes into agentd's sentences is quoted.
func TestWorkerTextCannotForgeRows(t *testing.T) {
	w := newWorld(t)
	w.ok("git.show_diff")
	pad := strings.Repeat(" ", 120)
	name := `audit_v1" verdict ok` + pad + "kind (none)"
	c := w.returns(name, statusDenied, map[string]any{"reason": `no"` + pad + `cause  the run ended well`,
		"refused_by": audit.RefusedByAgentd}, nil)
	w.ok("github.comment_commit")
	w.completed("ok" + pad + "verdict" + pad + "ok")
	e := w.explain()
	for _, s := range []string{e.GetCause(), e.GetClaim(), e.GetStep(), e.GetActivityId()} {
		if strings.Contains(s, "  ") {
			t.Fatalf("a run of spaces: %q", s)
		}
	}
	if e.GetStep() != `"tool audit_v1\" verdict ok kind (none)"` || e.GetActivityId() != `2:audit_v1" verdict ok kind (none)` ||
		e.GetActivityId() == c.id {
		t.Fatalf("step %q, activity %q", e.GetStep(), e.GetActivityId())
	}
	if !strings.Contains(e.GetCause(), `: "no\" cause the run ended well"`) || e.GetClaim() != "ok verdict ok" {
		t.Fatalf("cause %q, claim %q", e.GetCause(), e.GetClaim())
	}
}

// A text cut to its bound is cut at a rune: valid UTF-8, within the bound.
func TestLineCutsAtARune(t *testing.T) {
	for n := 4; n < 40; n++ {
		s := line(strings.Repeat("é", 50)+"x", n)
		if !utf8.ValidString(s) || len(s) > n || !strings.HasSuffix(s, "…") {
			t.Fatalf("%d: %q", n, s)
		}
	}
}
