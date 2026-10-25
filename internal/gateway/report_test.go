package gateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"maps"
	"regexp"
	"strings"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/timestamppb"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/audit"
	"github.com/jchen0426/agentos/internal/faults"
	"github.com/jchen0426/agentos/internal/githubtool"
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
	"github.com/jchen0426/agentos/internal/models"
	"github.com/jchen0426/agentos/internal/tasks"
	"github.com/jchen0426/agentos/internal/tools"
)

// at appends an event of the history at a time of its own, base and its ID's seconds after: two runs
// built alike have the same times, whenever they ran.
func (h *fakeHistory) at(base time.Time, typ enumspb.EventType, set func(e *historypb.HistoryEvent)) int64 {
	id := int64(len(h.events)) + 1
	e := &historypb.HistoryEvent{EventId: id, EventType: typ, EventTime: timestamppb.New(base.Add(time.Duration(id) * time.Second))}
	if set != nil {
		set(e)
	}
	h.events = append(h.events, e)
	return id
}

// scheduled is the workflow's call c under activity ID id, scheduled and started: the history's input
// is what the workflow wrote. It returns the two events.
func (h *fakeHistory) scheduled(t *testing.T, base time.Time, id string, c *agentosv1.Call) (int64, int64) {
	t.Helper()
	input, err := converter.GetDefaultDataConverter().ToPayloads(c)
	if err != nil {
		t.Fatal(err)
	}
	if len(h.events) == 0 {
		h.at(base, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, nil)
	}
	s := h.at(base, enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
			ActivityId: id, Input: input}}
	})
	started := h.at(base, enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
			ScheduledEventId: s, Attempt: 1}}
	})
	return s, started
}

// failed ends the call scheduled at s, started at started, with the failure fl.
func (h *fakeHistory) failed(base time.Time, s, started int64, fl *failurepb.Failure) {
	h.at(base, enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{ActivityTaskFailedEventAttributes: &historypb.ActivityTaskFailedEventAttributes{
			ScheduledEventId: s, StartedEventId: started, Failure: fl}}
	})
}

// through is c's call made through the gateway, and its history: completed with the result the
// gateway returned, or failed as the gateway's record of its error says Temporal has it.
func (h *fakeHistory) through(t *testing.T, base time.Time, g *Gateway, l *ledger.Ledger, c *agentosv1.Call) {
	t.Helper()
	res, err := invoke(g, c)
	p := payloadOf(t, lastRecord(t, l))
	s, started := h.scheduled(t, base, p["activity_id"].(string), c)
	if err != nil {
		typ, _ := p["error_type"].(string)
		h.failed(base, s, started, &failurepb.Failure{Message: err.Error(), FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: typ, NonRetryable: true}}})
		return
	}
	result, err := converter.GetDefaultDataConverter().ToPayloads(res)
	if err != nil {
		t.Fatal(err)
	}
	h.at(base, enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
			ScheduledEventId: s, StartedEventId: started, Result: result}}
	})
}

// commenter is github.comment_commit as agentd configures it, over the test's ledger and history.
func commenter(l *ledger.Ledger, h *fakeHistory) *githubtool.CommentCommit {
	registry := tasks.Registry{"security-audit": tasks.SecurityAudit(nil, nil, nil)}
	return &githubtool.CommentCommit{Repos: map[string]githubtool.Repo{"o/r": {Owner: "o", Name: "r"}}, Blobs: audit.RunBlobs{Ledger: l},
		Effects: RunRecords{l}, Verdicts: ReportVerdicts{Ledger: l, Temporal: h, Tasks: registry}, Task: "audit_v1"}
}

// reportCall is the report's call with args, as the gateway takes it to the effect protocol: the
// request, named as callTool names its effect, and the arguments' canonical form.
func reportCall(t *testing.T, cc *githubtool.CommentCommit, args map[string]any) (tools.Request, []byte) {
	t.Helper()
	canon, err := encodeCanonical(args)
	if err != nil {
		t.Fatal(err)
	}
	id, err := effectID(testWorkflowID, cc, args, canon)
	if err != nil {
		t.Fatal(err)
	}
	return tools.Request{WorkflowID: testWorkflowID, EffectID: id, Args: args}, canon
}

// payloadOfEffect is what github.comment_commit recorded of an effect: the owner, the repository,
// the commit, the body and the marker.
func payloadOfEffect(t *testing.T, l *ledger.Ledger, effectID string) string {
	t.Helper()
	e, err := l.Q.GetEffect(context.Background(), effectID)
	if err != nil {
		t.Fatal(err)
	}
	b, err := l.GetBlob(context.Background(), e.PayloadBlob.String)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// posted is the comment the run's report renders now, as github.comment_commit records its payload.
func posted(t *testing.T, g *Gateway, l *ledger.Ledger, h *fakeHistory, args map[string]any) string {
	t.Helper()
	cc := commenter(l, h)
	run, err := l.Q.GetRun(context.Background(), testWorkflowID)
	if err != nil {
		t.Fatal(err)
	}
	req, canon := reportCall(t, cc, args)
	if res, err := g.effect(context.Background(), run, cc, cc.Class(), 1, nil, req, canon, faults.Effect{}); err != nil ||
		res.GetStatus() != StatusOK {
		t.Fatalf("%v %v", res, err)
	}
	return payloadOfEffect(t, l, req.EffectID)
}

// Through the gateway, to the comment it posts: a call the workflow made up, whatever its kind and
// name, and however it ended (agentd refused it or failed it, or it timed out, failed or completed
// with no record of agentd's), leaves nothing of what the workflow chose in the comment (the call's
// kind and name, its activity ID, its failure's type and words, its result), and neither does what
// the report's own call sends besides why it reports and an error type the report names: two runs
// that differ only in those bytes post the same comment, byte for byte, its marker included, for
// either reason.
func TestAReportPostsNothingAWorkerChose(t *testing.T) {
	chosen := [2]string{"a", strings.Repeat("EXFILTRATED_ghp_0123456789abcdef \"\n\x1b[2J", 50)}
	// After the run's records, which each run's gateway writes as it starts: the same for both.
	base := time.Now().Add(time.Hour).Truncate(time.Second)
	for _, sc := range []struct {
		name string
		make func(t *testing.T, g *Gateway, l *ledger.Ledger, h *fakeHistory, chosen string)
	}{
		{"a tool agentd does not have, refused", func(t *testing.T, g *Gateway, l *ledger.Ledger, h *fakeHistory, c string) {
			h.through(t, base, g, l, &agentosv1.Call{Kind: "tool", Name: c, ArgsJson: `{}`})
		}},
		{"a model task agentd does not have, failed", func(t *testing.T, g *Gateway, l *ledger.Ledger, h *fakeHistory, c string) {
			h.through(t, base, g, l, &agentosv1.Call{Kind: "model", Name: c, ArgsJson: `{}`})
		}},
		{"a kind agentd does not have, failed", func(t *testing.T, g *Gateway, l *ledger.Ledger, h *fakeHistory, c string) {
			h.through(t, base, g, l, &agentosv1.Call{Kind: "mcp", Name: c, ArgsJson: `{}`})
		}},
		{"a kind of the workflow's own, failed", func(t *testing.T, g *Gateway, l *ledger.Ledger, h *fakeHistory, c string) {
			h.through(t, base, g, l, &agentosv1.Call{Kind: "k" + c, Name: "x", ArgsJson: `{}`})
		}},
		{"a call on a queue agentd does not serve, timed out", func(t *testing.T, g *Gateway, l *ledger.Ledger, h *fakeHistory, c string) {
			input, _ := converter.GetDefaultDataConverter().ToPayloads(&agentosv1.Call{Kind: "tool", Name: c, ArgsJson: `{}`})
			h.at(base, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, nil)
			s := h.at(base, enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, func(e *historypb.HistoryEvent) {
				e.Attributes = &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
					ActivityId: "7:" + c, Input: input}}
			})
			h.at(base, enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT, func(e *historypb.HistoryEvent) {
				e.Attributes = &historypb.HistoryEvent_ActivityTaskTimedOutEventAttributes{ActivityTaskTimedOutEventAttributes: &historypb.ActivityTaskTimedOutEventAttributes{
					ScheduledEventId: s, Failure: &failurepb.Failure{Message: "activity ScheduleToStart timeout",
						FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
							TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START}}}}}
			})
		}},
		{"a call the workflow failed on a queue of its own", func(t *testing.T, g *Gateway, l *ledger.Ledger, h *fakeHistory, c string) {
			s, started := h.scheduled(t, base, "7:"+c, &agentosv1.Call{Kind: "model", Name: c, ArgsJson: `{}`})
			h.failed(base, s, started, &failurepb.Failure{Message: c, FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "T" + c, NonRetryable: true}}})
		}},
		{"a call of the task's own step the workflow failed on a queue of its own", func(t *testing.T, g *Gateway, l *ledger.Ledger, h *fakeHistory, c string) {
			s, started := h.scheduled(t, base, "1:"+c, &agentosv1.Call{Kind: "tool", Name: "git.show_diff", ArgsJson: `{}`})
			h.failed(base, s, started, &failurepb.Failure{Message: "tool git.show_diff: " + c, FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "ToolFailed", NonRetryable: true}}})
		}},
		{"a call the workflow completed on a queue of its own", func(t *testing.T, g *Gateway, l *ledger.Ledger, h *fakeHistory, c string) {
			s, started := h.scheduled(t, base, "7:"+c, &agentosv1.Call{Kind: "tool", Name: c, ArgsJson: `{}`})
			value, _ := json.Marshal(map[string]any{"blob": c, "_audit": audit.Head{Seq: 1_000_000 + int64(len(c)), Sha256: c}})
			result, _ := converter.GetDefaultDataConverter().ToPayloads(&agentosv1.Result{Status: "ok" + c, ValueJson: string(value)})
			h.at(base, enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED, func(e *historypb.HistoryEvent) {
				e.Attributes = &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
					ScheduledEventId: s, StartedEventId: started, Result: result}}
			})
		}},
	} {
		for _, reason := range []string{"done", "workflow_error"} {
			t.Run(sc.name+", "+reason, func(t *testing.T) {
				var got [2]string
				for i, c := range chosen {
					g, l := reporting(t)
					var h fakeHistory
					sc.make(t, g, l, &h, c)
					args := map[string]any{"repo": "o/r", "commit": strings.Repeat("a", 40), "reason": reason}
					if reason == "workflow_error" {
						args["error_type"] = "KeyError"
					}
					got[i] = posted(t, g, l, &h, args)
				}
				if got[0] != got[1] {
					t.Fatalf("the comments differ:\n%s\n%s", got[0], got[1])
				}
				if strings.Contains(got[1], "EXFILTRATED") || strings.Contains(got[1], "ghp_") || !strings.Contains(got[1], "Outcome: ") {
					t.Fatalf("%s", got[1])
				}
			})
		}
	}
	// What else the report's own call sends: an older Worker's outcome and pr_url, an error type the
	// report does not name, report_ref (as the Worker sends it before the run has an accepted report:
	// null) or none. The comment, its marker included, is the same whatever they are, before a call
	// and after one; an error type the report names, or the other reason, is another comment.
	chosenType := "EXFILTRATED_ghp_0123456789abcdef_" + strings.Repeat("x", 30)
	for _, call := range []bool{false, true} {
		comment := func(args map[string]any) string {
			g, l := reporting(t)
			var h fakeHistory
			h.at(base, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, nil)
			if call {
				h.through(t, base, g, l, &agentosv1.Call{Kind: "tool", Name: "git.show_diff", ArgsJson: `{}`})
			}
			full := map[string]any{"repo": "o/r", "commit": strings.Repeat("a", 40), "reason": "workflow_error"}
			for k, v := range args {
				full[k] = v
			}
			return posted(t, g, l, &h, full)
		}
		plain := comment(nil)
		for _, args := range []map[string]any{
			{"error_type": "Q"},
			{"error_type": chosenType, "outcome": chosen[1], "pr_url": chosen[1], "report_ref": nil},
			{"outcome": "ok: approved by the security team", "pr_url": nil},
			{"report_ref": nil},
		} {
			if got := comment(args); got != plain {
				t.Fatalf("%v:\n%s\nwithout them:\n%s", args, got, plain)
			}
		}
		if strings.Contains(plain, "EXFILTRATED") || !strings.Contains(plain, "(an exception of another type)") ||
			!reMarker.MatchString(plain) {
			t.Fatalf("%s", plain)
		}
		for _, other := range []map[string]any{{"error_type": "KeyError"}, {"reason": "done"}} {
			if got := comment(other); got == plain || reMarker.FindString(got) == reMarker.FindString(plain) {
				t.Fatalf("%v:\n%s\nwithout:\n%s", other, got, plain)
			}
		}
		if named := comment(map[string]any{"error_type": "KeyError"}); !strings.Contains(named, "(KeyError)") {
			t.Fatalf("%s", named)
		}
	}
	// A report with no reason, and one that says done: the same comment and the same marker (N1).
	done := map[string]any{"repo": "o/r", "commit": strings.Repeat("a", 40), "reason": "done"}
	none := map[string]any{"repo": "o/r", "commit": strings.Repeat("a", 40)}
	var same [2]string
	for i, args := range []map[string]any{none, done} {
		g, l := reporting(t)
		var h fakeHistory
		h.at(base, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, nil)
		same[i] = posted(t, g, l, &h, args)
	}
	if same[0] != same[1] || !reMarker.MatchString(same[0]) {
		t.Fatalf("no reason:\n%s\ndone:\n%s", same[0], same[1])
	}
}

// An effect of the report an agentd from before recorded, by every argument of its call, and left
// unfinished: the call goes on with it after the upgrade (its payload, its marker), not with another
// effect, which the effect cap would refuse, so that the run would never report.
func TestAReportsEffectRecordedBeforeItsIdentity(t *testing.T) {
	ctx := context.Background()
	// legacyEffect records, as an agentd from before this change would, a report effect named by
	// every argument of args, PENDING and unsent, and returns the gateway, its ledger, the run, the
	// call and its legacy effect ID.
	legacyEffect := func(t *testing.T, args map[string]any) (*Gateway, *ledger.Ledger, db.Run, *githubtool.CommentCommit, tools.Request, []byte, string) {
		t.Helper()
		g, l := reporting(t)
		var h fakeHistory
		h.through(t, time.Now().Add(time.Hour).Truncate(time.Second), g, l, &agentosv1.Call{Kind: "tool", Name: "git.show_diff", ArgsJson: `{}`})
		cc := commenter(l, &h)
		run, err := l.Q.GetRun(ctx, testWorkflowID)
		if err != nil {
			t.Fatal(err)
		}
		req, canon := reportCall(t, cc, args)
		older := req
		older.EffectID = sha256Hex([]byte(testWorkflowID), []byte(cc.Name()), canon)
		if older.EffectID == req.EffectID {
			t.Fatal("the effect is named by every argument")
		}
		if res, err := g.record(ctx, run, cc, cc.Class(), 1, nil, older, canon); res != nil || err != nil {
			t.Fatalf("%v %v", res, err)
		}
		return g, l, run, cc, req, canon, older.EffectID
	}
	args := map[string]any{"repo": "o/r", "commit": strings.Repeat("a", 40), "reason": "done", "report_ref": nil, "pr_url": nil}

	// Nothing of the legacy effect went out: it is abandoned and a fresh one recorded under the
	// identity ID, rendered by current code, and the comment carries the identity ID's marker.
	t.Run("unsent, re-rendered fresh", func(t *testing.T) {
		g, l, run, cc, req, canon, legacy := legacyEffect(t, args)
		if res, err := g.effect(ctx, run, cc, cc.Class(), 1, nil, req, canon, faults.Effect{}); err != nil || res.GetStatus() != StatusOK {
			t.Fatalf("after the upgrade: %v %v", res, err)
		}
		effects, err := l.Q.ListRunEffects(ctx, db.ListRunEffectsParams{WorkflowID: testWorkflowID, Limit: 10})
		if err != nil {
			t.Fatal(err)
		}
		byID := map[string]db.Effect{}
		for _, e := range effects {
			byID[e.EffectID] = e
		}
		if len(effects) != 2 || byID[legacy].Status != effectAbandoned || byID[req.EffectID].Status != effectDryRun {
			t.Fatalf("%+v", effects)
		}
		if p := payloadOfEffect(t, l, req.EffectID); !strings.Contains(p, req.EffectID) || strings.Contains(p, legacy) {
			t.Fatalf("its marker: %s", p)
		}
		// A call of another identity finds no effect of its own: the cap (the fresh DRY_RUN counts,
		// the ABANDONED one does not) refuses it, as it would have refused a second report before.
		other, canon := reportCall(t, cc, map[string]any{"repo": "o/r", "commit": strings.Repeat("a", 40), "reason": "workflow_error", "error_type": "KeyError"})
		if res, err := g.effect(ctx, run, cc, cc.Class(), 1, nil, other, canon, faults.Effect{}); err != nil || res.GetStatus() != StatusDenied {
			t.Fatalf("another identity: %v %v", res, err)
		}
	})

	// The legacy effect may have gone out (an attempt marked it maybe-sent): the call goes on with
	// it, under the old marker, so no second comment is posted.
	t.Run("maybe sent, kept", func(t *testing.T) {
		g, l, run, cc, req, canon, legacy := legacyEffect(t, args)
		if _, err := l.DB.ExecContext(ctx, "UPDATE effects SET maybe_sent = 1 WHERE effect_id = ?", legacy); err != nil {
			t.Fatal(err)
		}
		// Not live here, and the effect may have been sent: agentd cannot tell, so the call ends
		// needs_reconcile. The point is it goes on with the legacy effect, and posts no second one.
		if res, err := g.effect(ctx, run, cc, cc.Class(), 1, nil, req, canon, faults.Effect{}); err != nil || res.GetStatus() != StatusNeedsReconcile {
			t.Fatalf("after the upgrade: %v %v", res, err)
		}
		effects, err := l.Q.ListRunEffects(ctx, db.ListRunEffectsParams{WorkflowID: testWorkflowID, Limit: 10})
		if err != nil || len(effects) != 1 || effects[0].EffectID != legacy {
			t.Fatalf("%+v %v", effects, err)
		}
		if p := payloadOfEffect(t, l, legacy); !strings.Contains(p, legacy) {
			t.Fatalf("its marker: %s", p)
		}
	})
}

// The abandon of an unsent legacy effect takes effect only on the effect as it was read: PENDING,
// unclaimed, maybe_sent 0, at that version. One that changed meanwhile (a cancel that settled it, an
// attempt that claimed it, one that may have sent it) is left as it is, and the call goes on with it.
func TestAbandoningAnUnsentEffectIsConditional(t *testing.T) {
	ctx := context.Background()
	g, l := reporting(t)
	var h fakeHistory
	cc := commenter(l, &h)
	run, err := l.Q.GetRun(ctx, testWorkflowID)
	if err != nil {
		t.Fatal(err)
	}
	// fresh records a PENDING report effect of its own identity (why, the error type it names).
	fresh := func(t *testing.T, why map[string]any) db.Effect {
		t.Helper()
		args := map[string]any{"repo": "o/r", "commit": strings.Repeat("a", 40)}
		maps.Copy(args, why)
		req, canon := reportCall(t, cc, args)
		if res, err := g.record(ctx, run, cc, cc.Class(), 0, nil, req, canon); res != nil || err != nil {
			t.Fatalf("%v %v", res, err)
		}
		e, err := l.Q.GetEffect(ctx, req.EffectID)
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	for name, c := range map[string]struct {
		why    map[string]any
		change string
	}{
		"settled meanwhile (a cancel)": {map[string]any{"reason": "done"},
			"UPDATE effects SET status = 'ABANDONED', version = version + 1 WHERE effect_id = ?"},
		"claimed meanwhile": {map[string]any{"reason": "workflow_error", "error_type": "KeyError"},
			"UPDATE effects SET claimed_by = 'another-agentd', version = version + 1 WHERE effect_id = ?"},
		"maybe sent meanwhile": {map[string]any{"reason": "workflow_error", "error_type": "ValueError"},
			"UPDATE effects SET maybe_sent = 1, version = version + 1 WHERE effect_id = ?"},
	} {
		t.Run(name, func(t *testing.T) {
			read := fresh(t, c.why)
			if _, err := l.DB.ExecContext(ctx, c.change, read.EffectID); err != nil {
				t.Fatal(err)
			}
			before, err := l.Q.GetEffect(ctx, read.EffectID)
			if err != nil {
				t.Fatal(err)
			}
			if abandoned, err := g.abandonUnsent(ctx, read, "re-rendered"); err != nil || abandoned {
				t.Fatalf("abandoned %v, %v", abandoned, err)
			}
			if after, err := l.Q.GetEffect(ctx, read.EffectID); err != nil || after.Version != before.Version || after.Status != before.Status {
				t.Fatalf("changed: %+v -> %+v %v", before, after, err)
			}
		})
	}
	// Unchanged since it was read: abandoned.
	read := fresh(t, map[string]any{"reason": "workflow_error"})
	if abandoned, err := g.abandonUnsent(ctx, read, "re-rendered"); err != nil || !abandoned {
		t.Fatalf("abandoned %v, %v", abandoned, err)
	}
	if after, err := l.Q.GetEffect(ctx, read.EffectID); err != nil || after.Status != effectAbandoned {
		t.Fatalf("%+v %v", after, err)
	}

	// What the call does with it: a fresh effect when the legacy one was abandoned; when it changed
	// meanwhile (here a cancel settled it), the legacy effect as it is now.
	unchanged := fresh(t, map[string]any{"reason": "workflow_error", "error_type": "TypeError"})
	if _, keep, err := g.takeOverLegacy(ctx, unchanged); err != nil || keep {
		t.Fatalf("an unchanged one: keep %v, %v", keep, err)
	}
	changed := fresh(t, map[string]any{"reason": "workflow_error", "error_type": "OSError"})
	if _, err := l.DB.ExecContext(ctx, "UPDATE effects SET status = 'ABANDONED', version = version + 1 WHERE effect_id = ?",
		changed.EffectID); err != nil {
		t.Fatal(err)
	}
	if cur, keep, err := g.takeOverLegacy(ctx, changed); err != nil || !keep || cur.EffectID != changed.EffectID ||
		cur.Version != changed.Version+1 {
		t.Fatalf("a changed one: %+v keep %v, %v", cur, keep, err)
	}
}

// foundComment is github.comment_commit whose lookup finds the comment an older agentd posted: the
// test has no GitHub to look in.
type foundComment struct{ *githubtool.CommentCommit }

func (foundComment) Find(context.Context, json.RawMessage) (map[string]any, bool, error) {
	return map[string]any{"url": "https://github.com/o/r/commit/a#commitcomment-1", "id": json.Number("1")}, true, nil
}

// Where it is used: the legacy effect, unsent when effect read it, is claimed between that read and
// the abandon, as an older agentd still finishing during an upgrade would claim it. The abandon does
// not take, and the call goes on with the legacy effect as it is now (claimed: looked up, and found),
// recording no second effect.
func TestALegacyEffectClaimedMeanwhileIsCarriedOn(t *testing.T) {
	ctx := context.Background()
	g, l := reporting(t)
	var h fakeHistory
	h.through(t, time.Now().Add(time.Hour).Truncate(time.Second), g, l, &agentosv1.Call{Kind: "tool", Name: "git.show_diff", ArgsJson: `{}`})
	cc := foundComment{commenter(l, &h)}
	run, err := l.Q.GetRun(ctx, testWorkflowID)
	if err != nil {
		t.Fatal(err)
	}
	args := map[string]any{"repo": "o/r", "commit": strings.Repeat("a", 40), "reason": "done", "report_ref": nil}
	req, canon := reportCall(t, cc.CommentCommit, args)
	older := req
	older.EffectID = sha256Hex([]byte(testWorkflowID), []byte(cc.Name()), canon)
	if res, err := g.record(ctx, run, cc, cc.Class(), 1, nil, older, canon); res != nil || err != nil {
		t.Fatalf("%v %v", res, err)
	}
	claimed := false
	beforeTakeOver = func(e db.Effect) {
		if e.EffectID != older.EffectID {
			return
		}
		claimed = true
		if _, err := l.DB.ExecContext(ctx, "UPDATE effects SET claimed_by = 'older-agentd', claimed_at = ?, maybe_sent = 1, "+
			"version = version + 1 WHERE effect_id = ?", ledger.Now(), e.EffectID); err != nil {
			t.Error(err)
		}
	}
	t.Cleanup(func() { beforeTakeOver = nil })
	res, err := g.effect(ctx, run, cc, cc.Class(), 1, nil, req, canon, faults.Effect{})
	if err != nil || res.GetStatus() != StatusOK || !claimed {
		t.Fatalf("%v %v, claimed %v", res, err, claimed)
	}
	effects, err := l.Q.ListRunEffects(ctx, db.ListRunEffectsParams{WorkflowID: testWorkflowID, Limit: 10})
	if err != nil || len(effects) != 1 || effects[0].EffectID != older.EffectID || effects[0].Status != effectSucceeded {
		t.Fatalf("%+v %v", effects, err)
	}
}

// Two runs auditing one commit, whose reports send the same arguments, are two effects: the effect's
// identity is the comment's repository, commit and reason, and the run's workflow ID names it too. A
// second run that shared the first's ID would find the first run's comment, and never post its own.
func TestTwoRunsReportingOneCommitAreTwoEffects(t *testing.T) {
	g, l := reporting(t)
	ctx := context.Background()
	const other = "wf-other"
	if err := l.Q.InsertRun(ctx, db.InsertRunParams{WorkflowID: other, Task: "security-audit", ParamsJson: "{}",
		BudgetLimitMicros: 1, CreatedAt: ledger.Now()}); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Q.SetRunID(ctx, db.SetRunIDParams{RunID: sql.NullString{String: "other-run-id", Valid: true}, WorkflowID: other}); err != nil {
		t.Fatal(err)
	}
	var h fakeHistory
	h.at(time.Now().Add(time.Hour).Truncate(time.Second), enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, nil)
	cc := commenter(l, &h)
	args := map[string]any{"repo": "o/r", "commit": strings.Repeat("a", 40), "reason": "done"}
	canon, err := encodeCanonical(args)
	if err != nil {
		t.Fatal(err)
	}
	var ids, markers [2]string
	for i, wf := range []string{testWorkflowID, other} {
		run, err := l.Q.GetRun(ctx, wf)
		if err != nil {
			t.Fatal(err)
		}
		id, err := effectID(wf, cc, args, canon)
		if err != nil {
			t.Fatal(err)
		}
		req := tools.Request{WorkflowID: wf, EffectID: id, Args: args}
		if res, err := g.effect(ctx, run, cc, cc.Class(), 1, nil, req, canon, faults.Effect{}); err != nil || res.GetStatus() != StatusOK {
			t.Fatalf("%s: %v %v", wf, res, err)
		}
		ids[i], markers[i] = id, reMarker.FindString(payloadOfEffect(t, l, id))
	}
	if ids[0] == ids[1] || markers[0] == "" || markers[0] == markers[1] {
		t.Fatalf("effects %v, markers %v", ids, markers)
	}
	for i, wf := range []string{testWorkflowID, other} {
		e, err := l.Q.GetEffect(ctx, ids[i])
		if err != nil || e.WorkflowID != wf {
			t.Fatalf("%s's effect: %+v %v", wf, e, err)
		}
	}
}

// reporting is a gateway whose ledger holds the test execution's run as StartRun leaves it, its
// records among it: the audit log began before the run's history did.
func reporting(t *testing.T) (*Gateway, *ledger.Ledger) {
	t.Helper()
	g, l, _ := invokable(t)
	g.Models = &models.Config{}
	if _, err := audit.Record(context.Background(), l, audit.Event{WorkflowID: testWorkflowID, Type: "run.created",
		Actor: "operator:test", Payload: map[string]any{"task": "security-audit"}}); err != nil {
		t.Fatal(err)
	}
	return g, l
}

// reMarker is the marker a comment names its effect by, as the payload has it in its body and on its
// own.
var reMarker = regexp.MustCompile(`\\u003c!-- agent-effect:[0-9a-f]{64} --\\u003e`)
