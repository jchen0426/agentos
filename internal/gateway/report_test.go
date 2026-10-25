package gateway

import (
	"context"
	"encoding/json"
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

// posted is the comment the run's report renders now, as github.comment_commit records its payload:
// the owner, the repository, the commit, the body and the marker.
func posted(t *testing.T, g *Gateway, l *ledger.Ledger, h *fakeHistory, args map[string]any) string {
	t.Helper()
	registry := tasks.Registry{"security-audit": tasks.SecurityAudit(nil, nil, nil)}
	cc := &githubtool.CommentCommit{Repos: map[string]githubtool.Repo{"o/r": {Owner: "o", Name: "r"}}, Blobs: audit.RunBlobs{Ledger: l},
		Effects: RunRecords{l}, Verdicts: ReportVerdicts{Ledger: l, Temporal: h, Tasks: registry}, Task: "audit_v1"}
	ctx := context.Background()
	run, err := l.Q.GetRun(ctx, testWorkflowID)
	if err != nil {
		t.Fatal(err)
	}
	canon, err := encodeCanonical(args)
	if err != nil {
		t.Fatal(err)
	}
	req := tools.Request{WorkflowID: testWorkflowID, EffectID: sha256Hex([]byte(testWorkflowID), []byte(cc.Name()), canon), Args: args}
	if res, err := g.effect(ctx, run, cc, cc.Class(), 1, nil, req, canon, faults.Effect{}); err != nil || res.GetStatus() != StatusOK {
		t.Fatalf("%v %v", res, err)
	}
	e, err := l.Q.GetEffect(ctx, req.EffectID)
	if err != nil {
		t.Fatal(err)
	}
	b, err := l.GetBlob(ctx, e.PayloadBlob.String)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// Through the gateway, to the comment it posts: a call the workflow made up, whatever its kind and
// name, and however it ended (agentd refused it or failed it, or it timed out, failed or completed
// with no record of agentd's), leaves nothing of what the workflow chose in the comment (the call's
// kind and name, its activity ID, its failure's type and words, its result), nor does an error type
// outside explain.PublicErrorTypes: two runs that differ only in those bytes post the same comment,
// byte for byte, for either reason.
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
	// The error type the workflow names, when it is not one of the set: as if it named none. The comment's
	// marker is the exception, and open (docs/m5-notes.md): it names the report's effect, whose ID is a
	// hash of the call's arguments, the error type among them.
	for _, call := range []bool{false, true} {
		var got [2]string
		for i, name := range []string{"Q", "EXFILTRATED_ghp_0123456789abcdef_" + strings.Repeat("x", 30)} {
			g, l := reporting(t)
			var h fakeHistory
			h.at(base, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, nil)
			if call {
				h.through(t, base, g, l, &agentosv1.Call{Kind: "tool", Name: "git.show_diff", ArgsJson: `{}`})
			}
			got[i] = reMarker.ReplaceAllString(posted(t, g, l, &h, map[string]any{"repo": "o/r", "commit": strings.Repeat("a", 40),
				"reason": "workflow_error", "error_type": name}), "")
		}
		if got[0] != got[1] || strings.Contains(got[1], "EXFILTRATED") || !strings.Contains(got[1], "(an exception of another type)") {
			t.Fatalf("the comments:\n%s\n%s", got[0], got[1])
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
