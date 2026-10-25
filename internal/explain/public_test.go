package explain

import (
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"

	"github.com/jchen0426/agentos/internal/audit"
	"github.com/jchen0426/agentos/internal/ledger/db"
)

// What a report, which is public, takes from the workflow, which a subverted Worker writes: its own
// words are why it reports, and an exception type from a fixed set. Of the rest it tells what the run
// did (which of the task's steps, how each ended), never the bytes the workflow chose: not the calls
// it makes up, nor their activity IDs, nor what it makes of a call it fails or completes itself (a
// Worker can, on a task queue of its own).

// A step whose name is the workflow's, not the task's, is a call the task does not name in the
// report, which quotes nothing of it: the name, and agentd's reason, which could quote the name.
// runs explain, which is local, names it.
func TestTheReportNamesOnlyTheTasksSteps(t *testing.T) {
	w := newWorld(t)
	w.returns("@mallory see www.evil.example", statusDenied, map[string]any{"reason": "no such tool @mallory",
		"refused_by": audit.RefusedByAgentd}, nil)
	r := w.schedule("github.comment_commit")
	o := w.report(ReasonDone, "")
	if o.Named || o.Kind != KindDenied || o.Step != unnamedStep || o.Cause != "agentd refused a call the task does not name" ||
		strings.Contains(fmt.Sprintf("%+v", o), "mallory") {
		t.Fatalf("%+v", o)
	}
	r.completes(1, statusOK, nil, nil, nil)
	w.completed("ok")
	check(t, w.explain(), want{verdict: VerdictStepFailed, kind: KindDenied, step: `"tool @mallory see www.evil.example"`,
		activity: "1:@mallory see www.evil.example", attempt: 1, cause: []string{`: "no such tool @mallory"`}})
}

// python312 are Python 3.12's built-in exceptions that derive from Exception, warnings left out, by
// their own names (aliases such as IOError are OSError's), as Python 3.12 lists them:
//
//	sorted({v.__name__ for v in vars(builtins).values() if isinstance(v, type) and issubclass(v, Exception)
//	        and not issubclass(v, Warning)})
const python312 = `ArithmeticError AssertionError AttributeError BlockingIOError BrokenPipeError BufferError
	ChildProcessError ConnectionAbortedError ConnectionError ConnectionRefusedError ConnectionResetError EOFError Exception
	ExceptionGroup FileExistsError FileNotFoundError FloatingPointError ImportError IndentationError IndexError
	InterruptedError IsADirectoryError KeyError LookupError MemoryError ModuleNotFoundError NameError NotADirectoryError
	NotImplementedError OSError OverflowError PermissionError ProcessLookupError RecursionError ReferenceError RuntimeError
	StopAsyncIteration StopIteration SyntaxError SystemError TabError TimeoutError TypeError UnboundLocalError
	UnicodeDecodeError UnicodeEncodeError UnicodeError UnicodeTranslateError ValueError ZeroDivisionError`

// A report is public, and the workflow (a subverted Worker) chooses the error type it names: the
// report says one only from a fixed set, Python 3.12's built-in exceptions that derive from Exception
// and json's JSONDecodeError, pinned here both ways. Any other name, however close to one of them, and
// none, is "an exception of another type": the report is then the same, byte for byte, as for no name
// at all, before a call and after one; a name of the set changes the claim only. runs explain, which
// is local, keeps the workflow's own words as its claim.
func TestTheReportNamesOnlyBuiltinExceptionTypes(t *testing.T) {
	set := append(strings.Fields(python312), "JSONDecodeError")
	slices.Sort(set)
	if got := slices.Sorted(maps.Keys(PublicErrorTypes)); len(set) != 51 || !slices.Equal(got, set) {
		t.Fatalf("PublicErrorTypes:\n%v\nwant:\n%v", got, set)
	}
	for name, public := range PublicErrorTypes {
		if !public {
			t.Errorf("%s is listed, as not named", name)
		}
	}
	others := []string{
		// Python 3.12's besides: not Exception's, its warnings, aliases; and what newer Pythons add.
		"BaseException", "BaseExceptionGroup", "SystemExit", "KeyboardInterrupt", "GeneratorExit", "Warning", "UserWarning",
		"DeprecationWarning", "PendingDeprecationWarning", "SyntaxWarning", "RuntimeWarning", "FutureWarning", "ImportWarning",
		"UnicodeWarning", "BytesWarning", "ResourceWarning", "EncodingWarning", "IOError", "EnvironmentError",
		"PythonFinalizationError", "_IncompleteInputError",
		// The Temporal SDK's, the workflow's own, names shaped like an exception's, short ones, the longest the
		// schema takes.
		"NondeterminismError", "RestrictedWorkflowAccessError", "ApplicationError", "CancelledError", "PatchUnreadable",
		"Error", "FooError", "SecretError", "KeyErrorError", "E", "x", "_", "Ab", "OSErr", "abc123", "Zz_9.q", "",
		strings.Repeat("E", 64), strings.Repeat("E", 65), "x y", "Fehlerä",
	}
	for _, n := range set {
		others = append(others, "builtins."+n, "json.decoder."+n, "json."+n, n+".x", n+"X", "X"+n, strings.ToLower(n),
			strings.ToUpper(n), n+"_", " "+n, n+"\n")
	}
	for _, before := range []func(w *world){func(*world) {}, func(w *world) { w.ok("git.show_diff") }} {
		w := newWorld(t)
		before(w)
		r := w.schedule("github.comment_commit")
		none := w.report(ReasonWorkflowError, "")
		if none.Claim != "the workflow reports that its own code failed (an exception of another type), and fails the run "+
			"once this report is out" || none.Verdict != VerdictFailed || none.Kind != KindWorkflowError {
			t.Fatalf("%+v", none)
		}
		for _, name := range set {
			named := none
			named.Claim = "the workflow reports that its own code failed (" + name + "), and fails the run once this report is out"
			if o := w.report(ReasonWorkflowError, name); !reflect.DeepEqual(o, named) {
				t.Errorf("%s:\n%+v\nwant\n%+v", name, o, named)
			}
		}
		for _, name := range others {
			if slices.Contains(set, name) {
				t.Fatalf("%q is in the set", name)
			}
			if o := w.report(ReasonWorkflowError, name); !reflect.DeepEqual(o, none) {
				t.Errorf("%q:\n%+v\nwant\n%+v", name, o, none)
			}
		}
		r.completes(1, statusOK, nil, nil, nil)
		w.failed("the workflow raised PatchUnreadable: the patch is not text", "PatchUnreadable")
		if e := w.explain(); !strings.Contains(e.GetClaim(), "PatchUnreadable") {
			t.Fatalf("claim %q", e.GetClaim())
		}
	}
}

// madeUp is how a run went with one call whose bytes the workflow chose (chosen), as a subverted
// Worker makes one: a call of kind and name (both its own, but for a step the task names) under an
// activity ID of its own, which agentd refused or failed, or which it failed, timed out or completed
// on a task queue of its own, agentd having no record of it.
type madeUp struct {
	name   string
	mapped bool // the call is one of the task's steps (git.show_diff): only what is not agentd's words is chosen
	end    func(w *world, c *call, chosen string)
	says   string // the report's cause, for a tool's call and reason done: all it says of the call
}

// failedBy has the call's attempt fail in Temporal, with no record of agentd's: the failure's type and
// message are typ's and message's.
func failedBy(c *call, typ, message string) {
	c.end(1, nil, func(e *historypb.HistoryEvent, started int64) {
		e.EventType = enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED
		e.Attributes = &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{ActivityTaskFailedEventAttributes: &historypb.ActivityTaskFailedEventAttributes{
			ScheduledEventId: c.scheduled, StartedEventId: started, Failure: appFailure(message, typ, true)}}
	})
}

var madeUps = []madeUp{
	{"denied by agentd, its reason quoting the call", false, func(w *world, c *call, chosen string) {
		c.completes(1, statusDenied, map[string]any{"reason": "no such tool " + c.name, "refused_by": audit.RefusedByAgentd},
			map[string]any{"reason": "no such tool " + c.name}, nil)
	}, "agentd refused a call the task does not name"},
	{"denied by the policy", false, func(w *world, c *call, chosen string) {
		c.completes(1, statusDenied, map[string]any{"reason": "tool not in policy", "refused_by": audit.RefusedByPolicy,
			"policy": map[string]any{"decision": "deny", "reason": "tool not in policy"}}, nil, nil)
	}, "the policy denied a call the task does not name"},
	{"denied by an older agentd's loop guard, its reason naming the call", false, func(w *world, c *call, chosen string) {
		c.completes(1, statusDenied, map[string]any{"reason": "loop guard: " + c.name + " called more than 3 times with the same " +
			"arguments", "refused_by": audit.RefusedByLoopGuard}, nil, nil)
	}, "agentd's loop guard refused a call the task does not name"},
	{"denied by a tool, its reason quoting the arguments", false, func(w *world, c *call, chosen string) {
		c.completes(1, statusDenied, map[string]any{"reason": fmt.Sprintf("%.100q is not a branch name agentd takes", chosen),
			"refused_by": audit.RefusedByTool}, nil, nil)
	}, "a call the task does not name refused the call as it stood"},
	{"failed: agentd's error names the call", false, func(w *world, c *call, chosen string) {
		c.failsForGood(1, "unknown model task "+c.name, "UnknownModelTask", nil)
	}, "a call the task does not name failed; agentd's log has the details"},
	{"failed: a tool's words quote the arguments", false, func(w *world, c *call, chosen string) {
		c.failsForGood(1, "tool "+c.name+": fatal: bad object "+chosen, "ToolFailed", nil)
	}, "a call the task does not name failed; agentd's log has the details"},
	{"refused: the model's words", false, func(w *world, c *call, chosen string) {
		c.completes(1, statusRefused, nil, nil, nil)
		w.l.Requests = append(w.l.Requests, ModelRequest{ModelCall: db.ModelCall{ActivityID: c.id, Attempt: 1, RequestSeq: 1,
			Provider: "anthropic", Model: "claude"}, Status: statusRefused, Category: chosen, Explanation: chosen})
	}, "a call the task does not name: the model declined to answer (anthropic/claude)"},
	{"invalid output: the reason quotes the answer", false, func(w *world, c *call, chosen string) {
		c.completes(1, statusInvalidOutput, map[string]any{"reason": "at /summary: " + chosen}, nil, nil)
	}, "a call the task does not name: the model's answer broke the task's output schema, and so did the answer agentd " +
		"asked for again"},
	{"returned a status this agentd does not know, its reason quoting the call", false, func(w *world, c *call, chosen string) {
		c.completes(1, "frozen", map[string]any{"reason": "frozen: " + c.name}, nil, nil)
	}, `a call the task does not name returned "frozen"`},
	{"returned ok", false, func(w *world, c *call, chosen string) { c.completes(1, statusOK, nil, nil, nil) },
		"the run has no accepted result of analyze, which the task requires"},
	{"failed, with no record of agentd's", false, func(w *world, c *call, chosen string) { failedBy(c, "T"+chosen, chosen) },
		"a call the task does not name failed, by a failure with no record of agentd's"},
	{"failed, with no record of agentd's, a tool's type", false, func(w *world, c *call, chosen string) {
		failedBy(c, "ToolFailed", "tool "+c.name+": "+chosen)
		// agentd started again during the attempt: the failure counts as agentd's own, cut short.
		failedAt := w.failedAt(w.t)
		w.start("agentd.started", audit.Agentd, nil)
		w.l.Starts[len(w.l.Starts)-1].At = failedAt.Add(-500 * time.Millisecond).Format(time.RFC3339Nano)
	}, "a call the task does not name failed; agentd has no record of its error"},
	{"timed out, never started", false, func(w *world, c *call, chosen string) {
		w.event(enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT, func(e *historypb.HistoryEvent) {
			e.Attributes = &historypb.HistoryEvent_ActivityTaskTimedOutEventAttributes{ActivityTaskTimedOutEventAttributes: &historypb.ActivityTaskTimedOutEventAttributes{
				ScheduledEventId: c.scheduled, Failure: timeoutFailure(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START)}}
		})
		w.task()
	}, "a call the task does not name timed out (Temporal's ScheduleToStart timeout)"},
	{"timed out, started", false, func(w *world, c *call, chosen string) {
		c.end(1, nil, func(e *historypb.HistoryEvent, started int64) {
			e.EventType = enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT
			e.Attributes = &historypb.HistoryEvent_ActivityTaskTimedOutEventAttributes{ActivityTaskTimedOutEventAttributes: &historypb.ActivityTaskTimedOutEventAttributes{
				ScheduledEventId: c.scheduled, StartedEventId: started, Failure: timeoutFailure(enumspb.TIMEOUT_TYPE_START_TO_CLOSE)}}
		})
	}, "a call the task does not name timed out (Temporal's StartToClose timeout)"},
	{"completed, by a result agentd did not send", false, func(w *world, c *call, chosen string) {
		seq := 1_000_000 + int64(len(chosen))
		c.result(1, "ok"+chosen, valueOf(w.t, map[string]any{"report_ref": chosen}, &audit.Head{Seq: seq, Sha256: sha(seq)}), nil)
	}, "a call the task does not name returned a result agentd did not send, and agentd has no record of how the call ended"},
}

// What a subverted Worker chooses of a call it makes up (its kind and name, its activity ID, what
// agentd's words of it quote, what it makes of the call when it fails or completes it itself) changes
// nothing a report says: two runs that differ only in those bytes report alike, byte for byte,
// whatever the call's kind, however it ended, and for either reason. So does what it chooses of a
// call of the task's own step that it fails, times out or completes itself, besides the step's name.
func TestTheReportSaysNothingAWorkerChose(t *testing.T) {
	chosen := [2]string{"a", strings.Repeat(`EXFILTRATED "ghp_0123456789abcdef"`+"\n\x1b[2J ", 60)}
	var cases []madeUp
	for _, m := range madeUps {
		cases = append(cases, m)
		if strings.Contains(m.name, "no record") || strings.Contains(m.name, "timed out") ||
			strings.Contains(m.name, "did not send") || m.name == "returned ok" {
			cases = append(cases, madeUp{m.name + ", of a step the task names", true, m.end, ""})
		}
	}
	for _, m := range cases {
		kinds := []string{"tool", "model", "mcp", ""}
		if m.mapped {
			kinds = []string{"tool"}
		}
		for _, kind := range kinds {
			for _, reason := range []string{ReasonDone, ReasonWorkflowError} {
				t.Run(fmt.Sprintf("%s, %q, %s", m.name, kind, reason), func(t *testing.T) {
					var got [2]Outcome
					for i, c := range chosen {
						w := newWorld(t)
						w.ok("git.show_diff")
						k, n := kind, c
						switch {
						case m.mapped:
							k, n = "tool", "git.show_diff"
						case kind == "": // the kind itself the workflow's
							k, n = "k"+c, ""
						}
						m.end(w, w.scheduleAs("2:"+c, k, n), c)
						w.schedule("github.comment_commit")
						got[i] = w.report(reason, map[string]string{ReasonWorkflowError: "KeyError"}[reason])
					}
					if !reflect.DeepEqual(got[0], got[1]) {
						t.Fatalf("the reports differ:\n%+v\n%+v", got[0], got[1])
					}
					if s := fmt.Sprintf("%#v", got[1]); strings.Contains(s, "EXFILTRATED") || strings.Contains(s, "ghp_") {
						t.Fatalf("%s", s)
					}
					if o := got[1]; !o.Named && o.Step != unnamedStep && o.Step != "" {
						t.Fatalf("%+v", o)
					}
					if o := got[1]; !m.mapped && kind == "tool" && reason == ReasonDone && o.Cause != m.says {
						t.Fatalf("cause %q, want %q", o.Cause, m.says)
					}
				})
			}
		}
	}
}
