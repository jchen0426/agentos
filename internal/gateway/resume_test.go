package gateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	"github.com/jchen0426/agentos/internal/audit"
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
	"github.com/jchen0426/agentos/internal/models"
)

// resumeFixture is a ledger with one run, in which the tests leave a model call's earlier
// activities and requests as agentd would have recorded them.
type resumeFixture struct {
	t   *testing.T
	g   *Gateway
	run db.Run
}

func newResumeFixture(t *testing.T) *resumeFixture {
	t.Helper()
	ctx := context.Background()
	l, err := ledger.Open(ctx, filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	if err := l.Q.InsertRun(ctx, db.InsertRunParams{WorkflowID: "wf", Task: "security-audit", ParamsJson: "{}",
		BudgetLimitMicros: 1, CreatedAt: ledger.Now()}); err != nil {
		t.Fatal(err)
	}
	return &resumeFixture{t: t, run: db.Run{WorkflowID: "wf"},
		g: &Gateway{Ledger: l, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}}
}

// call records an activity that sent the call (the loop guard's row) and what it returned: agentd
// first counts it at its scheduled time (created_at), and when it returned something, its call
// record too, a moment later.
func (f *resumeFixture) call(id string, at time.Time, outcome string) activity.Info {
	f.t.Helper()
	ctx := context.Background()
	scheduled := at.UTC().Format(time.RFC3339Nano)
	if _, err := f.g.Ledger.Q.InsertCall(ctx, db.InsertCallParams{WorkflowID: "wf", ActivityID: id, ScheduledAt: scheduled,
		Kind: "model", Name: "audit_v1", ArgsSha256: "args", CreatedAt: scheduled}); err != nil {
		f.t.Fatal(err)
	}
	if outcome != "" {
		if err := f.g.Ledger.Q.SetCallOutcome(ctx, db.SetCallOutcomeParams{Outcome: sql.NullString{String: outcome, Valid: true},
			WorkflowID: "wf", ActivityID: id, ScheduledAt: scheduled}); err != nil {
			f.t.Fatal(err)
		}
		f.ended(id, scheduled, at.Add(10*time.Millisecond), outcome)
	}
	return activity.Info{ActivityID: id, ScheduledTime: at, Attempt: 1}
}

// ended records the call record that ends an activity, at the time given, as the gateway writes it.
func (f *resumeFixture) ended(id, scheduled string, at time.Time, status string) {
	f.t.Helper()
	clock := ledger.Clock
	ledger.Clock = func() time.Time { return at }
	defer func() { ledger.Clock = clock }()
	if _, err := audit.Record(context.Background(), f.g.Ledger, audit.Event{WorkflowID: "wf", Type: "call", Actor: audit.Run,
		Payload: map[string]any{"activity_id": audit.Text(id), "scheduled_at": scheduled, "status": status}}); err != nil {
		f.t.Fatal(err)
	}
}

// request records one request of an activity to hop, answered with status unless it is "".
func (f *resumeFixture) request(id string, attempt, seq int64, hop models.Route, status string) *models.Response {
	f.t.Helper()
	ctx := context.Background()
	reqRef, err := f.g.Ledger.PutBlob(ctx, []byte(`{"task":"audit_v1"}`))
	if err != nil {
		f.t.Fatal(err)
	}
	if err := f.g.Ledger.Q.InsertModelCall(ctx, db.InsertModelCallParams{WorkflowID: "wf", ActivityID: id, Attempt: attempt,
		RequestSeq: seq, RequestBlob: reqRef, Provider: hop.Provider, Model: hop.Model, PromptVersion: hop.PromptVersion,
		Effort: sql.NullString{String: hop.Effort, Valid: hop.Effort != ""}, StartedAt: ledger.Now()}); err != nil {
		f.t.Fatal(err)
	}
	if status == "" {
		return nil // sent, never answered
	}
	resp := models.Response{Status: status, Model: hop.Name(), StopReason: map[string]string{
		StatusOK: "end_turn", StatusRefused: "refusal", StatusInvalidOutput: StatusInvalidOutput}[status]}
	b, _ := json.Marshal(resp)
	ref, err := f.g.Ledger.PutBlob(ctx, b)
	if err != nil {
		f.t.Fatal(err)
	}
	if err := f.g.Ledger.Q.FinishModelCall(ctx, db.FinishModelCallParams{ResponseBlob: sql.NullString{String: ref, Valid: true},
		StopReason: sql.NullString{String: resp.StopReason, Valid: true}, FinishedAt: sql.NullString{String: ledger.Now(), Valid: true},
		WorkflowID: "wf", ActivityID: id, Attempt: attempt, RequestSeq: seq}); err != nil {
		f.t.Fatal(err)
	}
	return &resp
}

func (f *resumeFixture) resume(info activity.Info, hops ...models.Route) ([]bool, *models.Response, *models.Response) {
	f.t.Helper()
	p, err := f.g.resume(context.Background(), info, f.run, "args", hops)
	if err != nil {
		f.t.Fatal(err)
	}
	return p.declined, p.refusal, p.answer
}

func hop(provider, model string) models.Route {
	return models.Route{Task: "audit_v1", Provider: provider, Model: model, PromptVersion: "audit.md@sha256:1",
		Effort: "high", Candidates: []string{provider + "/" + model}}
}

var (
	opus = hop("anthropic", "claude-opus-5-5")
	sol  = hop("openai", "gpt-6.1-sol")
	oss  = hop("local", "gpt-oss:20b")
)

func TestResumeWithinOneActivity(t *testing.T) {
	f := newResumeFixture(t)
	t0 := time.Date(2026, 10, 18, 10, 0, 0, 0, time.UTC)

	// The fallback answered and the completion was lost: the retry is served that answer,
	// not the refusal before it.
	info := f.call("1:audit_v1", t0, "")
	f.request(info.ActivityID, 1, 1, opus, StatusRefused)
	answered := f.request(info.ActivityID, 1, 2, sol, StatusOK)
	info.Attempt = 2
	if _, _, answer := f.resume(info, opus, sol, oss); answer == nil || answer.Model != answered.Model {
		t.Errorf("served %+v, want the fallback's answer", answer)
	}

	// The fallback failed (sent, never answered): the retry asks it, not the model that declined.
	info = f.call("2:audit_v1", t0.Add(time.Second), "")
	refused := f.request(info.ActivityID, 1, 1, opus, StatusRefused)
	f.request(info.ActivityID, 1, 2, sol, "")
	info.Attempt = 2
	declined, refusal, answer := f.resume(info, opus, sol, oss)
	if answer != nil || !slices.Equal(declined, []bool{true, false, false}) || refusal == nil || refusal.Model != refused.Model {
		t.Errorf("declined %v, refusal %+v, answer %+v", declined, refusal, answer)
	}

	// An answer that broke the schema is neither served nor a refusal: it counts against its
	// hop's generations, whichever attempt got it.
	info = f.call("3:audit_v1", t0.Add(2*time.Second), "")
	f.request(info.ActivityID, 1, 1, opus, StatusInvalidOutput)
	p, err := f.g.resume(context.Background(), info, f.run, "args", []models.Route{opus, sol})
	if err != nil || p.answer != nil || p.refusal != nil || slices.Contains(p.declined, true) ||
		!slices.Equal(p.invalid, []int{1, 0}) || p.lastInvalid[0] == nil {
		t.Errorf("an invalid answer: %+v, %v", p, err)
	}
}

func TestResumeAcrossBudgetPauses(t *testing.T) {
	f := newResumeFixture(t)
	// Scheduled times as RFC 3339 cuts them: as text, they sort the wrong way round.
	t0 := time.Date(2026, 10, 18, 10, 0, 0, 0, time.UTC)
	times := []time.Time{t0, t0.Add(100 * time.Millisecond), t0.Add(150 * time.Millisecond)}
	text := []string{}
	for _, at := range times {
		text = append(text, at.Format(time.RFC3339Nano))
	}
	if slices.IsSorted(text) {
		t.Fatalf("the times sort as text: %v", text)
	}
	// The first activity: Claude declined, OpenAI could not be reserved for. The second, sent
	// after the extension: OpenAI declined, the local model could not be reserved for.
	a := f.call("1:audit_v1", times[0], StatusBudgetExhausted)
	f.request(a.ActivityID, 1, 1, opus, StatusRefused)
	b := f.call("2:audit_v1", times[1], StatusBudgetExhausted)
	refused := f.request(b.ActivityID, 1, 1, sol, StatusRefused)
	c := f.call("3:audit_v1", times[2], "")
	declined, refusal, answer := f.resume(c, opus, sol, oss)
	if answer != nil || !slices.Equal(declined, []bool{true, true, false}) || refusal == nil || refusal.Model != refused.Model {
		t.Errorf("declined %v, refusal %+v, answer %+v", declined, refusal, answer)
	}

	// models.yaml reordered meanwhile: OpenAI first. Only the models that declined are passed
	// over, wherever they are now.
	if declined, _, _ := f.resume(c, oss, opus); !slices.Equal(declined, []bool{false, true}) {
		t.Errorf("reordered: declined %v", declined)
	}
	// The prompt edited since: its refusals were of another prompt, and nothing is passed over.
	edited := opus
	edited.PromptVersion = "audit.md@sha256:2"
	if declined, _, _ := f.resume(c, edited, oss); slices.Contains(declined, true) {
		t.Errorf("edited prompt: declined %v", declined)
	}
}

// Calls of the same arguments at once (a model task takes only its declared inputs, so two calls of
// the same diff are alike): one declined and broke the schema, then could not be reserved for on
// its fallback, and waited; the other waited too. Neither resumed call takes the other's past, its
// refusal or its broken answer, whatever order they were scheduled in, though every one of those
// requests matches the hops (their provider, model, prompt and effort). One call's own activities,
// one after another, carry theirs over (TestResumeAcrossBudgetPauses).
func TestResumeTakesNoOtherCallsPast(t *testing.T) {
	for _, otherFirst := range []bool{false, true} {
		f := newResumeFixture(t)
		t0 := time.Date(2026, 10, 18, 10, 0, 0, 0, time.UTC)
		a, b := t0, t0.Add(time.Millisecond) // b scheduled while a runs
		if otherFirst {
			a, b = b, a
		}
		mine := f.call("1:audit_v1", a, StatusBudgetExhausted)
		f.request(mine.ActivityID, 1, 1, opus, StatusInvalidOutput)
		f.request(mine.ActivityID, 1, 2, opus, StatusRefused)
		f.call("2:audit_v1", b, StatusBudgetExhausted)
		// Sent again after the extension, in either order.
		for i, at := range []time.Time{t0.Add(time.Second), t0.Add(2 * time.Second)} {
			again := f.call(fmt.Sprintf("%d:audit_v1", 3+i), at, "")
			p, err := f.g.resume(context.Background(), again, f.run, "args", []models.Route{opus, sol, oss})
			if err != nil || p.answer != nil || p.refusal != nil || slices.Contains(p.declined, true) ||
				!slices.Equal(p.invalid, []int{0, 0, 0}) || slices.ContainsFunc(p.lastInvalid, func(r *models.Response) bool { return r != nil }) {
				t.Errorf("other first %v, resent %d: %+v %v", otherFirst, i, p, err)
			}
		}
	}
	// One that ended after the next was scheduled overlapped it: not this call's either.
	f := newResumeFixture(t)
	t0 := time.Date(2026, 10, 18, 10, 0, 0, 0, time.UTC)
	late := f.call("1:audit_v1", t0, "")
	f.request(late.ActivityID, 1, 1, opus, StatusRefused)
	if err := f.g.Ledger.Q.SetCallOutcome(context.Background(), db.SetCallOutcomeParams{Outcome: sql.NullString{String: StatusBudgetExhausted, Valid: true},
		WorkflowID: "wf", ActivityID: late.ActivityID, ScheduledAt: t0.Format(time.RFC3339Nano)}); err != nil {
		t.Fatal(err)
	}
	f.ended(late.ActivityID, t0.Format(time.RFC3339Nano), t0.Add(2*time.Second), StatusBudgetExhausted)
	if declined, refusal, _ := f.resume(f.call("2:audit_v1", t0.Add(time.Second), ""), opus, sol); slices.Contains(declined, true) || refusal != nil {
		t.Errorf("an overlapping one: declined %v, refusal %+v", declined, refusal)
	}
	// One whose end agentd cannot find (its record past what it reads) may have overlapped it too.
	f = newResumeFixture(t)
	unknown := f.call("1:audit_v1", t0, "")
	f.request(unknown.ActivityID, 1, 1, opus, StatusRefused)
	if err := f.g.Ledger.Q.SetCallOutcome(context.Background(), db.SetCallOutcomeParams{Outcome: sql.NullString{String: StatusBudgetExhausted, Valid: true},
		WorkflowID: "wf", ActivityID: unknown.ActivityID, ScheduledAt: t0.Format(time.RFC3339Nano)}); err != nil {
		t.Fatal(err)
	}
	if declined, refusal, _ := f.resume(f.call("2:audit_v1", t0.Add(time.Second), ""), opus, sol); slices.Contains(declined, true) || refusal != nil {
		t.Errorf("one of no known end: declined %v, refusal %+v", declined, refusal)
	}
}

// Whether one call ended before the next began is read from agentd's own clock at both ends (the
// previous call's last record, the next call's created_at), not from Temporal's scheduled time on
// another host: a drift between the clocks must not make a short overlap look sequential. Here the
// scheduled times would call the two calls sequential (the first ends after the second was
// scheduled, so by scheduled time they overlap and nothing carries over), while agentd's own times
// make the chain: the first ended before agentd counted the second.
func TestResumeReadsOneClock(t *testing.T) {
	f := newResumeFixture(t)
	ctx := context.Background()
	t0 := time.Date(2026, 10, 18, 10, 0, 0, 0, time.UTC)
	// The paused call: scheduled at t0, agentd counted it at t0, its record at t0+10ms.
	sched := t0.UTC().Format(time.RFC3339Nano)
	if _, err := f.g.Ledger.Q.InsertCall(ctx, db.InsertCallParams{WorkflowID: "wf", ActivityID: "1:audit_v1", ScheduledAt: sched,
		Kind: "model", Name: "audit_v1", ArgsSha256: "args", CreatedAt: sched}); err != nil {
		t.Fatal(err)
	}
	if err := f.g.Ledger.Q.SetCallOutcome(ctx, db.SetCallOutcomeParams{Outcome: sql.NullString{String: StatusBudgetExhausted, Valid: true},
		WorkflowID: "wf", ActivityID: "1:audit_v1", ScheduledAt: sched}); err != nil {
		t.Fatal(err)
	}
	refused := f.request("1:audit_v1", 1, 1, opus, StatusRefused)
	f.ended("1:audit_v1", sched, t0.Add(10*time.Millisecond), StatusBudgetExhausted)
	// The resuming call: Temporal scheduled it at t0+5ms (before the paused call's record), but agentd
	// first counted it at t0+20ms (after it).
	if _, err := f.g.Ledger.Q.InsertCall(ctx, db.InsertCallParams{WorkflowID: "wf", ActivityID: "2:audit_v1",
		ScheduledAt: t0.Add(5 * time.Millisecond).UTC().Format(time.RFC3339Nano), Kind: "model", Name: "audit_v1",
		ArgsSha256: "args", CreatedAt: t0.Add(20 * time.Millisecond).UTC().Format(time.RFC3339Nano)}); err != nil {
		t.Fatal(err)
	}
	info := activity.Info{ActivityID: "2:audit_v1", ScheduledTime: t0.Add(5 * time.Millisecond), Attempt: 1}
	if declined, refusal, _ := f.resume(info, opus, sol); !slices.Equal(declined, []bool{true, false}) ||
		refusal == nil || refusal.Model != refused.Model {
		t.Errorf("declined %v, refusal %+v", declined, refusal)
	}
}

func TestResumeChainStopsAtACallThatDidNotWait(t *testing.T) {
	f := newResumeFixture(t)
	t0 := time.Date(2026, 10, 18, 10, 0, 0, 0, time.UTC)
	// An earlier call with the same arguments that ended in a refusal, or whose outcome was
	// never recorded, is another call: what it got is not this one's.
	for i, outcome := range []string{StatusRefused, ""} {
		earlier := f.call(string(rune('a'+i))+":audit_v1", t0.Add(time.Duration(2*i)*time.Second), outcome)
		f.request(earlier.ActivityID, 1, 1, opus, StatusRefused)
		now := f.call(string(rune('a'+i))+"-again:audit_v1", t0.Add(time.Duration(2*i+1)*time.Second), "")
		if declined, refusal, _ := f.resume(now, opus, sol); slices.Contains(declined, true) || refusal != nil {
			t.Errorf("after an outcome %q: declined %v", outcome, declined)
		}
	}
	// An answer in a call that waited for budget is not served (it could not have been returned).
	paused := f.call("p:audit_v1", t0.Add(10*time.Second), StatusBudgetExhausted)
	f.request(paused.ActivityID, 1, 1, opus, StatusOK)
	now := f.call("p-again:audit_v1", t0.Add(11*time.Second), "")
	if _, _, answer := f.resume(now, opus, sol); answer != nil {
		t.Errorf("served another activity's answer: %+v", answer)
	}
}

func TestTooLate(t *testing.T) {
	g := &Gateway{Limits: LoopLimits{Attempts: 10}}
	route := models.Route{Task: "audit_v1", Provider: "openai", Model: "gpt-6.1-sol", MaxTokens: 16000} // 7.5 minutes
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	policy := &temporal.RetryPolicy{MaximumAttempts: 5}

	// The attempt's first request goes out whatever is left: there is nothing better to do.
	if err := g.tooLate(ctx, activity.Info{Attempt: 1, RetryPolicy: policy}, route, 1); err != nil {
		t.Errorf("first request: %v", err)
	}
	// A later one waits for the next attempt...
	err := g.tooLate(ctx, activity.Info{Attempt: 1, RetryPolicy: policy}, route, 2)
	var app *temporal.ApplicationError
	if err == nil || (errors.As(err, &app) && app.NonRetryable()) {
		t.Errorf("a later request: %v", err)
	}
	// ...unless there is none: then it fails for good, and a fallback is passed over.
	for _, info := range []activity.Info{{Attempt: 5, RetryPolicy: policy}, {Attempt: 10}} {
		err := g.tooLate(ctx, info, route, 2)
		if !errors.As(err, &app) || !app.NonRetryable() || app.Type() != "NoTimeLeft" {
			t.Errorf("attempt %d: %v", info.Attempt, err)
		}
	}
	// With the time it needs, it goes out.
	roomy, cancel2 := context.WithTimeout(context.Background(), 9*time.Minute)
	defer cancel2()
	if err := g.tooLate(roomy, activity.Info{Attempt: 1, RetryPolicy: policy}, route, 3); err != nil {
		t.Errorf("enough time: %v", err)
	}
}
