package gateway

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"path/filepath"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"golang.org/x/time/rate"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
	"github.com/jchen0426/agentos/internal/tasks"
)

func TestCanonicalArgs(t *testing.T) {
	// Python's json.dumps(sort_keys=True) output and other spellings of the same object
	// hash the same.
	a, ca, err := canonicalArgs(`{"repo": "acme/payments", "commit": "3f2a", "n": 1.50, "s": "\u00e9<>"}`)
	if err != nil {
		t.Fatal(err)
	}
	_, cb, err := canonicalArgs(`{"s":"é<>","n":1.50,"commit":"3f2a","repo":"acme/payments"}`)
	if err != nil {
		t.Fatal(err)
	}
	if string(ca) != string(cb) {
		t.Errorf("canonical forms differ:\n%s\n%s", ca, cb)
	}
	if want := `{"commit":"3f2a","n":1.50,"repo":"acme/payments","s":"é<>"}`; string(ca) != want {
		t.Errorf("got %s, want %s", ca, want)
	}
	if a["repo"] != "acme/payments" {
		t.Errorf("decoded args: %v", a)
	}
}

func TestCanonicalArgsRejectsNonObjects(t *testing.T) {
	for _, s := range []string{``, `null`, `[]`, `"x"`, `{"a":1} {"b":2}`, `{"a":`} {
		if _, _, err := canonicalArgs(s); err == nil {
			t.Errorf("%q: accepted", s)
		}
	}
}

func TestHeartbeatIntervalNeverPanicsTheTicker(t *testing.T) {
	// The workflow chooses the heartbeat timeout; 1ns to 3ns would give a zero interval.
	for _, timeout := range []time.Duration{1, 2, 3, time.Millisecond, 299 * time.Millisecond} {
		if got := heartbeatInterval(timeout); got < 100*time.Millisecond {
			t.Errorf("heartbeatInterval(%v) = %v", timeout, got)
		}
	}
	if got := heartbeatInterval(30 * time.Second); got != 10*time.Second {
		t.Errorf("heartbeatInterval(30s) = %v, want 10s", got)
	}
}

func TestScopeComesFromTheRunsResolvedParameters(t *testing.T) {
	g := &Gateway{Tasks: tasks.Registry{"security-audit": tasks.SecurityAudit(nil, nil, nil)}}
	run := db.Run{Task: "security-audit", ParamsJson: `{"repo":"acme/payments","commit":"3f2a","base":"main"}`}
	got, err := g.scope(run)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"repo": "acme/payments", "commit": "3f2a", "base_branch": "main"}
	if !maps.Equal(got, want) {
		t.Errorf("scope: got %v, want %v", got, want)
	}
	for name, r := range map[string]db.Run{
		"unregistered task": {Task: "other", ParamsJson: run.ParamsJson},
		"missing parameter": {Task: "security-audit", ParamsJson: `{"repo":"acme/payments","commit":"3f2a"}`},
		"broken parameters": {Task: "security-audit", ParamsJson: `{`},
	} {
		if _, err := g.scope(r); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

func TestFillScopeAddsWhatTheCallLeftOut(t *testing.T) {
	args, _, err := canonicalArgs(`{"repo":"acme/payments","patch_ref":"blob:1"}`)
	if err != nil {
		t.Fatal(err)
	}
	scope := map[string]string{"repo": "acme/payments", "commit": "3f2a", "base_branch": "main"}
	declared := []string{"base_branch", "commit", "patch_ref", "repo"}
	filled, canon, err := fillScope(args, scope, declared)
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"base_branch":"main","commit":"3f2a","patch_ref":"blob:1","repo":"acme/payments"}`; string(canon) != want {
		t.Errorf("canonical form: got %s, want %s", canon, want)
	}
	if filled["commit"] != "3f2a" || filled["base_branch"] != "main" {
		t.Errorf("filled: %v", filled)
	}
	if _, ok := args["commit"]; ok {
		t.Error("the caller's map was changed")
	}

	// A value the call names is kept, so the policy can deny it.
	args = map[string]any{"repo": "acme/ledger"}
	if filled, _, _ := fillScope(args, scope, declared); filled["repo"] != "acme/ledger" {
		t.Errorf("a named value was overwritten: %v", filled)
	}

	// A tool that takes no base branch is not given one: its schema would refuse it.
	if filled, _, _ := fillScope(map[string]any{}, scope, []string{"commit", "repo"}); len(filled) != 2 {
		t.Errorf("filled beyond what the tool declares: %v", filled)
	}
}

func TestLoopGuard(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(ctx, filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err := l.Q.InsertRun(ctx, db.InsertRunParams{WorkflowID: "wf", Task: "security-audit", ParamsJson: "{}",
		BudgetLimitMicros: 1, CreatedAt: ledger.Now()}); err != nil {
		t.Fatal(err)
	}
	g := &Gateway{Ledger: l, Limits: LoopLimits{ModelCalls: 2, GatewayCalls: 6, SameToolCalls: 2}}
	scheduled := map[string]time.Time{} // the server's first-scheduled time, per activity ID
	call := func(activityID, kind, name, args string) string {
		t.Helper()
		if _, ok := scheduled[activityID]; !ok {
			scheduled[activityID] = time.Now()
		}
		reason, err := g.guard(ctx, db.Run{WorkflowID: "wf"}, activity.Info{ActivityID: activityID, ScheduledTime: scheduled[activityID]},
			&agentosv1.Call{Kind: kind, Name: name}, []byte(args), &callRecord{})
		if err != nil {
			t.Fatal(err)
		}
		return reason
	}
	steps := []struct {
		activityID, kind, name, args string
		refused                      bool
	}{
		{"1:git.show_diff", "tool", "git.show_diff", `{"a":1}`, false},
		{"2:git.show_diff", "tool", "git.show_diff", `{"a":1}`, false},
		{"2:git.show_diff", "tool", "git.show_diff", `{"a":1}`, false}, // a retry counts once
		{"3:git.show_diff", "tool", "git.show_diff", `{"a":1}`, true},  // a third time, same arguments
		{"4:git.show_diff", "tool", "git.show_diff", `{"a":2}`, false}, // other arguments
		{"5:audit_v1", "model", "audit_v1", `{"d":1}`, false},
		{"6:audit_v1", "model", "audit_v1", `{"d":1}`, false},        // sent again after a pause: not a loop
		{"7:audit_v1", "model", "audit_v1", `{"d":2}`, true},         // a third model call
		{"8:git.apply_check", "tool", "git.apply_check", `{}`, true}, // the seventh call of the run
	}
	for _, s := range steps {
		if reason := call(s.activityID, s.kind, s.name, s.args); (reason != "") != s.refused {
			t.Errorf("%s %s: refused=%q, want refused=%v", s.activityID, s.args, reason, s.refused)
		}
	}

	// A call that only waited (here for a reconciliation) is sent again after the wait: that
	// is a step, not a repeat of the call.
	g.Limits = LoopLimits{GatewayCalls: 10, SameToolCalls: 1}
	if err := l.Q.InsertRun(ctx, db.InsertRunParams{WorkflowID: "wf3", Task: "security-audit", ParamsJson: "{}",
		BudgetLimitMicros: 1, CreatedAt: ledger.Now()}); err != nil {
		t.Fatal(err)
	}
	waited := func(activityID, outcome string) string {
		at := time.Now()
		reason, err := g.guard(ctx, db.Run{WorkflowID: "wf3"}, activity.Info{ActivityID: activityID, ScheduledTime: at},
			&agentosv1.Call{Kind: "tool", Name: "github.open_draft_pr"}, []byte(`{}`), &callRecord{})
		if err != nil {
			t.Fatal(err)
		}
		if err := l.Q.SetCallOutcome(ctx, db.SetCallOutcomeParams{Outcome: sql.NullString{String: outcome, Valid: true},
			WorkflowID: "wf3", ActivityID: activityID, ScheduledAt: at.UTC().Format(time.RFC3339Nano)}); err != nil {
			t.Fatal(err)
		}
		return reason
	}
	for i, outcome := range []string{"needs_reconcile", "needs_reconcile", "needs_reconcile", "ok"} {
		if reason := waited(fmt.Sprintf("%d:github.open_draft_pr", i+1), outcome); reason != "" {
			t.Fatalf("call %d after %d waits was refused: %s", i+1, i, reason)
		}
	}
	if waited("5:github.open_draft_pr", "ok") == "" {
		t.Error("a repeat after the call went through was not refused")
	}

	// A Worker that reuses an activity ID for a new call cannot hide it: Temporal schedules
	// the new activity at a new time, and the guard counts it.
	g.Limits = LoopLimits{GatewayCalls: 2}
	if err := l.Q.InsertRun(ctx, db.InsertRunParams{WorkflowID: "wf2", Task: "security-audit", ParamsJson: "{}",
		BudgetLimitMicros: 1, CreatedAt: ledger.Now()}); err != nil {
		t.Fatal(err)
	}
	reused := func(at time.Time) string {
		reason, err := g.guard(ctx, db.Run{WorkflowID: "wf2"}, activity.Info{ActivityID: "1:x", ScheduledTime: at},
			&agentosv1.Call{Kind: "tool", Name: "git.show_diff"}, []byte(`{}`), &callRecord{})
		if err != nil {
			t.Fatal(err)
		}
		return reason
	}
	t0 := time.Now()
	if reused(t0) != "" || reused(t0) != "" || reused(t0.Add(time.Second)) != "" {
		t.Fatal("the first two calls were refused")
	}
	if reused(t0.Add(2*time.Second)) == "" {
		t.Error("a third call under a reused activity ID was not counted")
	}
}

func TestWaitTurnGivesUpRatherThanWaitLong(t *testing.T) {
	g := &Gateway{Limiters: map[string]*rate.Limiter{"slow": rate.NewLimiter(rate.Every(time.Hour), 1)}}
	ctx := context.Background()
	if err := g.waitTurn(ctx, "slow"); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := g.waitTurn(ctx, "slow"); err == nil {
		t.Error("a second request within the hour got a turn")
	}
	if time.Since(start) > time.Second {
		t.Error("waitTurn waited instead of giving up at once")
	}
	if err := g.waitTurn(ctx, "unlimited"); err != nil {
		t.Errorf("a provider without a limit: %v", err)
	}
}
