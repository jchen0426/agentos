package gateway

import (
	"maps"
	"testing"
	"time"

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
	g := &Gateway{Tasks: tasks.Registry{"security-audit": tasks.SecurityAudit(nil, nil)}}
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
	filled, canon, err := fillScope(args, scope)
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
	if filled, _, _ := fillScope(args, scope); filled["repo"] != "acme/ledger" {
		t.Errorf("a named value was overwritten: %v", filled)
	}
}
