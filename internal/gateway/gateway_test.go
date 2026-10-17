package gateway

import (
	"testing"
	"time"
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
