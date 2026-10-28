package faults

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A rule's tool error and its hold inside a tool server's call reach the attempt it matches.
func TestToolFaults(t *testing.T) {
	hold := filepath.Join(t.TempDir(), "hold")
	s := &Script{counts: map[string]int{}, Rules: []Rule{
		{Name: "git.show_diff", Attempt: 1, ToolError: "bad object"},
		{Name: "git.show_diff", Attempt: 2, HoldInToolWhileExists: hold},
	}}
	if e := s.Match("r", "git.show_diff", 1); e.ToolError != "bad object" || e.HoldInToolWhileExists != "" {
		t.Fatalf("%+v", e)
	}
	e := s.Match("r", "git.show_diff", 2)
	if e.ToolError != "" || e.HoldInToolWhileExists != hold {
		t.Fatalf("%+v", e)
	}
	// The hold goes with the call's context to the tool server's client, and holds while the file is there.
	if err := os.WriteFile(hold, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { StallInTool(e.InTool(context.Background())); close(done) }()
	select {
	case <-done:
		t.Fatal("did not hold")
	case <-time.After(200 * time.Millisecond):
	}
	os.Remove(hold)
	<-done
	// No hold, no context of its own: nothing holds.
	if ctx := (Effect{}).InTool(context.Background()); ctx != context.Background() {
		t.Fatal("a context for nothing")
	}
	StallInTool(context.Background())
}

// The Worker's hold, as the harness writes it, lasts while its file is there, and a script without
// one, or no script, holds nothing.
func TestHoldWorker(t *testing.T) {
	dir := t.TempDir()
	hold, path := filepath.Join(dir, "hold"), filepath.Join(dir, "faults.json")
	b, err := json.Marshal(map[string]any{"rules": []any{}, "hold_worker_while_exists": hold})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if s.HoldWorker() {
		t.Error("held with no file")
	}
	if err := os.WriteFile(hold, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if !s.HoldWorker() {
		t.Error("not held with the file there")
	}
	var none *Script
	if (&Script{}).HoldWorker() || none.HoldWorker() {
		t.Error("held by a script that names no file, or by no script")
	}
}

// A status fault's value is the result's, numbers as written: a script can give the Worker a value
// it cannot read. A value that is not an object is the script's mistake.
func TestAStatusValue(t *testing.T) {
	digits := strings.Repeat("9", 5000)
	s := &Script{counts: map[string]int{}, Rules: []Rule{
		{Name: "github.comment_commit", Status: "ok", Value: json.RawMessage(`{"url":"u","id":` + digits + `}`)},
		{Name: "git.show_diff", Status: "ok"},
		{Name: "audit_v1", Status: "ok", Value: json.RawMessage(`[1]`)},
	}}
	v, err := s.Match("r", "github.comment_commit", 1).Result()
	if err != nil || v["url"] != "u" || v["id"] != json.Number(digits) {
		t.Fatalf("%v %v", v, err)
	}
	if v, err := s.Match("r", "git.show_diff", 1).Result(); v != nil || err != nil {
		t.Fatalf("%v %v", v, err)
	}
	if _, err := s.Match("r", "audit_v1", 1).Result(); err == nil {
		t.Fatal("a value that is not an object")
	}
}
