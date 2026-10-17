package main

import (
	"bytes"
	"strings"
	"testing"
	"unicode"

	"google.golang.org/protobuf/encoding/protojson"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
)

// A failed run whose failure message the Worker wrote to rewrite the screen and forge a line.
var hostile = &agentosv1.Run{
	RunId: "r1", Task: "security-audit", State: "FAILED", CreatedAt: "2026-10-17T00:00:00Z",
	ResultJson: "{\"x\":\"\u009b2J‮\"}",
	Failure:    "boom\x1b]0;title\x07\nstate\tSUCCEEDED",
}

func TestTextOutputShowsWorkerTextInert(t *testing.T) {
	var b bytes.Buffer
	if err := writeRun(&b, hostile); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, r := range out {
		if !unicode.IsPrint(r) && r != '\n' {
			t.Fatalf("raw %U in the output:\n%s", r, out)
		}
	}
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	if len(lines) != 8 {
		t.Errorf("got %d lines, want 8 (one per field):\n%s", len(lines), out)
	}
	states := 0
	for _, l := range lines {
		if strings.HasPrefix(l, "state ") {
			states++
		}
	}
	if states != 1 {
		t.Errorf("got %d state lines, want 1:\n%s", states, out)
	}
}

func TestJSONOutputShowsWorkerTextInert(t *testing.T) {
	b, err := encodeJSON(hostile)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range string(b) {
		if !unicode.IsPrint(r) {
			t.Fatalf("raw %U in %s", r, b)
		}
	}
	var back agentosv1.Run
	if err := protojson.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if back.GetFailure() != hostile.GetFailure() || back.GetResultJson() != hostile.GetResultJson() {
		t.Errorf("the JSON no longer decodes to the same run: %s", b)
	}
}
