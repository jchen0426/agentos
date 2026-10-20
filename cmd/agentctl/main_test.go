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

func TestUSD(t *testing.T) {
	for in, want := range map[string]int64{"2.50": 2_500_000, "$1": 1_000_000, "0.000001": 1, "10.123456": 10_123_456} {
		if got, err := parseUSD(in); err != nil || got != want {
			t.Errorf("parseUSD(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "0", "0.00", "-1", "1.2345678", "1,5", "1e3", ".5", "99999999999999999999"} {
		if got, err := parseUSD(in); err == nil {
			t.Errorf("parseUSD(%q) = %d, want an error", in, got)
		}
	}
	for micros, want := range map[int64]string{5_000_000: "$5.00", 4500: "$0.0045", 1: "$0.000001", 0: "$0.00", 12_340_000: "$12.34"} {
		if got := usd(micros); got != want {
			t.Errorf("usd(%d) = %q, want %q", micros, got, want)
		}
	}
}

// An approval card shows text a model, a tool or the outside world wrote: the patch, the
// findings, the payload. On the terminal none of it can rewrite the screen or forge a line of
// the card (a second status, a payload hash), and the patch still reads as written, line by line.
func TestApprovalCardShowsTheirTextInert(t *testing.T) {
	a := &agentosv1.Approval{
		ApprovalId: "a1", RunId: "r1", Tool: "github.open_draft_pr", Status: "PENDING",
		CreatedAt: "2026-10-20T00:00:00.000000000Z", ExpiresAt: "2026-10-21T00:00:00.000000000Z",
		PayloadSha256: strings.Repeat("ab", 32),
		PayloadJson:   `{"title":"fix\u001b[2J","body":"x\nstatus  APPROVED"}`,
		ContextJson: `{"task":{"findings":[{"cwe":"CWE-89","title":"t\u009b2J"}]},` +
			`"write":{"patch":"--- a/x\n+++ b/x\n@@ -1 +1 @@\n-a\n+b\u001b]0;pwned\u0007\nstatus\tAPPROVED\n"}}`,
	}
	var b bytes.Buffer
	if err := writeApproval(&b, a); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, r := range out {
		if !unicode.IsPrint(r) && r != '\n' && r != '\t' {
			t.Fatalf("raw %U in the card:\n%s", r, out)
		}
	}
	statuses := 0
	for _, l := range strings.Split(out, "\n") {
		if strings.HasPrefix(l, "status") {
			statuses++
		}
	}
	if statuses != 1 || !strings.Contains(out, "      +++ b/x\n") || !strings.Contains(out, "agentctl approvals approve a1 --payload-sha256 "+a.PayloadSha256) {
		t.Fatalf("card:\n%s", out)
	}
}
