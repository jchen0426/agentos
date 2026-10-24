package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode"

	"google.golang.org/grpc"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
)

// Every text in an explanation can quote the Worker, a tool, a model or a person: shown, each stays
// on its line, and nothing in it reaches the terminal raw.
func TestTheExplanationShowsTheirTextInert(t *testing.T) {
	bad := "boom\x1b]0;title\x07\nverdict\tok\r\u202e"
	e := &agentosv1.Explanation{RunId: bad, State: bad, Verdict: bad, Kind: bad, Step: bad, Call: bad, ActivityId: bad,
		Attempt: 2, Cause: bad, Claim: bad, Next: bad, Evidence: []string{bad, bad},
		Incidents: []*agentosv1.Incident{{At: bad, Step: bad, Attempt: 1, Kind: bad, Cause: bad}}, IncidentsOmitted: 3,
		Cost:  &agentosv1.Cost{SpentMicros: 1, LimitMicros: 2},
		Usage: []*agentosv1.ModelUsage{{Model: bad, Requests: 1, Refusals: 2}}, Missing: []string{bad}}
	var b bytes.Buffer
	if err := writeExplanation(&b, e); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, r := range out {
		if !unicode.IsPrint(r) && r != '\n' {
			t.Fatalf("raw %U in the output:\n%s", r, out)
		}
	}
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	if len(lines) != 15 {
		t.Fatalf("%d lines, want one per value:\n%s", len(lines), out)
	}
	verdicts := 0
	for _, l := range lines {
		if strings.HasPrefix(l, "verdict") {
			verdicts++
		}
	}
	if verdicts != 1 || !strings.Contains(out, "1 request, 0 input tokens") || !strings.Contains(out, "2 refusals") ||
		!strings.Contains(out, "and 3 incidents more, not listed") {
		t.Fatalf("output:\n%s", out)
	}
}

// What the Worker or a model chose is printed quoted: a quote of its own cannot end it, and it
// cannot pass for agentctl's words.
func TestTheirWordsAreQuoted(t *testing.T) {
	claim := `ok" (its own claim, not checked)`
	e := &agentosv1.Explanation{Verdict: "ok", Step: "analyze", Call: `audit_v1", activity "x`, ActivityId: `2:a" b`,
		Claim: claim, Usage: []*agentosv1.ModelUsage{{Model: `m" 1 request`}},
		Incidents: []*agentosv1.Incident{{Step: "analyze", ActivityId: `2:a" b`, Kind: "model_error", Cause: "c"}}}
	var b bytes.Buffer
	if err := writeExplanation(&b, e); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, want := range []string{`workflow says  "ok\" (its own claim, not checked)" (its own claim, not checked)`,
		`step           analyze (call "audit_v1\", activity \"x", activity "2:a\" b")`, `usage          "m\" 1 request": 0 requests`,
		`analyze (activity "2:a\" b") model_error: c`} {
		if !strings.Contains(out, want) {
			t.Errorf("no %s in:\n%s", want, out)
		}
	}
}

// partStream plays an export's parts back.
type partStream struct{ parts []*agentosv1.AuditExportPart }

func (s *partStream) Recv() (*agentosv1.AuditExportPart, error) {
	if len(s.parts) == 0 {
		return nil, io.EOF
	}
	p := s.parts[0]
	s.parts = s.parts[1:]
	return p, nil
}

func history(h *agentosv1.ExecutionHistory) *agentosv1.AuditExportPart {
	return &agentosv1.AuditExportPart{Part: &agentosv1.AuditExportPart_History{History: h}}
}

func head(h *agentosv1.AuditExportHead) *agentosv1.AuditExportPart {
	return &agentosv1.AuditExportPart{Part: &agentosv1.AuditExportPart_Head{Head: h}}
}

func record(r *agentosv1.AuditRecord) *agentosv1.AuditExportPart {
	return &agentosv1.AuditExportPart{Part: &agentosv1.AuditExportPart_Record{Record: r}}
}

func pushed(h *agentosv1.PushedHead) *agentosv1.AuditExportPart {
	return &agentosv1.AuditExportPart{Part: &agentosv1.AuditExportPart_PushedHead{PushedHead: h}}
}

// An export is written as one JSON document, as the parts come; a stream out of order, or short of
// what its head says, is not an export.
func TestTheExportIsOneDocument(t *testing.T) {
	uid := int64(501)
	parts := func() []*agentosv1.AuditExportPart {
		return []*agentosv1.AuditExportPart{
			history(&agentosv1.ExecutionHistory{RunId: "r1", TemporalRunId: "e1", HistoryJson: `{"events":[{"eventId":"1"}]}`,
				Anchors: []*agentosv1.ExportedAnchor{{EventId: 7, ActivityId: "1:x", Attempt: 1, Seq: 2, Sha256: "s2",
					ResultJson: []byte(`{"a":"\u003c\b"}`)}}}),
			history(&agentosv1.ExecutionHistory{RunId: "r2", TemporalRunId: "e2", Unavailable: "gone"}),
			pushed(&agentosv1.PushedHead{AnchorWorkflowId: "agentos-anchor-x", Seq: 1, Sha256: "s1"}),
			head(&agentosv1.AuditExportHead{Seq: 2, Sha256: "s2", Records: 2, ExportedAt: "now"}),
			record(&agentosv1.AuditRecord{Seq: 1, Type: "agentd.started", Actor: "agentd", Payload: "{\"x\":\"\u009b\u2028\"}",
				PrevSha256: "0", Sha256: "s1"}),
			record(&agentosv1.AuditRecord{Seq: 2, WorkflowId: "r1", Type: "call", Actor: "operator:me", PeerUid: &uid,
				Payload: "{}", PrevSha256: "s1", Sha256: "s2"}),
		}
	}
	var b bytes.Buffer
	if cut, err := writeExport(&b, &partStream{parts()}); err != nil || cut != "" {
		t.Fatal(err)
	}
	if strings.ContainsRune(b.String(), '\u009b') {
		t.Fatal("a raw C1 control in the export")
	}
	var doc struct {
		Format  string
		Hashes  map[string]string
		RunID   string `json:"run_id"`
		Head    exportHead
		Records []exportRecord
		Runs    []exportRun
		Pushed  []exportPushedHead `json:"pushed_heads"`
	}
	if err := json.Unmarshal(b.Bytes(), &doc); err != nil {
		t.Fatalf("%v:\n%s", err, b.String())
	}
	if len(doc.Pushed) != 1 || doc.Pushed[0] != (exportPushedHead{AnchorWorkflowID: "agentos-anchor-x", Seq: 1, Sha256: "s1"}) ||
		string(doc.Runs[0].Anchors[0].ResultJSON) != `{"a":"\u003c\b"}` || !strings.Contains(b.String(), `"result_json":"eyJhIjoiXHUwMDNjXGIifQ=="`) {
		t.Fatalf("%+v\n%s", doc, b.String())
	}
	if doc.Format != exportFormat || len(doc.Hashes) != 4 || doc.Head.Seq != 2 || len(doc.Records) != 2 || len(doc.Runs) != 2 ||
		doc.Records[0].PeerUID != nil || *doc.Records[1].PeerUID != 501 || doc.Records[0].Payload != "{\"x\":\"\u009b\u2028\"}" ||
		string(doc.Runs[0].History) != `{"events":[{"eventId":"1"}]}` || doc.Runs[0].Anchors[0].Seq != 2 ||
		doc.Runs[1].Unavailable != "gone" || doc.Runs[1].Anchors == nil {
		t.Fatalf("%+v", doc)
	}
	// Out of order, short, or a history that is not JSON: refused.
	for name, mangle := range map[string]func([]*agentosv1.AuditExportPart) []*agentosv1.AuditExportPart{
		"no head":            func(p []*agentosv1.AuditExportPart) []*agentosv1.AuditExportPart { return p[:3] },
		"a record short":     func(p []*agentosv1.AuditExportPart) []*agentosv1.AuditExportPart { return p[:5] },
		"a history after it": func(p []*agentosv1.AuditExportPart) []*agentosv1.AuditExportPart { return append(p, p[0]) },
		"a record before it": func(p []*agentosv1.AuditExportPart) []*agentosv1.AuditExportPart { return append(p[4:5], p...) },
		"two heads":          func(p []*agentosv1.AuditExportPart) []*agentosv1.AuditExportPart { return append(p[:4], p[3:]...) },
		"a history after the pushed heads": func(p []*agentosv1.AuditExportPart) []*agentosv1.AuditExportPart {
			return append([]*agentosv1.AuditExportPart{p[0], p[2], p[1]}, p[3:]...)
		},
		"a pushed head after the chain": func(p []*agentosv1.AuditExportPart) []*agentosv1.AuditExportPart { return append(p, p[2]) },
		"a history not JSON": func(p []*agentosv1.AuditExportPart) []*agentosv1.AuditExportPart {
			p[0].GetHistory().HistoryJson = "{"
			return p
		},
		"a part it does not know": func(p []*agentosv1.AuditExportPart) []*agentosv1.AuditExportPart {
			return append(p[:1], append([]*agentosv1.AuditExportPart{{}}, p[1:]...)...)
		},
	} {
		if _, err := writeExport(io.Discard, &partStream{mangle(parts())}); err == nil {
			t.Errorf("%s: taken", name)
		}
	}
}

// An export cut short at agentd's bound says so in its head, and agentctl says it.
func TestACutExportSaysSo(t *testing.T) {
	parts := []*agentosv1.AuditExportPart{
		head(&agentosv1.AuditExportHead{Seq: 9, Sha256: "s9", Records: 1, ExportedAt: "now",
			Cut: "the chain's records from seq 2 on are not in the export"}),
		record(&agentosv1.AuditRecord{Seq: 1, Type: "agentd.started", Actor: "agentd", Payload: "{}", PrevSha256: "0", Sha256: "s1"}),
	}
	var b bytes.Buffer
	cut, err := writeExport(&b, &partStream{parts})
	var doc struct{ Head exportHead }
	if err != nil || !strings.Contains(cut, "from seq 2 on") || json.Unmarshal(b.Bytes(), &doc) != nil || doc.Head.Cut != cut {
		t.Fatalf("%q %v\n%s", cut, err, b.String())
	}
}

// exporter is an agentd whose export is the parts a test gives.
type exporter struct {
	agentosv1.UnimplementedControlServer
	parts []*agentosv1.AuditExportPart
}

func (e exporter) ExportAudit(_ *agentosv1.ExportAuditRequest, s agentosv1.Control_ExportAuditServer) error {
	for _, p := range e.parts {
		if err := s.Send(p); err != nil {
			return err
		}
	}
	return nil
}

// agentctl audit export exits 2 for an export agentd cut short, saying why (agentctl prints it on
// standard error), the document whole all the same, with the cut in its head: as audit verify does
// when not everything could be checked. A whole export exits 0.
func TestAgentctlSaysAnExportIsCut(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "agentctl-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	socket := filepath.Join(dir, "control.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	rec := record(&agentosv1.AuditRecord{Seq: 1, Type: "agentd.started", Actor: "agentd", Payload: "{}", PrevSha256: "0", Sha256: "s1"})
	ex := &exporter{}
	srv := grpc.NewServer()
	agentosv1.RegisterControlServer(srv, ex)
	go srv.Serve(ln)
	t.Cleanup(srv.Stop)
	out := filepath.Join(dir, "export.json")
	for _, cut := range []string{"the chain's records from seq 2 on are not in the export: \x1b[2Jbound", ""} {
		ex.parts = []*agentosv1.AuditExportPart{head(&agentosv1.AuditExportHead{Seq: 2, Sha256: "s2", Records: 1,
			ExportedAt: "now", Cut: cut}), rec}
		os.Remove(out)
		cmd := auditExportCmd(&globals{socket: socket, output: "text"})
		cmd.SetArgs([]string{"-o", out})
		cmd.SetOut(io.Discard)
		cmd.SetErr(io.Discard)
		err := cmd.Execute()
		var exit exitError
		switch {
		case cut == "" && err != nil:
			t.Fatalf("a whole export: %v", err)
		case cut != "" && (!errors.As(err, &exit) || exit.code != 2 ||
			!strings.Contains(err.Error(), "the export is cut short: the chain's records from seq 2 on") ||
			strings.Contains(err.Error(), "\x1b")):
			t.Fatalf("a cut export: %v", err)
		}
		var doc struct{ Head exportHead }
		if b, rerr := os.ReadFile(out); rerr != nil || json.Unmarshal(b, &doc) != nil || doc.Head.Cut != cut {
			t.Fatalf("the document: %v, head %+v", rerr, doc.Head)
		}
	}
}

// An export written to a file is whole or not there, and only its owner reads it.
func TestTheExportFileIsWholeOrNotThere(t *testing.T) {
	path := filepath.Join(t.TempDir(), "export.json")
	if err := writeFileWhole(path, func(w io.Writer) error { _, err := writeExport(w, &partStream{}); return err }); err == nil {
		t.Fatal("a broken export was written")
	}
	if entries, _ := os.ReadDir(filepath.Dir(path)); len(entries) != 0 {
		t.Fatalf("left behind: %v", entries)
	}
	if err := writeFileWhole(path, func(w io.Writer) error { _, err := w.Write([]byte("{}")); return err }); err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("%v %v", fi, err)
	}
}
