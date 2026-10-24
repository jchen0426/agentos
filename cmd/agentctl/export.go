package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"

	agentosv1 "github.com/jchen0426/agentos/gen/go/agentos/v1"
	"github.com/jchen0426/agentos/internal/safetext"
)

// exportFormat names the export's layout, which a reader checks it knows.
const exportFormat = "agentos-audit-export/1"

// How a reader recomputes what the export holds, without agentd (internal/audit's package comment).
var exportHashes = map[string]string{
	"record_sha256": "a record's sha256 is the lower-case hex sha256 of the netstrings (<length in bytes>:<bytes>,) of " +
		"\"agentos-audit/1\", then its seq in decimal, at, workflow_id, type, actor, peer_uid in decimal (empty when " +
		"null), payload (the text as given) and prev_sha256; the first record's prev_sha256 is 64 zeros, and each " +
		"other's is the sha256 of the record before it",
	"result_sha256": "a call record's result_sha256 (in its payload) is the lower-case hex sha256 of the bytes of the anchor " +
		"that names the record, its result_json (base64 here): the result's value_json, as the history holds it, without " +
		"its _audit key, as Go's encoding/json encodes it as a map: keys sorted, numbers as written, and in strings \" and " +
		"\\ escaped, \\b \\f \\n \\r \\t for those runes, \\u00XX (lower-case hex) for the other runes below U+0020, " +
		"\\u003c \\u003e \\u0026 for < > &, \\u2028 \\u2029 for those runes, \\ufffd for invalid UTF-8, and every " +
		"other rune as itself; a reader checks the sha256 of those bytes, and that they parse to the value in the history",
	"pushed_head": "a head agentd pushed to an anchor workflow in Temporal names the record at its seq: that record's " +
		"sha256 is the head's",
	"history": "a history is agentd's protojson encoding of the events Temporal returned to it, not Temporal's own bytes: " +
		"a reader with access to Temporal compares the events",
}

func auditExportCmd(g *globals) *cobra.Command {
	var run, out string
	var timeout time.Duration
	cmd := &cobra.Command{
		Use:   "export [--run <run-id>] [-o FILE]",
		Short: "Export the audit log and the runs' histories in Temporal, as one JSON document",
		Long: "Writes one JSON document: each run's executions' histories as agentd reads them from Temporal, with\n" +
			"which event carries which _audit anchor and the bytes its record's result_sha256 is of; the heads of\n" +
			"the chain agentd pushed to its anchor workflows in Temporal; then the chain's head at export time and\n" +
			"every record up to it, each with every field its sha256 covers, prev_sha256 and sha256. A reader\n" +
			"recomputes every record's sha256, the chain's links, each anchor's result sha256 and each pushed\n" +
			"head without agentd (\"hashes\" says how). With --run, that run's histories and records only: their\n" +
			"sha256s can be checked, not the chain's links, which go through other runs' records, nor its pushed\n" +
			"heads. The runs are those agentctl audit verify checks. Exits 0 when the export holds every record to\n" +
			"the head, 2 when agentd cut it short at its bound on an export's size (the document is whole JSON all\n" +
			"the same, and its head's cut says from which record on it holds none), as audit verify does when not\n" +
			"everything could be checked, and 1 when there is no export.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if out == "text" || out == "json" {
				return fmt.Errorf("-o names the file to write the export to (an export is always JSON): "+
					"for a file of that name, say ./%s", out)
			}
			return withClientTimeout(g, timeout, func(ctx context.Context, c agentosv1.ControlClient) error {
				stream, err := c.ExportAudit(ctx, &agentosv1.ExportAuditRequest{RunId: run})
				if err != nil {
					return err
				}
				var cut string
				if out == "" || out == "-" {
					cut, err = writeExport(os.Stdout, stream)
				} else {
					err = writeFileWhole(out, func(w io.Writer) error {
						var err error
						cut, err = writeExport(w, stream)
						return err
					})
				}
				if err == nil && cut != "" {
					return exitError{2, errors.New("the export is cut short: " + safetext.Line(cut))} // agentctl says it on stderr
				}
				return err
			})
		},
	}
	cmd.Flags().StringVar(&run, "run", "", "export this run's histories and records only")
	// Shadows the global --output: an export is always JSON, and -o names its file.
	cmd.Flags().StringVarP(&out, "output", "o", "", "write the export to this file, whole or not at all (default: standard output)")
	cmd.Flags().DurationVar(&timeout, "timeout", 10*time.Minute, "how long to wait for agentd's export")
	return cmd
}

// writeFileWhole writes path with fn, readable by its owner only (it holds the ledger's records):
// through a file next to it, renamed onto it once written, so that an export cut short leaves
// nothing behind under its name.
func writeFileWhole(path string, fn func(io.Writer) error) error {
	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err := fn(f); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}

// exportStream is what writeExport reads: the parts of an export, in order.
type exportStream interface {
	Recv() (*agentosv1.AuditExportPart, error)
}

type exportRecord struct {
	Seq        int64  `json:"seq"`
	At         string `json:"at"`
	WorkflowID string `json:"workflow_id"`
	Type       string `json:"type"`
	Actor      string `json:"actor"`
	PeerUID    *int64 `json:"peer_uid"` // null for NULL
	Payload    string `json:"payload"`
	PrevSha256 string `json:"prev_sha256"`
	Sha256     string `json:"sha256"`
}

type exportRun struct {
	RunID         string          `json:"run_id"`
	TemporalRunID string          `json:"temporal_run_id"`
	History       json.RawMessage `json:"history,omitempty"` // Temporal's JSON of temporal.api.history.v1.History
	Anchors       []exportAnchor  `json:"anchors"`
	Unavailable   string          `json:"unavailable,omitempty"`
}

type exportAnchor struct {
	EventID    int64  `json:"event_id"`
	ActivityID string `json:"activity_id"`
	Attempt    int32  `json:"attempt"`
	Seq        int64  `json:"seq"`
	Sha256     string `json:"sha256"`
	ResultJSON []byte `json:"result_json"` // base64
}

type exportPushedHead struct {
	AnchorWorkflowID string `json:"anchor_workflow_id"`
	Seq              int64  `json:"seq"`
	Sha256           string `json:"sha256"`
}

type exportHead struct {
	Seq        int64  `json:"seq"`
	Sha256     string `json:"sha256"`
	Records    int64  `json:"records"`
	ExportedAt string `json:"exported_at"`
	Cut        string `json:"cut,omitempty"` // why the export stops short of the head, past agentd's bound on its size
}

// writeExport writes the export's parts as they come, as one JSON document: its runs (the
// histories) first, then the heads pushed to Temporal, then the chain's head and its records, as
// agentd sends them. Nothing is held but the part at hand. It returns why the export stops short of
// the chain's head, when it does.
func writeExport(out io.Writer, stream exportStream) (string, error) {
	head, err := writeParts(out, stream)
	return head.GetCut(), err
}

func writeParts(out io.Writer, stream exportStream) (*agentosv1.AuditExportHead, error) {
	w := bufio.NewWriter(out)
	put := func(v any) error {
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		_, err = w.Write(safetext.JSON(b))
		return err
	}
	if _, err := fmt.Fprintf(w, `{"format":%q,"hashes":`, exportFormat); err != nil {
		return nil, err
	}
	if err := put(exportHashes); err != nil {
		return nil, err
	}
	if _, err := w.WriteString(`,"runs":[`); err != nil {
		return nil, err
	}
	// The parts come in phases: runs, pushed heads, the chain.
	const (
		inRuns = iota
		inHeads
		inChain
	)
	phase, n := inRuns, 0 // n: the parts of the current phase so far
	toHeads := func() error {
		phase, n = inHeads, 0
		_, err := w.WriteString("],\n\"pushed_heads\":[")
		return err
	}
	item := func(v any) error {
		sep := "\n"
		if n > 0 {
			sep = ",\n"
		}
		n++
		if _, err := w.WriteString(sep); err != nil {
			return err
		}
		return put(v)
	}
	records := int64(0)
	var head *agentosv1.AuditExportHead
	for {
		part, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		switch p := part.GetPart().(type) {
		case *agentosv1.AuditExportPart_History:
			if phase != inRuns {
				return nil, errors.New("agentd sent a history after the chain's heads")
			}
			h := p.History
			r := exportRun{RunID: h.GetRunId(), TemporalRunID: h.GetTemporalRunId(), Unavailable: h.GetUnavailable(),
				Anchors: []exportAnchor{}}
			if h.GetHistoryJson() != "" {
				r.History = json.RawMessage(h.GetHistoryJson()) // json.Marshal refuses one that is not JSON
			}
			for _, a := range h.GetAnchors() {
				r.Anchors = append(r.Anchors, exportAnchor{EventID: a.GetEventId(), ActivityID: a.GetActivityId(),
					Attempt: a.GetAttempt(), Seq: a.GetSeq(), Sha256: a.GetSha256(), ResultJSON: a.GetResultJson()})
			}
			if err := item(r); err != nil {
				return nil, err
			}
		case *agentosv1.AuditExportPart_PushedHead:
			switch phase {
			case inRuns:
				if err := toHeads(); err != nil {
					return nil, err
				}
			case inChain:
				return nil, errors.New("agentd sent a pushed head after the chain")
			}
			h := p.PushedHead
			if err := item(exportPushedHead{AnchorWorkflowID: h.GetAnchorWorkflowId(), Seq: h.GetSeq(), Sha256: h.GetSha256()}); err != nil {
				return nil, err
			}
		case *agentosv1.AuditExportPart_Head:
			switch phase {
			case inRuns:
				if err := toHeads(); err != nil {
					return nil, err
				}
			case inChain:
				return nil, errors.New("agentd sent two heads")
			}
			phase, n, head = inChain, 0, p.Head
			if _, err := w.WriteString(fmt.Sprintf("],\n\"run_id\":%s,\"head\":", jsonString(head.GetRunId()))); err != nil {
				return nil, err
			}
			if err := put(exportHead{Seq: head.GetSeq(), Sha256: head.GetSha256(), Records: head.GetRecords(),
				ExportedAt: head.GetExportedAt(), Cut: head.GetCut()}); err != nil {
				return nil, err
			}
			if _, err := w.WriteString(`,"records":[`); err != nil {
				return nil, err
			}
		case *agentosv1.AuditExportPart_Record:
			if phase != inChain {
				return nil, errors.New("agentd sent a record before the chain's head")
			}
			r := p.Record
			records++
			if err := item(exportRecord{Seq: r.GetSeq(), At: r.GetAt(), WorkflowID: r.GetWorkflowId(), Type: r.GetType(),
				Actor: r.GetActor(), PeerUID: r.PeerUid, Payload: r.GetPayload(), PrevSha256: r.GetPrevSha256(),
				Sha256: r.GetSha256()}); err != nil {
				return nil, err
			}
		default:
			return nil, errors.New("agentd sent a part agentctl does not know")
		}
	}
	switch {
	case head == nil:
		return nil, errors.New("the export ended before the chain's head: it is not whole")
	case records != head.GetRecords():
		return nil, fmt.Errorf("the export holds %d records, and agentd said it would send %d: it is not whole", records, head.GetRecords())
	}
	if _, err := w.WriteString("\n]}\n"); err != nil {
		return nil, err
	}
	return head, w.Flush()
}

func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(safetext.JSON(b))
}
