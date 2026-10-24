// Package audit is agentd's hash-chained audit log (design doc, section 8): every policy
// decision, side effect and approval, with who made it. A record is appended in the transaction
// of the change it records, so that the change and its record commit together or not at all.
//
// Each record carries the sha256 of the record before it, and its own sha256 over all its fields.
// Changing a record, removing one or putting two out of order breaks the chain at that record. A
// chain rewritten consistently from some record on does not break; but the results Invoke returns
// carry the chain's head into Temporal's history (the _audit anchor), and those no longer match.
//
// A call's record holds the sha256 of the result the call returned (result_sha256), which anyone
// can compute again from that result in Temporal's history, without agentd (ResultJSON):
//   - The result's value_json is one JSON object, and nothing after it. Its numbers are kept as
//     written (Go's json.Number): 1.50 stays 1.50, 1e2 stays 1e2, -0 stays -0, a 20-digit integer
//     keeps its digits. Of a key given twice, the last value counts. In a string, invalid UTF-8, and
//     an escaped surrogate that pairs with nothing, read as U+FFFD.
//   - The object's key "_audit" (the anchor) is removed; one inside a nested value is not.
//   - The rest is encoded as Go's encoding/json encodes a map: nothing between the tokens; each
//     object's keys sorted by their UTF-8 bytes; numbers as written; true, false and null; each
//     string between double quotes, with \" and \\, \n \r \t \b \f, any other character below
//     U+0020 as \u00XX in lower-case hex, < > & as \u003c \u003e \u0026, U+2028 and U+2029 as
//     \u2028 and \u2029, and every other character as its UTF-8 bytes (non-ASCII and astral
//     characters are not escaped; neither is /).
//   - result_sha256 is the lower-case hex of the sha256 of those bytes.
//
// The vectors in result_test.go pin these down, each with the bytes and their sha256.
package audit

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
)

// Actors, besides an operator's (Operator).
const (
	Run    = "run"    // what a run's call asked for: the call itself, a write it records, the approval that write needs
	Agentd = "agentd" // what agentd does of itself: its start, the effect protocol's steps, expiries, recovery
	// UnknownRun is the actor of a call from an execution agentd did not start, or cannot tell it
	// did (its run ID was never recorded): nobody vouches for who made it.
	UnknownRun = "unknown-run"
)

// Operator is the actor of a person who acted through the Control socket, named as agentctl or
// the operating system names them.
func Operator(name string) string { return "operator:" + name }

// Who refused a call that ended denied, as its record's refused_by says: agentd's own word, never a
// tool's text (agentctl runs explain judges by it).
const (
	RefusedByPolicy    = "policy"     // the policy's decision, or its limit on a run's writes
	RefusedByLoopGuard = "loop_guard" // agentd's loop guard
	RefusedByCancel    = "cancel"     // the run was cancelled
	RefusedByTool      = "tool"       // the tool refused the call as it stood
	RefusedByAgentd    = "agentd"     // agentd, otherwise
)

// ErrorCauseToolServer is a call record's error_cause when the git tool server failed: it did not
// start, stopped, timed out, or answered nothing agentd could read.
const ErrorCauseToolServer = "tool_server"

// Event is one record to append.
type Event struct {
	WorkflowID string        // the run's; "" for agentd's own events
	Type       string        // call, effect.claimed, approval.decided, ...
	Actor      string        // Run, Agentd or Operator(name)
	PeerUID    sql.NullInt64 // an operator's: the uid the Control socket's peer credentials gave
	// Payload is encoded with json.Marshal, which sorts map keys: a struct or a map, without
	// floats. The record holds the text, and its hash covers the text: a verifier needs no
	// canonical JSON of its own.
	Payload any
}

// Who is who made a change, as its record names them: an actor, and an operator's uid.
type Who struct {
	Actor   string
	PeerUID sql.NullInt64
}

// Head is a record of the chain: its seq and sha256. The chain's head is its last record.
type Head struct {
	Seq    int64  `json:"seq"`
	Sha256 string `json:"sha256"`
}

// Genesis is the prev_sha256 of the first record.
const Genesis = "0000000000000000000000000000000000000000000000000000000000000000"

// version names the hash below: a record's fields, in order, each a netstring.
const version = "agentos-audit/1"

// MaxText bounds text from the Worker or a tool that goes in a payload: reasons, error messages.
const MaxText = 2000

// Append appends e to the chain through q, which must be the caller's write transaction: it reads
// the last record and writes the next, and ledger.Tx begins IMMEDIATE, so appends serialize. It
// returns the new record, the chain's head once the transaction commits.
func Append(ctx context.Context, q *db.Queries, e Event) (Head, error) {
	if e.Type == "" || e.Actor == "" {
		return Head{}, fmt.Errorf("audit: an event needs a type and an actor: %q by %q", e.Type, e.Actor)
	}
	for _, s := range []string{e.WorkflowID, e.Type, e.Actor} {
		if !utf8.ValidString(s) {
			return Head{}, fmt.Errorf("audit: %q is not UTF-8", s)
		}
	}
	payload := []byte("{}")
	if e.Payload != nil {
		var err error
		if payload, err = json.Marshal(e.Payload); err != nil {
			return Head{}, fmt.Errorf("audit: the payload of %s: %w", e.Type, err)
		}
	}
	if len(payload) == 0 || payload[0] != '{' {
		return Head{}, fmt.Errorf("audit: the payload of %s is not a JSON object", e.Type)
	}
	prev := Head{Sha256: Genesis}
	var last Head // zero for an empty log
	switch r, err := q.LastAuditRecord(ctx); {
	case err == nil:
		last = Head{Seq: r.Seq, Sha256: r.Sha256}
		prev = last
	case !errors.Is(err, sql.ErrNoRows):
		return Head{}, err
	}
	// Through a transaction of Tx, as all of agentd's appends are: only onto the chain as this
	// process left it (tip.go).
	tx := ledger.TxOf(q)
	if tx != nil {
		if err := tipOf(tx.Ledger).onto(ctx, q, tx, last); err != nil {
			return Head{}, fmt.Errorf("audit: append %s: %w", e.Type, err)
		}
	}
	r := db.AuditLog{Seq: prev.Seq + 1, At: ledger.Stamp(ledger.Clock()), WorkflowID: e.WorkflowID, Type: e.Type,
		Actor: e.Actor, PeerUid: e.PeerUID, Payload: string(payload), PrevSha256: prev.Sha256}
	r.Sha256 = Hash(r)
	if err := q.InsertAuditRecord(ctx, db.InsertAuditRecordParams(r)); err != nil {
		return Head{}, fmt.Errorf("audit: append %s: %w", e.Type, err)
	}
	head := Head{Seq: r.Seq, Sha256: r.Sha256}
	if tx != nil {
		tipOf(tx.Ledger).appending(tx, head)
	}
	return head, nil
}

// Record appends e in a transaction of its own: for an event that records no other change of the
// ledger (agentd's start, a refused Runtime call, a call refused before it changed anything).
func Record(ctx context.Context, l *ledger.Ledger, e Event) (Head, error) {
	var h Head
	err := l.Tx(ctx, func(q *db.Queries) error {
		var err error
		h, err = Append(ctx, q, e)
		return err
	})
	return h, err
}

// Hash is a record's sha256, hex: of the netstrings (<length in bytes>:<bytes>,) of
// "agentos-audit/1", then the record's seq in decimal, at, workflow_id, type, actor, peer_uid in
// decimal (empty when NULL), payload and prev_sha256. A netstring says where each field ends,
// so that no two records hash the same bytes.
func Hash(r db.AuditLog) string {
	h := sha256.New()
	field := func(s string) {
		h.Write([]byte(strconv.Itoa(len(s))))
		h.Write([]byte{':'})
		h.Write([]byte(s))
		h.Write([]byte{','})
	}
	uid := ""
	if r.PeerUid.Valid {
		uid = strconv.FormatInt(r.PeerUid.Int64, 10)
	}
	for _, f := range []string{version, strconv.FormatInt(r.Seq, 10), r.At, r.WorkflowID, r.Type, r.Actor, uid,
		r.Payload, r.PrevSha256} {
		field(f)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Report is what Verify found.
type Report struct {
	Records  int64  // the records that verified, from seq 1
	Head     Head   // the last of them: the chain's head when it is whole; seq 0 and Genesis for none
	BrokenAt int64  // the first seq at which the chain breaks; 0 when it is whole
	Broken   string // why it breaks there
}

// page is how many records Verify reads at a time.
const page = 1000

// Verify walks the chain from seq 1, a page at a time: each seq is the one after the last, each
// prev_sha256 is the sha256 of the record before, and each sha256 is the record's hash. It stops
// at the first record that breaks the chain, and says why. Records are never changed once
// appended: a walk outside a transaction reads them as they were written, while agentd appends.
func Verify(ctx context.Context, q *db.Queries) (Report, error) {
	rep := Report{Head: Head{Sha256: Genesis}}
	for {
		rows, err := q.ListAuditRecords(ctx, db.ListAuditRecordsParams{Seq: rep.Head.Seq + 1, Limit: page})
		if err != nil {
			return Report{}, err
		}
		for _, r := range rows {
			next := rep.Head.Seq + 1
			switch {
			case r.Seq != next:
				rep.BrokenAt, rep.Broken = next, fmt.Sprintf("record %d is missing: the record after %d is %d", next, rep.Head.Seq, r.Seq)
			case r.PrevSha256 != rep.Head.Sha256:
				rep.BrokenAt, rep.Broken = r.Seq, fmt.Sprintf("its prev_sha256 %s is not the sha256 of record %d, %s",
					r.PrevSha256, rep.Head.Seq, rep.Head.Sha256)
			case Hash(r) != r.Sha256:
				rep.BrokenAt, rep.Broken = r.Seq, fmt.Sprintf("its sha256 %s is not the hash of its fields, %s", r.Sha256, Hash(r))
			}
			if rep.BrokenAt != 0 {
				return rep, nil
			}
			rep.Records++
			rep.Head = Head{Seq: r.Seq, Sha256: r.Sha256}
		}
		if len(rows) < page {
			return rep, nil
		}
	}
}

// ValueSHA256 is the sha256 of a gateway result's value, as the record of its call holds it: the
// value without _audit, encoded with json.Marshal (keys sorted), hex. ResultSHA256 makes the same
// from the value's JSON as Temporal's history holds it, with the anchor in it.
func ValueSHA256(value map[string]any) (string, error) {
	if _, ok := value["_audit"]; ok {
		value = maps.Clone(value)
		delete(value, "_audit")
	}
	b, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// ResultJSON is the encoding of a call's result whose sha256 the call's record holds, from the
// result's value_json as Temporal's history keeps it (the package comment says how).
func ResultJSON(valueJSON string) ([]byte, error) {
	dec := json.NewDecoder(strings.NewReader(valueJSON))
	dec.UseNumber()
	var v map[string]any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	if v == nil {
		return nil, errors.New("not a JSON object")
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("more after the JSON object")
	}
	delete(v, "_audit")
	return json.Marshal(v)
}

// ResultSHA256 is the sha256 of ResultJSON: a verifier gets from Temporal's history the sha256 the
// call's record holds.
func ResultSHA256(valueJSON string) (string, error) {
	b, err := ResultJSON(valueJSON)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// Text is s as it goes in a payload: valid UTF-8 (U+FFFD for what is not), and at most MaxText
// bytes, cut at a rune and marked with an ellipsis. It is text a Worker, a tool or the outside
// world wrote; JSON holds any of it, and what shows it later escapes it (internal/safetext).
func Text(s string) string {
	s = strings.ToValidUTF8(s, "�")
	if len(s) <= MaxText {
		return s
	}
	const ellipsis = "…"
	cut := MaxText - len(ellipsis)
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + ellipsis
}
