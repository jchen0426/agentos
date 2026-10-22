package audit

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/jchen0426/agentos/internal/ledger"
	"github.com/jchen0426/agentos/internal/ledger/db"
)

func open(t *testing.T) *ledger.Ledger {
	t.Helper()
	l, err := ledger.Open(context.Background(), filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func records(t *testing.T, l *ledger.Ledger) []db.AuditLog {
	t.Helper()
	rows, err := l.Q.ListAuditRecords(context.Background(), db.ListAuditRecordsParams{Seq: 1, Limit: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

// chain appends n records, of a run and of agentd, one by an operator.
func chain(t *testing.T, l *ledger.Ledger, n int) []Head {
	t.Helper()
	var heads []Head
	for i := range n {
		e := Event{WorkflowID: "wf", Type: "call", Actor: Run, Payload: map[string]any{"i": i, "status": "ok"}}
		switch i % 3 {
		case 1:
			e = Event{Type: "agentd.started", Actor: Agentd, Payload: map[string]any{"pid": 100 + i}}
		case 2:
			e = Event{WorkflowID: "wf", Type: "approval.decided", Actor: Operator("alice"),
				PeerUID: sql.NullInt64{Int64: 501, Valid: true}, Payload: map[string]any{"reason": "nö <&>"}}
		}
		h, err := Record(context.Background(), l, e)
		if err != nil {
			t.Fatal(err)
		}
		heads = append(heads, h)
	}
	return heads
}

func verify(t *testing.T, l *ledger.Ledger) Report {
	t.Helper()
	rep, err := Verify(context.Background(), l.Q)
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

// The hash of a record is fixed by its fields, as the package comment says: these were computed
// apart from this code, with Python's hashlib and with shasum on the netstrings written out.
func TestTheHashIsTheNetstringsOfTheFields(t *testing.T) {
	first := db.AuditLog{Seq: 1, At: "2026-10-22T09:00:00.000000000Z", WorkflowID: "wf", Type: "call", Actor: "run",
		Payload: `{"a":1}`, PrevSha256: Genesis}
	if got, want := Hash(first), "7038e4636d797a6c4b059471aacb3b0db8e9c2cf2542fe18453a5b67cf37ae4a"; got != want {
		t.Errorf("first record: %s, want %s", got, want)
	}
	second := db.AuditLog{Seq: 2, At: "2026-10-22T09:00:01.000000000Z", Type: "approval.decided", Actor: "operator:alice",
		PeerUid: sql.NullInt64{Int64: 501, Valid: true}, Payload: `{"reason":"nö"}`, PrevSha256: Hash(first)}
	if got, want := Hash(second), "5a9f1d53eefa1739c0cb55c775e6b09da21a1cd3a064e6f717d213eb6faf12cc"; got != want {
		t.Errorf("second record: %s, want %s", got, want)
	}
	// Every field counts, and where one ends: moving a byte from one field to the next is another record.
	for name, r := range map[string]db.AuditLog{
		"seq":         {Seq: 2, At: first.At, WorkflowID: "wf", Type: "call", Actor: "run", Payload: `{"a":1}`, PrevSha256: Genesis},
		"at":          {Seq: 1, At: "2026-10-22T09:00:00.000000001Z", WorkflowID: "wf", Type: "call", Actor: "run", Payload: `{"a":1}`, PrevSha256: Genesis},
		"a boundary":  {Seq: 1, At: first.At, WorkflowID: "w", Type: "fcall", Actor: "run", Payload: `{"a":1}`, PrevSha256: Genesis},
		"peer_uid":    {Seq: 1, At: first.At, WorkflowID: "wf", Type: "call", Actor: "run", PeerUid: sql.NullInt64{Valid: true}, Payload: `{"a":1}`, PrevSha256: Genesis},
		"payload":     {Seq: 1, At: first.At, WorkflowID: "wf", Type: "call", Actor: "run", Payload: `{"a":2}`, PrevSha256: Genesis},
		"prev_sha256": {Seq: 1, At: first.At, WorkflowID: "wf", Type: "call", Actor: "run", Payload: `{"a":1}`, PrevSha256: strings.Repeat("1", 64)},
	} {
		if Hash(r) == Hash(first) {
			t.Errorf("%s: the same hash as the first record", name)
		}
	}
}

func TestAppendLinksEachRecordToTheOneBefore(t *testing.T) {
	l := open(t)
	clock := ledger.Clock
	t.Cleanup(func() { ledger.Clock = clock })
	at := time.Date(2026, 10, 22, 9, 0, 0, 0, time.UTC)
	ledger.Clock = func() time.Time { return at }
	heads := chain(t, l, 4)
	rows := records(t, l)
	if len(rows) != 4 {
		t.Fatalf("%d records, want 4", len(rows))
	}
	prev := Genesis
	for i, r := range rows {
		if r.Seq != int64(i+1) || r.PrevSha256 != prev || r.Sha256 != Hash(r) || heads[i] != (Head{r.Seq, r.Sha256}) {
			t.Errorf("record %d: %+v, head %+v", i+1, r, heads[i])
		}
		if r.At != "2026-10-22T09:00:00.000000000Z" {
			t.Errorf("record %d: at %q, want the ledger's fixed-width time", r.Seq, r.At)
		}
		prev = r.Sha256
	}
	// The payload is json.Marshal's text, keys sorted and HTML escaped; an operator's record has the
	// peer's uid.
	if rows[0].Payload != `{"i":0,"status":"ok"}` || rows[0].PeerUid.Valid {
		t.Errorf("record 1: %+v", rows[0])
	}
	if rows[2].Actor != "operator:alice" || rows[2].PeerUid != (sql.NullInt64{Int64: 501, Valid: true}) ||
		rows[2].Payload != "{\"reason\":\"nö \\u003c\\u0026\\u003e\"}" {
		t.Errorf("record 3: %+v", rows[2])
	}
	if rows[1].WorkflowID != "" || rows[1].Actor != Agentd {
		t.Errorf("record 2, agentd's own: %+v", rows[1])
	}
	if rep := verify(t, l); rep != (Report{Records: 4, Head: heads[3]}) {
		t.Errorf("verify: %+v", rep)
	}
}

// A record is in the transaction of the change it records: rolled back, neither is there, and the
// next record takes its seq.
func TestARecordRollsBackWithItsTransaction(t *testing.T) {
	l := open(t)
	ctx := context.Background()
	chain(t, l, 1)
	failed := errors.New("the change failed")
	err := l.Tx(ctx, func(q *db.Queries) error {
		if _, err := Append(ctx, q, Event{WorkflowID: "wf", Type: "effect.claimed", Actor: Agentd}); err != nil {
			return err
		}
		return failed
	})
	if !errors.Is(err, failed) {
		t.Fatal(err)
	}
	h, err := Record(ctx, l, Event{Type: "agentd.started", Actor: Agentd})
	if err != nil || h.Seq != 2 {
		t.Fatalf("the next record: %+v, %v", h, err)
	}
	if rows := records(t, l); len(rows) != 2 || rows[1].Type != "agentd.started" || rows[1].Payload != "{}" {
		t.Errorf("records: %+v", rows)
	}
	if rep := verify(t, l); rep.BrokenAt != 0 || rep.Records != 2 {
		t.Errorf("verify: %+v", rep)
	}
}

// Appends from many goroutines at once still make one chain: each transaction reads the last
// record and writes the next, and SQLite lets one writer at a time. (As many as T7's calls at once:
// SQLite's busy handler backs off, and a few hundred writers at once wait past its timeout.)
func TestConcurrentAppendsMakeOneChain(t *testing.T) {
	l := open(t)
	var wg sync.WaitGroup
	errs := make(chan error, 50)
	for i := range 50 {
		wg.Go(func() {
			if _, err := Record(context.Background(), l, Event{WorkflowID: fmt.Sprintf("wf%d", i%7), Type: "call", Actor: Run,
				Payload: map[string]int{"i": i}}); err != nil {
				errs <- err
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if rep := verify(t, l); rep.BrokenAt != 0 || rep.Records != 50 || rep.Head.Seq != 50 {
		t.Errorf("verify: %+v", rep)
	}
}

func TestAppendRefusesWhatIsNotARecord(t *testing.T) {
	l := open(t)
	for name, e := range map[string]Event{
		"no type":             {Actor: Agentd},
		"no actor":            {Type: "call"},
		"not an object":       {Type: "call", Actor: Run, Payload: "text"},
		"a list":              {Type: "call", Actor: Run, Payload: []int{1}},
		"not UTF-8":           {Type: "call", Actor: "operator:\xff", Payload: map[string]int{}},
		"cannot be marshaled": {Type: "call", Actor: Run, Payload: map[string]any{"f": func() {}}},
	} {
		if _, err := Record(context.Background(), l, e); err == nil {
			t.Errorf("%s: appended", name)
		}
	}
	if rows := records(t, l); len(rows) != 0 {
		t.Errorf("records: %+v", rows)
	}
}

// tamper changes the ledger as someone with the file could: SQL on it.
func tamper(t *testing.T, l *ledger.Ledger, sql string, args ...any) {
	t.Helper()
	if _, err := l.DB.Exec(sql, args...); err != nil {
		t.Fatal(err)
	}
}

// Each change of a recorded chain breaks it where it was made: at the record changed, or at the
// record after it, once its own hash is made again to match.
func TestVerifyFindsTheFirstBreak(t *testing.T) {
	for name, tc := range map[string]struct {
		change func(t *testing.T, l *ledger.Ledger)
		at     int64
		why    string
	}{
		"a payload changed": {func(t *testing.T, l *ledger.Ledger) {
			tamper(t, l, `UPDATE audit_log SET payload = '{"i":3,"status":"denied"}' WHERE seq = 4`)
		}, 4, "not the hash of its fields"},
		"an actor changed": {func(t *testing.T, l *ledger.Ledger) {
			tamper(t, l, `UPDATE audit_log SET actor = 'run' WHERE seq = 3`)
		}, 3, "not the hash of its fields"},
		"a peer_uid changed": {func(t *testing.T, l *ledger.Ledger) {
			tamper(t, l, `UPDATE audit_log SET peer_uid = 0 WHERE seq = 3`)
		}, 3, "not the hash of its fields"},
		"a peer_uid removed": {func(t *testing.T, l *ledger.Ledger) {
			tamper(t, l, `UPDATE audit_log SET peer_uid = NULL WHERE seq = 6`)
		}, 6, "not the hash of its fields"},
		"a time changed": {func(t *testing.T, l *ledger.Ledger) {
			tamper(t, l, `UPDATE audit_log SET at = '2020-01-01T00:00:00.000000000Z' WHERE seq = 2`)
		}, 2, "not the hash of its fields"},
		"a record deleted": {func(t *testing.T, l *ledger.Ledger) {
			tamper(t, l, `DELETE FROM audit_log WHERE seq = 5`)
		}, 5, "record 5 is missing"},
		"the first record deleted": {func(t *testing.T, l *ledger.Ledger) {
			tamper(t, l, `DELETE FROM audit_log WHERE seq = 1`)
		}, 1, "record 1 is missing"},
		"a record changed with its hash made again": {func(t *testing.T, l *ledger.Ledger) {
			r := records(t, l)[3]
			r.Payload = `{"i":3,"status":"denied"}`
			tamper(t, l, `UPDATE audit_log SET payload = ?, sha256 = ? WHERE seq = 4`, r.Payload, Hash(r))
		}, 5, "prev_sha256"},
		"two records swapped": {func(t *testing.T, l *ledger.Ledger) {
			// Records 3 and 4 trade places, each keeping its own fields and hash.
			tamper(t, l, `UPDATE audit_log SET seq = seq + 100 WHERE seq IN (3, 4)`)
			tamper(t, l, `UPDATE audit_log SET seq = 107 - seq WHERE seq IN (103, 104)`)
		}, 3, "prev_sha256"},
		"a record linked past the one before it": {func(t *testing.T, l *ledger.Ledger) {
			tamper(t, l, `UPDATE audit_log SET prev_sha256 = (SELECT sha256 FROM audit_log WHERE seq = 1) WHERE seq = 3`)
		}, 3, "prev_sha256"},
	} {
		t.Run(name, func(t *testing.T) {
			l := open(t)
			heads := chain(t, l, 7)
			tc.change(t, l)
			rep := verify(t, l)
			if rep.BrokenAt != tc.at || !strings.Contains(rep.Broken, tc.why) {
				t.Errorf("broken at %d (%s), want %d (%s)", rep.BrokenAt, rep.Broken, tc.at, tc.why)
			}
			// What verified is the chain up to the break: as it was appended, but for a record
			// changed with its own hash made again, which verifies as itself.
			want := Head{Sha256: Genesis}
			if tc.at > 1 {
				want = heads[tc.at-2]
				for _, r := range records(t, l) {
					if r.Seq == tc.at-1 {
						want.Sha256 = r.Sha256
					}
				}
			}
			if rep.Records != tc.at-1 || rep.Head != want {
				t.Errorf("verified %d records up to %+v, want %d up to %+v", rep.Records, rep.Head, tc.at-1, want)
			}
		})
	}
}

func TestAChainRewrittenConsistentlyVerifies(t *testing.T) {
	l := open(t)
	heads := chain(t, l, 6)
	// From record 3 on, every record made again, each linked to the one before: the chain is whole.
	rows := records(t, l)
	prev := rows[1].Sha256
	for _, r := range rows[2:] {
		if r.Seq == 3 {
			r.Payload = `{"reason":"approved, as it says now"}`
		}
		r.PrevSha256 = prev
		r.Sha256 = Hash(r)
		if _, err := l.DB.Exec(`UPDATE audit_log SET payload = ?, prev_sha256 = ?, sha256 = ? WHERE seq = ?`,
			r.Payload, r.PrevSha256, r.Sha256, r.Seq); err != nil {
			t.Fatal(err)
		}
		prev = r.Sha256
	}
	rep := verify(t, l)
	if rep.BrokenAt != 0 || rep.Records != 6 {
		t.Fatalf("verify: %+v", rep)
	}
	// It is not the chain whose heads went out: an anchor from before the rewrite no longer matches.
	if rep.Head == heads[5] || records(t, l)[1].Sha256 != heads[1].Sha256 {
		t.Errorf("head %+v, the head that went out %+v", rep.Head, heads[5])
	}
}

func TestVerifyWalksPages(t *testing.T) {
	l := open(t)
	ctx := context.Background()
	// More than a page, in a few transactions.
	for range 3 {
		if err := l.Tx(ctx, func(q *db.Queries) error {
			for i := range page {
				if _, err := Append(ctx, q, Event{Type: "call", Actor: Run, Payload: map[string]int{"i": i}}); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if rep := verify(t, l); rep.BrokenAt != 0 || rep.Records != 3*page {
		t.Fatalf("verify: %+v", rep)
	}
	tamper(t, l, `DELETE FROM audit_log WHERE seq = ?`, 2*page+1)
	if rep := verify(t, l); rep.BrokenAt != 2*page+1 || rep.Records != 2*page {
		t.Errorf("verify: %+v", rep)
	}
	empty := open(t)
	if rep := verify(t, empty); rep != (Report{Head: Head{Sha256: Genesis}}) {
		t.Errorf("an empty chain: %+v", rep)
	}
}

func TestTextIsBoundedValidUTF8(t *testing.T) {
	for _, s := range []string{strings.Repeat("a", 5000), strings.Repeat("é", 3000), strings.Repeat("😀", 1000),
		"bad \xff\xfe bytes " + strings.Repeat("x", 3000)} {
		got := Text(s)
		if len(got) > MaxText || !utf8.ValidString(got) || !strings.HasSuffix(got, "…") {
			t.Errorf("Text of %d bytes: %d bytes, valid %v: %q", len(s), len(got), utf8.ValidString(got), got[len(got)-10:])
		}
	}
	if got := Text("short \xff"); got != "short �" {
		t.Errorf("Text: %q", got)
	}
	if s := strings.Repeat("b", MaxText); Text(s) != s {
		t.Error("text of exactly MaxText bytes was cut")
	}
	// In a payload it is JSON, whatever it holds.
	b, err := json.Marshal(map[string]string{"reason": Text("nul\x00 and \x1b[2J")})
	if err != nil || !json.Valid(b) {
		t.Errorf("%s %v", b, err)
	}
}

// A run's blob whose ownership the ledger drops is not taken for one the run owned before: the
// blob is not stored as the run's, and there is no record.
func TestADroppedOwnerIsNotTakenAsOwned(t *testing.T) {
	ctx := context.Background()
	l := open(t)
	l.UncheckedSchema = true // the trigger is the test's
	if _, err := l.DB.Exec(`CREATE TRIGGER dropped BEFORE INSERT ON blob_owners BEGIN SELECT RAISE(IGNORE); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := PutRunBlob(ctx, l, "wf", []byte("x")); !errors.Is(err, ledger.ErrWriteDropped) {
		t.Fatalf("err %v, want %v", err, ledger.ErrWriteDropped)
	}
	if got := records(t, l); len(got) != 0 {
		t.Fatalf("records %+v", got)
	}
}

// A call's record names the count of the call (scheduled_at) the loop guard took it under: one
// that names a count the log does not have (no call.counted before it) is a difference, whatever
// the calls table says.
func TestACallRecordOfACountTheLogDoesNotHave(t *testing.T) {
	ctx := context.Background()
	l := open(t)
	call := func(scheduled string) {
		t.Helper()
		p := map[string]any{"activity_id": "1:x", "attempt": 1, "status": "ok", "kind": "tool", "name": "x"}
		if scheduled != "" {
			p["scheduled_at"] = scheduled
		}
		if _, err := Record(ctx, l, Event{WorkflowID: "wf", Type: "call", Actor: Run, Payload: p}); err != nil {
			t.Fatal(err)
		}
	}
	differences := func() []Difference {
		t.Helper()
		var p Projection
		if err := p.Read(ctx, l.Q); err != nil {
			t.Fatal(err)
		}
		d, err := p.Differences(ctx, l.Q, Since{})
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	call("") // refused before it counted: names no count
	if d := differences(); len(d) != 0 {
		t.Fatalf("differences %v", d)
	}
	call("2026-10-22T01:00:00Z")
	if d := differences(); len(d) != 1 || d[0].Kind != "call" || !strings.Contains(d[0].Why, "names a count the audit log does not have") {
		t.Fatalf("differences %v", d)
	}
}
