// Package ledger holds agentd's ledgers in SQLite: what Temporal's history does not record
// (runs agentd started, exact model requests, external effects, blobs).
package ledger

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"modernc.org/sqlite" // database/sql driver "sqlite"

	"github.com/jchen0426/agentos/internal/ledger/db"
)

// Never change a migration a ledger may have applied, not even a comment: agentd records each
// file's sha256 and refuses a ledger that applied a different file (migrate.go). Add another.
//
//go:embed migrations/*.sql
var migrations embed.FS

var (
	// ErrUnknownRun means the caller is not an execution agentd started through Control.
	ErrUnknownRun = errors.New("unknown run")
	// ErrRunStarting means the run row exists but StartRun has not recorded the run ID yet.
	ErrRunStarting = errors.New("run registration in progress")
	// ErrNotFound is returned for missing blobs and records.
	ErrNotFound = errors.New("not found")
	// ErrWriteDropped means a write that changes nothing when what it would change is there
	// already (an insert of a row that exists, an update of a state reached) changed nothing, and
	// what it would change is not there: something in the ledger dropped it (schema.go). The
	// writer fails rather than take it as done.
	ErrWriteDropped = errors.New("a write to the ledger changed nothing, and what it would change is not there")
	// ErrMigrationMismatch means another agentd migrated the ledger: one with migrations this
	// one does not have, or with another file under one of its migration numbers.
	ErrMigrationMismatch = errors.New("migrated by another agentd")
)

// Ledger is agentd's SQLite store. Writers serialize on SQLite's write lock: every
// transaction starts with BEGIN IMMEDIATE (_txlock=immediate), after its turn in this process.
type Ledger struct {
	DB *sql.DB
	// Q reads. Writes go through Tx, which takes the write turn (notx_test.go holds every write
	// to that).
	Q *db.Queries
	// ReadHook, when set, wraps the connection each Read runs on: a test's way in between two
	// statements of one read, to write there and see that the read does not see it.
	ReadHook func(db.DBTX) db.DBTX

	// UncheckedSchema, when set, has Tx and the connections the ledger opens skip the check of the
	// schema: a test's way past it, to reach the checks behind it with a trigger it planted. Never
	// set outside tests.
	UncheckedSchema bool

	turn   chan struct{} // one write transaction of this process at a time (Tx)
	txs    atomic.Uint64 // the transactions Tx began, to tell them apart (TxInfo.ID)
	schema schema        // the migrations' (schema.go)
}

// TxInfo is one write transaction of Tx, as what writes through it sees it.
type TxInfo struct {
	Ledger *Ledger
	ID     uint64 // this transaction's, unique to the ledger

	committed []func()
}

// OnCommit has fn run once the transaction committed, before the next one of the process begins
// (it still holds the turn); never when it rolls back.
func (t *TxInfo) OnCommit(fn func()) { t.committed = append(t.committed, fn) }

var txs sync.Map // *db.Queries of a transaction in Tx -> *TxInfo

// TxOf is the transaction of Tx that q writes through; nil for any other q.
func TxOf(q *db.Queries) *TxInfo {
	t, _ := txs.Load(q)
	info, _ := t.(*TxInfo)
	return info
}

// Open opens (creating if needed) the ledger at path and applies pending migrations. It refuses
// a ledger another agentd migrated (ErrMigrationMismatch): all checkouts' development stacks
// share one, and goose ignores applied migrations it has no file for.
func Open(ctx context.Context, path string) (*Ledger, error) {
	sub, err := fs.Sub(migrations, "migrations")
	if err != nil {
		return nil, err
	}
	return open(ctx, path, sub)
}

// open is Open with the migrations in fsys: tests give it other branches' migrations.
func open(ctx context.Context, path string, fsys fs.FS) (*Ledger, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create ledger: %w", err)
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return nil, err
	}
	dsn := dsnOf(path)
	want, err := prepare(ctx, dsn, path, fsys)
	if err != nil {
		return nil, err
	}
	base, err := sqlite.NewConnector(dsn)
	if err != nil {
		return nil, err
	}
	conns := &connector{Connector: base}
	sqlDB := sql.OpenDB(conns)
	l := &Ledger{DB: sqlDB, Q: db.New(sqlDB), turn: make(chan struct{}, 1), schema: want}
	conns.ledger.Store(l) // from here on, each connection is checked as it opens
	return l, nil
}

// prepare checks the ledger's file (PRAGMA quick_check, before anything is written to it),
// migrates it, and checks that its schema is the migrations' and nothing else (schema.go), which it
// returns. It does so on connections of its own, and closes them: each connection of the ledger's
// is checked as it opens.
func prepare(ctx context.Context, dsn, path string, fsys fs.FS) (schema, error) {
	sqlDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	defer sqlDB.Close()
	if err := quickCheck(ctx, sqlDB); err != nil {
		return nil, fmt.Errorf("ledger %s: %w; agentd does not open it", path, err)
	}
	if err := migrate(ctx, sqlDB, path, fsys); err != nil {
		return nil, err
	}
	want, err := migrationsSchema(ctx, fsys)
	if err != nil {
		return nil, err
	}
	if err := checkSchema(ctx, sqlDB, want); err != nil {
		return nil, fmt.Errorf("ledger %s: %w; agentd does not open it", path, err)
	}
	return want, sqlDB.Close()
}

// dsnOf is how agentd opens the ledger at path. The driver runs the pragmas as it opens a
// connection, and they have the connection load the schema then (connector).
func dsnOf(path string) string {
	return "file:" + path +
		"?_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=busy_timeout(10000)" +
		"&_pragma=foreign_keys(ON)&_txlock=immediate"
}

func (l *Ledger) Close() error { return l.DB.Close() }

// Tx runs fn in one write transaction and commits it unless fn fails. Transactions begin
// IMMEDIATE (see the DSN): what fn reads cannot change before it writes.
//
// This process's write transactions take turns before they begin, waiting at most as long as ctx:
// left to SQLite, writers that find the lock taken back off and retry, and with hundreds at once
// some waited past busy_timeout and failed (a burst of refused calls lost their audit records). So
// fn must not begin another transaction: it would wait for the turn it holds (notx_test.go).
func (l *Ledger) Tx(ctx context.Context, fn func(q *db.Queries) error) error {
	select {
	case l.turn <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-l.turn }()
	tx, err := l.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	// Rolled back unless committed: when fn fails, and when it panics, which then goes on up with
	// the write lock and the turn given back. Left open, the transaction would hold SQLite's write
	// lock on its connection, and every later writer would wait for it in vain.
	defer tx.Rollback()
	// Whatever this transaction writes, it decides on what its writes change: only while the
	// schema its connection runs is the migrations' (schema.go).
	if !l.UncheckedSchema {
		if err := checkTx(ctx, tx, l.schema); errors.Is(err, ErrSchemaMismatch) {
			return fmt.Errorf("%w; agentd writes nothing to the ledger until it is theirs again", err)
		} else if err != nil {
			return err
		}
	}
	q := l.Q.WithTx(tx)
	info := &TxInfo{Ledger: l, ID: l.txs.Add(1)}
	txs.Store(q, info)
	defer txs.Delete(q)
	if err := fn(q); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	for _, fn := range info.committed {
		fn()
	}
	return nil
}

// Read runs fn in one read transaction: all it reads is one snapshot of the ledger, whatever is
// written meanwhile. In WAL mode a reader holds no lock a writer waits for, so a long read (a
// verification of the whole audit log) holds up nobody.
//
// It begins as a write does: its connection loads the schema again, from the snapshot, and the
// schema is compared with the migrations' (schema.go). A connection keeps the schema it loaded until
// schema_version changes, and whoever changes sqlite_master and then schema_version has every
// connection load theirs: a view in place of a table would show made-up rows. One that is not the
// migrations' fails the read, naming what differs; fn reads nothing.
func (l *Ledger) Read(ctx context.Context, fn func(q *db.Queries) error) error {
	return l.read(ctx, func(c db.DBTX) error { return fn(db.New(c)) })
}

func (l *Ledger) read(ctx context.Context, fn func(c db.DBTX) error) error {
	conn, err := l.DB.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	// A plain BEGIN, not BeginTx: the DSN makes those IMMEDIATE, which takes the write lock. The
	// snapshot is taken at the first read and held until the transaction ends.
	if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
		return err
	}
	defer conn.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
	if !l.UncheckedSchema {
		if err := checkRead(ctx, conn, l.schema); err != nil {
			return err
		}
	}
	var c db.DBTX = conn
	if l.ReadHook != nil {
		c = l.ReadHook(c)
	}
	return fn(c)
}

// ReadAtTurn is Read, with its snapshot taken while it holds this process's write turn (Tx), and at
// called then, before the turn goes. No write transaction of this process is under way at that
// moment, and each that committed has run what it had run on commit (TxInfo.OnCommit): what at reads
// of this process's writes as it keeps them in memory, as they commit (the audit log's tip), is what
// the snapshot holds of them, neither less nor more. at must not begin a transaction.
func (l *Ledger) ReadAtTurn(ctx context.Context, at func(), fn func(q *db.Queries) error) error {
	select {
	case l.turn <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	held := true
	defer func() {
		if held {
			<-l.turn
		}
	}()
	return l.read(ctx, func(c db.DBTX) error {
		// The snapshot begins with the transaction's first read (checkRead's, or this one), under the turn.
		if _, err := c.ExecContext(ctx, "PRAGMA schema_version"); err != nil {
			return err
		}
		at()
		held = false
		<-l.turn
		return fn(db.New(c))
	})
}

// IntegrityCheck is what SQLite's PRAGMA integrity_check finds wrong with the ledger, at most max
// lines of it, in one read transaction (Read): every page, and every index against its table, which
// quick_check (Open) leaves out. A file SQLite cannot read through is a finding too. None when it
// finds nothing.
func (l *Ledger) IntegrityCheck(ctx context.Context, max int) ([]string, error) {
	var found []string
	err := l.read(ctx, func(c db.DBTX) error {
		rows, err := c.QueryContext(ctx, fmt.Sprintf("PRAGMA integrity_check(%d)", max))
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var s string
			if err := rows.Scan(&s); err != nil {
				return err
			}
			found = append(found, s)
		}
		return rows.Err()
	})
	switch {
	case err != nil && errors.Is(damaged(err), ErrDamaged):
		return []string{err.Error()}, nil
	case err != nil:
		return nil, err
	case slices.Equal(found, []string{"ok"}):
		return nil, nil
	}
	return found, nil
}

// Clock is where the ledger's times come from, and the times the effect protocol compares them
// with (an approval's expiry, a request that may still take effect): time.Now, unless a test
// moves time itself.
var Clock = time.Now

// Now is the ledger's timestamp format: RFC 3339 in UTC with nanoseconds.
func Now() string { return Clock().UTC().Format(time.RFC3339Nano) }

// stampLayout is RFC 3339 in UTC with all nine digits of the fraction. Now drops trailing zeros,
// so its times do not sort as text; these do, and SQL compares them as text.
const stampLayout = "2006-01-02T15:04:05.000000000Z"

// Stamp is t in the fixed-width format of the times SQL compares (an approval's expiry, when a
// doorbell is due).
func Stamp(t time.Time) string { return t.UTC().Format(stampLayout) }

// LookupRun returns the run only when both the workflow ID and the run ID match the
// execution agentd started. Checking the run ID matters: Temporal lets a new execution
// (for example a child workflow started by a compromised Worker) reuse the workflow ID
// of a run that has already finished.
func (l *Ledger) LookupRun(ctx context.Context, workflowID, runID string) (db.Run, error) {
	r, err := l.Q.GetRun(ctx, workflowID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return db.Run{}, ErrUnknownRun
	case err != nil:
		return db.Run{}, err
	case !r.RunID.Valid:
		return db.Run{}, ErrRunStarting
	case r.RunID.String != runID:
		return db.Run{}, ErrUnknownRun
	}
	return r, nil
}

// PutBlob stores b and returns its reference, "sha256:<hex>".
func (l *Ledger) PutBlob(ctx context.Context, b []byte) (string, error) {
	h := sha256Hex(b)
	if err := l.Tx(ctx, func(q *db.Queries) error { return q.PutBlob(ctx, db.PutBlobParams{Sha256: h, Bytes: b}) }); err != nil {
		return "", err
	}
	return "sha256:" + h, nil
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// ErrBadBlob: the bytes the ledger holds under a blob's key do not hash to it. They were changed
// since they were stored, and nothing reads them.
var ErrBadBlob = errors.New("the blob's bytes do not hash to its key")

// GetBlob returns the bytes behind a reference returned by PutBlob.
// ErrNotOwner means a run asked for a blob it did not produce.
var ErrNotOwner = errors.New("the blob was not produced by this run")

// GetRunBlob returns the blob ref names when run produced it, and ErrNotOwner otherwise:
// blobs are shared by content, and a reference learned from another run's history must not
// carry that run's data into this one.
func (l *Ledger) GetRunBlob(ctx context.Context, workflowID, ref string) ([]byte, error) {
	h, ok := strings.CutPrefix(ref, "sha256:")
	if !ok {
		return nil, fmt.Errorf("bad blob reference %q", ref)
	}
	owned, err := l.Q.IsBlobOwner(ctx, db.IsBlobOwnerParams{WorkflowID: workflowID, Sha256: h})
	if err != nil {
		return nil, err
	}
	if owned == 0 {
		return nil, ErrNotOwner
	}
	return l.GetBlob(ctx, ref)
}

func (l *Ledger) GetBlob(ctx context.Context, ref string) ([]byte, error) {
	return BlobIn(ctx, l.Q, ref)
}

// BlobIn is GetBlob as q reads it: in a transaction of the caller's.
func BlobIn(ctx context.Context, q *db.Queries, ref string) ([]byte, error) {
	h, ok := strings.CutPrefix(ref, "sha256:")
	if !ok {
		return nil, fmt.Errorf("bad blob reference %q", ref)
	}
	b, err := q.GetBlob(ctx, h)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, ErrNotFound
	case err != nil:
		return nil, err
	case sha256Hex(b) != h:
		return nil, fmt.Errorf("%w: %s holds bytes with sha256 %s", ErrBadBlob, ref, sha256Hex(b))
	}
	return b, nil
}

// LatestEffectResult returns the result of the run's latest finished effect of tool, sent or
// dry-run.
func (l *Ledger) LatestEffectResult(ctx context.Context, workflowID, tool string) (map[string]any, bool, error) {
	e, err := l.Q.GetLatestEffect(ctx, db.GetLatestEffectParams{WorkflowID: workflowID, Tool: tool})
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, false, nil
	case err != nil:
		return nil, false, err
	case !e.ResultBlob.Valid:
		return nil, false, nil
	}
	b, err := l.GetBlob(ctx, e.ResultBlob.String)
	if err != nil {
		return nil, false, err
	}
	v, err := DecodeObject(b)
	return v, err == nil, err
}

// ModelOutputs returns what the workflow got from each of the run's accepted answers of a model
// task, newest first: the gateway's value (report_ref, the blob fields' refs, the other fields
// inline). Refusals and answers that broke the task's output schema are not answers.
func (l *Ledger) ModelOutputs(ctx context.Context, workflowID, task string) ([]map[string]any, error) {
	rows, err := l.Q.ListModelAnswers(ctx, db.ListModelAnswersParams{WorkflowID: workflowID, Task: task})
	if err != nil {
		return nil, err
	}
	type answer struct {
		at    time.Time
		value map[string]any
	}
	var answers []answer
	for _, r := range rows {
		finished, err := time.Parse(time.RFC3339Nano, r.FinishedAt.String)
		if err != nil || r.StopReason.String == "invalid_output" {
			continue
		}
		b, err := l.GetBlob(ctx, r.ResponseBlob.String)
		if err != nil {
			return nil, err
		}
		var resp struct {
			Status string          `json:"status"`
			Value  json.RawMessage `json:"value"`
		}
		if err := json.Unmarshal(b, &resp); err != nil || resp.Status != "ok" {
			continue
		}
		if v, err := DecodeObject(resp.Value); err == nil {
			answers = append(answers, answer{finished, v})
		}
	}
	slices.SortStableFunc(answers, func(a, b answer) int { return b.at.Compare(a.at) })
	out := make([]map[string]any, 0, len(answers))
	for _, a := range answers {
		out = append(out, a.value)
	}
	return out, nil
}

// RunApprovals lists a run's approvals, oldest first.
func (l *Ledger) RunApprovals(ctx context.Context, workflowID string) ([]db.Approval, error) {
	return l.Q.ListRunApprovals(ctx, workflowID)
}

// DecodeObject decodes a JSON object, keeping numbers exact.
func DecodeObject(b []byte) (map[string]any, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var v map[string]any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	if v == nil {
		return nil, errors.New("not a JSON object")
	}
	return v, nil
}
