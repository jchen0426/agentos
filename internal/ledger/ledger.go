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
	"strings"
	"time"

	"github.com/pressly/goose/v3"
	_ "modernc.org/sqlite" // database/sql driver "sqlite"

	"github.com/jchen0426/agentos/internal/ledger/db"
)

//go:embed migrations/*.sql
var migrations embed.FS

var (
	// ErrUnknownRun means the caller is not an execution agentd started through Control.
	ErrUnknownRun = errors.New("unknown run")
	// ErrRunStarting means the run row exists but StartRun has not recorded the run ID yet.
	ErrRunStarting = errors.New("run registration in progress")
	// ErrNotFound is returned for missing blobs and records.
	ErrNotFound = errors.New("not found")
	// ErrMigrationMismatch means another agentd migrated the ledger: one with migrations this
	// one does not have, or with another file under one of its migration numbers.
	ErrMigrationMismatch = errors.New("migrated by another agentd")
)

// Ledger is agentd's SQLite store. Writers serialize on SQLite's write lock: every
// transaction starts with BEGIN IMMEDIATE (_txlock=immediate).
type Ledger struct {
	DB *sql.DB
	Q  *db.Queries
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
	dsn := "file:" + path +
		"?_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=busy_timeout(10000)" +
		"&_pragma=foreign_keys(ON)&_txlock=immediate"
	sqlDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	provider, err := goose.NewProvider(goose.DialectSQLite3, sqlDB, fsys)
	if err != nil {
		sqlDB.Close()
		return nil, err
	}
	if err := checkMigrations(ctx, sqlDB, path, fsys, provider.ListSources()); err != nil {
		sqlDB.Close()
		return nil, err
	}
	if _, err := provider.Up(ctx); err != nil {
		sqlDB.Close()
		return nil, fmt.Errorf("migrate ledger: %w", err)
	}
	return &Ledger{DB: sqlDB, Q: db.New(sqlDB)}, nil
}

func (l *Ledger) Close() error { return l.DB.Close() }

// Tx runs fn in one write transaction and commits it unless fn fails. Transactions begin
// IMMEDIATE (see the DSN): what fn reads cannot change before it writes.
func (l *Ledger) Tx(ctx context.Context, fn func(q *db.Queries) error) error {
	tx, err := l.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(l.Q.WithTx(tx)); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// Now is the ledger's timestamp format: RFC 3339 in UTC with nanoseconds.
func Now() string { return time.Now().UTC().Format(time.RFC3339Nano) }

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
	sum := sha256.Sum256(b)
	h := hex.EncodeToString(sum[:])
	if err := l.Q.PutBlob(ctx, db.PutBlobParams{Sha256: h, Bytes: b}); err != nil {
		return "", err
	}
	return "sha256:" + h, nil
}

// GetBlob returns the bytes behind a reference returned by PutBlob.
// ErrNotOwner means a run asked for a blob it did not produce.
var ErrNotOwner = errors.New("the blob was not produced by this run")

// PutRunBlob stores b as a blob that run produced. Only that run may have it read into a
// model's prompt (GetRunBlob).
func (l *Ledger) PutRunBlob(ctx context.Context, workflowID string, b []byte) (string, error) {
	ref, err := l.PutBlob(ctx, b)
	if err != nil {
		return "", err
	}
	return ref, l.Q.AddBlobOwner(ctx, db.AddBlobOwnerParams{WorkflowID: workflowID, Sha256: strings.TrimPrefix(ref, "sha256:")})
}

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
	h, ok := strings.CutPrefix(ref, "sha256:")
	if !ok {
		return nil, fmt.Errorf("bad blob reference %q", ref)
	}
	b, err := l.Q.GetBlob(ctx, h)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return b, err
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
