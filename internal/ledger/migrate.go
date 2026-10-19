package ledger

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io/fs"
	"maps"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
)

// Migrations up to 7 were applied before agentd recorded which file each migration was: a
// ledger with no record of them is taken to have applied this agentd's files. Every later
// migration is recorded as it is applied.
const lastUnrecordedMigration = 7

// migration_files holds the file behind each applied migration, as the agentd that applied it had
// it. A row is written in the transaction that records its migration as applied, so the table
// cannot come from a migration: it has to be there first.
const createMigrationFiles = `CREATE TABLE IF NOT EXISTS migration_files (
  version INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  sha256 TEXT NOT NULL      -- of the file, hex
)`

type migrationFile struct{ name, sha256 string }

// migrate refuses a ledger another agentd migrated (checkMigrations), then applies this agentd's
// pending migrations, recording which file each one was.
func migrate(ctx context.Context, sqlDB *sql.DB, path string, fsys fs.FS) error {
	store, err := database.NewStore(goose.DialectSQLite3, goose.DefaultTablename)
	if err != nil {
		return err
	}
	rec := &recordingStore{Store: store}
	provider, err := goose.NewProvider(goose.DialectCustom, sqlDB, fsys, goose.WithStore(rec))
	if err != nil {
		return err
	}
	if rec.files, err = migrationFiles(fsys, provider.ListSources()); err != nil {
		return err
	}
	if err := checkMigrations(ctx, sqlDB, path, store, rec.files); err != nil {
		return err
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("migrate ledger: %w", err)
	}
	return nil
}

func migrationFiles(fsys fs.FS, sources []*goose.Source) (map[int64]migrationFile, error) {
	files := make(map[int64]migrationFile, len(sources))
	for _, s := range sources {
		if s.Type != goose.TypeSQL {
			return nil, fmt.Errorf("migration %d is not a SQL file: the ledger records only SQL migrations", s.Version)
		}
		b, err := fs.ReadFile(fsys, s.Path)
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256(b)
		files[s.Version] = migrationFile{name: s.Path, sha256: hex.EncodeToString(sum[:])}
	}
	return files, nil
}

// recordingStore is goose's SQLite store, recording which file a migration was in the transaction
// that records the migration as applied. goose runs each of the ledger's migrations in one
// transaction, so the migration, its version row and its record commit together: a record means
// that file was applied.
type recordingStore struct {
	database.Store
	files map[int64]migrationFile
}

func (s *recordingStore) Insert(ctx context.Context, db database.DBTxConn, req database.InsertRequest) error {
	if err := s.Store.Insert(ctx, db, req); err != nil || req.Version == 0 { // 0: goose's starting row
		return err
	}
	f, ok := s.files[req.Version]
	if !ok {
		return fmt.Errorf("no file for migration %d", req.Version)
	}
	_, err := db.ExecContext(ctx, "INSERT OR REPLACE INTO migration_files (version, name, sha256) VALUES (?, ?, ?)",
		req.Version, f.name, f.sha256)
	return err
}

// checkMigrations refuses a ledger another agentd migrated (ErrMigrationMismatch). goose records
// only version numbers, and ignores applied versions it has no file for: an older agentd would
// run on a schema it was not written for, and of two branches' migrations with the same number,
// the second would never run. A ledger that passes has this agentd's files recorded for its
// migrations up to 7 that have no record, and loses the records of migrations it has not applied
// (goose's down leaves them behind).
func checkMigrations(ctx context.Context, sqlDB *sql.DB, path string, store database.Store, files map[int64]migrationFile) error {
	// One IMMEDIATE transaction: what the check reads cannot change before it writes. A refusal
	// rolls it all back, the table included.
	tx, err := sqlDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, createMigrationFiles); err != nil {
		return err
	}
	applied, err := appliedVersions(ctx, tx, store)
	if err != nil {
		return err
	}
	recorded, err := recordedFiles(ctx, tx)
	if err != nil {
		return err
	}
	if problems := mismatches(applied, recorded, files); len(problems) > 0 {
		name := filepath.Base(path)
		return fmt.Errorf("ledger %s: %w (the ledger is at migration %d, this agentd's migrations go up to %d): %s. "+
			"Use the agentd that migrated it, or start a fresh development ledger: move %s, and %s-wal and %s-shm "+
			"if they exist, into a new directory in %s, out of Workers' reach; runs recorded in them cannot continue",
			path, ErrMigrationMismatch, slices.Max(slices.Collect(maps.Keys(applied))),
			slices.Max(slices.Collect(maps.Keys(files))), strings.Join(problems, "; "),
			name, name, name, filepath.Dir(path))
	}
	for v := range recorded {
		if !applied[v] {
			if _, err := tx.ExecContext(ctx, "DELETE FROM migration_files WHERE version = ?", v); err != nil {
				return err
			}
		}
	}
	for v := range applied {
		if _, ok := recorded[v]; !ok { // up to 7: the check refuses any later one
			if _, err := tx.ExecContext(ctx, "INSERT INTO migration_files (version, name, sha256) VALUES (?, ?, ?)",
				v, files[v].name, files[v].sha256); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

// mismatches says how the migrations the ledger applied differ from this agentd's files.
func mismatches(applied map[int64]bool, recorded, files map[int64]migrationFile) []string {
	var unknown, unrecorded, skipped []int64
	var other []string
	var last int64
	for _, v := range slices.Sorted(maps.Keys(applied)) {
		last = v
		f, ok := files[v]
		r, isRecorded := recorded[v]
		switch {
		case !ok:
			unknown = append(unknown, v)
		case isRecorded && r.sha256 != f.sha256 && r.name == f.name:
			other = append(other, fmt.Sprintf("its migration %d, %s, has changed since the ledger applied it "+
				"(sha256 %.12s then, %.12s now)", v, f.name, r.sha256, f.sha256))
		case isRecorded && r.sha256 != f.sha256:
			other = append(other, fmt.Sprintf("its migration %d was %s (sha256 %.12s), this agentd's is %s (sha256 %.12s)",
				v, r.name, r.sha256, f.name, f.sha256))
		case !isRecorded && v > lastUnrecordedMigration:
			unrecorded = append(unrecorded, v)
		}
	}
	for _, v := range slices.Sorted(maps.Keys(files)) {
		if v < last && !applied[v] {
			skipped = append(skipped, v)
		}
	}
	var problems []string
	if len(unknown) > 0 {
		problems = append(problems, fmt.Sprintf("it applied %s, which this agentd does not have", versionList(unknown, recorded)))
	}
	problems = append(problems, other...)
	if len(unrecorded) > 0 {
		problems = append(problems, fmt.Sprintf("its %s came from an agentd that kept no record of migration files",
			versionList(unrecorded, nil)))
	}
	if len(skipped) > 0 {
		problems = append(problems, fmt.Sprintf("it has not applied %s, yet it applied migration %d",
			versionList(skipped, nil), last))
	}
	return problems
}

// versionList says "migration 8" or "migrations 8, 9", with the file recorded for each, if any.
func versionList(vs []int64, recorded map[int64]migrationFile) string {
	s := make([]string, len(vs))
	for i, v := range vs {
		s[i] = strconv.FormatInt(v, 10)
		if r, ok := recorded[v]; ok {
			s[i] += " (" + r.name + ")"
		}
	}
	if len(s) == 1 {
		return "migration " + s[0]
	}
	return "migrations " + strings.Join(s, ", ")
}

// appliedVersions returns the migrations goose has applied: every version in its table but its
// starting 0, as goose's provider reads the table.
func appliedVersions(ctx context.Context, tx *sql.Tx, store database.Store) (map[int64]bool, error) {
	applied := map[int64]bool{}
	// Looked up as SQLite finds a table: in any case, and a view too.
	var n int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type IN ('table', 'view') "+
		"AND name = ? COLLATE NOCASE", store.Tablename()).Scan(&n); err != nil || n == 0 {
		return applied, err
	}
	rows, err := store.ListMigrations(ctx, tx)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		if r.Version != 0 {
			applied[r.Version] = true
		}
	}
	return applied, nil
}

func recordedFiles(ctx context.Context, tx *sql.Tx) (map[int64]migrationFile, error) {
	rows, err := tx.QueryContext(ctx, "SELECT version, name, sha256 FROM migration_files")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	recorded := map[int64]migrationFile{}
	for rows.Next() {
		var v int64
		var f migrationFile
		if err := rows.Scan(&v, &f.name, &f.sha256); err != nil {
			return nil, err
		}
		recorded[v] = f
	}
	return recorded, rows.Err()
}
