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
)

// Migrations up to 7 were applied before agentd recorded which file each migration was: a
// ledger with no record of them is taken to have applied this agentd's files. Every later
// migration is recorded before goose applies it.
const lastUnrecordedMigration = 7

// migration_files holds the file behind each migration version, as the agentd that applied it
// had it. A version's row is written before goose applies the migration, in a transaction of
// its own, so the table cannot come from a migration.
const createMigrationFiles = `CREATE TABLE IF NOT EXISTS migration_files (
  version INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  sha256 TEXT NOT NULL      -- of the file, hex
)`

type migrationFile struct{ name, sha256 string }

// checkMigrations refuses a ledger another agentd migrated (ErrMigrationMismatch), and records
// which file each of this agentd's migrations is before goose applies any. goose records only
// version numbers, and ignores applied versions it has no file for: an older agentd would run
// on a schema it was not written for, and of two branches' migrations with the same number,
// the second would never run.
func checkMigrations(ctx context.Context, sqlDB *sql.DB, path string, fsys fs.FS, sources []*goose.Source) error {
	files := make(map[int64]migrationFile, len(sources))
	for _, s := range sources {
		b, err := fs.ReadFile(fsys, s.Path)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(b)
		files[s.Version] = migrationFile{name: s.Path, sha256: hex.EncodeToString(sum[:])}
	}
	// One IMMEDIATE transaction: what the check reads cannot change before the records are
	// written.
	tx, err := sqlDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	applied, err := appliedVersions(ctx, tx)
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
			"if they exist, out of %s; runs recorded in them cannot continue",
			path, ErrMigrationMismatch, slices.Max(slices.Collect(maps.Keys(applied))),
			slices.Max(slices.Collect(maps.Keys(files))), strings.Join(problems, "; "),
			name, name, name, filepath.Dir(path))
	}
	if _, err := tx.ExecContext(ctx, createMigrationFiles); err != nil {
		return err
	}
	for v, f := range files {
		if _, ok := recorded[v]; ok && applied[v] {
			continue
		}
		// Applied without a record (up to 7): taken as this file. Not applied: goose applies
		// it next, and a record left by an agentd that never applied it is replaced.
		if _, err := tx.ExecContext(ctx, `INSERT OR REPLACE INTO migration_files (version, name, sha256)
			VALUES (?, ?, ?)`, v, f.name, f.sha256); err != nil {
			return err
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
		problems = append(problems, fmt.Sprintf("it applied %s, which this agentd does not have", versionList(unknown)))
	}
	problems = append(problems, other...)
	if len(unrecorded) > 0 {
		problems = append(problems, fmt.Sprintf("its %s came from an agentd that kept no record of migration files",
			versionList(unrecorded)))
	}
	if len(skipped) > 0 {
		problems = append(problems, fmt.Sprintf("it has not applied %s, yet it applied migration %d",
			versionList(skipped), last))
	}
	return problems
}

// versionList says "migration 8" or "migrations 8, 9".
func versionList(vs []int64) string {
	s := make([]string, len(vs))
	for i, v := range vs {
		s[i] = strconv.FormatInt(v, 10)
	}
	if len(s) == 1 {
		return "migration " + s[0]
	}
	return "migrations " + strings.Join(s, ", ")
}

// appliedVersions returns the migrations goose has applied: every version in its table but its
// starting 0, as goose's provider reads the table.
func appliedVersions(ctx context.Context, tx *sql.Tx) (map[int64]bool, error) {
	applied := map[int64]bool{}
	if ok, err := tableExists(ctx, tx, goose.DefaultTablename); err != nil || !ok {
		return applied, err
	}
	rows, err := tx.QueryContext(ctx, "SELECT version_id FROM "+goose.DefaultTablename+" WHERE version_id != 0")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		applied[v] = true
	}
	return applied, rows.Err()
}

func recordedFiles(ctx context.Context, tx *sql.Tx) (map[int64]migrationFile, error) {
	recorded := map[int64]migrationFile{}
	if ok, err := tableExists(ctx, tx, "migration_files"); err != nil || !ok {
		return recorded, err
	}
	rows, err := tx.QueryContext(ctx, "SELECT version, name, sha256 FROM migration_files")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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

func tableExists(ctx context.Context, tx *sql.Tx, name string) (bool, error) {
	var n int
	err := tx.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?", name).Scan(&n)
	return n > 0, err
}
