package ledger

import (
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/pressly/goose/v3"
)

// thisAgentd returns the migrations Open applies.
func thisAgentd(t *testing.T) fstest.MapFS {
	t.Helper()
	sub, err := fs.Sub(migrations, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	names, err := fs.Glob(sub, "*.sql")
	if err != nil {
		t.Fatal(err)
	}
	m := fstest.MapFS{}
	for _, name := range names {
		b, err := fs.ReadFile(sub, name)
		if err != nil {
			t.Fatal(err)
		}
		m[name] = &fstest.MapFile{Data: b}
	}
	return m
}

// next returns the first migration number m does not have: the one a branch adds next.
func next(t *testing.T, m fstest.MapFS) int64 {
	t.Helper()
	var v int64
	for name := range m {
		n, err := goose.NumericComponent(name)
		if err != nil {
			t.Fatal(err)
		}
		v = max(v, n)
	}
	return v + 1
}

// upTo returns the migrations in m up to version last: what an agentd built before the later
// ones had.
func upTo(t *testing.T, m fstest.MapFS, last int64) fstest.MapFS {
	t.Helper()
	out := maps.Clone(m)
	maps.DeleteFunc(out, func(name string, _ *fstest.MapFile) bool {
		v, err := goose.NumericComponent(name)
		if err != nil {
			t.Fatal(err)
		}
		return v > last
	})
	return out
}

func file(version int64, table string) string { return fmt.Sprintf("%05d_%s.sql", version, table) }

// with returns m plus the migration file name, which creates table: the migrations of an
// agentd built from another branch, or after an edit.
func with(m fstest.MapFS, name, table string) fstest.MapFS {
	out := maps.Clone(m)
	out[name] = &fstest.MapFile{Data: []byte("-- +goose Up\nCREATE TABLE " + table + " (id INTEGER PRIMARY KEY);\n\n" +
		"-- +goose Down\nDROP TABLE " + table + ";\n")}
	return out
}

// openAs opens the ledger at path as an agentd with the migrations m would, and closes it.
func openAs(t *testing.T, path string, m fs.FS) error {
	t.Helper()
	l, err := open(t.Context(), path, m)
	if err == nil {
		l.Close()
	}
	return err
}

// migrateUnrecorded migrates the ledger at path with m as agentd did before it recorded its
// migrations: goose alone.
func migrateUnrecorded(t *testing.T, path string, m fs.FS) {
	t.Helper()
	sqlDB, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	p, err := goose.NewProvider(goose.DialectSQLite3, sqlDB, m)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Up(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func execSQL(t *testing.T, path string, stmts ...string) {
	t.Helper()
	sqlDB, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	for _, s := range stmts {
		if _, err := sqlDB.ExecContext(t.Context(), s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
}

func hasTable(t *testing.T, path, table string) bool {
	t.Helper()
	sqlDB, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	var n int
	if err := sqlDB.QueryRowContext(t.Context(),
		"SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?", table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n > 0
}

// wantRefused checks that err refuses the ledger at path for its migrations, names each of
// words, and says how to start a fresh ledger: move it out of its directory, with its WAL and
// shared memory.
func wantRefused(t *testing.T, err error, path string, words ...string) {
	t.Helper()
	if !errors.Is(err, ErrMigrationMismatch) {
		t.Fatalf("opening the ledger: %v; want it refused for its migrations", err)
	}
	msg := err.Error()
	for _, s := range []string{filepath.Dir(path), "ledger.db-wal", "ledger.db-shm"} {
		if !strings.Contains(msg, s) {
			t.Errorf("the refusal does not say to move the ledger's files out of its directory (%s): %s", s, msg)
		}
	}
	// Not the digits of the ledger's directory.
	rest := strings.ReplaceAll(msg, filepath.Dir(path), "")
	for _, w := range words {
		if !regexp.MustCompile(`\b` + regexp.QuoteMeta(w) + `\b`).MatchString(rest) {
			t.Errorf("the refusal does not name %s: %s", w, msg)
		}
	}
}

// wantVersions checks that the refusal gives the ledger's highest migration and this agentd's.
func wantVersions(t *testing.T, err error, ledger, agentd int64) {
	t.Helper()
	want := fmt.Sprintf("the ledger is at migration %d, this agentd's migrations go up to %d", ledger, agentd)
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("the refusal does not say %q: %v", want, err)
	}
}

// Another branch's agentd, with a migration this one does not have, migrated the ledger all
// checkouts share. This agentd was not written for that schema.
func TestOpenRefusesALedgerANewerAgentdMigrated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.db")
	this := thisAgentd(t)
	v := next(t, this)
	newer := with(this, file(v, "newer"), "newer")
	if err := openAs(t, path, newer); err != nil {
		t.Fatal(err)
	}
	l, err := Open(t.Context(), path)
	if err == nil {
		l.Close()
	}
	// The migration, and the file the ledger applied as it.
	wantRefused(t, err, path, fmt.Sprint(v), file(v, "newer"))
	wantVersions(t, err, v, v-1)
	if err := openAs(t, path, newer); err != nil {
		t.Fatalf("the agentd that migrated the ledger, after the refusal: %v", err)
	}
}

// Two branches numbered their migrations apart: the ledger has one branch's, this agentd the
// other's, one number higher. The ledger's highest version is below this agentd's, yet it holds
// a migration this agentd does not know.
func TestOpenRefusesAnUnknownMigrationBelowItsOwnHighest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.db")
	this := thisAgentd(t)
	v := next(t, this)
	if err := openAs(t, path, with(this, file(v, "theirs"), "theirs")); err != nil {
		t.Fatal(err)
	}
	err := openAs(t, path, with(this, file(v+1, "ours"), "ours"))
	wantRefused(t, err, path, fmt.Sprint(v))
	wantVersions(t, err, v, v+1)
	if hasTable(t, path, "ours") {
		t.Error("the refused agentd applied its own migration first")
	}
}

// Two branches each added the same migration number. goose takes the number as applied, so the
// second branch's migration would never run.
func TestOpenRefusesAnotherBranchsMigrationUnderTheSameNumber(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.db")
	this := thisAgentd(t)
	v := next(t, this)
	theirs := with(this, file(v, "theirs"), "theirs")
	if err := openAs(t, path, theirs); err != nil {
		t.Fatal(err)
	}
	err := openAs(t, path, with(this, file(v, "ours"), "ours"))
	wantRefused(t, err, path, file(v, "theirs"), file(v, "ours"))
	if err := openAs(t, path, theirs); err != nil {
		t.Fatalf("the agentd that migrated the ledger, after the refusal: %v", err)
	}
}

// A migration edited after the ledger applied it: the ledger's schema is not what the file
// says now.
func TestOpenRefusesAMigrationEditedAfterItWasApplied(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.db")
	this := thisAgentd(t)
	name := file(next(t, this), "mine")
	if err := openAs(t, path, with(this, name, "mine")); err != nil {
		t.Fatal(err)
	}
	err := openAs(t, path, with(this, name, "mine_renamed"))
	wantRefused(t, err, path, name)
}

// A ledger from before agentd recorded its migrations: nothing says which files its migrations
// were. Those up to 00007, all there were then, are taken to be this agentd's, and held to that.
func TestOpenTakesTheMigrationsOfAnOlderLedgerAsItsOwn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.db")
	this := thisAgentd(t)
	older := upTo(t, this, 7)
	if len(older) != 7 {
		t.Fatalf("the migrations up to 00007: %v", slices.Sorted(maps.Keys(older)))
	}
	migrateUnrecorded(t, path, older)
	if err := openAs(t, path, this); err != nil {
		t.Fatalf("a ledger from before migrations were recorded: %v", err)
	}

	const seventh = "00007_effect_claims.sql"
	edited := maps.Clone(this)
	edited[seventh] = &fstest.MapFile{Data: append(slices.Clone(this[seventh].Data), "-- edited\n"...)}
	err := openAs(t, path, edited)
	wantRefused(t, err, path, seventh)
}

// An agentd from before agentd recorded its migrations applied a migration 8 of its own. Nothing
// says which file that was, so this agentd cannot tell it from its own 8. Migrations up to 7 are
// all there were before the records: 8 is the first that must have one, whatever this agentd has.
func TestOpenRefusesAMigration8AppliedWithoutARecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.db")
	this := thisAgentd(t)
	migrateUnrecorded(t, path, with(upTo(t, this, 7), file(8, "theirs"), "theirs"))
	ours := this
	if next(t, this) == 8 {
		ours = with(this, file(8, "ours"), "ours")
	}
	wantRefused(t, openAs(t, path, ours), path, "8")
}

// Another agentd could not apply its next migration. That holds back no agentd's migration under
// the same number.
func TestOpenAppliesItsMigrationAfterAnotherAgentdFailedToApplyTheirs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.db")
	this := thisAgentd(t)
	v := next(t, this)
	broken := maps.Clone(this)
	broken[file(v, "theirs")] = &fstest.MapFile{Data: []byte("-- +goose Up\nCREATE TABLE theirs (;\n")}
	if err := openAs(t, path, broken); err == nil || errors.Is(err, ErrMigrationMismatch) {
		t.Fatalf("a migration that cannot run: %v", err)
	}
	ours := with(this, file(v, "ours"), "ours")
	if err := openAs(t, path, ours); err != nil {
		t.Fatalf("after another agentd failed to apply its migration %d: %v", v, err)
	}
	if !hasTable(t, path, "ours") {
		t.Error("this agentd's migration did not run")
	}
	// The ledger's record is now this agentd's file.
	if err := openAs(t, path, ours); err != nil {
		t.Fatalf("reopening: %v", err)
	}
}

// The ledger skipped a migration this agentd has below one it applied: a branch numbered its
// migration past another branch's that had not merged yet, and this agentd has both.
func TestOpenRefusesALedgerThatSkippedOneOfItsMigrations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.db")
	this := thisAgentd(t)
	v := next(t, this)
	later := with(this, file(v+1, "later"), "later")
	if err := openAs(t, path, later); err != nil {
		t.Fatal(err)
	}
	err := openAs(t, path, with(later, file(v, "earlier"), "earlier"))
	wantRefused(t, err, path, fmt.Sprint(v))
}

// This agentd could not apply its next migration (the ledger's data broke it, or it was stopped
// halfway), and then an agentd that keeps no record applied its own under that number. Starting
// again, this agentd must not take that migration for its own.
func TestOpenRefusesAMigrationAnotherAgentdAppliedAfterItsOwnFailed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.db")
	this := thisAgentd(t)
	v := next(t, this)
	ours := with(this, file(v, "ours"), "ours")
	if err := openAs(t, path, this); err != nil {
		t.Fatal(err)
	}
	execSQL(t, path, "CREATE TABLE ours (x)") // what this agentd's migration trips over
	if err := openAs(t, path, ours); err == nil || errors.Is(err, ErrMigrationMismatch) {
		t.Fatalf("a migration that cannot run on this ledger: %v", err)
	}
	execSQL(t, path, "DROP TABLE ours")
	migrateUnrecorded(t, path, with(this, file(v, "theirs"), "theirs"))
	err := openAs(t, path, ours)
	wantRefused(t, err, path, fmt.Sprint(v))
}

// A record of a migration the ledger has not applied (goose's down leaves one behind) vouches for
// nothing, even when it names this agentd's file: the next agentd to start drops it.
func TestOpenDropsTheRecordOfAMigrationTheLedgerHasNotApplied(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.db")
	this := thisAgentd(t)
	v := next(t, this)
	ours := with(this, file(v, "ours"), "ours")
	if err := openAs(t, path, this); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(ours[file(v, "ours")].Data)
	execSQL(t, path, fmt.Sprintf("INSERT INTO migration_files (version, name, sha256) VALUES (%d, '%s', '%x')",
		v, file(v, "ours"), sum))
	if err := openAs(t, path, this); err != nil {
		t.Fatal(err)
	}
	migrateUnrecorded(t, path, with(this, file(v, "theirs"), "theirs"))
	err := openAs(t, path, ours)
	wantRefused(t, err, path, fmt.Sprint(v))
}

// SQLite finds goose's version table whatever the case of its name, and a view by that name too.
// So must the check, or a ledger at a migration this agentd lacks would get past it to goose.
func TestOpenFindsGoosesTableAsSQLiteDoes(t *testing.T) {
	for name, reshape := range map[string][]string{
		"capitals": {"ALTER TABLE goose_db_version RENAME TO gv", "ALTER TABLE gv RENAME TO GOOSE_DB_VERSION"},
		"view":     {"ALTER TABLE goose_db_version RENAME TO gv", "CREATE VIEW goose_db_version AS SELECT * FROM gv"},
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "ledger.db")
			this := thisAgentd(t)
			v := next(t, this)
			if err := openAs(t, path, with(this, file(v, "newer"), "newer")); err != nil {
				t.Fatal(err)
			}
			execSQL(t, path, reshape...)
			wantRefused(t, openAs(t, path, this), path, fmt.Sprint(v))
		})
	}
}
