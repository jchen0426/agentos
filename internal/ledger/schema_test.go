package ledger

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"modernc.org/sqlite"

	"github.com/jchen0426/agentos/internal/ledger/db"
)

// written has stmts write sqlite_master directly, as PRAGMA writable_schema lets anyone: objects
// CREATE refuses, and no change of schema_version.
func written(stmts ...string) string {
	return "PRAGMA writable_schema=ON; " + strings.Join(stmts, "; ") + "; PRAGMA writable_schema=OFF"
}

// noPut is a trigger that drops every insert into blobs, as a row of sqlite_master.
func noPut(name string) string {
	return fmt.Sprintf(`INSERT INTO sqlite_master (type, name, tbl_name, rootpage, sql) VALUES ('trigger', '%[1]s', 'blobs', 0,
		'CREATE TRIGGER %[1]s BEFORE INSERT ON blobs BEGIN SELECT RAISE(IGNORE); END')`, name)
}

// A ledger with an object no migration makes, or without one they make, or with one of theirs
// changed, is not opened, and the error names it: a trigger can make a write do nothing (agentd
// would read a call never counted as counted), an index can make an insert skip its row.
func TestALedgerWithAnotherSchemaIsNotOpened(t *testing.T) {
	ctx := context.Background()
	for name, c := range map[string]struct{ change, want string }{
		"a trigger":                {`CREATE TRIGGER no_count BEFORE INSERT ON calls BEGIN SELECT RAISE(IGNORE); END`, `trigger "no_count" on "calls", which no migration makes`},
		"an index":                 {`CREATE UNIQUE INDEX one_call ON calls (workflow_id)`, `index "one_call" on "calls", which no migration makes`},
		"a view":                   {`CREATE VIEW v AS SELECT 1`, `view "v" on "v", which no migration makes`},
		"no index":                 {`DROP INDEX effects_by_status`, `no index "effects_by_status", which the migrations make`},
		"a column":                 {`ALTER TABLE runs ADD COLUMN note TEXT`, `table "runs" on "runs" is not the migrations'`},
		"a table":                  {`CREATE TABLE extra (n INTEGER)`, `table "extra" on "extra", which no migration makes`},
		"a trigger, removed again": {`CREATE TRIGGER t BEFORE INSERT ON calls BEGIN SELECT 1; END; DROP TRIGGER t`, ""},
		// SQLite's own names are compared too: what CREATE refuses can be written into sqlite_master.
		"a trigger named sqlite_": {written(noPut("sqlite_x")), `trigger "sqlite_x" on "blobs", which no migration makes`},
		"a view named sqlite_": {written(`INSERT INTO sqlite_master VALUES ('view', 'sqlite_v', 'sqlite_v', 0, 'CREATE VIEW sqlite_v AS SELECT 1')`),
			`view "sqlite_v" on "sqlite_v", which no migration makes`},
		"an index of SQLite's, twice": {written(`INSERT INTO sqlite_master SELECT * FROM sqlite_master WHERE name = 'sqlite_autoindex_calls_1'`),
			`2 rows for index "sqlite_autoindex_calls_1", where the migrations make 1`},
		"an index of SQLite's, its SQL empty": {written(`UPDATE sqlite_master SET sql = '' WHERE name = 'sqlite_autoindex_calls_1'`),
			`index "sqlite_autoindex_calls_1" on "calls" is not the migrations' ("")`},
		"SQLite's sequence table, changed": {written(`UPDATE sqlite_master SET sql = 'CREATE TABLE sqlite_sequence(name,seq,x)' WHERE name = 'sqlite_sequence'`),
			`table "sqlite_sequence" on "sqlite_sequence" is not the migrations' ("CREATE TABLE sqlite_sequence(name,seq,x)")`},
		"ANALYZE's tables": {`ANALYZE`, ""},
		"ANALYZE's table, changed": {`ANALYZE; ` + written(`UPDATE sqlite_master SET sql = 'CREATE TABLE sqlite_stat1(tbl,idx,stat,x)' WHERE name = 'sqlite_stat1'`),
			`table "sqlite_stat1" on "sqlite_stat1", which no migration makes`},
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "ledger.db")
			l, err := Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			l.Close()
			raw, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := raw.Exec(c.change); err != nil {
				t.Fatal(err)
			}
			raw.Close()
			l, err = Open(ctx, path)
			if c.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				l.Close()
				return
			}
			if !errors.Is(err, ErrSchemaMismatch) || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("opened: %v; want %v naming %s", err, ErrSchemaMismatch, c.want)
			}
		})
	}
}

// A trigger planted while agentd runs: the next write transaction finds the schema changed, and
// fails before it writes; with the trigger gone, writes go on.
func TestAWriteFailsOnceTheSchemaChanged(t *testing.T) {
	ctx := context.Background()
	l, err := Open(ctx, filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	put := func() error {
		return l.Tx(ctx, func(q *db.Queries) error { return q.PutBlob(ctx, db.PutBlobParams{Sha256: "ab", Bytes: []byte("x")}) })
	}
	if err := put(); err != nil {
		t.Fatal(err)
	}
	if _, err := l.DB.Exec(`CREATE TRIGGER drop_blobs BEFORE INSERT ON blobs BEGIN SELECT RAISE(IGNORE); END`); err != nil {
		t.Fatal(err)
	}
	if err := put(); !errors.Is(err, ErrSchemaMismatch) || !strings.Contains(err.Error(), `trigger "drop_blobs"`) {
		t.Fatalf("a write with the trigger in place: %v", err)
	}
	if d, err := l.SchemaDifferences(ctx); err != nil || len(d) != 1 {
		t.Fatalf("differences %q, %v", d, err)
	}
	if _, err := l.DB.Exec(`DROP TRIGGER drop_blobs`); err != nil {
		t.Fatal(err)
	}
	if err := put(); err != nil {
		t.Fatal(err)
	}
	if d, err := l.SchemaDifferences(ctx); err != nil || len(d) != 0 {
		t.Fatalf("differences %q, %v", d, err)
	}
}

// rawLedger is a connection to the ledger at path of someone else's, as sqlite3 would be.
func rawLedger(t *testing.T, path string) *sql.DB {
	t.Helper()
	raw, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(10000)")
	if err != nil {
		t.Fatal(err)
	}
	raw.SetMaxOpenConns(1)
	t.Cleanup(func() { raw.Close() })
	return raw
}

func schemaVersion(t *testing.T, c *sql.DB) int64 {
	t.Helper()
	var v int64
	if err := c.QueryRow("PRAGMA schema_version").Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func hasBlob(t *testing.T, l *Ledger, key string) bool {
	t.Helper()
	var n int
	if err := l.DB.QueryRow(`SELECT count(*) FROM blobs WHERE sha256 = ?`, key).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n == 1
}

func putBlob(ctx context.Context, l *Ledger, key string) error {
	return l.Tx(ctx, func(q *db.Queries) error { return q.PutBlob(ctx, db.PutBlobParams{Sha256: key, Bytes: []byte(key)}) })
}

// A trigger written into sqlite_master directly leaves schema_version as it was: the next write
// transaction still finds it, and fails before it writes.
func TestAWriteFailsOnceSqliteMasterChangedUnderItsVersion(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	l, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	if err := putBlob(ctx, l, "a"); err != nil {
		t.Fatal(err)
	}
	raw := rawLedger(t, path)
	before := schemaVersion(t, raw)
	if _, err := raw.Exec(written(noPut("no_put"))); err != nil {
		t.Fatal(err)
	}
	if after := schemaVersion(t, raw); after != before {
		t.Fatalf("schema_version %d, then %d: the trigger did change it", before, after)
	}
	if err := putBlob(ctx, l, "b"); !errors.Is(err, ErrSchemaMismatch) || !strings.Contains(err.Error(), `trigger "no_put" on "blobs"`) {
		t.Fatalf("a write with the trigger in sqlite_master: %v", err)
	}
}

// A connection keeps the schema it loaded until schema_version changes: one that loaded a trigger
// since taken out of sqlite_master, schema_version left as it was, would still run it, with
// sqlite_master as the migrations make it. Each write transaction has its connection load the
// schema again first.
func TestAWriteRunsNoTriggerSinceTakenOut(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	l, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	l.DB.SetMaxOpenConns(1) // the connection that loads the trigger is the one that writes next
	if err := putBlob(ctx, l, "a"); err != nil {
		t.Fatal(err)
	}
	raw := rawLedger(t, path)
	v := schemaVersion(t, raw)
	if _, err := raw.Exec(written(noPut("no_put"), fmt.Sprintf("PRAGMA schema_version = %d", v+1))); err != nil {
		t.Fatal(err)
	}
	hasBlob(t, l, "a") // a read, outside any write transaction: the connection loads the trigger
	if _, err := raw.Exec(written(`DELETE FROM sqlite_master WHERE name = 'no_put'`)); err != nil {
		t.Fatal(err)
	}
	if d, err := l.SchemaDifferences(ctx); err != nil || len(d) != 0 {
		t.Fatalf("differences %q, %v", d, err)
	}
	if err := putBlob(ctx, l, "b"); err != nil {
		t.Fatal(err)
	}
	if !hasBlob(t, l, "b") {
		t.Fatal("the write was dropped, by a trigger sqlite_master no longer has")
	}
}

// A connection loads the schema sqlite_master has when it opens, whatever schema_version says: the
// ledger compares it with the migrations' then, and does not hand the connection out.
func TestANewConnectionIsCheckedAsItOpens(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	l, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	if err := putBlob(ctx, l, "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := rawLedger(t, path).Exec(written(noPut("no_put"))); err != nil {
		t.Fatal(err)
	}
	held, err := l.DB.Conn(ctx) // the connection the ledger has, opened before the trigger
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	c, err := l.DB.Conn(ctx) // a new one
	if err == nil {
		c.Close()
	}
	if !errors.Is(err, ErrSchemaMismatch) || !strings.Contains(err.Error(), `trigger "no_put" on "blobs"`) {
		t.Fatalf("a new connection with the trigger in sqlite_master: %v", err)
	}
}

// thenConnect has then run once the driver has opened a connection, before the ledger checks it.
type thenConnect struct {
	driver.Connector
	then func()
}

func (c thenConnect) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err == nil {
		c.then()
	}
	return conn, err
}

// A connection loads the schema as the driver opens it (its pragmas need it), before the ledger's
// check: an index of someone else's there then, and taken out of sqlite_master before the check,
// would stay with the connection, sqlite_master as the migrations make it. The check has the
// connection load the schema again, and the connection is left with what was compared.
func TestANewConnectionKeepsNothingItLoadedBeforeItsCheck(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	l, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	raw := rawLedger(t, path)
	if _, err := raw.Exec(`CREATE INDEX evil ON blobs (bytes)`); err != nil {
		t.Fatal(err)
	}
	base, err := sqlite.NewConnector(dsnOf(path))
	if err != nil {
		t.Fatal(err)
	}
	c := &connector{Connector: thenConnect{base, func() {
		if _, err := raw.Exec(written(`DELETE FROM sqlite_master WHERE name = 'evil'`)); err != nil {
			t.Error(err)
		}
	}}}
	c.ledger.Store(l)
	conn, err := c.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	rows, err := conn.(driver.QueryerContext).QueryContext(ctx, `SELECT name FROM pragma_index_list('blobs')`, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var indexes []string
	dest := make([]driver.Value, 1)
	for {
		if err := rows.Next(dest); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		indexes = append(indexes, fmt.Sprint(dest[0]))
	}
	if slices.Contains(indexes, "evil") {
		t.Fatalf("the connection has the index sqlite_master no longer has: %v", indexes)
	}
}

// A read transaction begins as a write does: its connection loads the schema again and compares it
// with the migrations'. A connection keeps the schema it loaded: whoever writes an index into
// sqlite_master, has every connection load it (schema_version), and takes it out again, leaves them
// reading through it. This one holds rows of its own: a read through it does not find a row the
// table has. A read transaction of the ledger's finds it; and while the index is in sqlite_master,
// the read fails, naming it.
func TestAReadLoadsTheSchemaAgainFirst(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	l, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	l.DB.SetMaxOpenConns(1) // the connection that loads the index is the one that reads next
	for _, key := range []string{"a", "b"} {
		if err := putBlob(ctx, l, key); err != nil {
			t.Fatal(err)
		}
	}
	raw := rawLedger(t, path)
	if _, err := raw.Exec(`CREATE INDEX evil ON blobs (bytes)`); err != nil {
		t.Fatal(err)
	}
	var root int64
	if err := raw.QueryRow(`SELECT rootpage FROM sqlite_master WHERE name = 'evil'`).Scan(&root); err != nil {
		t.Fatal(err)
	}
	evil := fmt.Sprintf(`INSERT INTO sqlite_master VALUES ('index', 'evil', 'blobs', %d, 'CREATE INDEX evil ON blobs (bytes)')`, root)
	if _, err := raw.Exec(written(`DELETE FROM sqlite_master WHERE name = 'evil'`)); err != nil {
		t.Fatal(err)
	}
	// A row the index does not have: written by a connection that never loaded it.
	other := rawLedger(t, path)
	if _, err := other.Exec(`INSERT INTO blobs (sha256, bytes) VALUES ('c', x'63')`); err != nil {
		t.Fatal(err)
	}
	other.Close()
	// The index back, and every connection made to load the schema again.
	v := schemaVersion(t, raw)
	if _, err := raw.Exec(written(evil, fmt.Sprintf("PRAGMA schema_version = %d", v+1))); err != nil {
		t.Fatal(err)
	}
	count := func(c interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	}) int {
		var n int
		if err := c.QueryRowContext(ctx, `SELECT count(*) FROM blobs WHERE bytes = x'63'`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if n := count(l.DB); n != 0 { // outside the ledger's transactions: through the index
		t.Fatalf("a read through the index found %d rows: the test's premise is gone", n)
	}
	if _, err := raw.Exec(written(`DELETE FROM sqlite_master WHERE name = 'evil'`)); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := l.read(ctx, func(c db.DBTX) error { n = count(c); return nil }); err != nil || n != 1 {
		t.Fatalf("a read transaction of the ledger's found %d rows (%v), want the table's 1", n, err)
	}
	if _, err := raw.Exec(written(evil)); err != nil {
		t.Fatal(err)
	}
	err = l.Read(ctx, func(*db.Queries) error { t.Error("read through a schema not the migrations'"); return nil })
	if !errors.Is(err, ErrSchemaMismatch) || !strings.Contains(err.Error(), `index "evil" on "blobs", which no migration makes`) {
		t.Fatalf("a read with the index in sqlite_master: %v", err)
	}
}

// PRAGMA integrity_check finds what a comparison of sqlite_master does not see: two indexes whose
// rootpages were swapped, each reading the other's tree, sqlite_master the migrations' but for its
// rootpages. A page SQLite cannot read is a finding too.
func TestIntegrityCheckFindsWhatTheSchemaDoesNot(t *testing.T) {
	ctx := context.Background()
	for name, c := range map[string]struct {
		damage func(t *testing.T, l *Ledger, path string)
		want   string
	}{
		"nothing": {func(*testing.T, *Ledger, string) {}, ""},
		"two indexes' rootpages swapped": {func(t *testing.T, l *Ledger, path string) {
			if _, err := rawLedger(t, path).Exec(written(`UPDATE sqlite_master SET rootpage = (SELECT sum(rootpage) FROM sqlite_master
				WHERE name IN ('audit_log_by_run', 'sqlite_autoindex_audit_log_1')) - rootpage
				WHERE name IN ('audit_log_by_run', 'sqlite_autoindex_audit_log_1')`)); err != nil {
				t.Fatal(err)
			}
		}, "audit_log_by_run"},
		"a page of no kind": {func(t *testing.T, l *Ledger, path string) {
			raw := rawLedger(t, path)
			var root, size int64
			if err := raw.QueryRow(`SELECT rootpage FROM sqlite_master WHERE name = 'audit_log'`).Scan(&root); err != nil {
				t.Fatal(err)
			}
			if err := raw.QueryRow(`PRAGMA page_size`).Scan(&size); err != nil {
				t.Fatal(err)
			}
			if _, err := raw.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil { // the pages in the file
				t.Fatal(err)
			}
			f, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			if _, err := f.WriteAt([]byte{0}, (root-1)*size); err != nil {
				t.Fatal(err)
			}
			l.DB.SetMaxIdleConns(0) // the next read on a connection of its own, which has no page of it yet
			l.DB.SetMaxIdleConns(2)
		}, "malformed"},
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "ledger.db")
			l, err := Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { l.Close() })
			for i := range 50 { // rows for the indexes to hold
				if _, err := l.DB.Exec(`INSERT INTO audit_log (seq, at, workflow_id, type, actor, payload, prev_sha256, sha256)
					VALUES (?, '2026-01-01T00:00:00.000000000Z', ?, 't', 'agentd', '{}', 'x', ?)`, i+1, fmt.Sprint("wf", i%3), fmt.Sprint("h", i)); err != nil {
					t.Fatal(err)
				}
			}
			c.damage(t, l, path)
			found, err := l.IntegrityCheck(ctx, 10)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("found %q", found)
			if c.want == "" && len(found) != 0 || c.want != "" && !slices.ContainsFunc(found, func(f string) bool { return strings.Contains(f, c.want) }) {
				t.Fatalf("found %q, want a line saying %q", found, c.want)
			}
		})
	}
}

// ANALYZE makes tables of SQLite's own (sqlite_stat1, sqlite_stat4) that steer its query planner:
// made before the ledger opens or while it is open, they stop nothing.
func TestAnAnalyzedLedgerIsOpened(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	l, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 20 {
		if err := putBlob(ctx, l, fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
	raw := rawLedger(t, path)
	if _, err := raw.Exec(`ANALYZE`); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := raw.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name IN ('sqlite_stat1', 'sqlite_stat4')`).Scan(&n); err != nil || n != 2 {
		t.Fatalf("ANALYZE made %d of its tables (%v)", n, err)
	}
	if err := putBlob(ctx, l, "after"); err != nil {
		t.Fatal(err)
	}
	l.Close()
	if l, err = Open(ctx, path); err != nil {
		t.Fatal(err)
	}
	l.Close()
}

// SQLite checks the file (PRAGMA quick_check) before anything is written to it: a ledger it finds
// damaged is not opened, whether SQLite says what it found or fails on a page it cannot read.
func TestADamagedLedgerIsNotOpened(t *testing.T) {
	ctx := context.Background()
	for name, c := range map[string]struct {
		damage func(f *os.File, root, size int64) error
		want   string
	}{
		// A b-tree page begins with its kind; 0 is none.
		"a page of no kind": {func(f *os.File, root, size int64) error { _, err := f.WriteAt([]byte{0}, (root-1)*size); return err },
			"database disk image is malformed"},
		// The file's header counts the pages on its freelist (offset 36); it has none.
		"a freelist miscounted": {func(f *os.File, _, _ int64) error { _, err := f.WriteAt([]byte{0, 0, 0, 5}, 36); return err },
			"Freelist: size is 0 but should be 5"},
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "ledger.db")
			l, err := Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			for i := range 20 {
				if err := putBlob(ctx, l, fmt.Sprint(i)); err != nil {
					t.Fatal(err)
				}
			}
			l.Close()
			raw := rawLedger(t, path)
			var root, size int64
			if err := raw.QueryRow(`SELECT rootpage FROM sqlite_master WHERE name = 'blobs'`).Scan(&root); err != nil {
				t.Fatal(err)
			}
			if err := raw.QueryRow(`PRAGMA page_size`).Scan(&size); err != nil {
				t.Fatal(err)
			}
			raw.Close() // the last connection: what the log held is in the file
			f, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			if err := c.damage(f, root, size); err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			l, err = Open(ctx, path)
			if err == nil {
				l.Close()
			}
			if !errors.Is(err, ErrDamaged) || !strings.Contains(err.Error(), c.want) || !strings.Contains(err.Error(), "agentd does not open it") {
				t.Fatalf("opened a damaged ledger: %v", err)
			}
		})
	}
}
