package ledger

import (
	"cmp"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"

	"github.com/jchen0426/agentos/internal/ledger/db"
)

// The ledger's schema is the one its migrations make, and nothing else. agentd decides on what its
// writes change (a call counted, a run cancelled, an effect claimed), and an object of anyone
// else's in the ledger changes that without a trace: a trigger can make a write do nothing
// (RAISE(IGNORE): no row, no error), an index can make an INSERT ... ON CONFLICT DO NOTHING skip
// its row.
//
// What is compared is every row of sqlite_master, duplicates and all: its type, name, tbl_name and
// sql (not its rootpage, which depends on the file's history). SQLite's own objects are compared
// with the rest: the migrations make sqlite_sequence and the sqlite_autoindex_* indexes too, and
// with PRAGMA writable_schema anyone can write into sqlite_master an object named sqlite_* that
// CREATE refuses, a trigger among them, which SQLite then loads like any other. Only the tables
// ANALYZE makes are left out (analyzed), each by its type, name and exact SQL: what they hold
// steers the query planner, not what a statement does.
//
// A connection loads the schema when it opens, and again only when schema_version changes; with
// writable_schema, sqlite_master can be changed, and changed back, without it changing. So the
// schema is compared wherever one is loaded or relied on: by Open, which refuses the ledger; on
// each connection of the ledger's pool as it opens (connector); in each write transaction, which
// has its connection load the schema again first (checkTx: a connection that loaded a trigger
// since taken out would still run it); and by verify (SchemaDifferences).

// ErrSchemaMismatch: the ledger's schema is not the one its migrations make.
var ErrSchemaMismatch = errors.New("the ledger's schema is not the one its migrations make")

// ErrDamaged: SQLite finds the ledger's file damaged (PRAGMA quick_check).
var ErrDamaged = errors.New("SQLite finds the ledger's file damaged")

// schemaObject is a row of sqlite_master but its rootpage: a table, index, trigger or view. Any of
// it can be NULL in a row written into sqlite_master directly.
type schemaObject struct{ typ, name, table, sql sql.NullString }

// schema is the rows of sqlite_master, sorted, duplicates kept.
type schema []schemaObject

const schemaQuery = `SELECT type, name, tbl_name, sql FROM sqlite_master`

// analyzed are the tables ANALYZE makes, with the SQL it gives them: the only ones this SQLite
// makes (it has STAT4).
var analyzed = map[string]string{
	"sqlite_stat1": "CREATE TABLE sqlite_stat1(tbl,idx,stat)",
	"sqlite_stat4": "CREATE TABLE sqlite_stat4(tbl,idx,neq,nlt,ndlt,sample)",
}

func text(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }

// analyzedTable says whether o is a table ANALYZE makes, as ANALYZE makes it.
func (o schemaObject) analyzedTable() bool {
	want, ok := analyzed[o.name.String]
	return ok && o.name.Valid && o.typ == text("table") && o.table == o.name && o.sql == text(want)
}

func compareText(a, b sql.NullString) int {
	switch {
	case a.Valid == b.Valid:
		return strings.Compare(a.String, b.String)
	case a.Valid:
		return 1
	}
	return -1
}

func compareObjects(a, b schemaObject) int {
	return cmp.Or(compareText(a.typ, b.typ), compareText(a.name, b.name), compareText(a.table, b.table),
		compareText(a.sql, b.sql))
}

// collect is the schema of the rows next reads, but the tables ANALYZE makes.
func collect(next func(*schemaObject) (bool, error)) (schema, error) {
	var out schema
	for {
		var o schemaObject
		more, err := next(&o)
		if err != nil {
			return nil, err
		}
		if !more {
			break
		}
		if !o.analyzedTable() {
			out = append(out, o)
		}
	}
	slices.SortFunc(out, compareObjects)
	return out, nil
}

// schemaOf is c's schema.
func schemaOf(ctx context.Context, c db.DBTX) (schema, error) {
	rows, err := c.QueryContext(ctx, schemaQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collect(func(o *schemaObject) (bool, error) {
		if !rows.Next() {
			return false, rows.Err()
		}
		return true, rows.Scan(&o.typ, &o.name, &o.table, &o.sql)
	})
}

// schemaOfConn is the schema of a connection of the driver's, before database/sql has it.
func schemaOfConn(ctx context.Context, q driver.QueryerContext) (schema, error) {
	rows, err := q.QueryContext(ctx, schemaQuery, nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	dest := make([]driver.Value, 4)
	return collect(func(o *schemaObject) (bool, error) {
		if err := rows.Next(dest); errors.Is(err, io.EOF) {
			return false, nil
		} else if err != nil {
			return false, err
		}
		for i, f := range []*sql.NullString{&o.typ, &o.name, &o.table, &o.sql} {
			if err := f.Scan(dest[i]); err != nil {
				return false, err
			}
		}
		return true, nil
	})
}

// migrationsSchema is the schema fsys's migrations make: applied to a database of its own, in
// memory.
func migrationsSchema(ctx context.Context, fsys fs.FS) (schema, error) {
	mem, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, err
	}
	defer mem.Close()
	mem.SetMaxOpenConns(1) // one connection, one database in memory
	if err := migrate(ctx, mem, ":memory:", fsys); err != nil {
		return nil, fmt.Errorf("the migrations' schema: %w", err)
	}
	return schemaOf(ctx, mem)
}

// differences says how got differs from want, object by object, in a stable order.
func differences(want, got schema) []string {
	type key struct{ typ, name sql.NullString }
	group := func(s schema) map[key]schema {
		m := map[key]schema{}
		for _, o := range s {
			k := key{o.typ, o.name}
			m[k] = append(m[k], o)
		}
		return m
	}
	w, g := group(want), group(got)
	all := maps.Clone(w)
	maps.Copy(all, g)
	keys := slices.SortedFunc(maps.Keys(all), func(a, b key) int {
		return cmp.Or(compareText(a.typ, b.typ), compareText(a.name, b.name))
	})
	var out []string
	for _, k := range keys {
		ws, gs := w[k], g[k]
		switch {
		case len(ws) == 0:
			for _, o := range gs {
				out = append(out, fmt.Sprintf("%s %s on %s, which no migration makes", kind(o.typ), show(o.name), show(o.table)))
			}
		case len(gs) == 0:
			out = append(out, fmt.Sprintf("no %s %s, which the migrations make", kind(k.typ), show(k.name)))
		case len(gs) != len(ws):
			out = append(out, fmt.Sprintf("%d rows for %s %s, where the migrations make %d", len(gs), kind(k.typ),
				show(k.name), len(ws)))
		default:
			for i, o := range gs {
				if o != ws[i] {
					out = append(out, fmt.Sprintf("%s %s on %s is not the migrations' (%s)", kind(o.typ), show(o.name),
						show(o.table), sqlText(o.sql)))
				}
			}
		}
	}
	return out
}

// kind is a row's type as a message says it: quoted unless it is one of SQLite's four.
func kind(t sql.NullString) string {
	switch t {
	case text("table"), text("index"), text("trigger"), text("view"):
		return t.String
	}
	return "object of type " + show(t)
}

func show(s sql.NullString) string {
	if !s.Valid {
		return "NULL"
	}
	return strconv.Quote(s.String)
}

func sqlText(s sql.NullString) string {
	if !s.Valid {
		return "no SQL"
	}
	return fmt.Sprintf("%.200q", s.String)
}

// mismatch is ErrSchemaMismatch naming the objects when got is not want, nil when it is.
func mismatch(want, got schema) error {
	if slices.Equal(want, got) {
		return nil
	}
	return fmt.Errorf("%w: %s", ErrSchemaMismatch, strings.Join(differences(want, got), "; "))
}

// checkSchema fails with ErrSchemaMismatch, naming the objects, when c's schema is not want.
func checkSchema(ctx context.Context, c db.DBTX, want schema) error {
	got, err := schemaOf(ctx, c)
	if err != nil {
		return err
	}
	return mismatch(want, got)
}

// checkTx has the connection of tx, a write transaction, load the schema again from what tx sees
// (PRAGMA writable_schema=RESET), then compares it with want. tx holds the write lock: what the
// connection loaded is what was compared, and nobody changes it before tx ends.
func checkTx(ctx context.Context, tx *sql.Tx, want schema) error {
	if _, err := tx.ExecContext(ctx, "PRAGMA writable_schema=RESET"); err != nil {
		return err
	}
	return checkSchema(ctx, tx, want)
}

// checkRead has the connection of a read transaction, just begun, take its snapshot (schema_version
// reads the file's header) and load the schema again from it, then compares it with want: what the
// transaction reads, it reads through the schema that was compared.
func checkRead(ctx context.Context, c db.DBTX, want schema) error {
	for _, s := range []string{"PRAGMA schema_version", "PRAGMA writable_schema=RESET"} {
		if _, err := c.ExecContext(ctx, s); err != nil {
			return err
		}
	}
	return checkSchema(ctx, c, want)
}

// SchemaDifferences says how the ledger's schema differs from its migrations' now; none when it
// does not.
func (l *Ledger) SchemaDifferences(ctx context.Context) ([]string, error) {
	got, err := schemaOf(ctx, l.DB)
	if err != nil {
		return nil, err
	}
	return differences(l.schema, got), nil
}

// quickCheck fails with ErrDamaged, saying what SQLite found, unless PRAGMA quick_check finds the
// file sound. SQLite says what it finds as rows, or fails on what it cannot read.
func quickCheck(ctx context.Context, c db.DBTX) error {
	rows, err := c.QueryContext(ctx, "PRAGMA quick_check(10)")
	if err != nil {
		return damaged(err)
	}
	defer rows.Close()
	var found []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return err
		}
		found = append(found, s)
	}
	if err := rows.Err(); err != nil {
		return damaged(err)
	}
	if slices.Equal(found, []string{"ok"}) {
		return nil
	}
	return fmt.Errorf("%w: %.500q", ErrDamaged, strings.Join(found, "; "))
}

// damaged is err as ErrDamaged when it is SQLite finding the file damaged, or no database.
func damaged(err error) error {
	var e *sqlite.Error
	if errors.As(err, &e) && (e.Code()&0xff == sqlite3.SQLITE_CORRUPT || e.Code()&0xff == sqlite3.SQLITE_NOTADB) {
		return fmt.Errorf("%w: %w", ErrDamaged, err)
	}
	return err
}

// connector opens the ledger's connections. Once the ledger is open, it compares each one's schema
// with the migrations' as it opens, before database/sql hands it out: a connection loads the
// schema sqlite_master has when it opens, whatever schema_version says.
type connector struct {
	driver.Connector
	ledger atomic.Pointer[Ledger]
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	l := c.ledger.Load()
	if err != nil || l == nil || l.UncheckedSchema {
		return conn, err
	}
	if err := checkConn(ctx, conn, l.schema); err != nil {
		conn.Close()
		return nil, fmt.Errorf("a new connection to the ledger: %w", err)
	}
	return conn, nil
}

// checkConn compares the schema of conn, just opened, with want, in one read transaction: its
// snapshot begins with schema_version (the file's header), and conn loads its schema again from it.
// The driver's pragmas had conn load the schema as it opened, and sqlite_master can have changed
// since without schema_version changing; loaded again, what conn has is what was compared. On an
// error, the caller closes conn.
func checkConn(ctx context.Context, conn driver.Conn, want schema) error {
	ex, ok := conn.(driver.ExecerContext)
	q, ok2 := conn.(driver.QueryerContext)
	if !ok || !ok2 {
		return fmt.Errorf("a %T cannot be queried", conn)
	}
	for _, s := range []string{"BEGIN", "PRAGMA schema_version", "PRAGMA writable_schema=RESET"} {
		if _, err := ex.ExecContext(ctx, s, nil); err != nil {
			return err
		}
	}
	got, err := schemaOfConn(ctx, q)
	if err != nil {
		return err
	}
	if _, err := ex.ExecContext(ctx, "ROLLBACK", nil); err != nil {
		return err
	}
	return mismatch(want, got)
}
