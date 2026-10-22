package ledger

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"maps"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// Tx gives this process's write transactions one at a time: a function passed to Tx that begins
// another transaction, itself or through a function that does, waits for the turn it holds, for
// ever. This reads agentd's code and finds none. A function begins a transaction when it calls Tx
// or audit.Record, or calls one that does: a function of its package, a method of its own receiver,
// or a function of another of agentd's packages. A call through a field (g.Budget.Extend) is not
// followed, so keep such calls out of Tx's functions.
func TestNoTransactionBeginsInsideAnother(t *testing.T) {
	const module = "github.com/jchen0426/agentos/"
	type fn struct{ pkg, recv, name string } // recv: the receiver's type, "" for a function
	type decl struct {
		fn
		recvVar string
		imports map[string]string // name -> package directory
		body    *ast.BlockStmt
	}
	fset := token.NewFileSet()
	var decls []decl
	for _, root := range []string{"../../internal", "../../cmd"} {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") ||
				strings.Contains(path, filepath.Join("ledger", "db")) {
				return err
			}
			f, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				return err
			}
			pkg := strings.TrimPrefix(filepath.ToSlash(filepath.Dir(path)), "../../")
			imports := map[string]string{}
			for _, im := range f.Imports {
				p, _ := strconv.Unquote(im.Path.Value)
				if !strings.HasPrefix(p, module) {
					continue
				}
				name := p[strings.LastIndex(p, "/")+1:]
				if im.Name != nil {
					name = im.Name.Name
				}
				imports[name] = strings.TrimPrefix(p, module)
			}
			for _, d := range f.Decls {
				fd, ok := d.(*ast.FuncDecl)
				if !ok || fd.Body == nil {
					continue
				}
				dc := decl{fn: fn{pkg: pkg, name: fd.Name.Name}, imports: imports, body: fd.Body}
				if fd.Recv != nil && len(fd.Recv.List) == 1 {
					typ := fd.Recv.List[0].Type
					if star, ok := typ.(*ast.StarExpr); ok {
						typ = star.X
					}
					if id, ok := typ.(*ast.Ident); ok {
						dc.recv = id.Name
					}
					if len(fd.Recv.List[0].Names) == 1 {
						dc.recvVar = fd.Recv.List[0].Names[0].Name
					}
				}
				decls = append(decls, dc)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	// resolve names what a call in dc calls, when it is one of agentd's functions; tx reports a call
	// of Tx itself, or of audit.Record.
	resolve := func(dc decl, c *ast.CallExpr) (target fn, tx bool) {
		switch f := c.Fun.(type) {
		case *ast.Ident:
			return fn{pkg: dc.pkg, name: f.Name}, false
		case *ast.SelectorExpr:
			if f.Sel.Name == "Tx" {
				return fn{}, true
			}
			x, ok := f.X.(*ast.Ident)
			switch {
			case !ok:
			case x.Name == dc.recvVar && dc.recv != "":
				return fn{pkg: dc.pkg, recv: dc.recv, name: f.Sel.Name}, false
			case dc.imports[x.Name] != "":
				if dc.imports[x.Name] == "internal/audit" && f.Sel.Name == "Record" {
					return fn{}, true
				}
				return fn{pkg: dc.imports[x.Name], name: f.Sel.Name}, false
			}
		}
		return fn{}, false
	}
	begins := map[fn]bool{}
	callsBegin := func(dc decl, n ast.Node) []*ast.CallExpr {
		var found []*ast.CallExpr
		ast.Inspect(n, func(n ast.Node) bool {
			if c, ok := n.(*ast.CallExpr); ok {
				if target, tx := resolve(dc, c); tx || begins[target] {
					found = append(found, c)
				}
			}
			return true
		})
		return found
	}
	for grew := true; grew; {
		grew = false
		for _, dc := range decls {
			if !begins[dc.fn] && len(callsBegin(dc, dc.body)) > 0 {
				begins[dc.fn], grew = true, true
			}
		}
	}
	var bad []string
	checked := 0
	for _, dc := range decls {
		ast.Inspect(dc.body, func(n ast.Node) bool {
			c, ok := n.(*ast.CallExpr)
			if !ok || len(c.Args) != 2 {
				return true
			}
			if _, tx := resolve(dc, c); !tx {
				return true
			}
			lit, ok := c.Args[1].(*ast.FuncLit)
			if !ok {
				return true
			}
			checked++
			for _, inner := range callsBegin(dc, lit.Body) {
				bad = append(bad, fset.Position(inner.Pos()).String())
			}
			return true
		})
	}
	if checked < 20 || !begins[fn{pkg: "internal/gateway", recv: "Gateway", name: "settle"}] {
		t.Fatalf("found %d functions passed to Tx, and %d functions that begin a transaction: the walk misses agentd's code",
			checked, len(begins))
	}
	slices.Sort(bad)
	for _, b := range bad {
		t.Errorf("a function passed to Tx begins another transaction: %s", b)
	}
}

// Every write of agentd's takes the write turn: it goes through Tx, on the queries Tx gives its
// function. A write through Ledger.Q (autocommit, beside the turn) waited for SQLite's lock while the
// turn went from transaction to transaction, for seconds under load. This reads the queries sqlc
// generated, finds those that write, and agentd's code, and finds no call of one through a field Q.
func TestEveryWriteTakesTheTurn(t *testing.T) {
	fset := token.NewFileSet()
	gen, err := parser.ParseFile(fset, filepath.Join("db", "ledger.sql.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	writes := map[string]bool{}
	for _, d := range gen.Decls {
		g, ok := d.(*ast.GenDecl)
		if !ok || g.Tok != token.CONST {
			continue
		}
		for _, spec := range g.Specs {
			for _, v := range spec.(*ast.ValueSpec).Values {
				lit, ok := v.(*ast.BasicLit)
				if !ok {
					continue
				}
				text, _ := strconv.Unquote(lit.Value)
				name, sql, ok := strings.Cut(strings.TrimPrefix(text, "-- name: "), "\n")
				if !ok {
					continue
				}
				for line := range strings.Lines(sql) {
					if l := strings.ToUpper(strings.TrimSpace(line)); l != "" && !strings.HasPrefix(l, "--") {
						if strings.HasPrefix(l, "INSERT") || strings.HasPrefix(l, "UPDATE") || strings.HasPrefix(l, "DELETE") ||
							strings.HasPrefix(l, "REPLACE") {
							writes[strings.Fields(name)[0]] = true
						}
						break
					}
				}
			}
		}
	}
	if len(writes) < 30 || !writes["PutBlob"] || !writes["InsertAuditRecord"] || writes["GetBlob"] {
		t.Fatalf("the write queries found: %v", slices.Sorted(maps.Keys(writes)))
	}
	// Agentd's functions, and those of them that write through a *db.Queries parameter (by its
	// index): budget.Settle writes through the q it is given, and given Ledger.Q, writes beside the
	// turn as surely as a query called on it.
	type fn struct{ pkg, recv, name string }
	type decl struct {
		fn
		recvVar string
		imports map[string]string
		params  map[string]int // the *db.Queries parameters, by name
		body    *ast.BlockStmt
	}
	const module = "github.com/jchen0426/agentos/"
	var decls []decl
	var files []*ast.File
	for _, root := range []string{"../../internal", "../../cmd"} {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") ||
				strings.Contains(path, filepath.Join("ledger", "db")) {
				return err
			}
			f, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				return err
			}
			files = append(files, f)
			pkg := strings.TrimPrefix(filepath.ToSlash(filepath.Dir(path)), "../../")
			imports := map[string]string{}
			for _, im := range f.Imports {
				p, _ := strconv.Unquote(im.Path.Value)
				if !strings.HasPrefix(p, module) {
					continue
				}
				name := p[strings.LastIndex(p, "/")+1:]
				if im.Name != nil {
					name = im.Name.Name
				}
				imports[name] = strings.TrimPrefix(p, module)
			}
			for _, d := range f.Decls {
				fd, ok := d.(*ast.FuncDecl)
				if !ok || fd.Body == nil {
					continue
				}
				dc := decl{fn: fn{pkg: pkg, name: fd.Name.Name}, imports: imports, params: map[string]int{}, body: fd.Body}
				if fd.Recv != nil && len(fd.Recv.List) == 1 {
					typ := fd.Recv.List[0].Type
					if star, ok := typ.(*ast.StarExpr); ok {
						typ = star.X
					}
					if id, ok := typ.(*ast.Ident); ok {
						dc.recv = id.Name
					}
					if len(fd.Recv.List[0].Names) == 1 {
						dc.recvVar = fd.Recv.List[0].Names[0].Name
					}
				}
				i := 0
				for _, field := range fd.Type.Params.List {
					star, ok := field.Type.(*ast.StarExpr)
					sel, ok2 := ast.Expr(nil), false
					if ok {
						sel, ok2 = star.X.(*ast.SelectorExpr)
					}
					queries := ok2 && sel.(*ast.SelectorExpr).Sel.Name == "Queries"
					for _, name := range field.Names {
						if queries {
							dc.params[name.Name] = i
						}
						i++
					}
					if len(field.Names) == 0 {
						i++
					}
				}
				decls = append(decls, dc)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	resolve := func(dc decl, c *ast.CallExpr) (fn, bool) {
		switch f := c.Fun.(type) {
		case *ast.Ident:
			return fn{pkg: dc.pkg, name: f.Name}, true
		case *ast.SelectorExpr:
			x, ok := f.X.(*ast.Ident)
			switch {
			case !ok:
			case x.Name == dc.recvVar && dc.recv != "":
				return fn{pkg: dc.pkg, recv: dc.recv, name: f.Sel.Name}, true
			case dc.imports[x.Name] != "":
				return fn{pkg: dc.imports[x.Name], name: f.Sel.Name}, true
			}
		}
		return fn{}, false
	}
	writers := map[fn]int{} // -> the index of the parameter it writes through
	for grew := true; grew; {
		grew = false
		for _, dc := range decls {
			if _, done := writers[dc.fn]; done || len(dc.params) == 0 {
				continue
			}
			ast.Inspect(dc.body, func(n ast.Node) bool {
				c, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				// p.Write(...), with p a *db.Queries parameter.
				if sel, ok := c.Fun.(*ast.SelectorExpr); ok && writes[sel.Sel.Name] {
					if x, ok := sel.X.(*ast.Ident); ok {
						if i, ok := dc.params[x.Name]; ok {
							writers[dc.fn], grew = i, true
							return false
						}
					}
				}
				// writer(..., p, ...), at the index the writer writes through.
				if target, ok := resolve(dc, c); ok {
					if wi, ok := writers[target]; ok && wi < len(c.Args) {
						if x, ok := c.Args[wi].(*ast.Ident); ok {
							if i, ok := dc.params[x.Name]; ok {
								writers[dc.fn], grew = i, true
								return false
							}
						}
					}
				}
				return true
			})
		}
	}
	if _, ok := writers[fn{pkg: "internal/budget", name: "Settle"}]; !ok {
		t.Fatalf("the walk found no writer budget.Settle: %d writers", len(writers))
	}
	var found []string
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok || !writes[sel.Sel.Name] {
				return true
			}
			if q, ok := sel.X.(*ast.SelectorExpr); ok && q.Sel.Name == "Q" {
				found = append(found, fset.Position(sel.Pos()).String()+": "+sel.Sel.Name)
			}
			return true
		})
	}
	for _, dc := range decls {
		ast.Inspect(dc.body, func(n ast.Node) bool {
			c, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			target, ok := resolve(dc, c)
			wi, isWriter := writers[target]
			if !ok || !isWriter || wi >= len(c.Args) {
				return true
			}
			if q, ok := c.Args[wi].(*ast.SelectorExpr); ok && q.Sel.Name == "Q" {
				found = append(found, fset.Position(c.Pos()).String()+": "+target.name+" given Ledger.Q")
			}
			return true
		})
	}
	slices.Sort(found)
	for _, f := range found {
		t.Errorf("a write beside the turn: %s", f)
	}
}
