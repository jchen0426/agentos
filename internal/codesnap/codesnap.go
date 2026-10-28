// Package codesnap pins the code agentd's own Worker runs (M5 C2c): a copy, made when agentd starts,
// of the Worker's Python environment without its bytecode, and the *.py and *.so files of its code
// directory and of each directory a .pth file names outside both; its files read-only, all of it
// hashed into one root. The Temporal SDK's sandbox imports workflow modules afresh at every workflow
// task, so a check of the code would not keep a running Worker on what it started with: only a copy
// does. Every generation runs from the copy; the next agentd makes its own, of the code as it is then.
// The standard library, outside both directories, is not copied.
//
// The copy holds only what the Worker could read before, but for the *.py and *.so files of a .pth's
// directory outside both. The launchers let the Worker read the Python environment and the code
// directory wherever they are; a .pth's directory the sandbox may have kept from it (srt: the homes
// and the temporary directories; bwrap: everything but /usr and the two directories), but a .pth that
// names it says the Worker is to import it, and only what that puts on the import path is taken, by
// the code directory's rule: no data, no dot-file. Nothing is taken from in or around a path agentd
// keeps from the Worker. A link is copied as a link, never as what it leads to, so the sandbox judges
// what it leads to as it would have.
package codesnap

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/jchen0426/agentos/internal/statedir"
)

// Kind names the snapshots' state directories: /tmp/agentos-code.<agentd's pid>.*.
const Kind = "agentos-code"

// Caps: a snapshot over either is refused, and agentd with it.
const (
	MaxBytes = 1 << 30
	MaxFiles = 100_000
)

// The caps a snapshot is held to (tests lower them).
var maxBytes, maxFiles int64 = MaxBytes, MaxFiles

// Snapshot is the copy agentd's Worker runs from.
type Snapshot struct {
	Dir    string // the state directory, which holds python and code
	Python string // the Python environment's copy
	Code   string // the code directory's copy: the Worker's working directory
	// Manifest lists every file and link of the copy, a line each, sorted by path: path, kind (file
	// or link), x if it is executable, size, and its sha256 or the link's target. Bytecode and times
	// are not in it. Root is its sha256: what agentd.started records as code_sha256.
	Manifest      []byte
	Root          string
	Files         int
	Bytes         int64
	Distributions []string // name==version of each distribution in the environment's site-packages
	Interpreter   string   // where the environment's bin/python leads
}

// Make copies python (a virtual environment) and code into a new state directory. private are the
// paths the Worker must not read: agentd's data directory, its sockets' directories, the Temporal
// client's certificate and key, the mirrors, and the deny list. No path the copy is made from may be
// in one or hold one, but that python and code may be in one (the launchers let the Worker read them
// wherever they are).
func Make(python, code string, private []string) (*Snapshot, error) {
	dir, err := statedir.Make(Kind)
	if err != nil {
		return nil, err
	}
	s, err := make1(dir, python, code, private)
	if err != nil {
		_ = statedir.Remove(dir)
		return nil, err
	}
	return s, nil
}

// Remove removes the snapshot.
func (s *Snapshot) Remove() error { return statedir.Remove(s.Dir) }

func make1(dir, python, code string, private []string) (*Snapshot, error) {
	python, code = filepath.Clean(python), filepath.Clean(code)
	b := &builder{root: dir, python: python, code: code, pinned: map[string]string{}}
	for _, p := range private {
		b.private = append(b.private, real(p))
	}
	if err := b.tree(python, "python", b.inEnvironment, true); err != nil {
		return nil, fmt.Errorf("worker.python %s: %w", python, err)
	}
	if err := b.tree(code, "code", b.inCode, true); err != nil {
		return nil, fmt.Errorf("worker.code %s: %w", code, err)
	}
	slices.SortFunc(b.entries, func(x, y entry) int { return strings.Compare(x.path, y.path) })
	var m bytes.Buffer
	for _, e := range b.entries {
		x := "-"
		if e.exec {
			x = "x"
		}
		fmt.Fprintf(&m, "%s\t%s\t%s\t%d\t%s\n", e.path, e.kind, x, e.size, e.sum)
	}
	sum := sha256.Sum256(m.Bytes())
	s := &Snapshot{Dir: dir, Python: filepath.Join(dir, "python"), Code: filepath.Join(dir, "code"), Manifest: m.Bytes(),
		Root: hex.EncodeToString(sum[:]), Files: b.files, Bytes: b.bytes}
	s.Distributions = distributions(s.Python)
	if real, err := filepath.EvalSymlinks(filepath.Join(python, "bin", "python")); err == nil {
		s.Interpreter = real
	}
	return s, nil
}

type entry struct {
	path string // in the snapshot, slash-separated
	kind string // file or link
	exec bool
	size int64
	sum  string // the file's sha256, or the link's target
}

type builder struct {
	root, python, code string
	entries            []entry
	files              int
	bytes              int64
	pinned             map[string]string // a directory a .pth names outside both, and its copy's path
	private            []string          // the paths the Worker must not read, as their real paths
}

// real is path's real path, or path itself, cleaned, where it does not resolve.
func real(path string) string {
	if r, err := filepath.EvalSymlinks(path); err == nil {
		return r
	}
	return filepath.Clean(path)
}

// keptFrom refuses a tree whose real path is root if it holds a path the Worker must not read, or is
// in one but where configured (python, code); it returns the check for each path read under it.
func (b *builder) keptFrom(root string, configured bool) (func(real string) error, error) {
	var holding []string // the private paths that hold the tree: those a configured tree may be in
	for _, p := range b.private {
		switch {
		case within(p, root):
			return nil, fmt.Errorf("it holds %s, which the Worker must not read", p)
		case within(root, p):
			if !configured {
				return nil, fmt.Errorf("it is in %s, which the Worker must not read", p)
			}
			holding = append(holding, p)
		}
	}
	return func(real string) error {
		for _, p := range b.private {
			if within(real, p) && !slices.Contains(holding, p) {
				return fmt.Errorf("%s is in %s, which the Worker must not read", real, p)
			}
		}
		return nil
	}, nil
}

// selection says what of a tree is copied: whether to skip a directory (path, rel in the tree), and
// whether a file goes.
type selection func(path, rel string, d fs.DirEntry) (skip bool, take bool)

// inEnvironment takes all of the Python environment but its bytecode.
func (b *builder) inEnvironment(path, rel string, d fs.DirEntry) (bool, bool) {
	name := d.Name()
	if d.IsDir() {
		return name == "__pycache__", false
	}
	return false, !strings.HasSuffix(name, ".pyc") && !strings.HasSuffix(name, ".pyo")
}

// inCode takes the code directory's *.py and *.so files, outside its dot-directories, __pycache__
// and any virtual environment in it.
func (b *builder) inCode(path, rel string, d fs.DirEntry) (bool, bool) {
	name := d.Name()
	if d.IsDir() {
		if rel == "." {
			return false, false
		}
		_, err := os.Lstat(filepath.Join(path, "pyvenv.cfg"))
		return strings.HasPrefix(name, ".") || name == "__pycache__" || err == nil, false
	}
	return false, strings.HasSuffix(name, ".py") || strings.HasSuffix(name, ".so")
}

// inPinned takes of a directory a .pth names what inCode takes of the code directory: what the .pth
// puts on the import path, its *.py and *.so files, and none of its data.
func (b *builder) inPinned(path, rel string, d fs.DirEntry) (bool, bool) {
	return b.inCode(path, rel, d)
}

// tree copies what sel takes of src to dst, a path in the snapshot: a configured directory (python,
// code), or a .pth's.
func (b *builder) tree(src, dst string, sel selection, configured bool) error {
	fi, err := os.Stat(src)
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return errors.New("not a directory")
	}
	root := real(src)
	check, err := b.keptFrom(root, configured)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(b.root, dst), 0o755); err != nil {
		return err
	}
	return filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		skip, take := sel(path, rel, d)
		if skip {
			return filepath.SkipDir
		}
		if rel == "." {
			return nil
		}
		// The walk follows no link: below the tree's real root, a path's real path is root/rel.
		if err := check(filepath.Join(root, rel)); err != nil {
			return err
		}
		out := filepath.ToSlash(filepath.Join(dst, rel))
		if strings.ContainsAny(out, "\t\n") {
			return fmt.Errorf("%s: a name with a tab or newline", path)
		}
		switch t := d.Type(); {
		case t.IsDir():
			return os.MkdirAll(filepath.Join(b.root, out), 0o755)
		case t&fs.ModeSymlink != 0:
			if take {
				return b.link(src, dst, path, rel, out)
			}
			return nil
		case t.IsRegular():
			if take {
				return b.file(path, out)
			}
			return nil
		case take:
			return fmt.Errorf("%s: neither a file, a directory nor a link (%s)", path, t)
		default:
			return nil
		}
	})
}

// link copies a link as a link, never what it leads to, so that the sandbox judges that as it would
// have: one that leads inside the tree it is in (a Linux environment's lib64, to lib) leads inside the
// copy, relative; one of the environment's bin/python* keeps its target (the interpreter, or another
// of them); any other leads where it led, by its absolute path. What a link leads outside to is not
// pinned: the manifest has the link's target.
func (b *builder) link(src, dst, path, rel, out string) error {
	target, err := os.Readlink(path)
	if err != nil {
		return err
	}
	slashed := filepath.ToSlash(rel)
	inBin := dst == "python" && strings.HasPrefix(slashed, "bin/python") && !strings.Contains(slashed[len("bin/"):], "/")
	resolved := target
	if !filepath.IsAbs(resolved) {
		resolved = filepath.Join(filepath.Dir(path), target)
	}
	switch {
	case inBin:
	case within(resolved, src):
		if target, err = filepath.Rel(filepath.Dir(path), resolved); err != nil {
			return err
		}
	default:
		target = resolved
	}
	if strings.ContainsAny(target, "\t\n") {
		return fmt.Errorf("%s: a link's target with a tab or newline", path)
	}
	b.entries = append(b.entries, entry{path: out, kind: "link", sum: target})
	return os.Symlink(target, filepath.Join(b.root, out))
}

// file copies a regular file, its content checked and hashed, at 0444 (0555 if it was executable).
func (b *builder) file(src, out string) error {
	fi, err := os.Stat(src)
	if err != nil {
		return err
	}
	if b.files++; int64(b.files) > maxFiles {
		return fmt.Errorf("more than %d files: too many to pin", maxFiles)
	}
	if b.bytes += fi.Size(); b.bytes > maxBytes {
		return fmt.Errorf("more than %d bytes: too much to pin", maxBytes)
	}
	dst := filepath.Join(b.root, out)
	exec := fi.Mode()&0o111 != 0
	var size int64
	var sum string
	if isPth(out) {
		content, err := b.pth(src)
		if err != nil {
			return fmt.Errorf("%s: %w", src, err)
		}
		if err := os.WriteFile(dst, content, 0o600); err != nil {
			return err
		}
		h := sha256.Sum256(content)
		size, sum = int64(len(content)), hex.EncodeToString(h[:])
	} else if size, sum, err = copyFile(src, dst); err != nil {
		return err
	}
	b.bytes += size - fi.Size()
	mode := os.FileMode(0o444)
	if exec {
		mode = 0o555
	}
	if err := os.Chmod(dst, mode); err != nil {
		return err
	}
	b.entries = append(b.entries, entry{path: out, kind: "file", exec: exec, size: size, sum: sum})
	return nil
}

// isPth reports whether out is a .pth file in the environment's site-packages, which Python reads
// at its start.
func isPth(out string) bool {
	parts := strings.Split(out, "/")
	return len(parts) == 5 && parts[0] == "python" && parts[1] == "lib" && strings.HasPrefix(parts[2], "python") &&
		parts[3] == "site-packages" && strings.HasSuffix(parts[4], ".pth")
}

// pth is a .pth file's content for the copy: each directory it names (as a path, absolute or relative
// to site-packages) becomes its copy's, relative to the copy's site-packages: one in the environment or
// the code directory, the copy of it there; one outside both, a copy of its own in python/pinned. A
// path that names nothing is left out. Import lines stay, but for an import-hook editable install
// (setuptools' __editable___*_finder), which would import the live code: it is refused.
func (b *builder) pth(src string) ([]byte, error) {
	data, err := os.ReadFile(src)
	if err != nil {
		return nil, err
	}
	site := filepath.Dir(src)
	rel, err := filepath.Rel(b.python, site)
	if err != nil {
		return nil, err
	}
	copySite := filepath.Join(b.root, "python", rel)
	var out bytes.Buffer
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		trimmed := strings.TrimRight(line, " \t\r")
		switch {
		case trimmed == "" || strings.HasPrefix(trimmed, "#"):
			out.WriteString(line + "\n")
			continue
		case strings.HasPrefix(trimmed, "import ") || strings.HasPrefix(trimmed, "import\t"):
			if strings.Contains(trimmed, "__editable__") {
				return nil, errors.New("an editable install by import hook (setuptools), which would import the live code: " +
					"install it as a path (pip install -e with a .pth of paths, or a wheel)")
			}
			out.WriteString(line + "\n")
			continue
		}
		dir := trimmed
		if !filepath.IsAbs(dir) {
			dir = filepath.Join(site, dir)
		}
		dir = filepath.Clean(dir)
		if fi, err := os.Stat(dir); err != nil || !fi.IsDir() {
			continue // Python leaves it out too
		}
		var copied string
		switch {
		case within(dir, b.python):
			r, _ := filepath.Rel(b.python, dir)
			copied = filepath.Join(b.root, "python", r)
		case within(dir, b.code):
			r, _ := filepath.Rel(b.code, dir)
			copied = filepath.Join(b.root, "code", r)
		default:
			name, ok := b.pinned[dir]
			if !ok {
				name = filepath.ToSlash(filepath.Join("python", "pinned", fmt.Sprint(len(b.pinned))))
				b.pinned[dir] = name
				if err := b.tree(dir, name, b.inPinned, false); err != nil {
					return nil, fmt.Errorf("the directory %s names: %w", dir, err)
				}
			}
			copied = filepath.Join(b.root, name)
		}
		r, err := filepath.Rel(copySite, copied)
		if err != nil {
			return nil, err
		}
		out.WriteString(r + "\n")
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

// copyFile copies src to dst, a new file, and returns its size and sha256: a clone where the file
// system makes one (APFS), the bytes otherwise.
func copyFile(src, dst string) (int64, string, error) {
	if cloneFile(src, dst) == nil {
		f, err := os.Open(dst)
		if err != nil {
			return 0, "", err
		}
		defer f.Close()
		h := sha256.New()
		n, err := io.Copy(h, f)
		return n, hex.EncodeToString(h.Sum(nil)), err
	}
	in, err := os.Open(src)
	if err != nil {
		return 0, "", err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return 0, "", err
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(out, h), in)
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	return n, hex.EncodeToString(h.Sum(nil)), err
}

// distributions lists name==version of each distribution in the environment's site-packages, from its
// dist-info's METADATA.
func distributions(python string) []string {
	metas, _ := filepath.Glob(filepath.Join(python, "lib", "python*", "site-packages", "*.dist-info", "METADATA"))
	var out []string
	for _, m := range metas {
		f, err := os.Open(m)
		if err != nil {
			continue
		}
		var name, version string
		sc := bufio.NewScanner(f)
		for sc.Scan() && sc.Text() != "" {
			if v, ok := strings.CutPrefix(sc.Text(), "Name: "); ok {
				name = strings.TrimSpace(v)
			} else if v, ok := strings.CutPrefix(sc.Text(), "Version: "); ok {
				version = strings.TrimSpace(v)
			}
		}
		f.Close()
		if name != "" {
			out = append(out, name+"=="+version)
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// within reports whether path is dir or inside it, by their names.
func within(path, dir string) bool {
	r, err := filepath.Rel(dir, path)
	return err == nil && r != ".." && !strings.HasPrefix(r, ".."+string(filepath.Separator))
}
