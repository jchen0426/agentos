// Package codesnap pins the code agentd's own Worker runs (M5 C2c): a copy, made when agentd starts,
// of the Worker's Python environment without its bytecode, the *.py and *.so files of its code
// directory, and the directories a .pth file names outside both; its files read-only, all of it
// hashed into one root. The Temporal SDK's sandbox imports workflow modules afresh at every workflow
// task, so a check of the code would not keep a running Worker on what it started with: only a copy
// does. Every generation runs from the copy; the next agentd makes its own, of the code as it is then.
// The standard library, outside both directories, is not copied.
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

// Make copies python (a virtual environment) and code into a new state directory.
func Make(python, code string) (*Snapshot, error) {
	dir, err := statedir.Make(Kind)
	if err != nil {
		return nil, err
	}
	s, err := make1(dir, python, code)
	if err != nil {
		_ = statedir.Remove(dir)
		return nil, err
	}
	return s, nil
}

// Remove removes the snapshot.
func (s *Snapshot) Remove() error { return statedir.Remove(s.Dir) }

func make1(dir, python, code string) (*Snapshot, error) {
	python, code = filepath.Clean(python), filepath.Clean(code)
	b := &builder{root: dir, python: python, code: code, pinned: map[string]string{}}
	if err := b.tree(python, "python", b.inEnvironment); err != nil {
		return nil, fmt.Errorf("worker.python %s: %w", python, err)
	}
	if err := b.tree(code, "code", b.inCode); err != nil {
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

// inPinned takes all of a directory a .pth names but its bytecode and dot-directories.
func (b *builder) inPinned(path, rel string, d fs.DirEntry) (bool, bool) {
	if d.IsDir() && rel != "." && strings.HasPrefix(d.Name(), ".") {
		return true, false
	}
	return b.inEnvironment(path, rel, d)
}

// tree copies what sel takes of src to dst, a path in the snapshot.
func (b *builder) tree(src, dst string, sel selection) error {
	fi, err := os.Stat(src)
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return errors.New("not a directory")
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
		out := filepath.ToSlash(filepath.Join(dst, rel))
		if strings.ContainsAny(out, "\t\n") {
			return fmt.Errorf("%s: a name with a tab or newline", path)
		}
		switch t := d.Type(); {
		case t.IsDir():
			return os.MkdirAll(filepath.Join(b.root, out), 0o755)
		case t&fs.ModeSymlink != 0:
			return b.link(src, dst, path, rel, out, sel)
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

// link copies a link: one of the environment's bin/python* stays a link to the interpreter, and so
// does one that leads inside the tree it is in (a Linux environment's lib64, to lib); one that leads
// outside is copied as the file it leads to. A link to a directory outside is refused.
func (b *builder) link(src, dst, path, rel, out string, sel selection) error {
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
	if inBin || within(resolved, src) {
		if !inBin { // a relative target, inside the copy as it was inside the tree
			if target, err = filepath.Rel(filepath.Dir(path), resolved); err != nil {
				return err
			}
		}
		b.entries = append(b.entries, entry{path: out, kind: "link", sum: target})
		return os.Symlink(target, filepath.Join(b.root, out))
	}
	fi, err := os.Stat(path)
	if err != nil {
		return err
	}
	if fi.IsDir() {
		return fmt.Errorf("%s: a link to a directory outside it (%s)", path, target)
	}
	if !fi.Mode().IsRegular() {
		return fmt.Errorf("%s: a link to neither a file nor a directory (%s)", path, target)
	}
	if _, take := sel(path, rel, fs.FileInfoToDirEntry(fi)); !take {
		return nil
	}
	return b.file(path, out)
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
				if err := b.tree(dir, name, b.inPinned); err != nil {
					return nil, fmt.Errorf("%s: %w", dir, err)
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
