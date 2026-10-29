// Package codesnap pins the code agentd's own Worker runs (M5 C2c): a copy, made when agentd starts,
// of the Worker's Python environment without its bytecode, and the *.py and *.so files of its code
// directory and of each directory a .pth file names outside both that the Worker's sandbox shows it;
// its files read-only, all of it hashed into one root. The Temporal SDK's sandbox imports workflow
// modules afresh at every workflow task, so a check of the code would not keep a running Worker on what
// it started with: only a copy does. Every generation runs from the copy; the next agentd makes its own,
// of the code as it is then. The standard library, outside both directories, is not copied.
//
// The copy holds nothing the Worker could not read before. The launchers let it read the Python
// environment and the code directory wherever they are. A directory a .pth names outside both goes in
// only where the sandbox let the Worker read it (Options.Visible), and only what the .pth puts on the
// import path: where the sandbox did not, the copy leaves the .pth's line out, and the directory: the
// sandboxed Worker imported nothing from it (macOS's sandbox lets a hidden directory be looked up, so
// that its site.py put it on the import path, but not listed or read; Linux's has no such directory).
// Without a sandbox the Worker reads everything anyway. Nothing comes from in or around a path agentd
// keeps from the Worker (Options.Private). A link is copied as a link, never as what it leads to, so
// the sandbox judges what it leads to as it would have.
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
	"regexp"
	"slices"
	"strings"
	"syscall"
	"unicode"
	"unicode/utf8"

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

// maxDistributions bounds the distributions a snapshot names (tests lower it).
var maxDistributions = 1000

// Options say what the copy may not hold.
type Options struct {
	// Private are the paths the Worker must not read: agentd's data directory, its sockets'
	// directories, the Temporal client's certificate and key, the mirrors, and the deny list. No path
	// the copy is made from may be in one or hold one, but that python and code may be in one (the
	// launchers let the Worker read them wherever they are).
	Private []string
	// Visible reports whether the Worker's sandbox shows it path (given as named; the function judges
	// its real path too): a directory a .pth names outside python and code goes into the copy only
	// then. nil shows everything (no sandbox).
	Visible func(path string) bool
}

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
	// LeftOut are the directories outside python and code that the Worker's sandbox hid from it, of
	// those a .pth names and those in one it names: the copy leaves them out, and a .pth's line that
	// names one.
	LeftOut []string
}

// Make copies python (a virtual environment) and code into a new state directory.
func Make(python, code string, o Options) (*Snapshot, error) {
	dir, err := statedir.Make(Kind)
	if err != nil {
		return nil, err
	}
	s, err := make1(dir, python, code, o)
	if err != nil {
		_ = statedir.Remove(dir)
		return nil, err
	}
	return s, nil
}

// Remove removes the snapshot.
func (s *Snapshot) Remove() error { return statedir.Remove(s.Dir) }

func make1(dir, python, code string, o Options) (*Snapshot, error) {
	// The real paths: a directory given as a link is walked where it is, and the .pth lines are
	// compared with where python and code really are.
	realPython, err := filepath.EvalSymlinks(python)
	if err != nil {
		return nil, fmt.Errorf("worker.python %s: %w", python, err)
	}
	realCode, err := filepath.EvalSymlinks(code)
	if err != nil {
		return nil, fmt.Errorf("worker.code %s: %w", code, err)
	}
	b := &builder{root: dir, self: real(dir), python: realPython, code: realCode, visible: o.Visible, pinned: map[string]string{}}
	for _, p := range o.Private {
		b.private = append(b.private, real(p))
	}
	if err := b.tree(realPython, "python", b.inEnvironment, true); err != nil {
		return nil, fmt.Errorf("worker.python %s: %w", python, err)
	}
	if err := b.tree(realCode, "code", b.inCode, true); err != nil {
		return nil, fmt.Errorf("worker.code %s: %w", code, err)
	}
	if err := venvConfig(filepath.Join(dir, "python")); err != nil {
		return nil, fmt.Errorf("worker.python %s: %w", python, err)
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
		Root: hex.EncodeToString(sum[:]), Files: b.files, Bytes: b.bytes, LeftOut: b.leftOut}
	s.Distributions = distributions(s.Python)
	if interpreter, err := filepath.EvalSymlinks(filepath.Join(realPython, "bin", "python")); err == nil {
		if err := plain(interpreter); err != nil {
			return nil, fmt.Errorf("worker.python %s: its interpreter: %w", python, err)
		}
		s.Interpreter = interpreter
	}
	return s, nil
}

// venvConfig refuses the environment at python (the copy, which the Worker runs) unless Python's site.py
// (Lib/site.py's venv), reading its pyvenv.cfg as it does, keeps the system's and the user's
// site-packages off: they would be imported live, past the copy. site.py reads bin/pyvenv.cfg if it is
// a file, else pyvenv.cfg; without either Python is no virtual environment, and the user's
// site-packages are on. It reads it as UTF-8 (a BOM is part of the first line), a line at a time
// (universal newlines); at the first "=" of a line it strips and lowercases the key and strips the
// value; the last include-system-site-packages wins, lowercased; and only "true" turns them on, which
// they are without one.
func venvConfig(python string) error {
	var cfg string
	for _, name := range []string{"bin/pyvenv.cfg", "pyvenv.cfg"} {
		c := filepath.Join(python, name)
		fi, err := os.Lstat(c)
		if err != nil {
			continue
		}
		if fi.Mode()&fs.ModeSymlink != 0 {
			return fmt.Errorf("its %s is a link: what Python reads there the copy does not pin", name)
		}
		if fi.Mode().IsRegular() {
			cfg = c
			break
		}
	}
	if cfg == "" {
		return errors.New("it has no pyvenv.cfg: it is no virtual environment, and Python would import the user's " +
			"site-packages live, past the copy; make one with python -m venv")
	}
	data, err := os.ReadFile(cfg)
	if err != nil {
		return err
	}
	if !utf8.Valid(data) {
		return errors.New("its pyvenv.cfg is not UTF-8, which Python reads it as")
	}
	system := "true"
	for _, line := range universalLines.Split(string(data), -1) {
		if key, value, ok := strings.Cut(line, "="); ok &&
			pyLower(strings.TrimFunc(key, pyIsSpace)) == "include-system-site-packages" {
			system = pyLower(strings.TrimFunc(value, pyIsSpace))
		}
	}
	if system == "true" {
		return errors.New("its pyvenv.cfg leaves the system's and the user's site-packages on (include-system-site-packages " +
			"is true, or missing): they would be imported live, past the copy; make the environment without " +
			"--system-site-packages")
	}
	return nil
}

// universalLines splits text at the line ends Python's text files read in universal newlines mode.
var universalLines = regexp.MustCompile("\r\n|\r|\n")

// pyIsSpace is Python's str.isspace, for a character: Go's unicode.IsSpace, and the information
// separators U+001C to U+001F.
func pyIsSpace(r rune) bool { return unicode.IsSpace(r) || r >= 0x1c && r <= 0x1f }

// pyLower is s lowercased as Python's str.lower does, as far as a comparison with an ASCII string
// tells: as Go's unicode.ToLower, but for U+0130, which Python lowers to two characters, "i\u0307".
func pyLower(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r == '\u0130' {
			b.WriteString("i\u0307")
			continue
		}
		b.WriteRune(unicode.ToLower(r))
	}
	return b.String()
}

type entry struct {
	path string // in the snapshot, slash-separated
	kind string // file or link
	exec bool
	size int64
	sum  string // the file's sha256, or the link's target
}

type builder struct {
	root, self   string // the snapshot's directory, and its real path
	python, code string // the real paths of the two directories
	visible      func(string) bool
	entries      []entry
	files        int
	bytes        int64
	pinned       map[string]string // a directory a .pth names outside both, and its copy's path
	private      []string          // the paths the Worker must not read, as their real paths
	leftOut      []string
}

// real is path's real path, or path itself, cleaned, where it does not resolve.
func real(path string) string {
	if r, err := filepath.EvalSymlinks(path); err == nil {
		return r
	}
	return filepath.Clean(path)
}

// plain refuses a name the manifest, a link or the record would carry that holds a control or format
// character (a newline, a tab, a bidirectional override), or is not UTF-8.
func plain(s string) error {
	if !utf8.ValidString(s) {
		return fmt.Errorf("%q is not UTF-8", s)
	}
	for _, r := range s {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return fmt.Errorf("%q holds a control or format character (%U)", s, r)
		}
	}
	return nil
}

// keptFrom refuses a tree whose real path is root if it holds the snapshot itself, or a path the
// Worker must not read, or is in one but where configured (python, code); it returns the check for
// each path read under it.
func (b *builder) keptFrom(root string, configured bool) (func(real string) error, error) {
	if within(b.self, root) {
		return nil, fmt.Errorf("it holds %s, the copy being made", b.self)
	}
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

// inCode takes the code directory's *.py and *.so files, what can be imported: none whose name begins
// with a dot, nor in a dot-directory, __pycache__ or a virtual environment in it.
func (b *builder) inCode(path, rel string, d fs.DirEntry) (bool, bool) {
	name := d.Name()
	if d.IsDir() {
		if rel == "." {
			return false, false
		}
		_, err := os.Lstat(filepath.Join(path, "pyvenv.cfg"))
		return strings.HasPrefix(name, ".") || name == "__pycache__" || err == nil, false
	}
	return false, !strings.HasPrefix(name, ".") && (strings.HasSuffix(name, ".py") || strings.HasSuffix(name, ".so"))
}

// inPinned takes of a directory a .pth names what inCode takes of the code directory: what the .pth
// puts on the import path, its *.py and *.so files, and none of its data.
func (b *builder) inPinned(path, rel string, d fs.DirEntry) (bool, bool) {
	return b.inCode(path, rel, d)
}

// tree copies what sel takes of the tree at src, a real path, to dst, a path in the snapshot: a
// configured directory (python, code), or a .pth's.
func (b *builder) tree(src, dst string, sel selection, configured bool) error {
	fi, err := os.Lstat(src)
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return errors.New("not a directory")
	}
	check, err := b.keptFrom(src, configured)
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
		if !configured && b.visible != nil && !b.visible(path) { // in a .pth's directory, what the sandbox hid
			b.leaveOut(path)
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		// The walk follows no link, from a real root: a path's real path is the path itself (but for a
		// directory swapped for a link while it walks).
		if err := check(path); err != nil {
			return err
		}
		t := d.Type()
		out := filepath.ToSlash(filepath.Join(dst, rel))
		if dst == "python" {
			if err := siteEntry(out, t); err != nil {
				return fmt.Errorf("%s: %w", path, err)
			}
		}
		if t.IsDir() || !take { // directories are made for what goes in them
			return nil
		}
		if err := plain(out); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		if err := os.MkdirAll(filepath.Join(b.root, filepath.Dir(out)), 0o755); err != nil {
			return err
		}
		switch {
		case t&fs.ModeSymlink != 0:
			return b.link(src, path, out)
		case t.IsRegular():
			return b.file(path, out)
		default:
			return fmt.Errorf("%s: neither a file, a directory nor a link (%s)", path, t)
		}
	})
}

// link copies a link as a link, never what it leads to, so that the sandbox judges that as it would
// have: one that leads inside the tree it is in (a Linux environment's lib64, to lib; bin/python3, to
// bin/python) leads inside the copy, relative; any other leads where it led, by its absolute path (the
// environment's bin/python, to the interpreter). What a link leads outside to is not pinned: the
// manifest has the link's target.
func (b *builder) link(src, path, out string) error {
	target, err := os.Readlink(path)
	if err != nil {
		return err
	}
	resolved := target
	if !filepath.IsAbs(resolved) {
		resolved = filepath.Join(filepath.Dir(path), target)
	}
	if within(resolved, src) {
		if target, err = filepath.Rel(filepath.Dir(path), resolved); err != nil {
			return err
		}
	} else if importRoot(out) {
		return fmt.Errorf("%s: a link out of the environment, to %s: Python would import all of its packages through it "+
			"live, past the copy; make the environment with directories of its own", path, resolved)
	} else {
		target = resolved
	}
	if err := plain(target); err != nil {
		return fmt.Errorf("%s: a link's target: %w", path, err)
	}
	b.entries = append(b.entries, entry{path: out, kind: "link", sum: target})
	return os.Symlink(target, filepath.Join(b.root, out))
}

// file copies a regular file, read through a descriptor opened without following a link, at 0444
// (0555 if it was executable); a .pth in site-packages is written as pth makes it.
func (b *builder) file(src, out string) error {
	dst := filepath.Join(b.root, out)
	var size int64
	var sum string
	var exec bool
	var err error
	if isPth(out) {
		size, sum, exec, err = b.writePth(src, dst)
	} else {
		size, sum, exec, err = copyFile(src, dst)
	}
	if err != nil {
		return fmt.Errorf("%s: %w", src, err)
	}
	if b.files++; int64(b.files) > maxFiles {
		return fmt.Errorf("more than %d files: too many to pin", maxFiles)
	}
	if b.bytes += size; b.bytes > maxBytes {
		return fmt.Errorf("more than %d bytes: too much to pin", maxBytes)
	}
	b.entries = append(b.entries, entry{path: out, kind: "file", exec: exec, size: size, sum: sum})
	return nil
}

// mode is a copy's mode: read-only, executable where the original was.
func mode(exec bool) os.FileMode {
	if exec {
		return 0o555
	}
	return 0o444
}

// openRegular opens path to read it, refusing a link (O_NOFOLLOW) and anything but a regular file,
// as fstat says; O_NONBLOCK, so that a FIFO put in a file's place does not hold the open up.
func openRegular(path string) (*os.File, os.FileInfo, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, nil, err
	}
	fi, err := f.Stat()
	if err == nil && !fi.Mode().IsRegular() {
		err = fmt.Errorf("not a regular file (%s)", fi.Mode().Type())
	}
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	return f, fi, nil
}

// copyFile copies src to dst, a new file, by descriptor: a clone where the file system makes one
// (APFS), the bytes otherwise. It returns the copy's size and sha256, hashed from the copy's
// descriptor, which it also sets read-only.
func copyFile(src, dst string) (int64, string, bool, error) {
	in, fi, err := openRegular(src)
	if err != nil {
		return 0, "", false, err
	}
	defer in.Close()
	exec := fi.Mode()&0o111 != 0
	h := sha256.New()
	var n int64
	if cloneFile(in, dst) == nil {
		out, _, err := openRegular(dst)
		if err != nil {
			return 0, "", false, err
		}
		defer out.Close()
		if n, err = io.Copy(h, out); err != nil {
			return 0, "", false, err
		}
		if err := out.Chmod(mode(exec)); err != nil {
			return 0, "", false, err
		}
		return n, hex.EncodeToString(h.Sum(nil)), exec, nil
	}
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0o600)
	if err != nil {
		return 0, "", false, err
	}
	n, err = io.Copy(io.MultiWriter(out, h), in)
	if err == nil {
		err = out.Chmod(mode(exec))
	}
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	return n, hex.EncodeToString(h.Sum(nil)), exec, err
}

// isPth reports whether out is a .pth file in one of the environment's site-packages, which Python
// reads at its start.
func isPth(out string) bool {
	name, ok := sitePackages(out)
	return ok && strings.HasSuffix(name, ".pth")
}

// sitePackages reports whether out, a path in the copy, is in one of the environment's site-packages
// directories, python/lib*/*/site-packages (lib, or lib64 as a directory of its own; python3.X, or
// PyPy's pypy3.X), and its name there.
func sitePackages(out string) (string, bool) {
	parts := strings.Split(out, "/")
	if len(parts) == 5 && parts[0] == "python" && strings.HasPrefix(parts[1], "lib") && parts[3] == "site-packages" {
		return parts[4], true
	}
	return "", false
}

// importRoot reports whether out is one of the environment's directories its packages are imported
// from: python/lib*, python/lib*/* or python/lib*/*/site-packages.
func importRoot(out string) bool {
	parts := strings.Split(out, "/")
	if len(parts) < 2 || parts[0] != "python" || !strings.HasPrefix(parts[1], "lib") {
		return false
	}
	return len(parts) <= 3 || len(parts) == 4 && parts[3] == "site-packages"
}

// siteEntry refuses, in one of the environment's site-packages, what Python's site.py would run, or
// read, past the copy: a sitecustomize or usercustomize module, which it imports at every start (agentd
// cannot judge what one does without running it, as with a .pth's import lines), and a .pth that is a
// link, whose content would be what it leads to.
func siteEntry(out string, t fs.FileMode) error {
	name, ok := sitePackages(out)
	if !ok {
		return nil
	}
	if stem, _, _ := strings.Cut(name, "."); stem == "sitecustomize" || stem == "usercustomize" {
		return fmt.Errorf("a %s module in site-packages, which Python imports at its start, and which agentd cannot "+
			"judge without running it: remove it", stem)
	}
	if t&fs.ModeSymlink != 0 && strings.HasSuffix(name, ".pth") {
		return errors.New("a .pth that is a link: Python would read what it leads to, which the copy does not pin")
	}
	return nil
}

// writePth writes the copy of the .pth at src, as pth makes it, to dst.
func (b *builder) writePth(src, dst string) (int64, string, bool, error) {
	in, fi, err := openRegular(src)
	if err != nil {
		return 0, "", false, err
	}
	data, err := io.ReadAll(io.LimitReader(in, 1<<20+1))
	in.Close()
	if err != nil {
		return 0, "", false, err
	}
	if len(data) > 1<<20 {
		return 0, "", false, errors.New("a .pth over 1 MiB")
	}
	content, err := b.pth(src, data)
	if err != nil {
		return 0, "", false, err
	}
	exec := fi.Mode()&0o111 != 0
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0o600)
	if err != nil {
		return 0, "", false, err
	}
	_, err = out.Write(content)
	if err == nil {
		err = out.Chmod(mode(exec))
	}
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	h := sha256.Sum256(content)
	return int64(len(content)), hex.EncodeToString(h[:]), exec, err
}

// pthImports are the import lines a .pth may hold, as virtualenv and setuptools write them, after
// their trailing whitespace: they import from site-packages, which the copy holds. Any other import
// line is refused: agentd would have to run Python to know what it imports.
var pthImports = []string{
	"import _virtualenv",
	"import os; var = 'SETUPTOOLS_USE_DISTUTILS'; enabled = os.environ.get(var, 'local') == 'local'; " +
		"enabled and __import__('_distutils_hack').add_shim();",
	// setuptools' before 60, whose default was the standard library's distutils
	"import os; var = 'SETUPTOOLS_USE_DISTUTILS'; enabled = os.environ.get(var, 'stdlib') == 'local'; " +
		"enabled and __import__('_distutils_hack').add_shim();",
}

// importHooks name the build backends whose editable installs import the live code through a hook.
var importHooks = []struct{ mark, backend string }{
	{"__editable__", "setuptools"},
	{"_editable_impl_", "hatchling (dev-mode-exact)"},
	{"editables", "editables"},
	{"_editable_loader", "meson-python"},
	{"scikit_build", "scikit-build-core"},
	{"_editable", "scikit-build-core"},
}

// pthSeparator finds what Python's str.splitlines breaks a line at, besides "\n" and "\r\n": a .pth
// that holds one would read differently there than here.
var pthSeparator = regexp.MustCompile("\r[^\n]|\r$|[\f\v\x1c\x1d\x1e]|\u0085|\u2028|\u2029")

// pth is a .pth file's content for the copy, as Python's site.py reads it (Lib/site.py's addpackage):
// a line that begins with # is a comment, a blank one nothing, one that begins with "import " or
// "import\t" is run (only pthImports are let through), and any other names a path, relative to
// site-packages unless absolute, which goes on the import path if it exists. Each directory it names
// becomes its copy's, relative to the copy's site-packages: one in the environment or the code
// directory, the copy of it there; one outside both, a copy of its own in python/pinned, by the code
// directory's rule, if the Worker's sandbox shows it, and left out if not. A path that names nothing,
// or a file (a zip, an egg), is left out.
func (b *builder) pth(src string, data []byte) ([]byte, error) {
	if !utf8.Valid(data) {
		return nil, errors.New("a .pth that is not UTF-8")
	}
	if m := pthSeparator.Find(data); m != nil {
		return nil, fmt.Errorf("a .pth with %q in it, which Python reads as the end of a line", m)
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
		line := strings.TrimSuffix(sc.Text(), "\r")
		switch {
		case strings.HasPrefix(line, "#") || strings.TrimFunc(line, pyIsSpace) == "":
			out.WriteString(line + "\n")
			continue
		case strings.HasPrefix(line, "import ") || strings.HasPrefix(line, "import\t"):
			if trimmed := strings.TrimRightFunc(line, pyIsSpace); !slices.Contains(pthImports, trimmed) {
				return nil, refusedImport(filepath.Base(src), trimmed)
			}
			out.WriteString(line + "\n")
			continue
		}
		dir := strings.TrimRightFunc(line, pyIsSpace) // as Python's str.rstrip
		if !filepath.IsAbs(dir) {
			dir = filepath.Join(site, dir)
		}
		dir = filepath.Clean(dir)
		if fi, err := os.Stat(dir); err != nil || !fi.IsDir() {
			continue // nothing there, or a file Python would read as a zip: no part of the copy
		}
		realDir := real(dir)
		var copied string
		switch {
		case within(realDir, b.python):
			r, _ := filepath.Rel(b.python, realDir)
			copied = filepath.Join(b.root, "python", r)
		case within(realDir, b.code):
			r, _ := filepath.Rel(b.code, realDir)
			copied = filepath.Join(b.root, "code", r)
		case b.visible != nil && !b.visible(dir):
			b.leaveOut(dir)
			continue // the Worker's sandbox hid it: the Worker imported nothing from it
		default:
			name, ok := b.pinned[realDir]
			if !ok {
				name = filepath.ToSlash(filepath.Join("python", "pinned", fmt.Sprint(len(b.pinned))))
				b.pinned[realDir] = name
				if err := b.tree(realDir, name, b.inPinned, false); err != nil {
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

// leaveOut notes a path the copy leaves out, as the Worker's sandbox hid it.
func (b *builder) leaveOut(path string) {
	if !slices.Contains(b.leftOut, path) {
		b.leftOut = append(b.leftOut, path)
	}
}

// refusedImport is the refusal of an import line of the .pth called name, naming the build backend that
// writes it where it is one agentd knows; a *-nspkg.pth is setuptools' own, for a namespace package.
func refusedImport(name, line string) error {
	if strings.HasSuffix(name, "-nspkg.pth") {
		return fmt.Errorf("%s, setuptools' .pth for a namespace package of an old wheel, has an import line agentd cannot "+
			"judge without running it (%.80q): install the package again from a newer wheel, or remove the .pth", name, line)
	}
	what := "an import line agentd cannot judge without running it"
	for _, h := range importHooks {
		if strings.Contains(line, h.mark) {
			what = "an editable install by import hook (" + h.backend + "), which would import the live code"
			break
		}
	}
	return fmt.Errorf("a .pth with %s (%.80q): install the package as a wheel, or as an editable install by path "+
		"(hatchling's default, setuptools' editable_mode=compat)", what, line)
}

// PEP 508's names and PEP 440's public versions, as distributions records them.
var (
	distName    = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9._-]{0,98}[A-Za-z0-9])?$`)
	distVersion = regexp.MustCompile(`^(?i)v?(\d+!)?\d+(\.\d+)*((a|b|rc)\d+)?(\.post\d+)?(\.dev\d+)?(\+[a-z0-9]+(\.[a-z0-9]+)*)?$`)
)

// distributions lists name==version of each distribution in the environment's site-packages, from its
// dist-info's METADATA: only names and versions as PEP 508 and PEP 440 spell them, at most
// maxDistributions.
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
		if distName.MatchString(name) && len(version) <= 64 && distVersion.MatchString(version) {
			out = append(out, name+"=="+version)
		}
	}
	slices.Sort(out)
	out = slices.Compact(out)
	return out[:min(len(out), maxDistributions)]
}

// within reports whether path is dir or inside it, by their names.
func within(path, dir string) bool {
	r, err := filepath.Rel(dir, path)
	return err == nil && r != ".." && !strings.HasPrefix(r, ".."+string(filepath.Separator))
}
