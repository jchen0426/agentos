package codesnap

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jchen0426/agentos/internal/statedir"
)

// world is a Python environment and a code directory as agentd's Worker has them, with an
// interpreter and a directory a .pth names outside both.
type world struct{ dir, python, code, outside, interpreter string }

func write(t *testing.T, path, content string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
}

func link(t *testing.T, target, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
}

func newWorld(t *testing.T) world {
	t.Helper()
	dir := t.TempDir()
	w := world{dir: dir, python: filepath.Join(dir, "venv"), code: filepath.Join(dir, "code"), outside: filepath.Join(dir, "editable"),
		interpreter: filepath.Join(dir, "base", "bin", "python3.12")}
	write(t, w.interpreter, "#!/bin/sh\n", 0o755)
	site := filepath.Join(w.python, "lib", "python3.12", "site-packages")
	link(t, w.interpreter, filepath.Join(w.python, "bin", "python"))
	link(t, "python", filepath.Join(w.python, "bin", "python3"))
	write(t, filepath.Join(w.python, "bin", "pip"), "#!/usr/bin/env python\n", 0o755)
	write(t, filepath.Join(w.python, "pyvenv.cfg"), "home = /usr/bin\n", 0o644)
	link(t, "lib", filepath.Join(w.python, "lib64"))
	write(t, filepath.Join(site, "pkg", "__init__.py"), "x = 1\n", 0o644)
	write(t, filepath.Join(site, "pkg", "data.json"), "{}\n", 0o644)
	write(t, filepath.Join(site, "pkg", "__pycache__", "__init__.cpython-312.pyc"), "bytecode", 0o644)
	write(t, filepath.Join(site, "pkg", "old.pyc"), "bytecode", 0o644)
	write(t, filepath.Join(dir, "shared", "license.txt"), "a file a link leads to\n", 0o644)
	link(t, filepath.Join(dir, "shared", "license.txt"), filepath.Join(site, "pkg", "LICENSE"))
	write(t, filepath.Join(site, "pkg-1.2.dist-info", "METADATA"), "Metadata-Version: 2.1\nName: pkg\nVersion: 1.2\n\nName: not this\n", 0o644)
	write(t, filepath.Join(site, "_editable.pth"), w.outside+"\n"+w.outside+"\n"+filepath.Join(w.code, "lib")+"\n"+
		filepath.Join(dir, "gone")+"\n# a comment\nimport _virtualenv\n", 0o644)
	write(t, filepath.Join(w.outside, "mypkg", "__init__.py"), "print('pinned')\n", 0o644)
	write(t, filepath.Join(w.outside, "mypkg", "schema.json"), "{}\n", 0o644)
	write(t, filepath.Join(w.outside, ".git", "config"), "", 0o644)
	write(t, filepath.Join(w.code, "workflows.py"), "v = 1\n", 0o644)
	write(t, filepath.Join(w.code, "lib", "helper.py"), "h = 1\n", 0o644)
	write(t, filepath.Join(w.code, "lib", "fast.so"), "elf", 0o755)
	write(t, filepath.Join(w.code, "README.md"), "not code\n", 0o644)
	write(t, filepath.Join(w.code, ".git", "hook.py"), "", 0o644)
	write(t, filepath.Join(w.code, "__pycache__", "w.py"), "", 0o644)
	write(t, filepath.Join(w.code, "venv2", "pyvenv.cfg"), "", 0o644)
	write(t, filepath.Join(w.code, "venv2", "site.py"), "", 0o644)
	return w
}

func makeOf(t *testing.T, w world) *Snapshot {
	t.Helper()
	s, err := Make(w.python, w.code)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Remove() })
	return s
}

func manifest(s *Snapshot) map[string][]string {
	m := map[string][]string{}
	for line := range strings.SplitSeq(strings.TrimSuffix(string(s.Manifest), "\n"), "\n") {
		f := strings.Split(line, "\t")
		m[f[0]] = f[1:]
	}
	return m
}

// The copy holds the environment without bytecode, the code directory's *.py and *.so, and the
// directory the .pth names outside both; its links and .pth lead within it; its files are read-only.
func TestWhatTheSnapshotHolds(t *testing.T) {
	w := newWorld(t)
	s := makeOf(t, w)
	m := manifest(s)
	var paths []string
	for p := range m {
		paths = append(paths, p)
	}
	slices.Sort(paths)
	want := []string{
		"code/lib/fast.so", "code/lib/helper.py", "code/workflows.py",
		"python/bin/pip", "python/bin/python", "python/bin/python3",
		"python/lib/python3.12/site-packages/_editable.pth", "python/lib/python3.12/site-packages/pkg-1.2.dist-info/METADATA",
		"python/lib/python3.12/site-packages/pkg/LICENSE", "python/lib/python3.12/site-packages/pkg/__init__.py",
		"python/lib/python3.12/site-packages/pkg/data.json", "python/lib64",
		"python/pinned/0/mypkg/__init__.py", "python/pinned/0/mypkg/schema.json", "python/pyvenv.cfg",
	}
	if !slices.Equal(paths, want) {
		t.Errorf("paths\n%s\nwant\n%s", strings.Join(paths, "\n"), strings.Join(want, "\n"))
	}
	for path, f := range map[string][]string{
		"python/bin/python":  {"link", "-", "0", w.interpreter},
		"python/bin/python3": {"link", "-", "0", "python"},
		"python/lib64":       {"link", "-", "0", "lib"},
		"python/bin/pip":     {"file", "x"},
		"code/lib/fast.so":   {"file", "x"},
		"code/workflows.py":  {"file", "-", "6"},
		"python/lib/python3.12/site-packages/pkg/LICENSE": {"file", "-", "23"}, // what the link leads to
	} {
		if got := m[path]; len(got) < len(f) || !slices.Equal(got[:len(f)], f) {
			t.Errorf("%s: %v, want %v...", path, got, f)
		}
	}
	for _, rel := range []string{"python/bin/pip", "code/workflows.py", "python/pinned/0/mypkg/__init__.py"} {
		fi, err := os.Stat(filepath.Join(s.Dir, rel))
		if err != nil || fi.Mode().Perm()&0o222 != 0 {
			t.Errorf("%s: %v, %v: want it read-only", rel, fi.Mode(), err)
		}
	}
	pth, err := os.ReadFile(filepath.Join(s.Python, "lib/python3.12/site-packages/_editable.pth"))
	if err != nil {
		t.Fatal(err)
	}
	if want := "../../../pinned/0\n../../../pinned/0\n../../../../code/lib\n# a comment\nimport _virtualenv\n"; string(pth) != want {
		t.Errorf(".pth %q, want %q", pth, want)
	}
	target, err := filepath.EvalSymlinks(filepath.Join(s.Python, "lib/python3.12/site-packages", "../../../pinned/0"))
	if pinned, _ := filepath.EvalSymlinks(filepath.Join(s.Python, "pinned", "0")); err != nil || target != pinned {
		t.Errorf("the .pth's directory leads to %s (%v), not %s", target, err, pinned)
	}
	interpreter, _ := filepath.EvalSymlinks(w.interpreter)
	if !slices.Equal(s.Distributions, []string{"pkg==1.2"}) || s.Interpreter != interpreter || s.Files != 12 {
		t.Errorf("distributions %v, interpreter %s, %d files", s.Distributions, s.Interpreter, s.Files)
	}
	if !strings.HasPrefix(filepath.Base(s.Dir), statedir.Prefix(Kind)) || s.Code != filepath.Join(s.Dir, "code") {
		t.Errorf("dir %s, code %s", s.Dir, s.Code)
	}
}

// The root is the same for the same code, whatever its bytecode and times; one byte changed in it
// changes the root.
func TestTheRoot(t *testing.T) {
	w := newWorld(t)
	first := makeOf(t, w)
	write(t, filepath.Join(w.python, "lib/python3.12/site-packages/pkg/__pycache__/data.cpython-312.pyc"), "more bytecode", 0o644)
	if err := os.Chtimes(filepath.Join(w.code, "workflows.py"), time.Unix(1, 0), time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}
	second := makeOf(t, w)
	if second.Root != first.Root || !bytes.Equal(second.Manifest, first.Manifest) || second.Dir == first.Dir {
		t.Errorf("roots %s, %s", first.Root, second.Root)
	}
	write(t, filepath.Join(w.code, "workflows.py"), "v = 2\n", 0o644)
	if third := makeOf(t, w); third.Root == first.Root {
		t.Error("a changed byte left the root as it was")
	}
}

// What the snapshot cannot pin is refused, and nothing of it stays.
func TestTheSnapshotRefuses(t *testing.T) {
	for name, c := range map[string]struct {
		change func(t *testing.T, w world)
		says   string
	}{
		"an import-hook editable install": {func(t *testing.T, w world) {
			write(t, filepath.Join(w.python, "lib/python3.12/site-packages/__editable__.x.pth"), "import __editable___x_finder; __editable___x_finder.install()\n", 0o644)
		}, "editable install by import hook"},
		"a link to a directory outside": {func(t *testing.T, w world) {
			link(t, w.outside, filepath.Join(w.python, "lib/python3.12/site-packages/elsewhere"))
		}, "a link to a directory outside"},
		"a FIFO": {func(t *testing.T, w world) {
			if err := syscall.Mkfifo(filepath.Join(w.python, "fifo"), 0o600); err != nil {
				t.Fatal(err)
			}
		}, "neither a file, a directory nor a link"},
		"too many files": {func(t *testing.T, w world) {
			old := maxFiles
			maxFiles = 5
			t.Cleanup(func() { maxFiles = old })
		}, "too many to pin"},
		"too many bytes": {func(t *testing.T, w world) {
			old := maxBytes
			maxBytes = 50
			t.Cleanup(func() { maxBytes = old })
		}, "too much to pin"},
	} {
		t.Run(name, func(t *testing.T) {
			w := newWorld(t)
			c.change(t, w)
			before, _ := filepath.Glob(filepath.Join(statedir.Root, statedir.Prefix(Kind)+"*"))
			s, err := Make(w.python, w.code)
			if err == nil {
				_ = s.Remove()
				t.Fatalf("no refusal")
			}
			if !strings.Contains(err.Error(), c.says) {
				t.Errorf("%v, want %q", err, c.says)
			}
			if after, _ := filepath.Glob(filepath.Join(statedir.Root, statedir.Prefix(Kind)+"*")); len(after) != len(before) {
				t.Errorf("a refused snapshot stays: %v", after)
			}
		})
	}
}

// A snapshot an agentd that no longer runs left behind goes at the next one's start, read-only files
// and all.
func TestTheSweepTakesALeftSnapshot(t *testing.T) {
	gone := exec.Command("true")
	if err := gone.Run(); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(statedir.Root, Kind+"."+strconv.Itoa(gone.Process.Pid)+".test")
	write(t, filepath.Join(dir, "python", "lib", "x.py"), "", 0o444)
	t.Cleanup(func() { _ = statedir.Remove(dir) })
	statedir.Sweep(Kind)
	if _, err := os.Lstat(dir); !os.IsNotExist(err) {
		t.Errorf("%s is still there: %v", dir, err)
	}
}
