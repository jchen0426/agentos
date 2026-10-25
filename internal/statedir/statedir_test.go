package statedir

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// A state directory goes whatever the sandboxed process left in it: a symbolic link out of it is
// removed, never followed, and a directory it took the permissions off goes too.
func TestRemove(t *testing.T) {
	outside := t.TempDir()
	keep := filepath.Join(outside, "keep")
	if err := os.WriteFile(keep, []byte("a file outside the state directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	dir, err := Make("agentos-statedir-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	if fi, err := os.Stat(dir); err != nil || fi.Mode().Perm() != 0o700 || !strings.HasPrefix(filepath.Base(dir), Prefix("agentos-statedir-test")) {
		t.Fatalf("made %s: %v %v", dir, fi, err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "work", "locked", "deep"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(dir, "work", "out")); err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{"work/locked/deep", "work/locked"} {
		if err := os.Chmod(filepath.Join(dir, d), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := Remove(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(dir); !os.IsNotExist(err) {
		t.Errorf("still there: %v", err)
	}
	if b, err := os.ReadFile(keep); err != nil || string(b) != "a file outside the state directory" {
		t.Errorf("Remove followed the link out: %q, %v", b, err)
	}
}

// The sweep removes the directories of agentd processes that no longer run, and only those: not
// this process's, not a running one's, not a file under such a name.
func TestSweep(t *testing.T) {
	const kind = "agentos-statedir-sweep"
	gone := exec.Command("true")
	if err := gone.Run(); err != nil {
		t.Fatal(err)
	}
	alive := exec.Command("sleep", "30")
	if err := alive.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = alive.Process.Kill(); _ = alive.Wait() })
	mine, err := Make(kind)
	if err != nil {
		t.Fatal(err)
	}
	dead := filepath.Join(Root, kind+"."+strconv.Itoa(gone.Process.Pid)+".x")
	running := filepath.Join(Root, kind+"."+strconv.Itoa(alive.Process.Pid)+".x")
	file := filepath.Join(Root, kind+"."+strconv.Itoa(gone.Process.Pid)+".file")
	for _, d := range []string{dead, running} {
		if err := os.MkdirAll(filepath.Join(d, "work"), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, p := range []string{mine, dead, running, file} {
			_ = os.RemoveAll(p)
		}
	})
	Sweep(kind)
	for p, want := range map[string]bool{mine: true, dead: false, running: true, file: true} {
		if _, err := os.Lstat(p); (err == nil) != want {
			t.Errorf("%s: there %v, want %v", p, err == nil, want)
		}
	}
}
