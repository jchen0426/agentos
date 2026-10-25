// Package statedir keeps the state directories of the sandboxed processes agentd starts: one
// directory per launch, Root/<kind>.<agentd's pid>.<random>, made afresh (mode 0700, a name
// nobody could take first) and removed when the launch ends, however it ended. There is no
// lasting directory for macOS's tmp_cleaner to remove when it is idle, or for another user to
// plant in its place. sandbox-runtime keeps its own state (its settings and sockets) in it, so the
// path stays short: a socket's path holds 104 bytes on macOS.
package statedir

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

// Root holds the state directories.
const Root = "/tmp"

// Make makes a state directory of kind for this process.
func Make(kind string) (string, error) {
	return os.MkdirTemp(Root, Prefix(kind))
}

// Prefix is the name every state directory of kind this process makes begins with.
func Prefix(kind string) string {
	return fmt.Sprintf("%s.%d.", kind, os.Getpid())
}

// Sweep removes the state directories of kind that agentd processes no longer running left
// behind (one killed mid-launch cannot clean up): real directories of this user's only, whatever
// else Root holds under such a name.
func Sweep(kind string) {
	dirs, _ := filepath.Glob(filepath.Join(Root, kind+".*"))
	for _, d := range dirs {
		parts := strings.SplitN(filepath.Base(d), ".", 3) // kind, <pid>, <random>
		if len(parts) < 2 {
			continue
		}
		pid, err := strconv.Atoi(parts[1])
		if err != nil || pid == os.Getpid() {
			continue
		}
		fi, err := os.Lstat(d)
		if err != nil || !fi.IsDir() {
			continue
		}
		if st, ok := fi.Sys().(*syscall.Stat_t); !ok || int(st.Uid) != os.Getuid() {
			continue
		}
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			_ = Remove(d)
		}
	}
}

// Remove removes a state directory. os.RemoveAll walks it through file descriptors and never
// follows a symbolic link. When that fails, the sandboxed process may have taken the permissions
// off part of it (a subverted one could, to leave its files behind; os.RemoveAll changes no
// modes): the owner gets them back, and the directory goes. A file a process pinned with the uchg
// flag stays (rm-state.py, the launcher's own remover, clears it): an error says so.
func Remove(dir string) error {
	if os.RemoveAll(dir) == nil {
		return nil
	}
	unlock(unix.AT_FDCWD, dir)
	return os.RemoveAll(dir)
}

// unlock gives the owner rwx on the directory name in parent and every directory below it,
// through file descriptors and never through a symbolic link: a process still running (one that
// left the process group) could swap a directory for a link to one of the user's. On Linux this
// needs fchmodat2 (Linux 6.6); there the sandboxes never mount a state directory anyway.
func unlock(parent int, name string) {
	_ = unix.Fchmodat(parent, name, 0o700, unix.AT_SYMLINK_NOFOLLOW)
	fd, err := unix.Openat(parent, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return // gone, or not a directory (any more)
	}
	f := os.NewFile(uintptr(fd), name)
	defer f.Close()
	entries, _ := f.ReadDir(-1)
	for _, e := range entries {
		if e.IsDir() {
			unlock(fd, e.Name())
		}
	}
}
