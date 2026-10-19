"""Remove a macOS Worker launcher's state directory (/tmp/agentos-sandbox.*) safely, even
while a sandboxed process that outlived srt is still writing inside it.

run-worker.sh runs this once the launch ends. The removal must not be subverted by the
untrusted task code: a Worker writes inside its own private directory (work/) under the state
directory, so it can plant a symlink there and, if the remover followed it, send a recursive
delete out of the tree. BSD `rm -rf` can be made to do exactly that by swapping a directory for
a symlink between the moment it decides a name is a directory and the moment it descends.

So this never decides from a name, and never builds a path to act on. Every filesystem call
is relative to a directory fd the remover opened itself: it descends only through an fd from
os.open(name, O_RDONLY | O_NOFOLLOW | O_DIRECTORY, dir_fd=parent) -- a symlink fails that open
(ELOOP) and is unlinked as the link it is, never followed; a non-directory fails it (ENOTDIR)
and is unlinked; only a real directory opens, and the recursion uses that fd, not the name
again. There is no window between a check and the descent. A Worker can also pin a file with
the uchg flag or leave a directory at mode 000; clearing those is best-effort, done fd-relative
on the single name (never a composed path, whose intermediate components a Worker could swap for
symlinks) with the symlink itself never followed, so it can only ever touch something inside the
directory it is already in, never out of the tree. Failing, it leaves that one entry behind
(still read-denied to every other Worker), never escapes.

Usage: rm-state.py <state-dir>
"""

import errno
import os
import sys

_OPEN_DIR = os.O_RDONLY | os.O_NOFOLLOW | os.O_DIRECTORY


def _clear(dir_fd: int, name: str) -> None:
    """Best-effort: drop a mode or the uchg flag that blocks opening or unlinking `name`. Done
    with the process cwd moved to `dir_fd` (a directory fd the remover holds), so `name` is a
    single component resolved in that real directory; follow_symlinks=False never follows the
    name itself. A Worker that swaps or renames `name` can thus only redirect this onto another
    entry of its own directory, never onto a path outside the tree."""
    try:
        keep = os.open(".", os.O_RDONLY | os.O_DIRECTORY)
    except OSError:
        return
    try:
        os.fchdir(dir_fd)
        for change in (lambda: os.chmod(name, 0o700, follow_symlinks=False),
                       lambda: os.chflags(name, 0, follow_symlinks=False)):
            try:
                change()
            except OSError:
                pass
    except OSError:
        pass
    finally:
        try:
            os.fchdir(keep)
        finally:
            os.close(keep)


def _unlink(dir_fd: int, name: str) -> None:
    try:
        os.unlink(name, dir_fd=dir_fd)
        return
    except OSError:
        pass
    _clear(dir_fd, name)  # a uchg file: clear the flag and try once more
    try:
        os.unlink(name, dir_fd=dir_fd)
    except OSError:
        pass


def _open_dir(dir_fd: int, name: str) -> int | None:
    """Open `name` as a directory without following a symlink, or None if it is not a real
    directory we can enter. A mode-000 directory we own is opened after clearing its mode."""
    for cleared in (False, True):
        try:
            return os.open(name, _OPEN_DIR, dir_fd=dir_fd)
        except OSError as e:
            if e.errno == errno.EACCES and not cleared:
                _clear(dir_fd, name)
                continue
            return None  # ELOOP (a symlink), ENOTDIR (a file, fifo, socket), or still denied
    return None


def _walk(dir_fd: int) -> None:
    try:
        entries = list(os.scandir(dir_fd))
    except OSError:
        return
    for entry in entries:
        name = entry.name
        child_fd = _open_dir(dir_fd, name)
        if child_fd is None:
            _unlink(dir_fd, name)
            continue
        try:
            _walk(child_fd)
        finally:
            os.close(child_fd)
        try:
            os.rmdir(name, dir_fd=dir_fd)
        except OSError:
            _clear(dir_fd, name)
            try:
                os.rmdir(name, dir_fd=dir_fd)
            except OSError:
                pass


def main(root: str) -> None:
    try:
        top = os.open(root, _OPEN_DIR)
    except OSError:
        return  # gone, or a symlink: nothing to do, and never followed
    try:
        _walk(top)
    finally:
        os.close(top)
    try:
        os.rmdir(root)
    except OSError:
        pass


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(f"usage: {sys.argv[0]} <state-dir>")
    main(sys.argv[1])
