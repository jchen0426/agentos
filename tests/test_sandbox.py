"""The Worker sandbox's launchers (design doc, section 8; what the sandbox lets the Worker
reach is T9, in tests/test_t9_isolation.py). On Linux the Worker can create no files at all; on
macOS only in a private directory of its own, which no other Worker can read and which goes when
the Worker does, and it reads nothing in the shared temporary directories. The launchers refuse a
code directory or Python environment that would expose dev data, a FIFO, or on Linux a Unix
socket.

Run with AGENTOS_SANDBOX=srt on macOS or AGENTOS_SANDBOX=bwrap on Linux.
"""

import contextlib
import json
import os
import pwd
import re
import secrets
import select
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.harness import (REPO, SANDBOX, Stack, command_of, descendants, free_port, outside_root,
                           remove_outside_root, wait_for)

pytestmark = pytest.mark.skipif(SANDBOX == "none", reason="needs AGENTOS_SANDBOX=bwrap or srt")

WRITE_PROBE = r"""
import errno, json, os, re, stat, sys

def refused(e):
    return "refused: " + errno.errorcode.get(e.errno, str(e.errno))

def write(path):
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except OSError as e:
        return refused(e)
    os.write(fd, b"x")
    os.close(fd)
    return "written"

def char_device(path):
    try:
        return stat.S_ISCHR(os.stat(path).st_mode)
    except OSError:
        return False

def writable_mounts():
    # Mounts not read-only, but for those nothing can be stored in: /proc and the device nodes
    # bwrap binds (/dev/null and the like).
    found = []
    for line in open("/proc/self/mountinfo"):
        fields = line.split()
        point = re.sub(r"\\([0-7]{3})", lambda m: chr(int(m.group(1), 8)), fields[4])
        fstype = fields[fields.index("-") + 1]
        if "rw" in fields[5].split(",") and fstype != "proc" and not char_device(point):
            found.append(f"{point} ({fstype})")
    return found

def new_userns():
    try:
        os.unshare(os.CLONE_NEWUSER)
    except OSError as e:
        return refused(e)
    return "created"

report = {"writes": {k: write(p) for k, p in json.loads(sys.argv[1]).items()},
          "writable_mounts": writable_mounts(), "ipc_ns": os.readlink("/proc/self/ns/ipc")}
report["new_userns"] = new_userns()  # last: once made, this process is in it
print(json.dumps(report))
"""


REACH_PROBE = r"""
import json, os, socket, stat, sys

def read(path):
    try:
        with open(path, "rb") as f:
            return f"read {len(f.read())} bytes"
    except OSError as e:
        return f"refused: {type(e).__name__}"

def way_out(path):
    # A socket to connect to, or a FIFO to write to: either reaches a process outside.
    try:
        if stat.S_ISFIFO(os.stat(path).st_mode):
            fd = os.open(path, os.O_WRONLY | os.O_NONBLOCK)
            os.write(fd, b"x")
            os.close(fd)
            return "written"
        with socket.socket(socket.AF_UNIX) as s:
            s.settimeout(3)
            s.connect(path)
            return "connected"
    except OSError as e:
        return f"refused: {type(e).__name__}"

print(json.dumps({"file": read(sys.argv[1]), "way_out": way_out(sys.argv[2])}))
"""


# macOS: where the Worker can write, and where its temporary files go.
SRT_WRITE_PROBE = r"""
import json, os, sys, tempfile

def attempt(fn):
    try:
        return fn()
    except OSError as e:
        return f"refused: {type(e).__name__}"

def write(path):
    def go():
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "x") as f:
            f.write("x")
        return "written"
    return attempt(go)

def temp_file():
    fd, path = tempfile.mkstemp()
    os.close(fd)
    return path

print(json.dumps({"home": os.environ["HOME"], "temp_file": attempt(temp_file),
                  "writes": {k: write(p) for k, p in json.loads(sys.argv[1]).items()}}))
"""


# macOS: a Worker that writes a file of its own, reads what it is given, reports, and then waits
# for a line on stdin, so that a test can hold it running and end it the way it likes.
STATE_PROBE = r"""
import json, os, sys

def read(path):
    try:
        with open(path, "rb") as f:
            return f"read {len(f.read())} bytes"
    except OSError as e:
        return f"refused: {type(e).__name__}"

own = os.path.join(os.environ["HOME"], "own")
with open(own, "w") as f:
    f.write("a Worker's own file")
reads = {"own": own, **json.loads(sys.argv[1])}
print(json.dumps({"home": os.environ["HOME"], "reads": {k: read(p) for k, p in reads.items()}}), flush=True)
sys.stdin.readline()
"""


def launcher(runtime: Path | int, pyenv: str | Path = sys.prefix, code: Path = REPO) -> list[str]:
    """How Workers are launched: forwarding to the Runtime socket (Linux) or port (macOS), with
    the Python environment and the code directory the sandbox lets them see."""
    script = "sandbox/linux/run-worker.sh" if SANDBOX == "bwrap" else "sandbox/macos/run-worker.sh"
    return [str(REPO / script), str(runtime), str(pyenv), str(code), "--"]


def runtime_entry(stack: Stack) -> Path | int:
    return stack.runtime_socket if SANDBOX == "bwrap" else stack.runtime_port


def reach(launch: list[str], file: Path, way_out: Path, env: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    """Runs REACH_PROBE in the sandbox. The interpreter itself, not a virtual environment's link
    to it: the Python environment the launcher is given need not be one."""
    return subprocess.run([*launch, os.path.realpath(sys.executable), "-I", "-c", REACH_PROBE, str(file), str(way_out)],
                          capture_output=True, text=True, timeout=60, cwd=REPO, env=env)


@contextlib.contextmanager
def scratch(outside: bool = False) -> Iterator[tuple[Path, Path | int]]:
    """A short directory under /tmp, and a Runtime entry the launcher accepts and nothing uses:
    on Linux a socket in a directory of its own, on macOS a port nobody dials. The macOS sandbox
    denies the whole of /tmp: a test of its other rules keeps its files outside (harness.outside_root)."""
    base = outside_root() if outside else Path(tempfile.mkdtemp(prefix="aos-", dir="/tmp"))
    try:
        if SANDBOX == "bwrap":
            (base / "runtime").mkdir()
            with socket.socket(socket.AF_UNIX) as runtime:
                runtime.bind(str(base / "runtime" / "runtime.sock"))
                yield base, base / "runtime" / "runtime.sock"
        else:
            yield base, free_port()
    finally:
        remove_outside_root(base)


@pytest.mark.parametrize("holder", ["code", "code-symlink", "python-env"])
def test_worker_launcher_refuses_agentd_data_in_reach(stack: Stack, holder: str) -> None:
    """A Worker sees the whole of its code directory and its Python environment. Given one that
    holds agentd's data directory, it could read the ledger and, on Linux, connect to the
    Control socket in it: a read-only mount does not stop connect(). The dev configs once kept
    their data in the repository, which the tests and the README give Workers as their code.
    The launchers refuse such a directory, so that mistake cannot go unnoticed."""
    exposed = stack.sock_dir / "exposed"  # under /tmp: the Control socket's path must stay short
    stack.data_dir = exposed / "data"
    if holder != "python-env":
        # As in the dev configs: the Control socket in the data directory. Otherwise the ledger
        # alone, which the launchers must recognize without a socket next to it.
        stack.control_socket = stack.data_dir / "control" / "control.sock"
    stack.start_agentd()
    clean = stack.sock_dir / "clean"
    clean.mkdir()
    given = exposed  # the directory as the launcher is given it
    if holder == "code-symlink":
        given = stack.sock_dir / "link"
        given.symlink_to(exposed, target_is_directory=True)
    pyenv, code = (given, clean) if holder == "python-env" else (sys.prefix, given)
    sock = stack.control_socket if holder == "python-env" else given / "data" / "control" / "control.sock"
    # As the home directory is on macOS: denied, so that only the directory the launcher is given
    # can let the Worker in. (Linux mounts nothing that is not given.)
    env = {**stack.sandbox_env(), "AGENTOS_SANDBOX_DENY_READ": json.dumps([str(stack.tmp), str(exposed)])}
    r = reach(launcher(runtime_entry(stack), pyenv, code), given / "data" / "ledger.db", sock, env=env)
    assert r.returncode != 0 and not r.stdout, f"the Worker started and reached {r.stdout}"
    assert f"{given / 'data'}{os.sep}" in r.stderr, r.stderr  # the launcher names what it found


@pytest.mark.parametrize("name", [".agentos", "temporal.db"])
def test_worker_launcher_refuses_dev_data_by_name(name: str) -> None:
    """Dev data without a ledger is still not for a Worker to read: a checkout's own .agentos
    from before the dev configs moved theirs (a mirror, Temporal's database), or Temporal's
    database wherever the dev server was told to keep it."""
    with scratch() as (base, runtime):
        code = base / "code"
        data = code / ".agentos" / "mirror" / "HEAD" if name == ".agentos" else code / name
        data.parent.mkdir(parents=True)
        data.write_text("dev data")
        r = reach(launcher(runtime, sys.prefix, code), data, code / "absent")
    assert r.returncode != 0 and not r.stdout, f"the Worker started and reached {r.stdout}"
    assert str(code / name) in r.stderr, r.stderr


@pytest.mark.parametrize("kind", [
    pytest.param("socket", marks=pytest.mark.skipif(SANDBOX != "bwrap", reason="the macOS sandbox connects to no socket")),
    "fifo",
])
def test_worker_launcher_refuses_a_way_out_in_the_code_dir(kind: str) -> None:
    """A Unix socket or a FIFO the sandbox can see joins the Worker to a process outside: a
    read-only mount stops neither connect() nor a write to a FIFO (Linux), and the macOS sandbox
    can still read one. The Runtime socket is reached just that way, from a directory that holds
    nothing else. The launchers refuse a code directory with another one in it, here standing in
    for ssh-agent's or Docker's socket, or a second agentd's."""
    with scratch() as (base, runtime):
        code = base / "code"
        code.mkdir()
        path = code / ("other.sock" if kind == "socket" else "other.fifo")
        with contextlib.ExitStack() as held:
            if kind == "socket":
                other = held.enter_context(socket.socket(socket.AF_UNIX))
                other.bind(str(path))
                other.listen()
            else:
                os.mkfifo(path)
                held.callback(os.close, os.open(path, os.O_RDONLY | os.O_NONBLOCK))  # a reader: writes get through
            r = reach(launcher(runtime, sys.prefix, code), code / "absent", path)
    assert r.returncode != 0 and not r.stdout, f"the Worker started and reached {r.stdout}"
    assert str(path) in r.stderr, r.stderr


def test_worker_launcher_refuses_a_directory_it_cannot_look_through() -> None:
    """What the launcher cannot list it cannot vouch for, and the Worker may still open a file
    there by name: a directory that can be searched but not read (0111), holding a ledger."""
    with scratch() as (base, runtime):
        code = base / "code"
        hidden = code / "hidden"
        hidden.mkdir(parents=True)
        (hidden / "ledger.db").write_text("a ledger")
        hidden.chmod(0o111)
        try:
            r = reach(launcher(runtime, sys.prefix, code), hidden / "ledger.db", code / "absent")
        finally:
            hidden.chmod(0o700)
    assert r.returncode != 0 and not r.stdout, f"the Worker started and reached {r.stdout}"
    assert str(hidden) in r.stderr, r.stderr  # find says what it could not read


@pytest.mark.skipif(SANDBOX != "bwrap", reason="the Linux sandbox's Runtime socket directory")
def test_worker_launcher_refuses_a_runtime_dir_that_holds_more() -> None:
    """The Linux launcher mounts the Runtime socket's directory, so it must hold nothing but the
    socket. It looks into it through a symlink too: bwrap mounts what the link points to."""
    with scratch() as (base, _):
        real = base / "data"
        real.mkdir()
        (real / "ledger.db").write_text("a ledger")
        link = base / "link"
        link.symlink_to(real, target_is_directory=True)
        code = base / "code"
        code.mkdir()
        with socket.socket(socket.AF_UNIX) as runtime:
            runtime.bind(str(real / "runtime.sock"))
            r = reach(launcher(link / "runtime.sock", sys.prefix, code), Path("/run/agentos/ledger.db"),
                      Path("/run/agentos/absent"))
    assert r.returncode != 0 and not r.stdout, f"the Worker started and reached {r.stdout}"
    assert str(link / "ledger.db") in r.stderr, r.stderr


@pytest.mark.skipif(SANDBOX != "srt", reason="the macOS sandbox's deny list")
@pytest.mark.parametrize("where", ["home-symlink", "listed-symlink"])
def test_worker_sandbox_denies_data_behind_a_symlink(where: str) -> None:
    """The macOS sandbox lets a Worker read anything outside the home directory that it is not
    told to deny, and Seatbelt matches real paths. agentd's default data directory, ~/.agentos,
    may be a symlink to somewhere else, and a path in AGENTOS_SANDBOX_DENY_READ may be spelled
    through one: either way the data behind it stays out of reach."""
    with scratch(outside=True) as (base, port):
        data = base / "elsewhere" / "data"
        data.mkdir(parents=True)
        (data / "ledger.db").write_text("a ledger")
        home = base / "home"
        home.mkdir()
        code = base / "code"
        code.mkdir()
        env = {**os.environ, "HOME": str(home)}
        env.pop("AGENTOS_SANDBOX_DENY_READ", None)
        if where == "home-symlink":
            (home / ".agentos").symlink_to(data, target_is_directory=True)
        else:
            (base / "link").symlink_to(data, target_is_directory=True)
            env["AGENTOS_SANDBOX_DENY_READ"] = json.dumps([str(base / "link")])
        r = reach(launcher(port, sys.prefix, code), data / "ledger.db", code / "absent", env=env)
    assert r.returncode == 0, r.stderr
    assert json.loads(r.stdout)["file"] == "refused: PermissionError", r.stdout


@pytest.mark.skipif(SANDBOX != "srt", reason="the macOS sandbox's deny list")
def test_worker_sandbox_denies_the_home_directory_whatever_home_says() -> None:
    """The macOS launcher denies the home directory the user database names as well as $HOME:
    with HOME pointing somewhere else, or relative, the real one stays out of reach."""
    canary = Path(pwd.getpwuid(os.getuid()).pw_dir) / f".agentos-canary-{secrets.token_hex(4)}"
    canary.write_text("a file the Worker must not read")
    try:
        with scratch() as (base, port):
            code = base / "code"
            code.mkdir()
            (base / "home").mkdir()
            env = {**os.environ, "HOME": str(base / "home")}
            env.pop("AGENTOS_SANDBOX_DENY_READ", None)
            r = reach(launcher(port, sys.prefix, code), canary, code / "absent", env=env)
    finally:
        canary.unlink()
    assert r.returncode == 0, r.stderr
    assert json.loads(r.stdout)["file"] == "refused: PermissionError", r.stdout


@pytest.mark.skipif(SANDBOX != "srt", reason="the macOS sandbox's write rules")
def test_worker_sandbox_writes_only_its_private_directory() -> None:
    """The macOS sandbox lets a Worker write to a private directory of its own, its HOME and
    TMPDIR, and nowhere else. sandbox-runtime lets every command it runs write to /tmp/claude, and
    hands it that as TMPDIR, unless told otherwise: a directory every Worker would share with every
    other command run under srt on the machine."""
    tag = secrets.token_hex(4)
    writes = {"srt_tmp": Path(f"/tmp/claude/agentos-planted-{tag}/x"),
              "srt_tmp_real": Path(f"/private/tmp/claude/agentos-planted-{tag}-real/x"),
              "shared_tmp": Path(f"/tmp/agentos-planted-{tag}"), "var_tmp": Path(f"/var/tmp/agentos-planted-{tag}"),
              "user_home": Path(pwd.getpwuid(os.getuid()).pw_dir) / f".agentos-planted-{tag}"}
    escaped: list[Path] = []
    with scratch() as (base, port):
        code = base / "code"
        code.mkdir()
        writes["code"] = code / "planted"
        try:
            r = subprocess.run([*launcher(port, sys.prefix, code), os.path.realpath(sys.executable), "-I", "-c",
                                SRT_WRITE_PROBE, json.dumps({k: str(p) for k, p in writes.items()})],
                               capture_output=True, text=True, timeout=60, cwd=REPO)
        finally:
            escaped = [p for p in writes.values() if p.exists()]
            for p in escaped:
                p.unlink()
            for p in (writes["srt_tmp"], writes["srt_tmp_real"]):
                shutil.rmtree(p.parent, ignore_errors=True)
    assert r.returncode == 0, r.stderr
    report = json.loads(r.stdout.strip().splitlines()[-1])
    print(json.dumps(report, indent=2))

    assert not escaped, escaped
    for key, result in report["writes"].items():
        assert result == "refused: PermissionError", (key, result)
    # Temporary files work, and land in the private directory.
    assert Path(report["temp_file"]).parent == Path(report["home"]), report


@contextlib.contextmanager
def held_worker(port: int, code: Path, reads: dict[str, Path] | None = None) -> Iterator[tuple[subprocess.Popen, dict, Path]]:
    """STATE_PROBE under the macOS launcher, started as the harness starts a Worker (a session of
    its own), and held running until the test ends it. Yields the process, the probe's report, and
    the settings file srt was given: the launcher execs srt, so it is on that process's command line."""
    p = subprocess.Popen([*launcher(port, sys.prefix, code), os.path.realpath(sys.executable), "-I", "-c", STATE_PROBE,
                          json.dumps({k: str(v) for k, v in (reads or {}).items()})],
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, cwd=REPO,
                         start_new_session=True)
    try:
        assert select.select([p.stdout], [], [], 60)[0], "the Worker did not report in 60 s"
        line = p.stdout.readline()
        assert line, f"the Worker did not start (launcher exited {p.poll()}): {p.stderr.read()}"
        found = re.search(r" --settings (\S+) ", command_of(p.pid))
        assert found, command_of(p.pid)
        yield p, json.loads(line), Path(found.group(1))
    finally:
        if p.poll() is None:  # not reaped, so its pid is still this process's
            for pid in [*descendants(p.pid), p.pid]:
                with contextlib.suppress(ProcessLookupError):
                    os.kill(pid, signal.SIGKILL)
        p.communicate(timeout=10)


def running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


@pytest.mark.skipif(SANDBOX != "srt", reason="the macOS launcher's state directory")
@pytest.mark.parametrize("end", ["exit", "sigterm", "group-sigterm", "interrupt", "group-hup", "kill-tree"])
def test_worker_launcher_leaves_no_state_behind(end: str) -> None:
    """The macOS launcher makes a directory under /tmp for each Worker it starts: srt's settings,
    srt's own sockets, the Worker's private directory. It execs srt, so nothing of it is left to
    clean up after; yet the directory goes when the Worker does, however that ends. On its own; on
    SIGTERM, to it or to its whole process group, which still reaches the Worker (srt passes it on,
    and exits 0 when the Worker dies of it); on Ctrl-C, which a terminal sends the whole process
    group; on SIGHUP, which srt does not catch (it dies of it) and a closed terminal sends the
    group; or with kill -9 for the whole process tree, as the harness does to a Worker that will
    not stop, or to crash one."""
    # What srt exits with for each end: 0 when it catches the signal and the Worker dies of it,
    # else killed by that signal. The point of the test is the cleanup; the status guards that
    # signals still reach srt and the Worker as before.
    status_of = {"exit": 0, "sigterm": 0, "group-sigterm": 0, "interrupt": 0,
                 "group-hup": -signal.SIGHUP, "kill-tree": -signal.SIGKILL}
    with scratch() as (base, port):
        code = base / "code"
        code.mkdir()
        with held_worker(port, code) as (p, report, settings):
            home = Path(report["home"])
            assert home.is_dir() and settings.is_file(), (home, settings)  # there, from outside, to go
            worker = descendants(p.pid)
            if end == "exit":
                p.stdin.write("\n")
                p.stdin.flush()
            elif end == "sigterm":
                p.send_signal(signal.SIGTERM)
            elif end == "group-sigterm":
                os.killpg(p.pid, signal.SIGTERM)
            elif end == "interrupt":
                os.killpg(p.pid, signal.SIGINT)
            elif end == "group-hup":
                os.killpg(p.pid, signal.SIGHUP)
            else:
                for pid in [*worker, p.pid]:
                    with contextlib.suppress(ProcessLookupError):
                        os.kill(pid, signal.SIGKILL)
            assert p.wait(timeout=30) == status_of[end]
            wait_for(lambda: not any(map(running, worker)), "the Worker to end", timeout=10)
            wait_for(lambda: not settings.parent.exists() and not home.exists(), "the launch's state to go", timeout=10)


@pytest.mark.skipif(sys.platform != "darwin", reason="rm-state.py is the macOS launcher's remover")
def test_rm_state_removes_a_tree_without_following_a_symlink_out_of_it() -> None:
    """rm-state.py removes a launch's state directory even against a hostile Worker. It must not
    follow a symlink the Worker planted in its writable directory (that is how BSD `rm -rf` can be
    turned into a delete of files outside the sandbox), and it must get past a file the Worker
    pinned with the uchg flag or a directory it left at mode 000."""
    base = Path(tempfile.mkdtemp(prefix="aos-rmstate-", dir="/tmp"))
    canary = base / "canary"
    (canary / "sub").mkdir(parents=True)
    keep = canary / "sub" / "keep"
    keep.write_text("a file outside the state directory")
    state = base / "agentos-sandbox.test"
    work = state / "work"
    work.mkdir(parents=True)
    (work / "ordinary").write_text("x")
    (work / "nested").mkdir()
    (work / "nested" / "f").write_text("x")
    (work / "evil").symlink_to(canary, target_is_directory=True)  # must be unlinked, not followed
    pinned = work / "pinned"
    pinned.write_text("x")
    subprocess.run(["chflags", "uchg", str(pinned)], check=True)
    locked = work / "locked"
    locked.mkdir()
    (locked / "f").write_text("x")
    locked.chmod(0o000)
    try:
        r = subprocess.run([sys.executable, "-I", str(REPO / "sandbox/macos/rm-state.py"), str(state)],
                           capture_output=True, text=True, timeout=30)
        state_gone = not state.exists()  # read the outcome before cleanup removes the canary with base
        kept = keep.read_text() if keep.exists() else None
    finally:
        subprocess.run(["chflags", "-R", "0", str(base)], stderr=subprocess.DEVNULL)
        if locked.exists():
            locked.chmod(0o700)
        shutil.rmtree(base, ignore_errors=True)
    assert r.returncode == 0, r.stderr
    assert state_gone, "the state directory was not removed (a pinned file or locked dir held it)"
    assert kept == "a file outside the state directory", "rm-state.py followed a symlink out of the tree"


@pytest.mark.skipif(SANDBOX != "srt", reason="the macOS launcher's state directory")
def test_worker_sandbox_cannot_read_another_launchs_state() -> None:
    """The macOS sandbox lets a Worker read anything outside the home directory that it is not
    told to deny, and each launch's state lives under /tmp. Workers run side by side, and a launch
    whose state could not be removed (everything killed at once) leaves it behind. A Worker reads
    its own private directory there and nothing else: not another Worker's files, not the settings
    another srt was given, not what is left over."""
    with scratch() as (base, port):
        code = base / "code"
        code.mkdir()
        leftover = Path(tempfile.mkdtemp(prefix="agentos-sandbox.", dir="/tmp"))  # as the launcher names it
        try:
            (leftover / "work").mkdir()
            (leftover / "work" / "own").write_text("a Worker's own file, left behind")
            with held_worker(port, code) as (_, other, other_settings):
                targets = {"other_home": Path(other["home"]) / "own", "other_settings": other_settings,
                           "leftover": leftover / "work" / "own"}
                for t in targets.values():
                    t.read_bytes()  # from outside the sandbox
                with held_worker(port, code, targets) as (_, report, _):
                    pass
        finally:
            shutil.rmtree(leftover, ignore_errors=True)
    print(json.dumps(report, indent=2))
    assert report["reads"]["own"].startswith("read"), report
    for key in targets:
        assert report["reads"][key] == "refused: PermissionError", (key, report["reads"][key])


@pytest.mark.skipif(SANDBOX != "bwrap", reason="the Linux sandbox's own mounts")
def test_worker_sandbox_can_create_no_files() -> None:
    """Memory in a tmpfs counts against no process: its data, and also its inodes and extended
    attributes, which --size does not limit. The OOM killer would pick agentd or Temporal, not a
    Worker that filled one. bwrap makes the sandbox's root a tmpfs as large as half the memory.
    The Worker writes no files at all, so every mount is read-only, /tmp and /dev included, but
    for /proc and the five device nodes /dev holds (no pseudo-terminals). Nor can the Worker make
    a user namespace, in which it could mount a tmpfs of its own; and its IPC namespace is its
    own, so what it leaves there goes when the sandbox does."""
    tag = secrets.token_hex(4)
    writes = {"root": f"/agentos-planted-{tag}", "dev_shm": f"/dev/shm/agentos-planted-{tag}",
              "run": f"/run/agentos-planted-{tag}", "tmp": f"/tmp/agentos-planted-{tag}"}
    # Outside first: this machine's /dev/shm and /tmp can be written and a process may make a
    # user namespace, so what the probe meets inside is the sandbox's doing.
    for d in ("/dev/shm", "/tmp"):
        outside = Path(d) / f"agentos-outside-{tag}"
        outside.write_text("x")
        outside.unlink()
    subprocess.run([sys.executable, "-I", "-c", "import os; os.unshare(os.CLONE_NEWUSER)"], check=True)
    # The launcher wants a Runtime socket to forward to; nothing connects to this one.
    sock_dir = Path(tempfile.mkdtemp(prefix="aos-", dir="/tmp"))
    try:
        with socket.socket(socket.AF_UNIX) as runtime:
            runtime.bind(str(sock_dir / "runtime.sock"))
            r = subprocess.run([str(REPO / "sandbox/linux/run-worker.sh"), str(sock_dir / "runtime.sock"), sys.prefix,
                                str(REPO), "--", sys.executable, "-I", "-c", WRITE_PROBE, json.dumps(writes)],
                               capture_output=True, text=True, timeout=60)
    finally:
        shutil.rmtree(sock_dir, ignore_errors=True)
        escaped = [p for p in writes.values() if os.path.exists(p)]
        for p in escaped:
            os.unlink(p)
    assert r.returncode == 0, r.stderr
    report = json.loads(r.stdout.strip().splitlines()[-1])
    print(json.dumps(report, indent=2))

    assert not escaped, escaped  # the sandbox's /tmp and /dev are not this machine's
    # EROFS, not EACCES or ENOENT: a read-only mount, not file permissions or a missing directory.
    for key, result in report["writes"].items():
        assert result == "refused: EROFS", (key, result)
    assert report["writable_mounts"] == [], report["writable_mounts"]
    assert report["new_userns"] == "refused: ENOSPC", report["new_userns"]  # the limit --disable-userns sets
    assert report["ipc_ns"] != os.readlink("/proc/self/ns/ipc"), report["ipc_ns"]
