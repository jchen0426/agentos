"""The Worker sandbox and its bridge to Temporal (design doc, section 8; the M1 prototype
of T9). Inside the sandbox the Temporal SDK reaches Temporal through agentd's Runtime entry,
and nothing else: not Temporal's own port, not the Control socket, not the network, not the
home directory, not the caller's environment. On Linux it can create no files at all. The
launchers refuse a code directory or Python environment that would expose agentd's data or, on
Linux, a way out (a Unix socket, a FIFO).

Run with AGENTOS_SANDBOX=srt on macOS or AGENTOS_SANDBOX=bwrap on Linux.
"""

import contextlib
import json
import os
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.harness import REPO, SANDBOX, Stack, free_port

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
    # Mounts not read-only, but for those nothing can be stored in: /proc, the pseudo-terminals,
    # and the device nodes bwrap binds (/dev/null and the like).
    found = []
    for line in open("/proc/self/mountinfo"):
        fields = line.split()
        point = re.sub(r"\\([0-7]{3})", lambda m: chr(int(m.group(1), 8)), fields[4])
        fstype = fields[fields.index("-") + 1]
        if "rw" in fields[5].split(",") and fstype not in ("proc", "devpts") and not char_device(point):
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
def scratch() -> Iterator[tuple[Path, Path | int]]:
    """A short directory under /tmp, and a Runtime entry the launcher accepts and nothing uses:
    on Linux a socket in a directory of its own, on macOS a port nobody dials."""
    base = Path(tempfile.mkdtemp(prefix="aos-", dir="/tmp"))
    try:
        if SANDBOX == "bwrap":
            (base / "runtime").mkdir()
            with socket.socket(socket.AF_UNIX) as runtime:
                runtime.bind(str(base / "runtime" / "runtime.sock"))
                yield base, base / "runtime" / "runtime.sock"
        else:
            yield base, free_port()
    finally:
        shutil.rmtree(base, ignore_errors=True)


def probe(stack: Stack, canary: Path) -> dict:
    args = [sys.executable, REPO / "tests/sandbox_probe.py",
            "--temporal-port", stack.temporal_address.rsplit(":", 1)[1],
            "--control-socket", stack.control_socket, "--home", Path.home(), "--home-canary", canary,
            "--ledger", stack.data_dir / "ledger.db"]
    if SANDBOX == "bwrap":
        args += ["--runtime", "127.0.0.1:7233"]
    else:
        args += ["--runtime", f"127.0.0.1:{stack.runtime_port}", "--proxy", "env"]
    r = subprocess.run([*launcher(runtime_entry(stack)), *map(str, args)], capture_output=True, text=True, timeout=120,
                       cwd=REPO, env={**stack.sandbox_env(), "AGENTOS_TEST_CANARY": "a secret the Worker must not see"})
    assert r.returncode == 0, r.stderr
    return json.loads(r.stdout.strip().splitlines()[-1])


def test_worker_sandbox_reaches_temporal_only_through_agentd(stack: Stack) -> None:
    stack.start_agentd()
    canary = Path.home() / f".agentos-canary-{secrets.token_hex(4)}"
    canary.write_text("a file the Worker must not read")
    try:
        report = probe(stack, canary)
    finally:
        canary.unlink()
    print(json.dumps(report, indent=2))

    assert report["runtime_sdk"].startswith("connected"), report
    assert report["forbidden"] and all(v.startswith("refused: PERMISSION_DENIED")
                                       for v in report["forbidden"].values()), report["forbidden"]
    for key in ("temporal_sdk", "temporal_tcp", "internet_tcp", "control_socket", "home_canary", "ssh_dir", "ledger",
                "keychains", "keychain_items"):
        assert report[key].startswith("refused"), (key, report[key])
    assert "AGENTOS_TEST_CANARY" not in report["env"]


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
    env = {**stack.sandbox_env(), "AGENTOS_SANDBOX_DENY_READ": f"{stack.tmp}:{exposed}"}
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


@pytest.mark.skipif(SANDBOX != "bwrap", reason="Linux only: the macOS sandbox connects to no socket, writes no FIFO")
@pytest.mark.parametrize("kind", ["socket", "fifo"])
def test_worker_launcher_refuses_a_way_out_in_the_code_dir(kind: str) -> None:
    """A Unix socket or a FIFO the Linux sandbox can see is a way out of it: a read-only mount
    stops neither connect() nor a write to a FIFO. The Runtime socket is reached just that way,
    from a directory that holds nothing else. The launcher refuses a code directory with another
    one in it, here standing in for ssh-agent's or Docker's socket, or a second agentd's."""
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
    with scratch() as (base, port):
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
            env["AGENTOS_SANDBOX_DENY_READ"] = str(base / "link")
        r = reach(launcher(port, sys.prefix, code), data / "ledger.db", code / "absent", env=env)
    assert r.returncode == 0, r.stderr
    assert json.loads(r.stdout)["file"].startswith("refused"), r.stdout


@pytest.mark.skipif(SANDBOX != "bwrap", reason="the Linux sandbox's own mounts")
def test_worker_sandbox_can_create_no_files() -> None:
    """Memory in a tmpfs counts against no process: its data, and also its inodes and extended
    attributes, which --size does not limit. The OOM killer would pick agentd or Temporal, not a
    Worker that filled one. bwrap makes the sandbox's root and /dev as tmpfs mounts as large as
    half the memory. The Worker writes no files at all, so every mount is read-only, /tmp
    included, but for /proc, the pseudo-terminals and the device nodes. Nor can the Worker make
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
