"""The Worker sandbox and its bridge to Temporal (design doc, section 8; the M1 prototype
of T9). Inside the sandbox the Temporal SDK reaches Temporal through agentd's Runtime entry,
and nothing else: not Temporal's own port, not the Control socket, not the network, not the
home directory, not the caller's environment. On Linux it can create no files at all.

Run with AGENTOS_SANDBOX=srt on macOS or AGENTOS_SANDBOX=bwrap on Linux.
"""

import json
import os
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

from tests.harness import REPO, SANDBOX, Stack

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


def probe(stack: Stack, canary: Path) -> dict:
    args = [sys.executable, REPO / "tests/sandbox_probe.py",
            "--temporal-port", stack.temporal_address.rsplit(":", 1)[1],
            "--control-socket", stack.control_socket, "--home", Path.home(), "--home-canary", canary,
            "--ledger", stack.data_dir / "ledger.db"]
    if SANDBOX == "bwrap":
        launcher = [REPO / "sandbox/linux/run-worker.sh", stack.runtime_socket, sys.prefix, REPO, "--"]
        args += ["--runtime", "127.0.0.1:7233"]
    else:
        launcher = [REPO / "sandbox/macos/run-worker.sh", stack.runtime_port, sys.prefix, REPO, "--"]
        args += ["--runtime", f"127.0.0.1:{stack.runtime_port}", "--proxy", "env"]
    r = subprocess.run([str(a) for a in [*launcher, *args]], capture_output=True, text=True, timeout=120,
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
