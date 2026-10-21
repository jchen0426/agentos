"""The git tool server's sandbox (design doc, section 8). agentd runs the server once per call
through sandbox/<os>/run-tool.sh, which gives it the one mirror read-only, a private
temporary directory, and nothing else: no network, not the home directory, not agentd's data,
its control socket or its Runtime endpoint, not another mirror or another call's files, not
the keychain, not agentd's environment. Here a probe runs in the server's place, behind the
deny list agentd itself hands its launcher. Each thing it must not reach is first reached from
outside the sandbox, so a refusal is the sandbox's doing.

Run with AGENTOS_SANDBOX=srt on macOS or AGENTOS_SANDBOX=bwrap on Linux.
"""

import json
import os
import pwd
import secrets
import shlex
import shutil
import socket
import subprocess
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.conftest import _stack
from tests.harness import KEYCHAIN_ITEMS, LOGIN_KEYCHAIN, REPO, SANDBOX, TOOL_LAUNCHER, Stack, in_keychain

pytestmark = pytest.mark.skipif(SANDBOX not in TOOL_LAUNCHER, reason="needs AGENTOS_SANDBOX=bwrap or srt")

PROBE = r"""
import json, os, socket, subprocess, sys

def attempt(fn):
    try:
        return fn()
    except OSError as e:
        return f"refused: {type(e).__name__}: {e}"

def connect(url):  # unix:///path or tcp://host:port
    def go():
        if url.startswith("unix://"):
            with socket.socket(socket.AF_UNIX) as s:
                s.settimeout(3)
                s.connect(url[len("unix://"):])
        else:
            host, port = url[len("tcp://"):].rsplit(":", 1)
            socket.create_connection((host, int(port)), timeout=3).close()
        return "connected"
    return attempt(go)

def read(path):
    def go():
        with open(path, "rb") as f:
            return f"read {len(f.read())} bytes"
    return attempt(go)

def write(path):
    def go():
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write("x")
        return "written"
    return attempt(go)

def keychain(service, file):  # attributes only: no -w, no -g
    r = subprocess.run(["/usr/bin/security", "find-generic-password", "-s", service, file],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return "found" if r.returncode == 0 else f"refused: exit {r.returncode}"

a = json.loads(sys.argv[1])
print(json.dumps({
    "mirror_read": read(os.path.join(a["mirror"], "app.py")),
    "tmp_write": write(os.path.join(os.environ.get("AGENTOS_TMPDIR") or "/tmp", "index")),
    "reads": {k: read(p) for k, p in a["reads"].items()},
    "home_list": attempt(lambda: f"listed {len(os.listdir(a['home']))} entries"),
    "writes": {k: write(p) for k, p in a["writes"].items()},
    "sockets": {k: connect(u) for k, u in a["sockets"].items()},
    "keychain": {s: keychain(s, a["keychain_file"]) for s in a["keychain"]},
    "env": sorted(os.environ),
}))
"""


@pytest.fixture
def outside_stack(request: pytest.FixtureRequest) -> Iterator[Stack]:
    """A stack outside what the macOS launcher denies wholesale (the home directory, /tmp,
    /var/folders), so that only agentd's own list keeps the server out of its data and its
    other mirrors. /var/tmp is also reached through a symlink on macOS (/private/var/tmp),
    and Seatbelt matches real paths."""
    tmp = Path(tempfile.mkdtemp(prefix="agentos-probe-", dir="/var/tmp"))
    try:
        yield from _stack(request, Stack(tmp))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def connect(url: str) -> None:
    if url.startswith("unix://"):
        with socket.socket(socket.AF_UNIX) as s:
            s.connect(url[len("unix://"):])
    else:
        host, port = url[len("tcp://"):].rsplit(":", 1)
        socket.create_connection((host, int(port)), timeout=3).close()


def probe_env(state: str, deny: list[str]) -> dict:
    """The environment agentd gives the launcher, plus a secret the server must not see."""
    return {"PATH": os.environ["PATH"], "HOME": state, "TMPDIR": state, "AGENTOS_TMPDIR": state, "LANG": "C.UTF-8",
            "AGENTOS_SANDBOX_STATE": state, "AGENTOS_SANDBOX_DENY_READ": json.dumps(deny),
            "AGENTOS_TEST_CANARY": "a secret the tool server must not see"}


def probe(mirror: Path, args: dict, deny: list[str], env: dict[str, str] | None = None) -> dict:
    state = tempfile.mkdtemp(prefix="agentos-tool-", dir="/tmp")
    # The probe runs isolated (-I): Python would look for packages in the home directory.
    try:
        r = subprocess.run([str(TOOL_LAUNCHER[SANDBOX]), str(mirror), "--", "/usr/bin/python3", "-I", "-c",
                            PROBE, json.dumps(args)], capture_output=True, text=True, timeout=120,
                           env={**probe_env(state, deny), **(env or {})})
    finally:
        subprocess.run(["rm", "-rf", state], check=False)
    assert r.returncode == 0, r.stderr
    return json.loads(r.stdout.strip().splitlines()[-1])


def test_the_git_tool_server_sees_its_mirror_and_nothing_else(outside_stack: Stack) -> None:
    stack = outside_stack
    tag = secrets.token_hex(4)
    mirror, other_mirror = stack.tmp / "mirror", stack.tmp / "other-mirror"
    subprocess.run([REPO / "scripts/make-audit-target.sh", other_mirror], check=True, capture_output=True)
    # agentd knows the other mirror by a symlink of its own: sandbox-runtime resolves only
    # /tmp and /var to /private, and Seatbelt matches real paths, so the launcher must.
    other_link = stack.tmp / "other-mirror-link"
    other_link.symlink_to(other_mirror)
    # A launcher in front of the real one writes down the deny list agentd hands it: agentd's
    # self-check at start goes through it.
    record = stack.tmp / "deny-read.json"
    recorder = stack.tmp / "record-launcher.sh"
    recorder.write_text(f'#!/bin/sh\nprintf %s "$AGENTOS_SANDBOX_DENY_READ" > {shlex.quote(str(record))}\n'
                        f'exec {shlex.quote(str(TOOL_LAUNCHER[SANDBOX]))} "$@"\n')
    recorder.chmod(0o755)
    stack.start_agentd(extra={
        "repos": {"acme/payments": {"mirror": str(mirror), "github": "acme/payments"},
                  "acme/ledger": {"mirror": str(other_link), "github": "acme/ledger"}},
        "git_tools": {"sandbox": [str(recorder)]},
    })
    deny = json.loads(record.read_text())
    assert str(stack.data_dir) in deny and str(mirror) in deny and str(other_link) in deny, deny

    canary = Path.home() / f".agentos-canary-{tag}"
    another_call = None
    writes = {"mirror": mirror / "planted.py", "home": Path.home() / f".agentos-planted-{tag}",
              "shared_tmp": Path(f"/tmp/agentos-planted-{tag}"), "var_tmp": Path(f"/var/tmp/agentos-planted-{tag}"),
              # sandbox-runtime lets every process it runs write there, unless told otherwise
              "srt_tmp": Path(f"/tmp/claude/agentos-planted-{tag}/x")}
    if SANDBOX == "bwrap":
        writes["dev_shm"] = Path(f"/dev/shm/agentos-planted-{tag}")  # a tmpfs as large as half the memory
    escaped: list[Path] = []
    try:
        canary.write_text("a file the tool server must not read")
        another_call = Path(tempfile.mkdtemp(prefix="agentos-tools.1.", dir="/tmp"))  # as another agentd's
        (another_call / "work").mkdir()
        (another_call / "work" / "index").write_text("another call's files")
        reads = {"ledger": stack.data_dir / "ledger.db", "other_mirror": other_mirror / "app.py",
                 "home_canary": canary, "another_call": another_call / "work" / "index"}
        sockets = {"control": f"unix://{stack.control_socket}", "runtime": stack.runtime_listen(),
                   "temporal": f"tcp://{stack.temporal_address}",
                   "github": "tcp://" + stack.github_url.removeprefix("http://").rstrip("/")}
        for p in reads.values():
            p.read_bytes()
        for url in sockets.values():
            connect(url)
        keychain = [s for s in KEYCHAIN_ITEMS if in_keychain(s)]
        report = probe(mirror, {"mirror": str(mirror), "home": str(Path.home()),
                               "reads": {k: str(p) for k, p in reads.items()},
                               "writes": {k: str(p) for k, p in writes.items()},
                               "sockets": {**sockets, "internet": "tcp://1.1.1.1:443"},
                               "keychain": keychain, "keychain_file": str(LOGIN_KEYCHAIN)}, deny)
    finally:
        canary.unlink(missing_ok=True)
        if another_call:
            shutil.rmtree(another_call, ignore_errors=True)
        escaped = [p for p in writes.values() if p.exists()]
        for p in escaped:
            p.unlink()
        shutil.rmtree(writes["srt_tmp"].parent, ignore_errors=True)
    print(json.dumps(report, indent=2))

    assert report["mirror_read"].startswith("read"), report  # though agentd denies every mirror by name
    assert report["tmp_write"] == "written", report  # apply_check's throwaway index goes there
    assert report["home_list"].startswith("refused"), report["home_list"]
    for group in ("reads", "sockets", "keychain"):
        for key, result in report[group].items():
            assert result.startswith("refused"), (group, key, result)
    if not keychain:
        print("no agentos item in the login keychain: its check did not run")
    # Nothing written outside the call's directory reaches the host. On Linux /tmp is the
    # sandbox's own, so a write there succeeds and vanishes; everything else is read-only
    # there, the root and /dev included. On macOS every such write is refused.
    assert not escaped, escaped
    for key, result in report["writes"].items():
        if not (SANDBOX == "bwrap" and key in ("shared_tmp", "srt_tmp")):
            assert result.startswith("refused"), (key, result)
    assert "AGENTOS_TEST_CANARY" not in report["env"] and "AGENTOS_SANDBOX_DENY_READ" not in report["env"], report["env"]


@pytest.mark.skipif(SANDBOX != "srt", reason="the macOS launcher's deny list")
def test_the_git_tool_server_cannot_read_a_home_directory_named_through_a_symlink() -> None:
    """The macOS launcher denies the home directory the user database names. Seatbelt matches real
    paths, and sandbox-runtime keeps a symlink's spelling when it leads elsewhere, so a home named
    through one (/Users/x leading to another volume, say) is denied by its real path too. A test
    cannot change the user database, so it changes the answer where the launcher asks for it,
    Node's os.userInfo(), through NODE_OPTIONS."""
    home = Path(pwd.getpwuid(os.getuid()).pw_dir)
    canary = home / f".agentos-canary-{secrets.token_hex(4)}"
    base = Path(tempfile.mkdtemp(prefix="agentos-probe-", dir="/var/tmp"))  # not denied wholesale
    try:
        canary.write_text("a file the tool server must not read")
        (base / "mirror").mkdir()
        (base / "home").symlink_to(home, target_is_directory=True)
        user_info = base / "user-info.js"
        user_info.write_text('const os = require("os"), userInfo = os.userInfo;\n'
                             'os.userInfo = (...a) => ({...userInfo(...a), homedir: process.env.AGENTOS_TEST_HOMEDIR});\n')
        report = probe(base / "mirror", {"mirror": str(base / "mirror"), "home": str(home),
                                         "reads": {"home_canary": str(canary)}, "writes": {}, "sockets": {},
                                         "keychain": [], "keychain_file": str(LOGIN_KEYCHAIN)}, [],
                       env={"NODE_OPTIONS": f"--require={user_info}", "AGENTOS_TEST_HOMEDIR": str(base / "home")})
    finally:
        canary.unlink(missing_ok=True)
        shutil.rmtree(base, ignore_errors=True)
    assert report["reads"]["home_canary"].startswith("refused"), report["reads"]
    assert report["home_list"].startswith("refused"), report["home_list"]
