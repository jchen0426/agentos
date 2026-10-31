"""Black-box harness: runs Temporal's dev server, a simulated GitHub, agentd and Workers as
separate processes and drives them the way an operator would, through agentctl. Tests look at
the results through the Temporal API, agentd's ledger and its log, and the outside world's own
records: the simulated GitHub's request log and the test tool's external log. Nothing here
depends on how agentd is implemented.

Environment:
  AGENTOS_SANDBOX  none (default), bwrap (Linux) or srt (macOS): how Workers are launched
  AGENTOS_BIN      directory with the agentd, agentctl and githubsim binaries (default: bin/)

Most tests start their Workers themselves, and agentd with --no-worker; start_agentd(worker=True)
has agentd start and supervise its own (stack.worker()).
"""

from __future__ import annotations

import asyncio
import json
import os
import platform
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.request
from collections.abc import Callable
from pathlib import Path
from typing import Any, TypeVar

from temporalio.api.enums.v1 import EventType
from temporalio.api.history.v1 import HistoryEvent
from temporalio.client import Client
from temporalio.service import TLSConfig

REPO = Path(__file__).resolve().parents[1]
BIN = Path(os.environ.get("AGENTOS_BIN", REPO / "bin"))
SANDBOX = os.environ.get("AGENTOS_SANDBOX", "none")
TERMINAL = {"SUCCEEDED", "FAILED", "CANCELLED"}

# Most tests run without approvals; approval_policy() is the same policy with the design doc's
# rule for opening the PR: a person approves it first (section 10).
TEST_POLICY = """
repos: [acme/payments]
tools:
  git.show_diff:         {decision: allow, effect: none}
  git.apply_check:       {decision: allow, effect: none, deny_paths: [".github/**", "Makefile", "scripts/**"]}
  github.open_draft_pr:  {decision: allow, effect: lookup, deny_paths: [".github/**", "Makefile", "scripts/**"]}
  github.comment_commit: {decision: allow, effect: lookup}
models:
  audit_v1: {allowed_providers: [fake, anthropic]}
"""

AUDIT_SCHEMA = REPO / "config/schemas/audit_report.json"


def approval_policy(ttl: str = "1h", policy: str | None = None) -> str:
    """The test policy, or `policy`, with github.open_draft_pr waiting for a person's approval,
    which stays open for `ttl` (a Go duration)."""
    base = TEST_POLICY if policy is None else policy
    out = base.replace("github.open_draft_pr:  {decision: allow,",
                       f"github.open_draft_pr:  {{decision: require_approval, approval_ttl: {ttl},")
    assert out != base, "the policy has no github.open_draft_pr rule to change"
    return out

# AGENTOS_GITHUB=live lets tests/test_t2_live.py, and only it (through the live_stack fixture),
# write to a real test repository instead of the simulated GitHub: the one in AGENTOS_GITHUB_REPO
# (owner/name), holding scripts/make-audit-target.sh's fixture, with a fine-grained token for it
# in the login keychain item agentos-github-test-token. The gh CLI reads GitHub for the checks.
GITHUB_LIVE_REPO = os.environ.get("AGENTOS_GITHUB_REPO", "") if os.environ.get("AGENTOS_GITHUB") == "live" else ""
# The audited commit of scripts/make-audit-target.sh's fixture: its commits have fixed authors
# and dates. A live repository must hold exactly that.
FIXTURE_COMMIT = "01f7b30c0dd024bac65d30d17395693903c759ed"

# The login keychain items agentd may hold secrets in (README). Tests ask only for their
# attributes, never a password, and name the keychain: a sandboxed probe's HOME is not the user's,
# and without it security(1) finds no keychain to search, sandbox or not.
KEYCHAIN_ITEMS = ["agentos-anthropic-api-key", "agentos-openai-api-key", "agentos-github-token",
                  "agentos-github-test-token"]
LOGIN_KEYCHAIN = Path.home() / "Library/Keychains/login.keychain-db"


def outside_root() -> Path:
    """A directory for test data that the macOS sandboxes do not deny wholesale (unlike the home
    directory, /tmp, /var/tmp and /var/folders), so that only the deny list a test checks keeps a
    sandbox out of it, reached through a symlink: Seatbelt matches real paths. Linux mounts only
    what a sandbox is given, so /var/tmp does there. The caller removes it, and the symlink's target."""
    if sys.platform != "darwin":
        return Path(tempfile.mkdtemp(prefix="agentos-probe-", dir="/var/tmp"))
    real = Path(tempfile.mkdtemp(prefix="agentos-probe-", dir="/Users/Shared"))
    link = real.with_name(real.name + "-link")
    link.symlink_to(real, target_is_directory=True)
    return link


def user_temp_dir() -> Path:
    """The user's own temporary directory on macOS, under /private/var/folders, whatever TMPDIR
    says."""
    found = Path(subprocess.run(["getconf", "DARWIN_USER_TEMP_DIR"], capture_output=True, text=True,
                                check=True).stdout.strip()).resolve()
    assert found.is_relative_to("/private/var/folders"), found
    return found


def remove_outside_root(root: Path) -> None:
    if root.is_symlink():
        shutil.rmtree(root.resolve(), ignore_errors=True)
        root.unlink()
    else:
        shutil.rmtree(root, ignore_errors=True)


def in_keychain(service: str) -> bool:
    """Whether the item is there to find, asked the way the probes ask, outside any sandbox."""
    if not LOGIN_KEYCHAIN.exists():
        return False
    with tempfile.TemporaryDirectory(dir="/tmp") as home:
        return subprocess.run(["/usr/bin/security", "find-generic-password", "-s", service, str(LOGIN_KEYCHAIN)],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                              env={"PATH": "/usr/bin:/bin", "HOME": home}).returncode == 0

# How agentd launches the git tool server, and its own Worker, in each sandbox mode.
TOOL_LAUNCHER = {"srt": REPO / "sandbox/macos/run-tool.sh", "bwrap": REPO / "sandbox/linux/run-tool.sh"}
WORKER_LAUNCHER = {"srt": REPO / "sandbox/macos/run-worker.sh", "bwrap": REPO / "sandbox/linux/run-worker.sh"}
DEFAULT_QUEUE = "agentos-workflows"  # the Worker's task queue, unless a stack's agentd runs its own

# agentd's default resource limits of its Worker and of each git tool call (M5 C3a), which the Linux
# launchers hold in a systemd user scope of each run's own: memory in bytes, and tasks.
LIMITS = {"worker": (1 << 30, 512), "tool": (512 << 20, 128)}


def launcher_limits(kind: str = "worker") -> dict[str, str]:
    """What agentd gives a Linux launcher of its limits (internal/scope), for one run by hand: the
    defaults of kind's (worker, tool), in a scope of the run's own, which the user manager at this
    process's XDG_RUNTIME_DIR runs. The launchers refuse to run without them (or AGENTOS_LIMITS=off).
    Nothing elsewhere: the macOS launchers take none."""
    if SANDBOX != "bwrap":
        return {}
    memory, tasks = LIMITS[kind]
    return {"AGENTOS_LIMIT_MEMORY": str(memory), "AGENTOS_LIMIT_TASKS": str(tasks),
            "AGENTOS_LIMIT_UNIT": f"agentos-test-{os.urandom(8).hex()}-{kind}-1.scope",
            "XDG_RUNTIME_DIR": os.environ.get("XDG_RUNTIME_DIR") or f"/run/user/{os.getuid()}"}


# Runs its arguments with the terminal on its stdin as its controlling terminal: started in a
# session of its own, as a shell on a terminal starts a session, then exec'd into the command.
CTTY_EXEC = "import fcntl, os, sys, termios; fcntl.ioctl(0, termios.TIOCSCTTY, 0); os.execv(sys.argv[1], sys.argv[1:])"

# AGENTOS_MODEL=claude, openai or local sends the model task to a real model: Claude, OpenAI's
# gpt-6.1-sol, or gpt-oss:20b on the Ollama server on this machine (README), with the real
# prompt, schema and prices, and the key from the keychain. Claude and OpenAI cost money; the
# tests that make sense with a real model are T1's, which check that crashes never repeat or
# lose a model call.
MODEL = os.environ.get("AGENTOS_MODEL", "fake")
REAL_MODEL = MODEL != "fake"
# How long a model call may take: a real one thinks for a while, one on this machine longer.
MODEL_TIMEOUT = {"fake": 30, "local": 480}.get(MODEL, 240)
# Claude's and OpenAI's keys are real secrets (keyring:), next to which agentd runs no Worker of its
# own without a sandbox (C.1): a test of agentd's own Worker is skipped then (start_agentd(worker=True)),
# for this reason.
KEYED_MODEL = MODEL in ("claude", "openai")
OWN_WORKER_REFUSED = ("a real model's key is a real secret, beside which agentd runs no Worker of its own without a "
                      "sandbox (--insecure-dev): run it under AGENTOS_SANDBOX=srt") if KEYED_MODEL and SANDBOX == "none" else ""
if MODEL in ("openai", "local"):
    TEST_POLICY = TEST_POLICY.replace("allowed_providers: [fake, anthropic]", f"allowed_providers: [fake, anthropic, {MODEL}]")

if MODEL == "fake":
    TEST_MODELS = f"""
providers:
  fake: {{adapter: fake}}
tasks:
  audit_v1: {{model: fake/audit-fixture, max_tokens: 4096, inputs: [diff_ref], output_schema: {AUDIT_SCHEMA}, blobs: [patch]}}
"""
elif MODEL == "claude":
    # As config/models.yaml, but max_tokens 8000: a request that runs to the limit on the model
    # and then on its fallback costs about $0.36, which keeps a T1 run under $2.
    TEST_MODELS = f"""
providers:
  anthropic: {{adapter: anthropic, key: "keyring:agentos-anthropic-api-key", requests_per_minute: 20}}
tasks:
  audit_v1:
    model: anthropic/claude-opus-5-5
    max_tokens: 8000
    inputs: [diff_ref]
    effort: high
    prompt: {REPO / "config/prompts/audit_v1/claude.md"}
    output_schema: {AUDIT_SCHEMA}
    blobs: [patch]
    fallback: [anthropic/claude-opus-4-8]
"""
elif MODEL == "openai":
    # As config/models.yaml's openai provider, with max_tokens 8000: a request that runs to the
    # limit costs about $0.08 at gpt-6.1-sol's prices, so a T1 run stays well under $1.
    TEST_MODELS = f"""
providers:
  openai:
    adapter: openai_compat
    api: responses
    base_url: "https://api.openai.com/v1"
    key: "keyring:agentos-openai-api-key"
    requests_per_minute: 20
    efforts: [low, medium, high, xhigh, max]
tasks:
  audit_v1:
    model: openai/gpt-6.1-sol
    max_tokens: 8000
    inputs: [diff_ref]
    effort: high
    prompt: {REPO / "config/prompts/audit_v1/claude.md"}
    output_schema: {AUDIT_SCHEMA}
    blobs: [patch]
"""
elif MODEL == "local":
    # As config/models.yaml's local provider: free, and its window must be the server's
    # (OLLAMA_CONTEXT_LENGTH). Effort medium leaves gpt-oss's reasoning room in max_tokens.
    TEST_MODELS = f"""
providers:
  local:
    adapter: openai_compat
    api: chat
    base_url: "http://127.0.0.1:11434/v1"
    context_tokens: 65536
    efforts: [low, medium]
tasks:
  audit_v1:
    model: local/gpt-oss:20b
    max_tokens: 8000
    inputs: [diff_ref]
    effort: medium
    prompt: {REPO / "config/prompts/audit_v1/claude.md"}
    output_schema: {AUDIT_SCHEMA}
    blobs: [patch]
"""
else:
    raise ValueError(f"unknown AGENTOS_MODEL {MODEL!r}")

# Like config/dev/prices.yaml: the fake models cost money, or the budget goes untested.
TEST_PRICES = """
price_table_version: test-1
models:
  fake/audit-fixture:  {input: 1.00, output: 5.00}
  fake/audit-fallback: {input: 2.00, output: 10.00}
""" if not REAL_MODEL else (REPO / "config/prices.yaml").read_text()

T = TypeVar("T")


def wait_for(predicate: Callable[[], T], what: str, timeout: float = 30, interval: float = 0.2) -> T:
    deadline = time.monotonic() + timeout
    while True:
        value = predicate()
        if value:
            return value
        if time.monotonic() > deadline:
            raise TimeoutError(f"timed out after {timeout}s waiting for {what}")
        time.sleep(interval)


def free_port() -> int:
    """A port that no other process asking the OS for a port gets for a while; the server meant
    to have it must bind it with SO_REUSEADDR, as Go does. Linux may give a port just let go to
    the next process that asks, and the Temporal dev server asks for its own services' ports
    after it has checked the one it was given: its worker service took the frontend's port, and
    the server answered, then died. Like Temporal's own port picker, leave a connection to the
    port in TIME_WAIT: until that ends (60 s), Linux gives the port to nobody who binds port 0
    or connects out, while a bind by number with SO_REUSEADDR still succeeds. macOS hands out
    ports in turn, so a port let go does not come back soon there anyway."""
    with socket.socket() as listener:
        # The connection inherits SO_REUSEADDR; without it, Linux refuses even the server's
        # SO_REUSEADDR bind while the connection waits.
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        port = listener.getsockname()[1]
        with socket.create_connection(("127.0.0.1", port)):
            listener.accept()[0].close()  # this end closes first, so its side waits in TIME_WAIT
    return port


def descendants(pid: int) -> list[int]:
    # ps in a session of its own: a Ctrl-C at the terminal, which the whole foreground process
    # group gets, does not cut short a stack's stopping by killing it.
    out = subprocess.run(["ps", "-A", "-o", "pid=,ppid="], capture_output=True, text=True, check=True,
                         start_new_session=True).stdout
    children: dict[int, list[int]] = {}
    for line in out.splitlines():
        p, pp = map(int, line.split())
        children.setdefault(pp, []).append(p)
    found, todo = [], [pid]
    while todo:
        for c in children.get(todo.pop(), []):
            found.append(c)
            todo.append(c)
    return found


def started_at(pid: int) -> str:
    """When a process started, as ps says it (to the second; empty once it is gone): with its
    PID, which process it is, whatever it has exec'd since."""
    return subprocess.run(["ps", "-o", "lstart=", "-p", str(pid)], capture_output=True, text=True,
                          env={**os.environ, "LC_ALL": "C", "TZ": "UTC"}, start_new_session=True).stdout.strip()


def command_of(pid: int) -> str:
    # -ww: no width at all. Without it, a ps that finds a terminal width (an 80-column pty, a
    # COLUMNS) cuts the line there, and a long checkout path leaves argv[0] a stub ("…/bin/li").
    return subprocess.run(["ps", "-ww", "-o", "command=", "-p", str(pid)], capture_output=True, text=True,
                          start_new_session=True).stdout


class Proc:
    """A child process with its output in a log file. With terminal, a pseudo-terminal is its
    controlling terminal, as a terminal is a command's that a person starts in a shell; else stdin
    is the file descriptor given, or this process's own."""

    def __init__(self, name: str, args: list[str], log: Path, env: dict[str, str] | None = None,
                 cwd: Path | None = None, pass_fds: tuple[int, ...] = (), terminal: bool = False,
                 stdin: int | None = None) -> None:
        self.name, self.log = name, log
        self._out = open(log, "w")
        self._pty: int | None = None
        tty = None
        if terminal:
            self._pty, tty = os.openpty()
            stdin = tty
            args = [sys.executable, "-c", CTTY_EXEC, *args]
        try:
            self.popen = subprocess.Popen([str(a) for a in args], stdin=stdin, stdout=self._out, stderr=subprocess.STDOUT,
                                          env=env, cwd=cwd, start_new_session=True, pass_fds=pass_fds)
        finally:
            if tty is not None:
                os.close(tty)  # the child has it; the terminal stays while _pty, its other end, is open

    def text(self) -> str:
        return self.log.read_text(errors="replace")

    def alive(self) -> bool:
        return self.popen.poll() is None

    def signal_all(self, sig: int) -> None:
        try:
            pids = [*descendants(self.popen.pid), self.popen.pid]
        except subprocess.CalledProcessError:  # ps did not finish: the process group, at least
            pids = []
            try:
                os.killpg(self.popen.pid, sig)  # each Proc leads a session, and its group, of its own
            except (ProcessLookupError, PermissionError):
                pass
        for pid in pids:
            try:
                os.kill(pid, sig)
            except ProcessLookupError:
                pass

    def kill(self) -> None:
        """kill -9 the whole process tree."""
        self.signal_all(signal.SIGKILL)
        self.popen.wait(timeout=10)

    def stop(self, timeout: float = 15) -> None:
        if self.alive():
            self.popen.send_signal(signal.SIGTERM)
            try:
                self.popen.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                self.kill()
        self.signal_all(signal.SIGKILL)  # anything left behind, e.g. socat in a sandbox
        self._out.close()
        if self._pty is not None:  # closed while the process runs, its terminal would hang up on it
            os.close(self._pty)
            self._pty = None


class Worker(Proc):
    def worker_pid(self) -> int:
        """The Python process itself, which may sit under a sandbox wrapper (bwrap, srt)."""
        for pid in [self.popen.pid, *descendants(self.popen.pid)]:
            argv = command_of(pid).split()
            # macOS framework builds re-exec as .../Python.app/Contents/MacOS/Python.
            if argv and Path(argv[0]).name.lower().startswith("python") and "agentos_worker" in argv:
                return pid
        raise AssertionError(f"no Worker process under {self.popen.pid}")

    def pause(self) -> None:
        """SIGSTOP: the Worker keeps its connection and its outstanding polls, but does nothing."""
        os.kill(self.worker_pid(), signal.SIGSTOP)


class SupervisedWorker:
    """The Worker the stack's agentd runs itself (start_agentd(worker=True)), a generation after
    another, as the current agentd's log tells of it."""

    def __init__(self, stack: Stack) -> None:
        self.stack = stack

    def log(self, msg: str | None = None) -> list[dict]:
        """The current agentd's log of its Worker, or the entries with msg."""
        assert self.stack.agentd is not None
        out = []
        for line in self.stack.agentd.text().splitlines():
            try:
                e = json.loads(line)
            except json.JSONDecodeError:
                continue
            if e.get("component") == "worker" and (msg is None or e.get("msg") == msg):
                out.append(e)
        return out

    def wait_ready(self, generation: int, timeout: float = 90) -> int:
        """The Python process of generation, once agentd says it is ready."""
        wait_for(lambda: any(e.get("generation") == generation for e in self.log("worker ready")),
                 f"the Worker's generation {generation}", timeout=timeout)
        return self.pid()

    def pid(self) -> int:
        """The running generation's Python process, under its launcher (bwrap, srt) under agentd."""
        assert self.stack.agentd is not None
        for pid in descendants(self.stack.agentd.popen.pid):
            argv = command_of(pid).split()
            # macOS framework builds re-exec as .../Python.app/Contents/MacOS/Python.
            if argv and Path(argv[0]).name.lower().startswith("python") and "agentos_worker" in argv:
                return pid
        raise AssertionError(f"no Worker under agentd {self.stack.agentd.popen.pid}")

    def pause(self) -> None:
        """SIGSTOP: the Worker keeps its connection and its outstanding polls, but does nothing."""
        os.kill(self.pid(), signal.SIGSTOP)

    def kill(self) -> None:
        """kill -9 the Python Worker: its launcher ends with it."""
        os.kill(self.pid(), signal.SIGKILL)

    def output(self) -> list[str]:
        """The Worker's output lines that agentd logged."""
        return [e["line"] for e in self.log("worker output")]


def make_mirror(path: Path, live_repo: str = "") -> str:
    """The read-only mirror of acme/payments: the audit fixture, whose last commit (the one
    audited) introduces a SQL injection that the fake model's patch fixes. For a live run, a
    clone of the test repository, which must hold that same fixture. Returns the audited commit."""
    if live_repo:
        subprocess.run(["git", "clone", "-q", f"https://github.com/{live_repo}.git", str(path)], check=True)
        head = subprocess.run(["git", "-C", str(path), "rev-parse", "HEAD"], check=True, capture_output=True,
                              text=True).stdout.strip()
        assert head == FIXTURE_COMMIT, f"{live_repo} does not hold the fixture: HEAD is {head}"
        return head
    r = subprocess.run([REPO / "scripts/make-audit-target.sh", path], check=True, capture_output=True, text=True)
    assert r.stdout.strip() == FIXTURE_COMMIT, r.stdout
    return FIXTURE_COMMIT



class Watch:
    """Who hears of every agentd a Stack starts, and of every Stack closing: --verify-after-each
    (tests/verify_each.py) while a test runs, to verify each agentd's log before it stops; nobody
    otherwise. A stack is heard of whatever built it: a fixture, a test, or code a test calls."""

    on_start: Callable[[Stack], None] | None = None
    on_close: Callable[[Stack], None] | None = None


WATCH = Watch()


class Stack:
    """One Temporal dev server, one agentd and its Workers, isolated in a temporary directory.
    With fixture=False there is neither the audit fixture's mirror nor the simulated GitHub: the
    caller names agentd's repositories (start_agentd's repos), and the GitHub tools only dry-run
    (the evaluation, evals/runner.py)."""

    # Set by subclasses that run Temporal with mTLS: the harness's client certificate and
    # the temporal.tls section of agentd.yaml.
    temporal_tls: TLSConfig | None = None
    agentd_tls: dict | None = None

    def __init__(self, tmp: Path, live_repo: str = "", temporal_db: bool = False, fixture: bool = True,
                 hold_fds: tuple[int, ...] = (), name: str = "stack") -> None:
        self.tmp = tmp
        self.name = name  # what an audit_verify mark calls it (tests/verify_each.py)
        self.live_repo = live_repo  # a real GitHub repository to write to, instead of the simulator
        # Temporal's dev server keeps everything in memory unless it has a database file: with one,
        # restart_temporal() keeps the runs.
        self.temporal_db = tmp / "temporal.db" if temporal_db else None
        # Unix socket paths must stay under 104 bytes (macOS), so they live in a short /tmp dir.
        self.sock_dir = Path(tempfile.mkdtemp(prefix="aos-", dir="/tmp"))
        self.procs: list[Proc] = []
        self.workers: list[Worker] = []
        self.agentd: Proc | None = None
        # agentd's own Worker (start_agentd(worker=True)) polls a queue of the stack's own, which
        # tells its processes from any other's, wherever they run.
        self.supervised = False
        self.workflow_queue = DEFAULT_QUEUE
        self._agentd_terminal = False
        self._fault_script: dict | None = None  # the one agentd runs with, if any
        self.deny_read: list[Path] = []  # more paths the Worker's sandbox may not read
        # Temporal keeps these open, and with them any lock on them: the evaluation's state
        # directory stays locked while the Temporal a killed runner left behind still runs. Not
        # agentd: the processes it starts (a sandboxed git tool) would inherit them.
        self.hold_fds = hold_fds
        self.fixture = fixture
        self.commit = make_mirror(tmp / "mirror", live_repo) if fixture else ""
        self.data_dir = tmp / "agentd" / "data"
        # Separate directories: the Linux sandbox mounts the Runtime socket's directory.
        self.control_socket = self.sock_dir / "control" / "control.sock"
        self.runtime_port = free_port()
        self.runtime_socket = self.sock_dir / "runtime" / "runtime.sock"
        self.temporal_address = f"127.0.0.1:{free_port()}"
        try:
            self._start_temporal()
            if fixture:
                self._start_github()
        except BaseException:  # a signal while Temporal starts, too: nothing is left running
            self.close()
            raise

    # --- processes ---------------------------------------------------------------------------

    def _start_temporal(self) -> None:
        # free_port() keeps the port from processes that ask the OS for any port, not from one
        # that binds it by number before the server does. Then try another.
        for attempt in range(3):
            port = self.temporal_address.rsplit(":", 1)[1]
            n = sum(p.name == "temporal" for p in self.procs)
            p = Proc("temporal", ["temporal", "server", "start-dev", "--ip", "127.0.0.1", "--port", port,
                                  "--ui-port", str(free_port()), "--headless", "--log-level", "warn",
                                  "--search-attribute", "AgentState=Keyword",
                                  *(["--db-filename", str(self.temporal_db)] if self.temporal_db else [])],
                     self.tmp / f"temporal{n or ''}.log", pass_fds=self.hold_fds)
            self.procs.append(p)
            self._remember()
            wait_for(lambda: self._temporal_ready() or not p.alive(), "the Temporal dev server", timeout=60)
            if p.alive():
                return
            if "address already in use" not in p.text():
                break
            self.temporal_address = f"127.0.0.1:{free_port()}"
        raise AssertionError(p.text())

    def restart_temporal(self) -> None:
        """Stop the Temporal dev server and start it again on the same port and database: the
        runs, their timers and their history stay (only with temporal_db)."""
        assert self.temporal_db is not None, "restart_temporal needs Stack(..., temporal_db=True)"
        [p] = [p for p in self.procs if p.name == "temporal" and p.alive()]
        p.stop()
        self._start_temporal()

    def _temporal_ready(self) -> bool:
        try:
            asyncio.run(Client.connect(self.temporal_address, tls=self.temporal_tls or False))
            return True
        except Exception:
            return False

    def _start_github(self) -> None:
        """The simulated GitHub the GitHub tools write to: its log is the record of what
        really reached "GitHub". It holds the mirror's commits and trees, as GitHub would."""
        addr = self.tmp / "githubsim.addr"
        self.github_log_path = self.tmp / "github.jsonl"
        self.github_faults = self.tmp / "github-faults.json"
        p = Proc("githubsim", [BIN / "githubsim", "--listen", "127.0.0.1:0", "--addr-file", addr,
                               "--log", self.github_log_path, "--faults", self.github_faults,
                               "--repo", f"acme/payments={self.tmp / 'mirror'}"],
                 self.tmp / "githubsim.log")
        self.procs.append(p)
        self._remember()
        wait_for(lambda: addr.exists() or not p.alive(), "the simulated GitHub")
        assert p.alive(), p.text()
        self.github_url = f"http://{addr.read_text()}/"

    def start_modelsim(self, answers: list[dict] | str | None = None) -> str:
        """The simulated model providers T13 injects faults through (internal/modelsim): one
        server for Anthropic's Messages API, OpenAI's Responses API and Chat Completions, which
        answer as `answers` scripts (modelsim's rules), or with the fixture's report. `answers` is
        the rules, or the text of a file of them (a number too long for Python's json to write, say).
        Returns its base URL."""
        addr = self.tmp / "modelsim.addr"
        self.modelsim_log = self.tmp / "modelsim.jsonl"
        self.modelsim_faults = self.tmp / "modelsim-faults.json"
        args = ["--listen", "127.0.0.1:0", "--addr-file", addr, "--log", self.modelsim_log,
                "--faults", self.modelsim_faults]
        if answers is not None:
            (self.tmp / "modelsim-answers.json").write_text(answers if isinstance(answers, str) else json.dumps(answers))
            args += ["--answers", self.tmp / "modelsim-answers.json"]
        p = Proc("modelsim", [BIN / "modelsim", *args], self.tmp / "modelsim.log")
        self.procs.append(p)
        wait_for(lambda: addr.exists() or not p.alive(), "the simulated model providers")
        assert p.alive(), p.text()
        return f"http://{addr.read_text()}"

    def model_faults(self, script: dict) -> None:
        """The faults the simulated model providers answer with next, each once, per API."""
        self.modelsim_faults.write_text(json.dumps(script))

    def modelsim_requests(self, api: str) -> list[dict]:
        """The simulated model providers' own record of the requests one API got."""
        lines = self.modelsim_log.read_text().splitlines() if self.modelsim_log.exists() else []
        return [e for e in map(json.loads, lines) if e["api"] == api]

    def hold_github(self, action: str, path: Path) -> None:
        """The simulated GitHub holds its answer to the next successful `action` (create_pull,
        create_comment, ...) after the write took effect, while `path` exists."""
        faults = self._github_faults()
        faults.setdefault("hold_after", {})[action] = str(path)
        self.github_faults.write_text(json.dumps(faults))

    def change_github_entry(self, name: str, **entry: str) -> None:
        """The simulated GitHub's trees show the entry `name` with these fields (mode, type,
        sha) in place of the mirror's: GitHub and the mirror disagree."""
        faults = self._github_faults()
        faults.setdefault("entries", {})[name] = entry
        self.github_faults.write_text(json.dumps(faults))

    def _github_faults(self) -> dict:
        return json.loads(self.github_faults.read_text()) if self.github_faults.exists() else {}

    def github_requests(self, action: str | None = None) -> list[dict]:
        """The simulated GitHub's own record of the requests it answered; writes that took
        effect have status 201."""
        lines = self.github_log_path.read_text().splitlines() if self.github_log_path.exists() else []
        return [e for e in map(json.loads, lines) if action is None or e["action"] == action]

    def github_comments(self) -> list[dict]:
        """The comments on the audited commit, as the simulated GitHub shows them."""
        url = f"{self.github_url}repos/acme/payments/commits/{self.commit}/comments?per_page=100"
        with urllib.request.urlopen(url, timeout=10) as r:
            return json.load(r)

    def report_comment(self, run: dict) -> str:
        """The body of the run's report, as the simulated GitHub shows it: the comment whose last line
        is the marker of the run's github.comment_commit effect."""
        markers = {f"<!-- agent-effect:{e['effect_id']} -->" for e in self.effects(run) if e["tool"] == "github.comment_commit"}
        [body] = [c["body"] for c in self.github_comments() if c["body"].rstrip().splitlines()[-1] in markers]
        return body

    def explain(self, run: dict) -> dict:
        """agentctl runs explain, as JSON."""
        return self.agentctl("runs", "explain", run["run_id"])

    def runtime_listen(self) -> str:
        if SANDBOX == "bwrap":
            return f"unix://{self.runtime_socket}"
        return f"tcp://127.0.0.1:{self.runtime_port}"

    def worker_config(self, workflows: str | None = None) -> dict:
        """The worker section of agentd.yaml: the launcher for AGENTOS_SANDBOX (none for none,
        with --insecure-dev), this Python environment and the repository as its code directory, as
        the harness's own Workers have them, and the same paths denied."""
        w: dict = {"python": sys.prefix, "code": str(REPO), "deny_read": [str(p) for p in [self.tmp, *self.deny_read]]}
        if SANDBOX in WORKER_LAUNCHER:
            w["sandbox"] = [str(WORKER_LAUNCHER[SANDBOX])]
        if workflows:
            w["workflows"] = workflows
        return w

    def start_agentd(self, faults: list[dict] | None = None, policy: str = TEST_POLICY,
                     models: str = TEST_MODELS, prices: str = TEST_PRICES, extra: dict | None = None,
                     test_tools: bool = False, run_id_delay: str | None = None,
                     hold_doorbells: Path | None = None, repos: dict | None = None,
                     worker: bool = False, workflows: str | None = None, terminal: bool = False,
                     hold_worker: Path | None = None, insecure_dev: bool = False) -> Proc:
        """`faults` are fault rules for gateway calls (internal/faults). `run_id_delay` (a Go
        duration) makes StartRun record each run's ID that much later. While `hold_doorbells`
        exists, agentd delivers no doorbell: they wait in its outbox. While `hold_worker` exists,
        agentd does not start its own Worker again: a window with no Worker running, once the test
        has killed it (stack.worker().kill()); where agentd would refuse its own Worker (a real
        key without a sandbox, OWN_WORKER_REFUSED), the test is skipped. `repos` replaces the
        fixture's acme/payments; a stack without the fixture has no GitHub section at all, so
        the GitHub tools only dry-run. With `worker`, agentd starts and supervises the Worker
        itself, serving `workflows` (stack.worker()); otherwise it runs with --no-worker, and the
        test starts its own (start_worker). With `terminal`, agentd runs on a pseudo-terminal of its
        own, its controlling terminal. agentd runs with --insecure-dev for AGENTOS_SANDBOX=none, or
        with `insecure_dev`, whose launchers then run with their resource limits off."""
        if worker and OWN_WORKER_REFUSED:
            import pytest  # only tests get here: the evaluation runs agentd with --no-worker

            pytest.skip(OWN_WORKER_REFUSED)
        d = self.tmp / "agentd"
        d.mkdir(exist_ok=True)
        (d / "policy.yaml").write_text(policy)
        (d / "models.yaml").write_text(models)
        (d / "prices.yaml").write_text(prices)
        config = {  # JSON is YAML
            "data_dir": str(self.data_dir),
            "control_socket": str(self.control_socket),
            "runtime_listen": self.runtime_listen(),
            "temporal": {"address": self.temporal_address, **({"tls": self.agentd_tls} if self.agentd_tls else {})},
            "policy_file": "policy.yaml",
            "models_file": "models.yaml",
            "prices_file": "prices.yaml",
            "repos": repos if repos is not None else
                     {"acme/payments": {"mirror": str(self.tmp / "mirror"), "github": self.live_repo or "acme/payments"}},
            # The simulated GitHub needs no token, and applies a write as it receives it: a
            # lookup that finds nothing past the request timeout means it never took effect.
            **({"github": ({"token": "keyring:agentos-github-test-token"} if self.live_repo
                           else {"api_url": self.github_url, "request_timeout": "2s", "settle": "3s"})}
               if self.fixture else {}),
            **({"git_tools": {"sandbox": [str(TOOL_LAUNCHER[SANDBOX])]}} if SANDBOX in TOOL_LAUNCHER else {}),
        }
        if worker:
            if self.workflow_queue == DEFAULT_QUEUE:
                self.workflow_queue = f"workflows-{os.urandom(4).hex()}"
            config["task_queues"] = {"workflows": self.workflow_queue}
            config["worker"] = self.worker_config(workflows)
        config.update(extra or {})
        if worker:  # the queue agentd serves, whatever extra says
            self.workflow_queue = config.get("task_queues", {}).get("workflows", DEFAULT_QUEUE)
        (d / "agentd.yaml").write_text(json.dumps(config))
        self._agentd_args = [BIN / "agentd", "--config", d / "agentd.yaml"]
        if SANDBOX == "none" or insecure_dev:
            self._agentd_args.append("--insecure-dev")  # with none, the git tool server runs unsandboxed too
        if not worker:
            self._agentd_args.append("--no-worker")  # the test starts its own
        self.supervised, self._agentd_terminal = worker, terminal
        if test_tools:
            self._agentd_args.append("--test-tools")
        self._fault_script = None
        if faults or run_id_delay or hold_doorbells or hold_worker:
            # A rule's "attempt" is Temporal's: it counts attempts the gateway refused before
            # the rules were consulted. A run's opening call that reaches agentd before StartRun
            # has recorded the run ID waits for it (up to 5 s) rather than being refused, so it
            # keeps attempt 1 even with a Worker that is already polling.
            self._write_fault_script({"rules": faults or [], **({"run_id_delay": run_id_delay} if run_id_delay else {}),
                                      **({"hold_doorbells_while_exists": str(hold_doorbells)} if hold_doorbells else {}),
                                      **({"hold_worker_while_exists": str(hold_worker)} if hold_worker else {})})
        return self._launch_agentd()

    def _write_fault_script(self, script: dict) -> None:
        path = self.tmp / "agentd" / "faults.json"
        path.write_text(json.dumps(script))
        if self._fault_script is None:
            self._agentd_args += ["--fault-script", path]
        self._fault_script = script

    def _launch_agentd(self) -> Proc:
        n = sum(p.name.startswith("agentd") for p in self.procs)
        name = "agentd" if n == 0 else f"agentd{n + 1}"
        p = Proc(name, self._agentd_args, self.tmp / f"{name}.log", terminal=self._agentd_terminal)
        self.procs.append(p)
        self._remember()
        self.agentd = p
        if WATCH.on_start is not None:
            WATCH.on_start(self)
        # A Worker of agentd's own has 60 s to say it is ready, before agentd does.
        wait_for(lambda: '"agentd ready"' in p.text() or not p.alive(), "agentd", timeout=90 if self.supervised else 30)
        assert p.alive(), p.text()
        return p

    def restart_agentd(self, kill: bool = False, faults: list[dict] | None = None, workflows: str | None = None) -> Proc:
        """Stop agentd (SIGTERM, or kill -9 with its whole process tree) and start it again on the
        same config, ledger and sockets. With `faults`, on these fault rules instead: rules count
        per agentd process, so a restart arms an "occurrence" rule again, unless faults leaves it
        out. With `workflows`, agentd's own Worker serves these instead (new code, shipped)."""
        assert self.agentd is not None
        if kill:
            self.agentd.kill()
        else:
            self.agentd.stop()
        if faults is not None:
            self._write_fault_script({**(self._fault_script or {}), "rules": faults})
        if workflows is not None:
            assert self.supervised, "workflows are for agentd's own Worker: start_agentd(worker=True)"
            path = self.tmp / "agentd" / "agentd.yaml"
            config = json.loads(path.read_text())
            config["worker"]["workflows"] = workflows
            path.write_text(json.dumps(config))
        return self._launch_agentd()

    def kill_agentd(self) -> None:
        """kill -9 agentd alone, as a crash ends it: what it started is left to end without it.
        restart_agentd() starts it again."""
        assert self.agentd is not None
        os.kill(self.agentd.popen.pid, signal.SIGKILL)
        self.agentd.popen.wait(timeout=10)

    def worker(self) -> SupervisedWorker:
        """The Worker agentd runs itself (start_agentd(worker=True))."""
        assert self.supervised, "start_agentd(worker=True) first"
        return SupervisedWorker(self)

    def worker_processes(self) -> list[int]:
        """Every Python Worker on this machine that polls this stack's own workflow queue (a stack
        whose agentd runs its Worker has one), whoever started it and wherever it runs now."""
        out = subprocess.run(["ps", "-A", "-ww", "-o", "pid=,command="], capture_output=True, text=True, check=True,
                             start_new_session=True).stdout
        found = []
        for line in out.splitlines():
            pid, _, command = line.strip().partition(" ")
            argv = command.split()
            if argv and Path(argv[0]).name.lower().startswith("python") and "agentos_worker" in argv \
                    and self.workflow_queue in argv:
                found.append(int(pid))
        return found

    def worker_command(self, workflows: str | None) -> list[str]:
        cmd = [sys.executable, "-m", "agentos_worker"]
        if workflows:
            cmd += ["--workflows", workflows]
        if SANDBOX == "none":
            return [*cmd, "--endpoint", f"127.0.0.1:{self.runtime_port}"]
        if SANDBOX == "bwrap":
            return [REPO / "sandbox/linux/run-worker.sh", self.runtime_socket, sys.prefix, REPO, "--",
                    *cmd, "--endpoint", "127.0.0.1:7233"]
        if SANDBOX == "srt":
            return [REPO / "sandbox/macos/run-worker.sh", str(self.runtime_port), sys.prefix, REPO, "--",
                    *cmd, "--endpoint", f"127.0.0.1:{self.runtime_port}", "--http-connect-proxy", "env"]
        raise ValueError(f"unknown AGENTOS_SANDBOX {SANDBOX!r}")

    def sandbox_env(self) -> dict[str, str]:
        # In production agentd's data lives under the home directory, which the macOS sandbox
        # cannot read. Test data lives elsewhere, so keep it out of reach explicitly. On Linux the
        # launcher holds the Worker to agentd's default limits, as agentd's own launches do.
        denied = [str(p) for p in [self.tmp, *self.deny_read]]
        return {**os.environ, "AGENTOS_SANDBOX_DENY_READ": json.dumps(denied), **launcher_limits("worker")}

    def start_worker(self, workflows: str | None = None, args: tuple[str, ...] = (), stdin: int | None = None,
                     ready: bool = True) -> Worker:
        """A Worker run by hand (agentd with --no-worker), with args after its own, and stdin; once it
        is ready, unless not ready."""
        n = len(self.workers) + 1
        w = Worker(f"worker{n}", [*self.worker_command(workflows), *args], self.tmp / f"worker{n}.log",
                   env=self.sandbox_env(), cwd=REPO, stdin=stdin)
        self.procs.append(w)
        self._remember()
        self.workers.append(w)
        if ready:
            wait_for(lambda: "worker ready" in w.text() or not w.alive(), f"worker{n}")
            assert w.alive(), w.text()
        return w

    def _remember(self) -> None:
        """The stack's processes, each the leader of its own process group, and when each started:
        what to stop when whatever started them was killed (pids, in the stack's directory; gone
        once the stack has stopped)."""
        (self.tmp / "pids").write_text(json.dumps([[p.name, p.popen.pid, started_at(p.popen.pid)]
                                                   for p in self.procs]))

    def close(self) -> None:
        if WATCH.on_close is not None:
            WATCH.on_close(self)  # while its agentd still runs
        errors = []
        for p in reversed(self.procs):
            try:
                p.stop()
            except Exception as e:  # noqa: BLE001 - stop the others first
                errors.append(e)
        if self.supervised:  # a Worker whose agentd was killed, and whose launcher did not end it
            for pid in self.worker_processes():
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        shutil.rmtree(self.sock_dir, ignore_errors=True)
        if errors:
            raise errors[0]
        (self.tmp / "pids").unlink(missing_ok=True)

    def logs(self) -> str:
        return "\n".join(f"===== {p.name} ({p.log})\n{p.text()[-6000:]}" for p in self.procs)

    # --- agentctl ----------------------------------------------------------------------------

    def agentctl(self, *args: str) -> dict:
        r = self.agentctl_run(*args)
        if r.returncode != 0:
            raise AssertionError(f"agentctl {' '.join(args)} failed: {r.stderr.strip()}")
        return json.loads(r.stdout)

    def agentctl_run(self, *args: str) -> subprocess.CompletedProcess:
        """agentctl, with JSON output, as it ended: for the commands expected to fail."""
        return subprocess.run(self.agentctl_args(*args), capture_output=True, text=True, timeout=60)

    def agentctl_args(self, *args: str) -> list:
        return [BIN / "agentctl", "--socket", self.control_socket, "-o", "json", *args]

    def audit_verify(self, *args: str) -> subprocess.CompletedProcess:
        """agentctl audit verify against this stack's agentd, as a person reads it, as it ended."""
        return subprocess.run([BIN / "agentctl", "--socket", self.control_socket, "audit", "verify", *args],
                              capture_output=True, text=True, timeout=360)

    def approvals(self) -> list[dict]:
        """What waits for approval, as agentctl approvals list shows it."""
        return self.agentctl("approvals", "list")["items"]

    def approval(self, approval_id: str) -> dict:
        """One approval card (agentctl approvals show), with its JSON fields decoded."""
        a = self.agentctl("approvals", "show", approval_id)
        return {**a, "payload": json.loads(a["payload_json"]), "context": json.loads(a["context_json"])}

    def decide(self, approval: dict, decision: str, *extra: str) -> dict:
        """A reviewer's decision (approve, edit or reject) on an approval whose payload they saw."""
        return self.agentctl("approvals", decision, approval["id"] if "id" in approval else approval["approval_id"],
                             "--payload-sha256", approval["payload_sha256"], *extra)

    def wait_approval(self, run: dict, count: int = 1, timeout: float = 60) -> dict:
        """The run's newest approval waiting for a decision, once it has `count` approvals in all."""
        def check() -> dict | None:
            rows = self.ledger("SELECT approval_id FROM approvals WHERE workflow_id = ? ORDER BY created_at", run["run_id"])
            if len(rows) < count:
                return None
            mine = [a for a in self.approvals() if a["id"] == rows[-1]["approval_id"]]
            return mine[0] if mine else None
        return wait_for(check, f"approval {count} of run {run['run_id']}", timeout=timeout, interval=0.5)

    def doorbells(self, run: dict) -> list[sqlite3.Row]:
        """The run's doorbells in agentd's outbox."""
        return self.ledger("SELECT * FROM outbox WHERE workflow_id = ? ORDER BY seq", run["run_id"])

    def start_run(self, budget_micros: int | None = None, dry_run: bool = False, base: str = "main") -> dict:
        budget = ["--budget-micros", str(budget_micros)] if budget_micros else []
        dry = ["--dry-run"] if dry_run else []
        return self.agentctl("runs", "start", "security-audit", "--repo", "acme/payments",
                             "--commit", self.commit[:7], "--base", base, *budget, *dry)

    def get_run(self, run: dict) -> dict:
        return self.agentctl("runs", "get", run["run_id"])

    def wait_state(self, run: dict, states: set[str], timeout: float = 60) -> dict:
        def check() -> dict | None:
            r = self.get_run(run)
            return r if r["state"] in states else None
        return wait_for(check, f"run {run['run_id']} to reach {sorted(states)}", timeout=timeout, interval=0.5)

    # --- Temporal (the harness is trusted and talks to Temporal directly) --------------------

    def _temporal(self, fn: Callable[[Client], Any]) -> Any:
        async def go() -> Any:
            return await fn(await Client.connect(self.temporal_address, tls=self.temporal_tls or False))
        return asyncio.run(go())

    def history(self, run: dict) -> list[HistoryEvent]:
        async def fetch(c: Client) -> list[HistoryEvent]:
            h = await c.get_workflow_handle(run["run_id"], run_id=run["temporal_run_id"]).fetch_history()
            return list(h.events)
        return self._temporal(fetch)

    def history_json(self, run: dict) -> str:
        async def fetch(c: Client) -> str:
            h = await c.get_workflow_handle(run["run_id"], run_id=run["temporal_run_id"]).fetch_history()
            return h.to_json()
        return self._temporal(fetch)

    def start_workflow_directly(self, workflow_id: str, workflow_input: dict) -> str:
        """Start a security-audit execution without agentd, as anything with direct access to
        Temporal could. Returns its run ID."""
        async def start(c: Client) -> str:
            h = await c.start_workflow("security-audit", workflow_input, id=workflow_id,
                                       task_queue=self.workflow_queue)
            return h.first_execution_run_id or h.result_run_id or ""
        return self._temporal(start)

    def execution_times(self, run: dict) -> tuple[float, float | None]:
        """When Temporal started the run's execution and when it closed (None while open), in
        seconds since the epoch."""
        async def describe(c: Client) -> tuple[float, float | None]:
            d = await c.get_workflow_handle(run["run_id"], run_id=run["temporal_run_id"]).describe()
            return d.start_time.timestamp(), d.close_time.timestamp() if d.close_time else None
        return self._temporal(describe)

    def execution_closed(self, workflow_id: str, run_id: str) -> bool:
        async def describe(c: Client) -> bool:
            d = await c.get_workflow_handle(workflow_id, run_id=run_id).describe()
            return d.close_time is not None
        return self._temporal(describe)

    def signal(self, run: dict, name: str, arg: Any = None) -> None:
        """Signal the run, with arg if one is given."""
        def send(c: Client) -> Any:
            handle = c.get_workflow_handle(run["run_id"], run_id=run["temporal_run_id"])
            return handle.signal(name) if arg is None else handle.signal(name, arg)
        self._temporal(send)

    # --- agentd's ledger and logs ------------------------------------------------------------

    def ledger(self, sql: str, *params: Any) -> list[sqlite3.Row]:
        db = sqlite3.connect((self.data_dir / "ledger.db").absolute().as_uri() + "?mode=ro", uri=True)
        db.row_factory = sqlite3.Row
        try:
            return db.execute(sql, params).fetchall()
        finally:
            db.close()

    def model_calls(self, run: dict) -> list[sqlite3.Row]:
        return self.ledger("SELECT * FROM model_calls WHERE workflow_id = ? ORDER BY started_at", run["run_id"])

    def budget_entries(self, run: dict) -> list[sqlite3.Row]:
        return self.ledger("SELECT * FROM budget_entries WHERE workflow_id = ? ORDER BY entry_id", run["run_id"])

    def effects(self, run: dict) -> list[sqlite3.Row]:
        return self.ledger("SELECT * FROM effects WHERE workflow_id = ? ORDER BY created_at", run["run_id"])

    def external(self, run: dict, tool: str) -> list[dict]:
        """The external writes of a run's tool that took effect, as the outside world recorded
        them: the simulated GitHub's log, the real GitHub in live mode, or the test tool's own."""
        if self.live_repo and tool.startswith("github."):
            return self.live_github(run, tool)
        if tool == "test.unsafe_write":
            path = self.data_dir / "test-external.jsonl"
            lines = path.read_text().splitlines() if path.exists() else []
            return [e for e in map(json.loads, lines) if e["workflow_id"] == run["run_id"]]
        if tool == "github.open_draft_pr":
            # The branch name carries the run's first 8 characters.
            return [e for e in self.github_requests("create_pull")
                    if e["status"] == 201 and f"-{run['run_id'][:8]}-" in e["head"]]
        if tool == "github.comment_commit":
            mine = {e["effect_id"] for e in self.effects(run)}
            return [e for e in self.github_requests("create_comment") if e["status"] == 201 and e["effect"] in mine]
        raise ValueError(f"no external record for {tool}")

    def live_github(self, run: dict, tool: str) -> list[dict]:
        """The run's pull requests (by its head branch) or report comments (by their markers) on
        the real test repository."""
        def api(path: str) -> list[dict]:
            out = subprocess.run(["gh", "api", "--paginate", "--slurp", path], check=True, capture_output=True,
                                 text=True).stdout
            return [item for page in json.loads(out) for item in page]
        if tool == "github.open_draft_pr":
            return [{"number": pr["number"], "url": pr["html_url"], "head": pr["head"]["ref"]}
                    for pr in api(f"repos/{self.live_repo}/pulls?state=all&per_page=100")
                    if f"-{run['run_id'][:8]}-" in pr["head"]["ref"]]
        markers = {f"<!-- agent-effect:{e['effect_id']} -->" for e in self.effects(run)}
        return [{"id": c["id"], "url": c["html_url"]} for c in api(f"repos/{self.live_repo}/commits/{self.commit}/comments")
                if c["body"].rstrip().splitlines()[-1] in markers]

    def effect(self, run: dict, tool: str) -> sqlite3.Row:
        [e] = [e for e in self.effects(run) if e["tool"] == tool]
        return e

    def provider_requests(self, run: dict) -> int:
        """How many requests reached the model provider for the run: the fake provider's own
        record, or for a real provider the ledger's reservations that were not released."""
        if not REAL_MODEL:
            return len(self.provider_calls(run))
        entries = self.budget_entries(run)
        released = {e["request_id"] for e in entries if e["kind"] == "RELEASE"}
        return sum(1 for e in entries if e["kind"] == "RESERVE" and e["request_id"] not in released)

    def model_answered(self, run: dict) -> bool:
        """Whether the provider has answered a request of the run, recorded or not."""
        return any(e["msg"] == "model answered" and e.get("workflow_id") == run["run_id"] for e in self.agentd_log())

    def model_value(self, run: dict) -> dict:
        """What the workflow got from the run's accepted model answer: the one answer with
        status ok, whatever refusals (a fallback's) or broken answers came before it."""
        answers = []
        for r in self.model_calls(run):
            if r["response_blob"]:
                [blob] = self.ledger("SELECT bytes FROM blobs WHERE sha256 = ?", r["response_blob"].removeprefix("sha256:"))
                answers.append(json.loads(blob["bytes"]))
        [accepted] = [a for a in answers if a["status"] == "ok"]
        return accepted["value"]

    def provider_calls(self, run: dict) -> list[dict]:
        """The fake model provider's own record of the requests it received for the run."""
        path = self.data_dir / "fake-provider.jsonl"
        lines = path.read_text().splitlines() if path.exists() else []
        return [e for e in map(json.loads, lines) if e["workflow_id"] == run["run_id"]]

    def agentd_log(self) -> list[dict]:
        """agentd's JSON log lines, across restarts."""
        out = []
        text = "".join(p.text() for p in self.procs if p.name.startswith("agentd"))
        for line in text.splitlines():
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                pass
        return out


# --- reading histories -------------------------------------------------------------------------

def gateway_calls(events: list[HistoryEvent]) -> list[dict]:
    """The gateway calls a history scheduled, in order, with their outcome events."""
    calls, by_scheduled = [], {}
    for e in events:
        if e.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
            a = e.activity_task_scheduled_event_attributes
            call = json.loads(a.input.payloads[0].data)
            c = {"scheduled": e, "activity_id": a.activity_id, "kind": call["kind"], "name": call["name"],
                 "args": json.loads(call.get("argsJson") or "{}"), "started": None, "completed": None}
            by_scheduled[e.event_id] = c
            calls.append(c)
        elif e.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED:
            by_scheduled[e.activity_task_started_event_attributes.scheduled_event_id]["started"] = e
        elif e.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
            by_scheduled[e.activity_task_completed_event_attributes.scheduled_event_id]["completed"] = e
    return calls


def event_types(events: list[HistoryEvent]) -> list[str]:
    return [EventType.Name(e.event_type).removeprefix("EVENT_TYPE_") for e in events]


def is_linux() -> bool:
    return platform.system() == "Linux"
