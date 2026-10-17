"""Black-box harness: runs Temporal's dev server, agentd and Workers as separate processes
and drives them the way an operator would, through agentctl. Tests look at the results
through the Temporal API, agentd's ledger and its log, and the stub tools' external log.
Nothing here depends on how agentd is implemented.

Environment:
  AGENTOS_SANDBOX  none (default), bwrap (Linux) or srt (macOS): how Workers are launched
  AGENTOS_BIN      directory with the agentd and agentctl binaries (default: bin/)
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

# Tests run without approvals, like M3's test policy; M1 has no approvals table yet.
TEST_POLICY = """
repos: [acme/payments]
tools:
  git.show_diff:         {decision: allow, effect: none}
  git.apply_check:       {decision: allow, effect: none}
  github.open_draft_pr:  {decision: allow, effect: lookup}
  github.comment_commit: {decision: allow, effect: lookup}
models:
  audit_v1: {allowed_providers: [fake, anthropic]}
"""

AUDIT_SCHEMA = REPO / "config/schemas/audit_report.json"

# AGENTOS_MODEL=claude sends the model task to Claude: the real prompt, schema, fallback and
# prices, with the key from the keychain. It costs money; the tests that make sense with a
# real model are T1's, which check that crashes never repeat or lose a model call.
MODEL = os.environ.get("AGENTOS_MODEL", "fake")
REAL_MODEL = MODEL != "fake"
# How long a model call may take: a real one thinks for a while.
MODEL_TIMEOUT = 240 if REAL_MODEL else 30

if MODEL == "fake":
    TEST_MODELS = f"""
providers:
  fake: {{adapter: fake}}
tasks:
  audit_v1: {{model: fake/audit-fixture, max_tokens: 4096, output_schema: {AUDIT_SCHEMA}, blobs: [patch]}}
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
    effort: high
    prompt: {REPO / "config/prompts/audit_v1/claude.md"}
    output_schema: {AUDIT_SCHEMA}
    blobs: [patch]
    fallback: [anthropic/claude-opus-4-8]
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
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def descendants(pid: int) -> list[int]:
    out = subprocess.run(["ps", "-A", "-o", "pid=,ppid="], capture_output=True, text=True, check=True).stdout
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


def command_of(pid: int) -> str:
    return subprocess.run(["ps", "-o", "command=", "-p", str(pid)], capture_output=True, text=True).stdout


class Proc:
    """A child process with its output in a log file."""

    def __init__(self, name: str, args: list[str], log: Path, env: dict[str, str] | None = None,
                 cwd: Path | None = None) -> None:
        self.name, self.log = name, log
        self._out = open(log, "w")
        self.popen = subprocess.Popen([str(a) for a in args], stdout=self._out, stderr=subprocess.STDOUT,
                                      env=env, cwd=cwd, start_new_session=True)

    def text(self) -> str:
        return self.log.read_text(errors="replace")

    def alive(self) -> bool:
        return self.popen.poll() is None

    def signal_all(self, sig: int) -> None:
        for pid in [*descendants(self.popen.pid), self.popen.pid]:
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


def make_mirror(path: Path) -> str:
    """A one-commit git repository standing in for the read-only mirror of acme/payments."""
    path.mkdir(parents=True)
    env = {**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example.com",
           "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.com"}
    run = lambda *a: subprocess.run(["git", "-C", str(path), *a], env=env, check=True, capture_output=True, text=True)
    run("init", "-q", "-b", "main")
    (path / "app.py").write_text("print('hello')\n")
    run("add", ".")
    run("commit", "-q", "-m", "init")
    return run("rev-parse", "HEAD").stdout.strip()


class Stack:
    """One Temporal dev server, one agentd and its Workers, isolated in a temporary directory."""

    # Set by subclasses that run Temporal with mTLS: the harness's client certificate and
    # the temporal.tls section of agentd.yaml.
    temporal_tls: TLSConfig | None = None
    agentd_tls: dict | None = None

    def __init__(self, tmp: Path) -> None:
        self.tmp = tmp
        # Unix socket paths must stay under 104 bytes (macOS), so they live in a short /tmp dir.
        self.sock_dir = Path(tempfile.mkdtemp(prefix="aos-", dir="/tmp"))
        self.procs: list[Proc] = []
        self.workers: list[Worker] = []
        self.agentd: Proc | None = None
        self.commit = make_mirror(tmp / "mirror")
        self.data_dir = tmp / "agentd" / "data"
        # Separate directories: the Linux sandbox mounts the Runtime socket's directory.
        self.control_socket = self.sock_dir / "control" / "control.sock"
        self.runtime_port = free_port()
        self.runtime_socket = self.sock_dir / "runtime" / "runtime.sock"
        self.temporal_address = f"127.0.0.1:{free_port()}"
        self._start_temporal()

    # --- processes ---------------------------------------------------------------------------

    def _start_temporal(self) -> None:
        port = self.temporal_address.rsplit(":", 1)[1]
        p = Proc("temporal", ["temporal", "server", "start-dev", "--ip", "127.0.0.1", "--port", port,
                              "--ui-port", str(free_port()), "--headless", "--log-level", "warn",
                              "--search-attribute", "AgentState=Keyword"],
                 self.tmp / "temporal.log")
        self.procs.append(p)
        wait_for(lambda: self._temporal_ready() or not p.alive(), "the Temporal dev server", timeout=60)
        assert p.alive(), p.text()

    def _temporal_ready(self) -> bool:
        try:
            asyncio.run(Client.connect(self.temporal_address, tls=self.temporal_tls or False))
            return True
        except Exception:
            return False

    def runtime_listen(self) -> str:
        if SANDBOX == "bwrap":
            return f"unix://{self.runtime_socket}"
        return f"tcp://127.0.0.1:{self.runtime_port}"

    def start_agentd(self, faults: list[dict] | None = None, policy: str = TEST_POLICY,
                     models: str = TEST_MODELS, prices: str = TEST_PRICES, extra: dict | None = None) -> Proc:
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
            "repos": {"acme/payments": str(self.tmp / "mirror")},
            **(extra or {}),
        }
        (d / "agentd.yaml").write_text(json.dumps(config))
        self._agentd_args = [BIN / "agentd", "--config", d / "agentd.yaml"]
        if faults:
            (d / "faults.json").write_text(json.dumps({"rules": faults}))
            self._agentd_args += ["--fault-script", d / "faults.json"]
        return self._launch_agentd()

    def _launch_agentd(self) -> Proc:
        n = sum(p.name.startswith("agentd") for p in self.procs)
        name = "agentd" if n == 0 else f"agentd{n + 1}"
        p = Proc(name, self._agentd_args, self.tmp / f"{name}.log")
        self.procs.append(p)
        self.agentd = p
        wait_for(lambda: '"agentd ready"' in p.text() or not p.alive(), "agentd")
        assert p.alive(), p.text()
        return p

    def restart_agentd(self, kill: bool = False) -> Proc:
        """Stop agentd (SIGTERM, or kill -9) and start it again on the same config, ledger and sockets."""
        assert self.agentd is not None
        if kill:
            self.agentd.kill()
        else:
            self.agentd.stop()
        return self._launch_agentd()

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
        # cannot read. Test data lives elsewhere, so keep it out of reach explicitly.
        return {**os.environ, "AGENTOS_SANDBOX_DENY_READ": str(self.tmp)}

    def start_worker(self, workflows: str | None = None) -> Worker:
        n = len(self.workers) + 1
        w = Worker(f"worker{n}", self.worker_command(workflows), self.tmp / f"worker{n}.log",
                   env=self.sandbox_env(), cwd=REPO)
        self.procs.append(w)
        self.workers.append(w)
        wait_for(lambda: "worker ready" in w.text() or not w.alive(), f"worker{n}")
        assert w.alive(), w.text()
        return w

    def close(self) -> None:
        for p in reversed(self.procs):
            p.stop()
        shutil.rmtree(self.sock_dir, ignore_errors=True)

    def logs(self) -> str:
        return "\n".join(f"===== {p.name} ({p.log})\n{p.text()[-6000:]}" for p in self.procs)

    # --- agentctl ----------------------------------------------------------------------------

    def agentctl(self, *args: str) -> dict:
        r = subprocess.run([BIN / "agentctl", "--socket", self.control_socket, "-o", "json", *args],
                           capture_output=True, text=True, timeout=60)
        if r.returncode != 0:
            raise AssertionError(f"agentctl {' '.join(args)} failed: {r.stderr.strip()}")
        return json.loads(r.stdout)

    def start_run(self, budget_micros: int | None = None) -> dict:
        budget = ["--budget-micros", str(budget_micros)] if budget_micros else []
        return self.agentctl("runs", "start", "security-audit", "--repo", "acme/payments",
                             "--commit", self.commit[:7], "--base", "main", *budget)

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
                                       task_queue="agentos-workflows")
            return h.first_execution_run_id or h.result_run_id or ""
        return self._temporal(start)

    def execution_closed(self, workflow_id: str, run_id: str) -> bool:
        async def describe(c: Client) -> bool:
            d = await c.get_workflow_handle(workflow_id, run_id=run_id).describe()
            return d.close_time is not None
        return self._temporal(describe)

    def signal(self, run: dict, name: str) -> None:
        self._temporal(lambda c: c.get_workflow_handle(run["run_id"], run_id=run["temporal_run_id"]).signal(name))

    # --- agentd's ledger and logs ------------------------------------------------------------

    def ledger(self, sql: str, *params: Any) -> list[sqlite3.Row]:
        db = sqlite3.connect(f"file:{self.data_dir / 'ledger.db'}?mode=ro", uri=True)
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

    def external(self, run: dict, tool: str | None = None) -> list[dict]:
        """What the stub tools were asked to do: the stand-in for GitHub and git."""
        path = self.data_dir / "stub-external.jsonl"
        lines = path.read_text().splitlines() if path.exists() else []
        return [e for e in map(json.loads, lines)
                if e["workflow_id"] == run["run_id"] and (tool is None or e["tool"] == tool)]

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
        """What the workflow got from the run's accepted model answer."""
        [row] = [r for r in self.model_calls(run) if r["response_blob"] and r["stop_reason"] != "invalid_output"]
        [blob] = self.ledger("SELECT bytes FROM blobs WHERE sha256 = ?", row["response_blob"].removeprefix("sha256:"))
        return json.loads(blob["bytes"])["value"]

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
