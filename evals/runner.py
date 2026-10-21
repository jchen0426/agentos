"""Runs the case set through the runtime (design doc, section 11): the same Temporal, agentd and
sandboxed Worker as any run, brought up by the black-box harness (tests/harness.py), every run
started with agentctl in dry-run. Nothing is written anywhere: agentd has no GitHub section,
so the GitHub tools only dry-run, and a dry run's writes ask for no approval.

Each profile is one models.yaml, with a stack of its own (a Temporal database, agentd's data)
in the state directory: a run can only ever be answered by its own profile's provider, however
a runner is stopped and started again. The runner runs every case `runs` times under each
profile, one rep of every case before the next, and judges each run by its outcome and the
ledger (evals/score.py). A journal in the state directory records each run as it starts and as
it ends, so a runner started again on it picks up the runs in flight and skips the ones done.
The first run of a profile goes alone: if the model gave it no answer (evals/score.py's void),
the runner stops rather than spend the rest. So does a dead Temporal, agentd or Worker. After any
void run, runs go one at a time again until one gets an answer, and VOID_STREAK void runs in a
row stop the runner (a provider that went away: each of its requests may cost the whole
reservation). Void runs run again only when asked (--retry-void), after the runs never run; one
that is void again does not stop the runner as a first run would.

Money: a paid profile needs a cap on what the state directory's runs spend, every runner on it
together; a new state directory is a new evaluation, with a cap of its own. A run starts only
if what the ledgers say the state directory's runs have spent (settled, or reserved and not yet
settled), plus the budgets of the paid runs not yet ended, plus its own budget stay within the
cap. agentd never lets a run spend past its budget, so the runs never spend past the cap; if
one ever did (a reservation bound that is wrong, which agentd logs as an error), no paid run
starts from then on, and every paid run still in flight is cancelled, this runner's and those
an earlier one left. A run the ledger has and the journal does not (a runner stopped between
starting it and writing it down) is cancelled before the Worker comes up: nobody would watch
it. The cap counts the price table's prices, not the provider's bill.

One runner at a time uses a state directory, and its lock stays held while the Temporal a runner
started still runs: a runner killed with SIGKILL leaves the directory locked until it is
stopped, rather than to a second Temporal on the same database. Each stack lists its processes
in its directory (pids), and the refusal names the ones still running. A signal while a stack
starts or stops waits until it has (tests/harness.py's signals_deferred).

On macOS the git tool's sandbox denies agentd's directories, the home directory and the
temporary directories, and allows the rest: the state directory and the cache (deny_read)
must be in one of those, or the tool could read the journal and the other profiles' ledgers.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import pwd
import re
import sqlite3
import subprocess
import time
from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from evals.cases import Case, CaseSet
from evals.mirrors import inside_repo
from evals.score import detected, false_positive, void, void_reason
from tests.harness import BIN, SANDBOX, TERMINAL, Stack, signals_deferred, wait_for

REPO = Path(__file__).resolve().parents[1]
BUDGET = 1_500_000  # micros a run may spend: above the gateway's worst-case reservation (section 9)
TASK = "security-audit"
_NAME = re.compile(r"[a-z0-9][a-z0-9-]{0,31}")
ENDED = TERMINAL | {"UNKNOWN"}
VOID_STREAK = 3  # runs in a row the model gives no answer, after the first, that stop the runner


@dataclass(frozen=True)
class Profile:
    name: str  # also its directory in the state directory
    models: str  # models.yaml, as agentd reads it (JSON is YAML)
    prices: str  # prices.yaml
    expect: str  # the "<provider>/<model>" the ledger should record for its runs
    paid: bool  # its requests cost money: running it needs a cap
    real: bool = False  # a real provider: the Worker must run in its sandbox
    concurrency: int = 4
    timeout: float = 1800  # seconds a run may take before the runner cancels it


def _task(model: str, **extra) -> dict:
    # config/models.yaml's audit_v1, with this model.
    return {"model": model, "max_tokens": 16000, "effort": "high",
            "prompt": str(REPO / "config/prompts/audit_v1/claude.md"),
            "output_schema": str(REPO / "config/schemas/audit_report.json"), "blobs": ["patch"], **extra}


def _models(provider: str, settings: dict, task: dict) -> str:
    # Only the profile's provider: agentd reads every listed provider's key when it starts.
    return json.dumps({"providers": {provider: settings}, "tasks": {"audit_v1": task}})


PRICES = (REPO / "config/prices.yaml").read_text()
# config/models.yaml's providers, one at a time.
PROFILES = {
    "claude": Profile(
        "claude", _models("anthropic", {"adapter": "anthropic", "key": "keyring:agentos-anthropic-api-key",
                                        "requests_per_minute": 20},
                          _task("anthropic/claude-opus-5-5", fallback=["anthropic/claude-opus-4-8"])),
        PRICES, "anthropic/claude-opus-5-5", paid=True, real=True),
    "openai": Profile(
        "openai", _models("openai", {"adapter": "openai_compat", "api": "responses",
                                     "base_url": "https://api.openai.com/v1", "key": "keyring:agentos-openai-api-key",
                                     "requests_per_minute": 20, "efforts": ["low", "medium", "high", "xhigh", "max"]},
                          _task("openai/gpt-6.1-sol")),
        PRICES, "openai/gpt-6.1-sol", paid=True, real=True),
    # Ollama answers one request at a time; a queued one would eat into its 10-minute attempt.
    "local": Profile(
        "local", _models("local", {"adapter": "openai_compat", "api": "chat", "base_url": "http://127.0.0.1:11434/v1",
                                   "context_tokens": 65536, "efforts": ["low", "medium", "high"]},
                         _task("local/gpt-oss:20b")),
        PRICES, "local/gpt-oss:20b", paid=False, real=True, concurrency=1, timeout=3600),
}


def policy(repo_ids: list[str]) -> str:
    """config/policy.yaml, for the case set's repositories instead of acme/payments."""
    text, n = re.subn(r"^repos: \[[^\]]*\]", "repos: " + json.dumps(sorted(repo_ids)),
                      (REPO / "config/policy.yaml").read_text(), flags=re.M)
    assert n == 1, "config/policy.yaml has no repos line to replace"
    return text


def repo_id(project: str) -> str:
    return f"eval/{project}"


class Journal:
    """One JSON line per event: a run started ("start"), a run ended ("result"). A line a crash
    cut short is skipped: a lost start leaves a run the journal does not know, which the next
    runner cancels, its spending counted from the ledger; a lost result leaves the run to be
    collected again."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def entries(self) -> list[dict]:
        if not self.path.exists():
            return []
        out = []
        for line in self.path.read_text(errors="replace").splitlines():
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(entry, dict) and entry.get("event") in ("start", "result"):
                out.append(entry)
        return out

    def append(self, entry: dict) -> None:
        with open(self.path, "ab+") as f:  # appends go to the end, wherever it reads
            end, cut = f.seek(0, 2), False
            if end:
                f.seek(end - 1)
                cut = f.read(1) != b"\n"
            f.write((b"\n" if cut else b"") + json.dumps(entry, sort_keys=True, allow_nan=False).encode() + b"\n")
            f.flush()
            os.fsync(f.fileno())

    def _results(self) -> tuple[list[dict], list[dict]]:
        latest: dict = {}
        replaced = []
        for e in self.entries():
            if e["event"] == "result":
                key = (e["profile"], e["case"], e["rep"])
                if key in latest:
                    replaced.append(latest[key])
                latest[key] = e
        return list(latest.values()), replaced

    def results(self) -> list[dict]:
        """Each (profile, case, rep)'s latest result."""
        return self._results()[0]

    def replaced(self) -> list[dict]:
        """The results a later one of the same (profile, case, rep) replaced: void runs run again."""
        return self._results()[1]

    def unfinished(self) -> list[dict]:
        ended = {e["run_id"] for e in self.entries() if e["event"] == "result"}
        return [e for e in self.entries() if e["event"] == "start" and e["run_id"] not in ended]


@dataclass
class _Run:
    """What one runner shares across its profiles."""
    cs: CaseSet
    journal: Journal
    state: Path
    repos: dict
    deny_read: list[Path]
    budget_micros: int
    cap_micros: int | None
    lock: int  # the state directory's lock, which the stacks' Temporal holds too
    agentos: dict
    log: object


def run(cs: CaseSet, profiles: list[Profile], mirrors: dict[str, Path], state: Path, runs: int = 3,
        budget_micros: int = BUDGET, cap_micros: int | None = None, cases: list[Case] | None = None,
        retry_void: bool = False, deny_read: Sequence[Path] = (), log=print) -> list[dict]:
    """Runs each of `cases` (all of the set's, by default) `runs` times under each profile, and
    returns the journal's latest results: these runs' and any an earlier runner on `state`
    finished. `mirrors` names every project's mirror, whatever cases this runner runs: runs in
    flight from an earlier runner may audit others. With retry_void, void runs run again. The
    sandboxed Worker reads none of the state directory, the mirrors and `deny_read`."""
    if any(p.paid for p in profiles) and cap_micros is None:
        raise ValueError("a paid profile needs a cap on what the runs may spend")
    if budget_micros <= 0:  # agentd takes 0 for its default budget, which the cap would not see
        raise ValueError("each run needs a budget above 0")
    if any(p.real for p in profiles) and SANDBOX == "none":
        raise ValueError("real models run with the Worker in a sandbox: set AGENTOS_SANDBOX")
    if any(not _NAME.fullmatch(p.name) for p in profiles) or len({p.name for p in profiles}) != len(profiles):
        raise ValueError("profile names are distinct, lowercase letters, digits and dashes")
    cases = list({c.id: c for c in (cs.cases if cases is None else cases)}.values())  # each case once
    missing = [c.id for c in cases if not c.commit]
    if missing:
        raise ValueError(f"cases without a commit ID (build their mirrors first): {missing}")
    state = state.resolve()
    if inside_repo(state):
        raise ValueError(f"the state directory {state} must be outside the repository: the sandboxed Worker reads it")
    mirrors = {name: Path(path).resolve() for name, path in mirrors.items()}
    hidden = [state, *mirrors.values(), *(Path(p).resolve() for p in deny_read)]
    if SANDBOX == "srt":
        # Mirrors are denied by agentd itself; the rest only where the tool's sandbox denies anyway:
        # sandbox/macos/run-tool.sh's list, with the home directory from the user database, not $HOME.
        denied = [Path(pwd.getpwuid(os.getuid()).pw_dir).resolve()]
        denied += [Path(d) for d in ("/private/tmp", "/private/var/tmp", "/private/var/folders")]
        exposed = [p for p in [state, *(Path(d).resolve() for d in deny_read)]
                   if not any(p.is_relative_to(d) for d in denied)]
        if exposed:
            raise ValueError(f"{exposed}: on macOS the state directory and the cache go under the home directory "
                             "or a temporary directory, which the git tool's sandbox cannot read")
    state.mkdir(parents=True, exist_ok=True)
    lock = os.open(state / "lock", os.O_RDONLY | os.O_CREAT, 0o600)
    try:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            left = "; ".join(f"{name} (process group {pid})" for name, pid in _left_running(state))
            raise RuntimeError(f"another runner is using {state}, or what is left of one, which holds it until "
                               f"its Temporal stops: {left or 'lsof ' + str(state / 'lock')}; stop them with "
                               "kill -TERM -<group> (agentd may take a minute; -KILL does not wait)") from None
        journal = Journal(state / "journal.jsonl")
        gone = sorted(p for p in {e["profile"] for e in journal.entries()} if not _ledger(state, p).exists())
        if gone:
            raise RuntimeError(f"the journal in {state} has runs of {gone}, whose ledgers are gone: what those runs "
                               "spent is unknown, and the cap cannot count it (a new state directory starts afresh)")
        r = _Run(cs, journal, state, {repo_id(n): {"mirror": str(p)} for n, p in mirrors.items()}, hidden,
                 budget_micros, cap_micros, lock, {"commit": _agentos(), "bin": _binaries()}, log)
        for profile in profiles:
            _run_profile(r, profile, cases, runs, retry_void)
        return journal.results()
    finally:
        os.close(lock)


def plan(journal: Journal, profile: str, cases: list[Case], runs: int, retry_void: bool) -> tuple[list, list[dict]]:
    """The profile's runs to start, in order, as (case, rep, run before), and its runs in flight
    to pick up. Rep by rep, the runs never run come first: the first run goes alone, and a void
    run may well be void again."""
    latest = {(e["case"], e["rep"]): e for e in journal.results() if e["profile"] == profile}
    resumed = [e for e in journal.unfinished() if e["profile"] == profile]
    flying = {(e["case"], e["rep"]) for e in resumed}
    fresh, again = [], []
    for rep in range(1, runs + 1):
        for c in cases:
            last = latest.get((c.id, rep))
            if (c.id, rep) in flying or (last is not None and not (retry_void and void(last))):
                continue
            (fresh if last is None else again).append((c, rep, last is not None))
    return fresh + again, resumed


def _run_profile(r: _Run, profile: Profile, cases: list[Case], runs: int, retry_void: bool) -> None:
    log, journal = r.log, r.journal
    planned, resumed = plan(journal, profile.name, cases, runs, retry_void)
    todo = deque(planned)
    if not todo and not resumed:
        return
    stack_dir = r.state / profile.name / "stack"
    stack_dir.mkdir(parents=True, exist_ok=True)
    stack = None
    try:
        with signals_deferred():  # a signal while it starts arrives once it is up, and it stops whole
            stack = Stack(stack_dir, temporal_db=True, fixture=False, hold_fds=(r.lock,))
        stack.deny_read = r.deny_read  # the journal, the mirrors and the cache, besides agentd's data
        stack.start_agentd(policy=policy(list(r.repos)), models=profile.models, prices=profile.prices, repos=r.repos)
        if not _ledger(r.state, profile.name).exists():
            raise RuntimeError(f"agentd is up, but its ledger is not at {_ledger(r.state, profile.name)}")
        # Before the Worker comes up: agentd refuses the calls of the runs it has cancelled.
        unknown = _cancel_unknown(stack, journal, log)
        if profile.paid and resumed:
            # Started under an earlier runner's cap: they stop if this one's cannot hold them.
            committed = sum(e["budget_micros"] for e in journal.unfinished() if e["paid"])
            why = ("overspent" if _overspent(r.state) else
                   "cap" if r.cap_micros is not None and _spent(r.state) + committed > r.cap_micros else None)
            if why:
                log(f"[{profile.name}] the {len(resumed)} paid runs left in flight stop: "
                    + ("a run spent past its budget" if why == "overspent" else "they could take the spending past the cap"))
                for e in resumed:
                    e["stopped"] = why
                    _cancel(stack, e)
        stack.start_worker()
        for gone in unknown:
            _ended(stack, gone)
        log(f"[{profile.name}] {len(todo)} runs to start ({sum(t[2] for t in todo)} again), "
            f"{len(resumed)} to pick up")
        watching = {e["run_id"]: {**e, "since": time.monotonic(), "resumed": True} for e in resumed}
        confirmed = False  # a run of this profile has got an answer from the model
        streak = 0  # runs this runner started that ended void, in a row: one at a time until one answers
        while todo or watching:
            _alive(stack)
            while todo and len(watching) < (profile.concurrency if confirmed and not streak else 1):
                if profile.paid and _overspent(r.state):
                    log(f"[{profile.name}] {len(todo)} runs not started: a run spent past its budget")
                    todo.clear()
                    break
                if profile.paid and r.cap_micros is not None:
                    committed = sum(e["budget_micros"] for e in journal.unfinished() if e["paid"])
                    if _spent(r.state) + committed + r.budget_micros > r.cap_micros:
                        if not watching:  # no run in flight will free any room
                            log(f"[{profile.name}] {len(todo)} runs not started: one more could take the "
                                f"spending past the cap of ${r.cap_micros / 1e6:.2f}")
                            todo.clear()
                        break
                c, rep, retry = todo.popleft()
                entry = _start(stack, r.cs, c, rep, profile, r.budget_micros, r.agentos)
                journal.append(entry)
                watching[entry["run_id"]] = {**entry, "since": time.monotonic(), "again": retry}
            for run_id, entry in list(watching.items()):
                record = _watch(stack, entry, profile)
                if record is None:
                    continue
                journal.append(record)
                del watching[run_id]
                log(_line(r.cs, record))
                stopping = [e for e in watching.values() if "stopped" not in e]
                if profile.paid and stopping and _overspent(r.state):
                    log(f"[{profile.name}] a run spent past its budget: no more paid runs, and the "
                        f"{len(stopping)} in flight stop")
                    for other in stopping:
                        other["stopped"] = "overspent"
                        _cancel(stack, other)
                if not void(record):
                    confirmed, streak = True, 0
                elif not record["resumed"]:
                    streak += 1
                    if not confirmed and not entry.get("again"):
                        raise RuntimeError(f"[{profile.name}] the first run ended without the model: "
                                           f"{void_reason(record)}")
                    if streak >= VOID_STREAK:
                        raise RuntimeError(f"[{profile.name}] {streak} runs in a row ended without the model, the last "
                                           f"{void_reason(record)}: stopping; the next runner picks up the runs in "
                                           "flight, and --retry-void runs the void ones again")
                    log(f"[{profile.name}] {record['case']} #{record['rep']} is void ({void_reason(record)}): "
                        "the next run goes alone")
            if watching:
                time.sleep(1)
    finally:
        if stack is not None:
            with signals_deferred():  # rather than leave half of it running
                stack.close()


def _left_running(state: Path) -> list[tuple[str, int]]:
    """The processes the state directory's stacks list that still run: what a runner killed with
    SIGKILL left behind (or a live runner's)."""
    out = []
    for path in sorted(state.glob("*/stack/pids")):
        try:
            procs = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        for name, pid in procs:
            try:
                os.kill(pid, 0)
            except (ProcessLookupError, PermissionError):
                continue
            out.append((name, pid))
    return out


def _alive(stack: Stack) -> None:
    """Stops the runner if a process of the stack died: its runs would only time out."""
    temporal = [p for p in stack.procs if p.name == "temporal"][-1:]
    dead = [p.name for p in [stack.agentd, *stack.workers, *temporal] if p is not None and not p.alive()]
    if dead:
        raise RuntimeError(f"{', '.join(dead)} died: stopping; the next runner picks up the runs in flight")


SPENT = ("SELECT (SELECT COALESCE(SUM(amount_micros), 0) FROM budget_entries WHERE kind = 'SETTLE') + "
         "(SELECT COALESCE(SUM(r.amount_micros), 0) FROM budget_entries r WHERE r.kind = 'RESERVE' AND NOT EXISTS "
         "(SELECT 1 FROM budget_entries c WHERE c.request_id = r.request_id AND c.kind IN ('SETTLE', 'RELEASE')))")
# The runs that settled and still hold more than their limit and its extensions: agentd's own
# totals (internal/ledger/queries' GetBudgetTotals), for every run.
OVERSPENT = """
SELECT r.workflow_id FROM runs r WHERE
  COALESCE((SELECT SUM(e.amount_micros) FROM budget_entries e WHERE e.workflow_id = r.workflow_id AND e.kind = 'SETTLE'), 0)
  + COALESCE((SELECT SUM(e.amount_micros) FROM budget_entries e WHERE e.workflow_id = r.workflow_id AND e.kind = 'RESERVE'
              AND NOT EXISTS (SELECT 1 FROM budget_entries c WHERE c.request_id = e.request_id
                              AND c.kind IN ('SETTLE', 'RELEASE'))), 0)
  > r.budget_limit_micros
    + COALESCE((SELECT SUM(e.amount_micros) FROM budget_entries e WHERE e.workflow_id = r.workflow_id AND e.kind = 'EXTEND'), 0)
"""


def _ledger(state: Path, profile: str) -> Path:
    return state / profile / "stack" / "agentd" / "data" / "ledger.db"


def _query(ledger: Path, sql: str) -> list[tuple]:
    db = sqlite3.connect(ledger.as_uri() + "?mode=ro", uri=True)
    try:
        return db.execute(sql).fetchall()
    finally:
        db.close()


def _ledgers(state: Path) -> list[Path]:
    return sorted(state.glob("*/stack/agentd/data/ledger.db"))


def _spent(state: Path) -> int:
    """What every run in the state directory's ledgers has spent, or may have: settled, and
    reserved but not yet settled or released."""
    return sum(_query(path, SPENT)[0][0] for path in _ledgers(state))


def _overspent(state: Path) -> bool:
    """Whether a run in the state directory's ledgers has spent past its budget, journal or not."""
    return any(_query(path, OVERSPENT) for path in _ledgers(state))


def spent_in(state: Path) -> int:
    """The state directory's spending, all of it: for the report."""
    return _spent(state.resolve())


def _cancel(stack: Stack, run: dict) -> None:
    """Cancels a run, unless it has ended meanwhile."""
    done = stack.agentctl_run("runs", "cancel", run["run_id"])
    if done.returncode != 0 and stack.get_run(run)["state"] not in ENDED:
        raise AssertionError(f"agentctl runs cancel {run['run_id']} failed: {done.stderr.strip()}")


def _ended(stack: Stack, run: dict) -> str:
    """The run's state once it has ended."""
    def state() -> str | None:
        s = stack.get_run(run)["state"]
        return s if s in ENDED else None
    return wait_for(state, f"run {run['run_id']} to end", timeout=300, interval=1)


def _cancel_unknown(stack: Stack, journal: Journal, log) -> list[dict]:
    """Cancels the runs agentd has that the journal does not (a runner stopped between starting
    them and writing them down), and returns them. Their spending is in the ledger, and counts
    against the cap."""
    known = {e["run_id"] for e in journal.entries()}
    out = []
    for row in stack.ledger("SELECT workflow_id, run_id FROM runs"):
        if row["workflow_id"] in known or row["run_id"] is None:
            continue
        run = {"run_id": row["workflow_id"], "temporal_run_id": row["run_id"]}
        if stack.get_run(run)["state"] not in ENDED:
            log(f"cancelling run {row['workflow_id']}: started, but not in the journal")
            _cancel(stack, run)
            out.append(run)
    return out


def _start(stack: Stack, cs: CaseSet, c: Case, rep: int, profile: Profile, budget: int, agentos: dict) -> dict:
    started = stack.agentctl("runs", "start", TASK, "--repo", repo_id(c.project), "--commit", c.commit,
                             "--base", cs.projects[c.project].base, "--budget-micros", str(budget), "--dry-run")
    return {"event": "start", "profile": profile.name, "case": c.id, "rep": rep, "run_id": started["run_id"],
            "temporal_run_id": started["temporal_run_id"], "budget_micros": budget, "paid": profile.paid,
            "expect": profile.expect, "commit": c.commit, "cases_sha256": cs.sha256, "agentos": agentos,
            "at": time.time()}


def _binaries() -> dict[str, str]:
    """What the runs ran: the binaries make build put in bin/, by sha256 (a checkout's commit
    does not say whether they were built from it)."""
    return {name: hashlib.sha256((BIN / name).read_bytes()).hexdigest()[:16]
            for name in ("agentd", "agentctl", "agentos-mcp-git") if (BIN / name).exists()}


def _agentos() -> str:
    """The agentos commit the runs ran with, and whether the checkout had changes, files git does
    not track but does not ignore either among them."""
    head = subprocess.run(["git", "-C", str(REPO), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    dirty = subprocess.run(["git", "-C", str(REPO), "status", "--porcelain"],
                           capture_output=True, text=True).stdout.strip()
    return head + ("+changes" if dirty else "")


def _watch(stack: Stack, entry: dict, profile: Profile) -> dict | None:
    """The run's result once it has ended; None while it runs. A run waiting for budget, an
    approval or a reconciliation, or past the profile's timeout, is cancelled and ended. A run
    agentd does not have (someone else's ledger in its place) is lost."""
    run = {"run_id": entry["run_id"], "temporal_run_id": entry["temporal_run_id"]}
    got = stack.agentctl_run("runs", "get", run["run_id"])
    if got.returncode != 0:
        if "code = NotFound" in got.stderr:
            return collect(stack, entry, "lost", {"state": "NOT_FOUND", "failure": "agentd has no such run"})
        if time.monotonic() - entry["since"] > profile.timeout:
            raise AssertionError(f"agentctl runs get {run['run_id']} failed: {got.stderr.strip()}")
        return None  # ask again on the next poll, until the timeout
    state = json.loads(got.stdout)["state"]
    if state in TERMINAL:  # the runner stopped it if it asked to and the run ended cancelled
        return collect(stack, entry, entry.get("stopped", "run") if state == "CANCELLED" else "run")
    if state == "UNKNOWN":  # Temporal no longer has it
        return collect(stack, entry, "lost")
    ended_by = {"PAUSED_BUDGET": "budget", "WAITING_APPROVAL": "waiting", "WAITING_RECONCILE": "waiting"}.get(state)
    if ended_by is None and time.monotonic() - entry["since"] > profile.timeout:
        ended_by = "timeout"
    if ended_by is None:
        return None
    _cancel(stack, run)
    if _ended(stack, run) != "CANCELLED":  # it ended by itself meanwhile
        ended_by = "run"
    return collect(stack, entry, ended_by)


def _cut(value: object, n: int) -> object:
    """A model's string, without the spaces around it and cut to n characters: it may be
    anything, of any length."""
    return value.strip()[:n] if isinstance(value, str) else value


def _int(digits: str) -> int | None:
    """A JSON integer, or None for one longer than any line number: Python refuses to read one
    of over 4300 digits, and a model may well write one."""
    return int(digits) if len(digits) <= 20 else None


def _float(digits: str) -> float | None:
    """A JSON number with a fraction or an exponent, or None for one past any float (1e400)."""
    value = float(digits)
    return value if math.isfinite(value) else None


def _object(raw: str | bytes) -> dict:
    """A JSON object that holds what a model wrote: numbers past any line are None, and so are
    the NaN and Infinity Python's json would take, so that the journal and the report stay JSON."""
    value = json.loads(raw, parse_int=_int, parse_float=_float, parse_constant=lambda _: None)
    return value if isinstance(value, dict) else {}


def _patch(stack: Stack, ref: object) -> bool:
    """Whether the report has a patch: a blank one is none, though the workflow checks it (and
    finds it rejected)."""
    if not isinstance(ref, str) or not ref:
        return False
    rows = stack.ledger("SELECT bytes FROM blobs WHERE sha256 = ?", ref.removeprefix("sha256:"))
    return not rows or bool(bytes(rows[0]["bytes"]).strip())


def collect(stack: Stack, entry: dict, ended_by: str, info: dict | None = None) -> dict:
    """What a run that has ended did, from agentctl (or `info`, for a run agentd does not have),
    Temporal and agentd's ledger. The report keeps each finding's severity, CWE, file, line and
    title, cut short; not its explanation, nor the patch, which may quote the project's code."""
    run = {"run_id": entry["run_id"], "temporal_run_id": entry["temporal_run_id"]}
    info = info if info is not None else stack.get_run(run)
    try:
        started, closed = stack.execution_times(run)
    except Exception:  # noqa: BLE001 - Temporal no longer has it
        started, closed = None, None
    answers, accepted, prompts = [], [], set()
    for row in stack.ledger("SELECT provider, model, prompt_version, response_blob, usage_json FROM model_calls "
                            "WHERE workflow_id = ? ORDER BY started_at, activity_id, attempt, request_seq",
                            entry["run_id"]):
        model = row["model"] if row["model"].startswith(row["provider"] + "/") else f"{row['provider']}/{row['model']}"
        prompts.add(row["prompt_version"])
        billed = json.loads(row["usage_json"] or "[]")
        usage: dict[str, int] = {}
        for b in billed:
            for k, v in b.items():
                if k.endswith("_tokens"):
                    usage[k] = usage.get(k, 0) + v
        answer = {"model": model, "status": None, "answered_by": None, "usage": usage,
                  "billed": [b.get("model") for b in billed]}
        if row["response_blob"]:
            [blob] = stack.ledger("SELECT bytes FROM blobs WHERE sha256 = ?", row["response_blob"].removeprefix("sha256:"))
            resp = _object(blob["bytes"])
            answer["status"], answer["answered_by"] = resp.get("status"), resp.get("model")
            if resp.get("status") == "ok":
                value = resp.get("value")
                accepted.append(value if isinstance(value, dict) else {})
        # A server-side fallback (section 9) answers in the same request: the request billed, or
        # was answered by, a model other than the one asked. A service tier ("@flex") is no other.
        answer["fallback"] = any(n.split("@", 1)[0] != model for n in [*answer["billed"], answer["answered_by"]]
                                 if isinstance(n, str))
        answers.append(answer)
    report = None
    if accepted:
        value = accepted[-1]  # the workflow asks once; a retry is answered from the ledger
        report = {"findings": [{"severity": _cut(f.get("severity"), 16), "cwe": _cut(f.get("cwe"), 64),
                                "file": _cut(f.get("file"), 300), "line": f.get("line"),
                                "title": _cut(f.get("title"), 200)}  # the model's words: a line, not the code
                               for f in value.get("findings") or [] if isinstance(f, dict)],
                  "patch": _patch(stack, value.get("patch_ref"))}
    checks = stack.ledger("SELECT outcome FROM calls WHERE workflow_id = ? AND name = 'git.apply_check' "
                          "ORDER BY created_at", entry["run_id"])
    result = _object(info.get("result_json") or "{}")
    return {"event": "result", "profile": entry["profile"], "case": entry["case"], "rep": entry["rep"],
            "commit": entry.get("commit"), "cases_sha256": entry.get("cases_sha256"), "expect": entry.get("expect"),
            "agentos": entry.get("agentos"), "prompts": sorted(prompts), "resumed": entry.get("resumed", False),
            "run_id": entry["run_id"], "state": info["state"], "outcome": result.get("outcome", ""),
            "failure": (info.get("failure") or "")[:500], "ended_by": ended_by,
            "seconds": (closed - started) if started is not None and closed is not None else None,
            "spent_micros": int(info.get("budget_spent_micros", 0)) + int(info.get("budget_reserved_micros", 0)),
            "models": list(dict.fromkeys(a["model"] for a in answers)), "answers": answers,
            "accepted_answers": len(accepted), "report": report,
            "patch_applies": (checks[-1]["outcome"] == "ok") if checks else None}


def _line(cs: CaseSet, r: dict) -> str:
    c = cs.case(r["case"])
    found = len((r["report"] or {}).get("findings", [])) if r["report"] else "-"
    if void(r):
        verdict = f"void, {void_reason(r)}"
    elif c.vulnerable:
        verdict = "detected" if detected(c, r) else "missed"
    else:
        verdict = "false positive" if false_positive(r) else "clean"
    seconds = f"{r['seconds']:.0f}s" if r["seconds"] is not None else "?"
    return (f"[{r['profile']}] {r['case']} #{r['rep']}: {r['outcome'] or r['ended_by']}, {found} findings, {verdict}, "
            f"${r['spent_micros'] / 1e6:.4f}, {seconds}")
