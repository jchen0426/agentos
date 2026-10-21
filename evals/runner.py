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
The first run of a profile goes alone: if the infrastructure ends it (evals/score.py's void),
the runner stops rather than spend the rest. So does a dead Temporal, agentd or Worker.

Money: a paid profile needs a cap. A run starts only if what the ledgers say the state
directory's runs have spent (settled, or reserved and not yet settled), plus the budgets of the
paid runs not yet ended, plus its own budget stay within the cap. agentd never lets a run spend
past its budget, so the runs never spend past the cap; if one ever did (a reservation bound
that is wrong, which agentd logs as an error), no paid run starts from then on and the ones in
flight are cancelled. A run the ledger has and the journal does not (a runner stopped between
starting it and writing it down) is cancelled before the Worker comes up: nobody would watch
it. The cap counts the price table's prices, not the provider's bill.
"""

from __future__ import annotations

import fcntl
import json
import os
import re
import sqlite3
import subprocess
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path

from evals.cases import Case, CaseSet
from evals.mirrors import inside_repo
from evals.score import detected, false_positive, void
from tests.harness import SANDBOX, TERMINAL, Stack, wait_for

REPO = Path(__file__).resolve().parents[1]
BUDGET = 1_500_000  # micros a run may spend: above the gateway's worst-case reservation (section 9)
TASK = "security-audit"
_NAME = re.compile(r"^[a-z0-9][a-z0-9-]{0,31}$")
ENDED = TERMINAL | {"UNKNOWN"}


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
            f.write((b"\n" if cut else b"") + json.dumps(entry, sort_keys=True).encode() + b"\n")
            f.flush()
            os.fsync(f.fileno())

    def results(self) -> list[dict]:
        """Each (profile, case, rep)'s latest result: a void run run again is superseded."""
        latest: dict = {}
        for e in self.entries():
            if e["event"] == "result":
                latest[(e["profile"], e["case"], e["rep"])] = e
        return list(latest.values())

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
    log: object
    overspent: bool = False


def run(cs: CaseSet, profiles: list[Profile], mirrors: dict[str, Path], state: Path, runs: int = 3,
        budget_micros: int = BUDGET, cap_micros: int | None = None, cases: list[Case] | None = None,
        retry_void: bool = False, log=print) -> list[dict]:
    """Runs each of `cases` (all of the set's, by default) `runs` times under each profile, and
    returns the journal's latest results: these runs' and any an earlier runner on `state`
    finished. `mirrors` names every project's mirror, whatever cases this runner runs: runs in
    flight from an earlier runner may audit others. With retry_void, void runs run again."""
    if any(p.paid for p in profiles) and cap_micros is None:
        raise ValueError("a paid profile needs a cap on what the runs may spend")
    if budget_micros <= 0:  # agentd takes 0 for its default budget, which the cap would not see
        raise ValueError("each run needs a budget above 0")
    if any(p.real for p in profiles) and SANDBOX == "none":
        raise ValueError("real models run with the Worker in a sandbox: set AGENTOS_SANDBOX")
    if any(not _NAME.match(p.name) for p in profiles) or len({p.name for p in profiles}) != len(profiles):
        raise ValueError("profile names are distinct, lowercase letters, digits and dashes")
    cases = list(cs.cases if cases is None else cases)
    missing = [c.id for c in cases if not c.commit]
    if missing:
        raise ValueError(f"cases without a commit ID (build their mirrors first): {missing}")
    state = state.resolve()
    if inside_repo(state):
        raise ValueError(f"the state directory {state} must be outside the repository: the sandboxed Worker reads it")
    state.mkdir(parents=True, exist_ok=True)
    with open(state / "lock", "w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError(f"another runner is using {state}") from None
        mirrors = {name: Path(path).resolve() for name, path in mirrors.items()}
        r = _Run(cs, Journal(state / "journal.jsonl"), state, {repo_id(n): {"mirror": str(p)} for n, p in mirrors.items()},
                 [state, *mirrors.values()], budget_micros, cap_micros, log)
        for profile in profiles:
            _run_profile(r, profile, cases, runs, retry_void)
        return r.journal.results()


def _overspent(journal: Journal) -> bool:
    budgets = {e["run_id"]: e["budget_micros"] for e in journal.entries() if e["event"] == "start"}
    return any(e["spent_micros"] > budgets.get(e["run_id"], e["spent_micros"]) for e in journal.results())


def _run_profile(r: _Run, profile: Profile, cases: list[Case], runs: int, retry_void: bool) -> None:
    log, journal = r.log, r.journal
    r.overspent = r.overspent or _overspent(journal)
    done = {(e["case"], e["rep"]) for e in journal.results()
            if e["profile"] == profile.name and not (retry_void and void(e))}
    resumed = [e for e in journal.unfinished() if e["profile"] == profile.name]
    todo = deque((c, rep) for rep in range(1, runs + 1) for c in cases
                 if (c.id, rep) not in done and not any(e["case"] == c.id and e["rep"] == rep for e in resumed))
    if not todo and not resumed:
        return
    stack_dir = r.state / profile.name / "stack"
    stack_dir.mkdir(parents=True, exist_ok=True)
    stack = Stack(stack_dir, temporal_db=True, fixture=False)
    stack.deny_read = r.deny_read  # the journal and the mirrors, besides agentd's data
    try:
        stack.start_agentd(policy=policy(list(r.repos)), models=profile.models, prices=profile.prices, repos=r.repos)
        if not (stack.data_dir / "ledger.db").exists():
            raise RuntimeError(f"agentd is up, but its ledger is not at {stack.data_dir / 'ledger.db'}")
        unknown = _cancel_unknown(stack, journal, log)  # before the Worker: agentd refuses their calls
        stack.start_worker()
        for gone in unknown:
            wait_for(lambda gone=gone: stack.get_run(gone)["state"] in ENDED, f"run {gone['run_id']} to end",
                     timeout=300, interval=1)
        log(f"[{profile.name}] {len(todo)} runs to start, {len(resumed)} to pick up")
        watching = {e["run_id"]: {**e, "since": time.monotonic(), "resumed": True} for e in resumed}
        confirmed = False  # a run of this profile has ended without the infrastructure ending it
        while todo or watching:
            _alive(stack)
            while todo and len(watching) < (profile.concurrency if confirmed else 1):
                if profile.paid and r.overspent:
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
                c, rep = todo.popleft()
                entry = _start(stack, r.cs, c, rep, profile, r.budget_micros)
                journal.append(entry)
                watching[entry["run_id"]] = {**entry, "since": time.monotonic()}
            for run_id, entry in list(watching.items()):
                record = _watch(stack, entry, profile)
                if record is None:
                    continue
                journal.append(record)
                del watching[run_id]
                log(_line(r.cs, record))
                if record["spent_micros"] > entry["budget_micros"]:
                    r.overspent = True
                    log(f"[{profile.name}] run {run_id} spent ${record['spent_micros'] / 1e6:.4f}, past its budget "
                        f"of ${entry['budget_micros'] / 1e6:.4f}: no more paid runs, and the ones in flight stop")
                    if profile.paid:
                        for other in watching.values():
                            _cancel(stack, other)
                if not void(record):
                    confirmed = True
                elif not confirmed and not record["resumed"]:
                    raise RuntimeError(f"[{profile.name}] the first run ended without the model: "
                                       f"{record['outcome'] or record['ended_by']} {record['failure']}".rstrip())
            if watching:
                time.sleep(1)
    finally:
        stack.close()


def _alive(stack: Stack) -> None:
    """Stops the runner if a process of the stack died: its runs would only time out."""
    temporal = [p for p in stack.procs if p.name == "temporal"][-1:]
    dead = [p.name for p in [stack.agentd, *stack.workers, *temporal] if p is not None and not p.alive()]
    if dead:
        raise RuntimeError(f"{', '.join(dead)} died: stopping; the next runner picks up the runs in flight")


SPENT = ("SELECT (SELECT COALESCE(SUM(amount_micros), 0) FROM budget_entries WHERE kind = 'SETTLE') + "
         "(SELECT COALESCE(SUM(r.amount_micros), 0) FROM budget_entries r WHERE r.kind = 'RESERVE' AND NOT EXISTS "
         "(SELECT 1 FROM budget_entries c WHERE c.request_id = r.request_id AND c.kind IN ('SETTLE', 'RELEASE')))")


def _spent(state: Path) -> int:
    """What every run in the state directory's ledgers has spent, or may have: settled, and
    reserved but not yet settled or released."""
    total = 0
    for path in sorted(state.glob("*/stack/agentd/data/ledger.db")):
        db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            total += db.execute(SPENT).fetchone()[0]
        finally:
            db.close()
    return total


def spent_in(state: Path) -> int:
    """The state directory's spending, all of it: for the report."""
    return _spent(state.resolve())


def _cancel(stack: Stack, run: dict) -> None:
    """Cancels a run, unless it has ended meanwhile."""
    done = stack.agentctl_run("runs", "cancel", run["run_id"])
    if done.returncode != 0 and stack.get_run(run)["state"] not in ENDED:
        raise AssertionError(f"agentctl runs cancel {run['run_id']} failed: {done.stderr.strip()}")


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


def _start(stack: Stack, cs: CaseSet, c: Case, rep: int, profile: Profile, budget: int) -> dict:
    started = stack.agentctl("runs", "start", TASK, "--repo", repo_id(c.project), "--commit", c.commit,
                             "--base", cs.projects[c.project].base, "--budget-micros", str(budget), "--dry-run")
    return {"event": "start", "profile": profile.name, "case": c.id, "rep": rep, "run_id": started["run_id"],
            "temporal_run_id": started["temporal_run_id"], "budget_micros": budget, "paid": profile.paid,
            "expect": profile.expect, "commit": c.commit, "cases_sha256": cs.sha256, "agentos": _agentos(),
            "at": time.time()}


def _agentos() -> str:
    """The agentos commit the runs ran with, and whether the checkout had changes."""
    head = subprocess.run(["git", "-C", str(REPO), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    dirty = subprocess.run(["git", "-C", str(REPO), "status", "--porcelain", "--untracked-files=no"],
                           capture_output=True, text=True).stdout.strip()
    return head + ("+changes" if dirty else "")


def _watch(stack: Stack, entry: dict, profile: Profile) -> dict | None:
    """The run's result once it has ended; None while it runs. A run waiting for budget, an
    approval or a reconciliation, or past the profile's timeout, is cancelled and ended."""
    run = {"run_id": entry["run_id"], "temporal_run_id": entry["temporal_run_id"]}
    try:
        state = stack.get_run(run)["state"]
    except AssertionError:  # agentctl failed: ask again on the next poll, until the timeout
        if time.monotonic() - entry["since"] > profile.timeout:
            raise
        return None
    if state in TERMINAL:
        return collect(stack, entry, "run")
    if state == "UNKNOWN":  # Temporal no longer has it
        return collect(stack, entry, "lost")
    ended_by = {"PAUSED_BUDGET": "budget", "WAITING_APPROVAL": "waiting", "WAITING_RECONCILE": "waiting"}.get(state)
    if ended_by is None and time.monotonic() - entry["since"] > profile.timeout:
        ended_by = "timeout"
    if ended_by is None:
        return None
    _cancel(stack, run)
    wait_for(lambda: stack.get_run(run)["state"] in ENDED, f"run {entry['run_id']} to end", timeout=300, interval=1)
    return collect(stack, entry, ended_by)


def _cut(value: object, n: int) -> object:
    """A model's string, cut to n characters: it may be anything, of any length."""
    return value[:n] if isinstance(value, str) else value


def collect(stack: Stack, entry: dict, ended_by: str) -> dict:
    """What a run that has ended did, from agentctl, Temporal and agentd's ledger. The report
    keeps each finding's severity, CWE, file, line and title, cut short; not its explanation,
    nor the patch, which may quote the project's code."""
    run = {"run_id": entry["run_id"], "temporal_run_id": entry["temporal_run_id"]}
    info = stack.get_run(run)
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
        # A server-side fallback bills a second model in the same request (section 9).
        answer = {"model": model, "status": None, "answered_by": None, "usage": usage,
                  "billed": [b.get("model") for b in billed], "fallback": len(billed) > 1}
        if row["response_blob"]:
            [blob] = stack.ledger("SELECT bytes FROM blobs WHERE sha256 = ?", row["response_blob"].removeprefix("sha256:"))
            resp = json.loads(blob["bytes"])
            answer["status"], answer["answered_by"] = resp.get("status"), resp.get("model")
            if resp.get("status") == "ok":
                accepted.append(resp.get("value") or {})
        answers.append(answer)
    report = None
    if accepted:
        value = accepted[-1]  # the workflow asks once; a retry is answered from the ledger
        report = {"findings": [{"severity": _cut(f.get("severity"), 16), "cwe": _cut(f.get("cwe"), 64),
                                "file": _cut(f.get("file"), 300), "line": f.get("line"),
                                "title": _cut(f.get("title"), 200)}  # the model's words: a line, not the code
                               for f in value.get("findings") or [] if isinstance(f, dict)],
                  "patch": bool(value.get("patch_ref"))}
    checks = stack.ledger("SELECT outcome FROM calls WHERE workflow_id = ? AND name = 'git.apply_check' "
                          "ORDER BY created_at", entry["run_id"])
    result = json.loads(info.get("result_json") or "{}")
    return {"event": "result", "profile": entry["profile"], "case": entry["case"], "rep": entry["rep"],
            "commit": entry.get("commit"), "cases_sha256": entry.get("cases_sha256"), "expect": entry.get("expect"),
            "agentos": entry.get("agentos"), "prompts": sorted(prompts), "resumed": entry.get("resumed", False),
            "run_id": entry["run_id"], "state": info["state"], "outcome": result.get("outcome", ""),
            "failure": (info.get("failure") or "")[:500], "ended_by": ended_by,
            "seconds": (closed - started) if started is not None and closed is not None else None,
            "spent_micros": int(info["budget_spent_micros"]) + int(info["budget_reserved_micros"]),
            "models": list(dict.fromkeys(a["model"] for a in answers)), "answers": answers,
            "accepted_answers": len(accepted), "report": report,
            "patch_applies": (checks[-1]["outcome"] == "ok") if checks else None}


def _line(cs: CaseSet, r: dict) -> str:
    c = cs.case(r["case"])
    found = len((r["report"] or {}).get("findings", [])) if r["report"] else "-"
    if void(r):
        verdict = "void"
    elif c.vulnerable:
        verdict = "detected" if detected(c, r) else "missed"
    else:
        verdict = "false positive" if false_positive(r) else "clean"
    seconds = f"{r['seconds']:.0f}s" if r["seconds"] is not None else "?"
    return (f"[{r['profile']}] {r['case']} #{r['rep']}: {r['outcome'] or r['ended_by']}, {found} findings, {verdict}, "
            f"${r['spent_micros'] / 1e6:.4f}, {seconds}")
