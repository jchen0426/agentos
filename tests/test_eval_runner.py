"""The evaluation end to end, with simulated models (no money): python -m evals's runner brings
up Temporal, agentd and a sandboxed Worker, audits a tiny case set (tests/eval_fixture.py) in
dry-run under two profiles, and the metrics come out as the scripted answers make them."""

import collections
import dataclasses
import fcntl
import json
import os
import signal
import sqlite3
import subprocess
import threading
import time
import types
from pathlib import Path

import pytest

from evals import __main__ as cli
from evals import cases as casefile
from evals import runner, score
from tests import eval_fixture, harness
from tests.harness import AUDIT_SCHEMA, BIN, Proc, Stack, command_of, descendants, wait_for

PRICES = """
price_table_version: eval-test
models:
  anthropic/claude-opus-5-5: {input: 1.00, cache_read: 0.10, output: 5.00}
  local/gpt-oss:20b:         {input: 0, output: 0}
"""
ANSWER = 1000 * 1 + 500 * 5  # micros: an answer of 1000 input and 500 output tokens, at model a's prices
CACHED = 200 * 1 + 80 + 500 * 5  # 200 input tokens, 800 read from the cache at $0.10, 500 output
# A run's budget: room for a request's reservation (1100 input tokens and 4096 output, at most).
BUDGET = 50_000


def profiles(url: str) -> list[runner.Profile]:
    """Model a, over Anthropic's API and priced; model b, a local server on Chat Completions."""
    def task(model: str, **extra) -> dict:
        return {"model": model, "max_tokens": 4096, "output_schema": str(AUDIT_SCHEMA), "blobs": ["patch"], **extra}
    a = runner.Profile("a", json.dumps({"providers": {"anthropic": {"adapter": "anthropic", "base_url": url}},
                                        "tasks": {"audit_v1": task("anthropic/claude-opus-5-5")}}),
                       PRICES, "anthropic/claude-opus-5-5", paid=True)
    # Model b's provider lists efforts up to medium: asked for high, it is sent medium.
    b = runner.Profile("b", json.dumps({"providers": {"local": {"adapter": "openai_compat", "api": "chat",
                                                                "base_url": url + "/v1", "context_tokens": 65536,
                                                                "efforts": ["low", "medium"]}},
                                        "tasks": {"audit_v1": task("local/gpt-oss:20b", effort="high")}}),
                       PRICES, "local/gpt-oss:20b", paid=False)
    return [a, b]


@pytest.fixture
def evalset(tmp_path: Path) -> tuple[casefile.CaseSet, dict[str, Path], Path]:
    """The case set, with the IDs of the commits we make recorded (python -m evals mirrors
    --record), its repositories, and its file."""
    root = tmp_path / "repos"
    path = tmp_path / "cases.json"
    path.write_text(json.dumps(eval_fixture.cases_doc(root, eval_fixture.make_repos(root))))
    assert cli.main(["--cases", str(path), "mirrors", "--cache", str(tmp_path / "cache"), "--record"]) == 0
    return casefile.load(path), {"shop": root / "shop", "cparse": root / "cparse"}, path


@pytest.fixture
def modelsim(tmp_path: Path):
    """The simulated providers, answering as tmp_path/answers.json scripts. Yields their URL."""
    addr = tmp_path / "modelsim.addr"
    p = Proc("modelsim", [BIN / "modelsim", "--listen", "127.0.0.1:0", "--addr-file", addr,
                          "--log", tmp_path / "modelsim.jsonl", "--answers", tmp_path / "answers.json"],
             tmp_path / "modelsim.log")
    wait_for(lambda: addr.exists() or not p.alive(), "modelsim")
    assert p.alive(), p.text()
    try:
        yield f"http://{addr.read_text()}"
    finally:
        p.stop()


def run(*args, **kwargs) -> list[dict]:
    """runner.run, printing the stacks' logs if it fails."""
    try:
        return runner.run(*args, **kwargs)
    except BaseException:
        for log in sorted(args[3].glob("*/stack/*.log")):
            print(f"===== {log}\n{log.read_text(errors='replace')[-6000:]}")
        raise


def open_files(pid: int) -> set[str]:
    """The paths a process has open: /proc on Linux, lsof on macOS."""
    fds = Path(f"/proc/{pid}/fd")
    if fds.exists():
        return {os.path.realpath(fd) for fd in fds.iterdir()}
    out = subprocess.run(["lsof", "-n", "-P", "-p", str(pid), "-Fn"], capture_output=True, text=True).stdout
    return {os.path.realpath(line[1:]) for line in out.splitlines() if line.startswith("n/")}


def ledger(state: Path, profile: str) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{state / profile / 'stack/agentd/data/ledger.db'}?mode=ro", uri=True)


def stranded_run(state: Path, profile: runner.Profile, repos: dict, case, budget: int = BUDGET,
                 worker: bool = False) -> dict:
    """A run of `case` started on the profile's stack by hand, and left there as a runner stopped
    at that moment would leave it: in flight (without a Worker), or ended (with one)."""
    (state / profile.name / "stack").mkdir(parents=True, exist_ok=True)
    stack = Stack(state / profile.name / "stack", temporal_db=True, fixture=False)
    try:
        ids = {runner.repo_id(name): {"mirror": str(path)} for name, path in repos.items()}
        stack.start_agentd(policy=runner.policy(list(ids)), models=profile.models, prices=profile.prices, repos=ids)
        if worker:
            stack.start_worker()
        started = stack.agentctl("runs", "start", "security-audit", "--repo", runner.repo_id(case.project),
                                 "--commit", case.commit, "--base", "main", "--budget-micros", str(budget), "--dry-run")
        if worker:
            stack.wait_state(started, runner.TERMINAL | {"PAUSED_BUDGET"})
        return started
    finally:
        stack.close()


class LedgerOf:
    """agentctl and agentd's ledger as runner.collect reads them, for one run that succeeded."""

    def __init__(self, calls: list[dict], blobs: dict[str, bytes]) -> None:
        self.calls, self.blobs = calls, blobs

    def get_run(self, run: dict) -> dict:
        return {"state": "SUCCEEDED", "result_json": '{"outcome": "ok"}', "budget_spent_micros": "10",
                "budget_reserved_micros": "0"}

    def execution_times(self, run: dict) -> tuple[float, float]:
        return 100.0, 110.0

    def ledger(self, sql: str, *params) -> list[dict]:
        if "FROM model_calls" in sql:
            return self.calls
        return [{"bytes": self.blobs[params[0]]}] if "FROM blobs" in sql else []


def test_collect_tells_a_fallback_by_the_models_billed_and_reads_any_number() -> None:
    primary, fallback = "anthropic/claude-opus-5-5", "anthropic/claude-opus-4-8"

    def call(blob: str, billed: list[str], provider: str = "anthropic", model: str = "claude-opus-5-5") -> dict:
        return {"provider": provider, "model": model, "prompt_version": "audit_v1", "request_blob": None, "response_blob": "sha256:" + blob,
                "usage_json": json.dumps([{"model": b, "input_tokens": 10} for b in billed])}

    def answer(model: str) -> bytes:
        return json.dumps({"status": "ok", "model": model, "value": {"findings": []}}).encode()

    # A line of 5000 digits, which Python's json refuses to read as an int; a CWE after 70 spaces.
    huge = (b'{"status": "ok", "model": "anthropic/claude-opus-5-5", "value": {"findings": [{"severity": " high ", '
            b'"cwe": "' + b" " * 70 + b'CWE-89", "file": "app/db.py", "line": ' + b"9" * 5000 +
            b', "title": "t"}], "patch_ref": null}}')
    record = runner.collect(LedgerOf(
        [call("both", [primary, fallback]),  # the model declined, the fallback answered: both billed
         call("one", [fallback]),  # billed as one entry, under the fallback's name
         call("tier", ["openai/gpt-6.1-sol@flex"], "openai", "gpt-6.1-sol"),  # another tier, the same model
         call("answerer", [primary]),  # billed as the model asked, answered by another
         call("huge", [primary])],
        {"both": answer(fallback), "one": answer(fallback), "tier": answer("openai/gpt-6.1-sol"),
         "answerer": answer(fallback), "huge": huge}),
        {"run_id": "r", "temporal_run_id": "t", "profile": "p", "case": "py-sqli", "rep": 1}, "run")
    assert [a["fallback"] for a in record["answers"]] == [True, True, False, True, False]
    assert record["report"]["findings"] == [{"severity": "high", "cwe": "CWE-89", "file": "app/db.py", "line": None,
                                             "title": "t"}]
    assert not score.void(record) and record["spent_micros"] == 10


def test_a_signal_waits_for_the_stack_and_later_ones_cannot_cut_the_stopping_short() -> None:
    signals, done = runner._Signals(), []
    before = signal.getsignal(signal.SIGTERM)
    with signals.installed():
        with pytest.raises(SystemExit) as ended:
            with signals.held():  # a stack starting
                # Handed to another thread, as the OS may hand it to one of the Temporal SDK's:
                # blocking it in this thread would not keep it from cutting the start short.
                t = threading.Thread(target=lambda: signal.pthread_kill(threading.get_ident(), signal.SIGTERM))
                t.start()
                t.join()
                time.sleep(0.05)
                done.append("started")
        assert ended.value.code == 128 + signal.SIGTERM and done == ["started"]
        # The runner is ending: another signal, while the stack stops or after, only waits its turn.
        with signals.held():
            os.kill(os.getpid(), signal.SIGTERM)
            time.sleep(0.05)
            done.append("stopped")
        os.kill(os.getpid(), signal.SIGTERM)
        time.sleep(0.05)
    assert done == ["started", "stopped"] and signals.got == [signal.SIGTERM] * 3
    assert signal.getsignal(signal.SIGTERM) is before
    # Outside any stack starting or stopping, the first signal ends the runner at once.
    signals = runner._Signals()
    with signals.installed(), pytest.raises(SystemExit):
        os.kill(os.getpid(), signal.SIGTERM)
        time.sleep(0.05)


def test_a_process_listed_is_the_one_started_not_a_pid_used_again(tmp_path: Path) -> None:
    pids = tmp_path / "state" / "a" / "stack" / "pids"
    pids.parent.mkdir(parents=True)
    me = os.getpid()
    pids.write_text(json.dumps([["agentd", me, "Thu Jan  1 00:00:00 1970"], ["temporal", me, harness.started_at(me)],
                                ["worker1", 999_999_999, "Thu Jan  1 00:00:00 1970"]]))
    assert runner._left_running(tmp_path / "state") == [("temporal", me)]
    pids.write_text(json.dumps([["agentd", me]]))  # an older stack's list: no start times, nothing listed
    assert runner._left_running(tmp_path / "state") == []


def test_numbers_past_any_line_are_none_and_the_journal_stays_json(tmp_path: Path) -> None:
    raw = '{"line": 1e400, "low": -1e400, "nan": NaN, "inf": Infinity, "ok": 12.0, "n": 7}'
    assert runner._object(raw) == {"line": None, "low": None, "nan": None, "inf": None, "ok": 12.0, "n": 7}
    assert runner._object('{"a":' * 100_000 + "1" + "}" * 100_000) == {}  # deeper than Python reads: unread
    with pytest.raises(ValueError):
        runner.Journal(tmp_path / "journal.jsonl").append({"event": "result", "x": float("inf")})


def test_an_answer_too_deep_to_read_keeps_the_status_agentd_gave_it() -> None:
    deep = '{"status": "invalid_output", "output": ' + "[" * 100_000 + "]" * 100_000 + "}"
    call = {"provider": "local", "model": "gpt-oss:20b", "prompt_version": "audit_v1", "request_blob": None, "response_blob": "sha256:d",
            "usage_json": "[]", "stop_reason": "invalid_output"}
    record = runner.collect(LedgerOf([call], {"d": deep.encode()}), {"run_id": "r", "temporal_run_id": "t",
                                                                     "profile": "p", "case": "py-sqli", "rep": 1}, "run")
    assert [a["status"] for a in record["answers"]] == ["invalid_output"]


def test_a_blank_patch_is_no_patch() -> None:
    def record(patch: bytes) -> dict:
        value = {"findings": [], "patch_ref": "sha256:patch"}
        call = {"provider": "local", "model": "gpt-oss:20b", "prompt_version": "audit_v1", "request_blob": None, "response_blob": "sha256:a",
                "usage_json": json.dumps([{"model": "local/gpt-oss:20b"}])}
        blobs = {"a": json.dumps({"status": "ok", "model": "local/gpt-oss:20b", "value": value}).encode(), "patch": patch}
        return runner.collect(LedgerOf([call], blobs), {"run_id": "r", "temporal_run_id": "t", "profile": "p",
                                                        "case": "py-benign", "rep": 1}, "run")
    assert record(b" \n\t")["report"]["patch"] is False
    assert record(b"--- a/x\n+++ b/x\n")["report"]["patch"] is True


class Ended(LedgerOf):
    """A run in `state`, which a cancel turns into `after`."""

    def __init__(self, state: str, after: str | None = None) -> None:
        super().__init__([], {})
        self.state, self.after = state, after

    def agentctl_run(self, *args: str) -> subprocess.CompletedProcess:
        if args[:2] == ("runs", "cancel"):
            self.state = self.after
        return subprocess.CompletedProcess(args, 0, json.dumps({"state": self.state}), "")

    def get_run(self, run: dict) -> dict:
        return {**super().get_run(run), "state": self.state}


def test_the_runner_stopped_a_run_only_if_it_ended_cancelled() -> None:
    a = profiles("http://127.0.0.1:9")[0]
    entry = {"run_id": "r", "temporal_run_id": "t", "profile": "a", "case": "py-benign", "rep": 1, "since": 0}
    stopped = {**entry, "stopped": "overspent"}
    assert runner._watch(Ended("CANCELLED"), stopped, a)["ended_by"] == "overspent"
    assert runner._watch(Ended("SUCCEEDED"), stopped, a)["ended_by"] == "run"  # it ended before the cancel
    assert runner._watch(Ended("PAUSED_BUDGET", after="CANCELLED"), entry, a)["ended_by"] == "budget"
    assert runner._watch(Ended("PAUSED_BUDGET", after="SUCCEEDED"), entry, a)["ended_by"] == "run"
    # Stopped for the cap, still paused when polled: the reason stays the runner's.
    assert runner._watch(Ended("PAUSED_BUDGET", after="CANCELLED"), {**entry, "stopped": "cap"}, a)["ended_by"] == "cap"


def test_which_void_runs_stop_the_runner() -> None:
    fresh, again = {}, {"again": True}
    void = {"resumed": False, "ended_by": "run", "spent_micros": 0}
    assert runner._about_the_model(void) and runner._breaks(void, fresh)
    # A case void every time (too long, denied) costs nothing when run again: it keeps no other
    # void run from running again. A provider failing again does cost money.
    assert not runner._breaks(void, again) and runner._breaks({**void, "spent_micros": 1}, again)
    # Runs the runner stopped itself for money, or picked up from an earlier runner, say nothing.
    for ended_by in runner.STOPPED_BY_CAP:
        assert not runner._about_the_model({**void, "ended_by": ended_by})
    assert not runner._about_the_model({**void, "resumed": True})
    assert runner._about_the_model({**void, "ended_by": "timeout"})


def test_a_patch_check_that_did_not_finish_says_nothing() -> None:
    class Checked(LedgerOf):
        def __init__(self, outcome: str | None) -> None:
            super().__init__([], {})
            self.outcome = outcome

        def ledger(self, sql: str, *params) -> list[dict]:
            return [{"outcome": self.outcome}] if "FROM calls" in sql else super().ledger(sql, *params)
    entry = {"run_id": "r", "temporal_run_id": "t", "profile": "p", "case": "py-sqli", "rep": 1}
    assert [runner.collect(Checked(o), entry, "run")["patch_applies"] for o in (None, "denied", "ok")] == [
        None, False, True]


def _flow(text: str) -> object:
    """A YAML flow value as config/models.yaml writes them: "s", 20, [a, b], {k: v, ...}."""
    text = text.strip()
    if text.startswith("{") or text.startswith("["):
        parts, depth, cur = [], 0, ""
        for ch in text[1:-1]:
            depth += ch in "[{"
            depth -= ch in "]}"
            if ch == "," and depth == 0:
                parts.append(cur)
                cur = ""
            else:
                cur += ch
        parts = [part for part in [*parts, cur] if part.strip()]
        if text.startswith("["):
            return [_flow(part) for part in parts]
        return {k.strip(): _flow(v) for k, v in (part.split(":", 1) for part in parts)}
    if text.startswith('"'):
        return text.strip('"')
    return int(text) if text.isdigit() else text


def _product() -> tuple[dict, dict]:
    """config/models.yaml's providers and its audit_v1, read as the simple YAML they are."""
    providers, task, block, entry = {}, {}, None, ""
    for line in (runner.REPO / "config/models.yaml").read_text().splitlines():
        code = line.split(" #", 1)[0].rstrip() if not line.lstrip().startswith("#") else ""
        if not code:
            continue
        if not code.startswith(" "):
            block = code.rstrip(":")
        elif block == "providers":
            entry = (entry + " " + code.strip()) if entry else code.strip()
            if entry.count("{") == entry.count("}"):
                name, value = entry.split(":", 1)
                providers[name.strip()] = _flow(value)
                entry = ""
        elif block == "tasks" and code.startswith("    "):
            key, value = code.strip().split(":", 1)
            task[key] = _flow(value)
    return providers, task


def test_the_profiles_are_the_products_configuration() -> None:
    providers, product = _product()
    assert set(product) == {"model", "max_tokens", "effort", "prompt", "output_schema", "blobs", "fallback"}
    assert providers["local"]["efforts"] == ["low", "medium"]  # gpt-oss at medium (the pilot)
    for p in runner.PROFILES.values():
        config = json.loads(p.models)
        # Each profile lists one provider, exactly as the product does.
        assert config["providers"] == {k: providers[k] for k in config["providers"]}, p.name
        task = config["tasks"]["audit_v1"]
        # The product's audit_v1 with the profile's model; the server-side fallback is Claude's alone.
        want = {**product, "model": task["model"], "prompt": str(runner.REPO / "config" / product["prompt"]),
                "output_schema": str(runner.REPO / "config" / product["output_schema"])}
        if task["model"] != product["model"]:
            del want["fallback"]
        assert task == want, p.name


def test_the_plan_puts_the_runs_never_run_before_the_runs_again(tmp_path: Path) -> None:
    j = runner.Journal(tmp_path / "journal.jsonl")

    def ended(case: str, rep: int, outcome: str, profile: str = "p") -> None:
        run_id = f"{profile}-{case}-{rep}"
        j.append({"event": "start", "profile": profile, "case": case, "rep": rep, "run_id": run_id})
        j.append({"event": "result", "profile": profile, "case": case, "rep": rep, "run_id": run_id, "outcome": outcome,
                  "report": {"findings": []} if outcome == "ok" else None})
    ended("x", 1, "failed: boom")  # void
    ended("y", 1, "ok")
    ended("x", 2, "failed: boom", profile="q")  # another profile's
    j.append({"event": "start", "profile": "p", "case": "y", "rep": 2, "run_id": "flying"})
    x, y = types.SimpleNamespace(id="x"), types.SimpleNamespace(id="y")
    todo, resumed = runner.plan(j, "p", [x, y], 3, retry_void=True)
    assert [(c.id, rep, again) for c, rep, again in todo] == [
        ("x", 2, False), ("x", 3, False), ("y", 3, False), ("x", 1, True)]
    assert [e["run_id"] for e in resumed] == ["flying"]
    assert [(c.id, rep) for c, rep, _ in runner.plan(j, "p", [x, y], 3, retry_void=False)[0]] == [
        ("x", 2), ("x", 3), ("y", 3)]


def fake_ledger(path: Path, runs: dict[str, int], entries: list[tuple]) -> None:
    """The two tables of agentd's ledger that hold the money."""
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path)
    db.executescript("CREATE TABLE IF NOT EXISTS runs (workflow_id TEXT, budget_limit_micros INTEGER);"
                     "CREATE TABLE IF NOT EXISTS budget_entries (workflow_id TEXT, kind TEXT, amount_micros INTEGER,"
                     " request_id TEXT);")
    db.executemany("INSERT INTO runs VALUES (?, ?)", runs.items())
    db.executemany("INSERT INTO budget_entries VALUES (?, ?, ?, ?)", entries)
    db.commit()
    db.close()


def test_spending_and_overspending_come_from_every_profiles_ledger(tmp_path: Path) -> None:
    state = tmp_path / "state #1?%"  # what a file: URI must escape
    fake_ledger(runner._ledger(state, "a"), {"r1": 100},
                [("r1", "RESERVE", 60, "q1"), ("r1", "SETTLE", 50, "q1"), ("r1", "RESERVE", 40, "q2")])
    fake_ledger(runner._ledger(state, "b"), {"r2": 100}, [("r2", "RESERVE", 100, "q3"), ("r2", "RELEASE", 0, "q3")])
    assert runner._spent(state) == 50 + 40 and not runner._overspent(state)
    # A settlement past its reservation: run r2 has spent past its limit, journal or not.
    fake_ledger(runner._ledger(state, "b"), {}, [("r2", "RESERVE", 50, "q4"), ("r2", "SETTLE", 120, "q4")])
    assert runner._spent(state) == 90 + 120 and runner._overspent(state)
    fake_ledger(runner._ledger(state, "b"), {}, [("r2", "EXTEND", 20, None)])  # a budget extended
    assert not runner._overspent(state)


def test_the_metrics_are_what_the_answers_make_them(tmp_path: Path, evalset, modelsim: str) -> None:
    cs, repos, path = evalset
    (tmp_path / "answers.json").write_text(json.dumps(eval_fixture.answers()))
    state = tmp_path / "state"
    results = run(cs, profiles(modelsim), repos, state, runs=3, budget_micros=BUDGET, cap_micros=1_000_000)

    # Every case ran three times under each profile, every request got a scripted answer, and
    # no rule was left with answers: 16 for model a (one asked for again), 15 for model b.
    assert sorted((r["profile"], r["case"], r["rep"]) for r in results) == sorted(
        (p, c.id, n) for p in "ab" for c in cs.cases for n in (1, 2, 3))
    assert all(not rule["answers"] for rule in json.loads((tmp_path / "answers.json").read_text()))
    sent = [json.loads(line) for line in (tmp_path / "modelsim.jsonl").read_text().splitlines()]
    assert sum(e["path"] == "/v1/messages" for e in sent) == 16
    assert sum(e["path"] == "/v1/chat/completions" for e in sent) == 15

    # Dry runs: every write was only recorded, and none asked for an approval. Each profile had
    # a stack, and a ledger, of its own.
    for profile in "ab":
        db = ledger(state, profile)
        assert {s for (s,) in db.execute("SELECT DISTINCT status FROM effects")} == {"DRY_RUN"}
        assert db.execute("SELECT count(*) FROM approvals").fetchone() == (0,)
        assert db.execute("SELECT count(*) FROM runs WHERE dry_run = 0").fetchone() == (0,)
        assert db.execute("SELECT count(*) FROM runs").fetchone() == (15,)
        db.close()

    m = score.metrics(cs, results)
    a, b = m["a"], m["b"]
    hits = lambda r: (r["hits"], r["runs"])  # noqa: E731
    # The labels are the ledger's, call by call.
    assert a["models"] == ["anthropic/claude-opus-5-5"] and b["models"] == ["local/gpt-oss:20b"]
    assert a["unexpected_models"] == b["unexpected_models"] == 0

    # Model a: py-sqli 2 of 3, py-path 1 (2 lenient), c-oob 1 (2 lenient).
    assert hits(a["detection"]) == (4, 9) and hits(a["detection_lenient"]) == (6, 9)
    assert hits(a["detection_by_source"]["blame"]) == (3, 6) and hits(a["detection_by_source"]["reverse"]) == (1, 3)
    assert hits(a["detection_by_language"]["python"]) == (3, 6) and hits(a["detection_by_language"]["c"]) == (1, 3)
    cut = a["detection_by_cutoff"]
    assert cut["cutoff"] == "2026-06-30" and hits(cut["after"]) == (3, 6) and hits(cut["before"]) == (1, 3)
    assert hits(a["false_positive"]) == (1, 3) and hits(a["false_positive_any"]) == (2, 3)
    assert hits(a["injection_false_pass"]) == (2, 3)
    drop = a["injection_drop"]
    assert (drop["pairs"], drop["base"], drop["injected"]) == (1, pytest.approx(2 / 3), pytest.approx(1 / 3))
    assert drop["drop"] == pytest.approx(1 / 3)
    assert a["patch_applicable"] == {"patches": 3, "applied": 2, "rate": pytest.approx(2 / 3)}
    assert (a["refusals"], a["refused_runs"]) == (1, 1)
    assert a["invalid_output"] == {"answers": 16, "invalid": 1, "rate": 1 / 16}
    assert a["cache_hit_rate"] == pytest.approx(800 / 16_000)
    assert a["cost_usd"]["total"] == pytest.approx((15 * ANSWER + CACHED) / 1e6)
    assert a["cost_usd"]["per_audit"] == pytest.approx((15 * ANSWER + CACHED) / 15 / 1e6)
    assert a["outcomes"] == {"ok": 13, "patch rejected": 1, "refused": 1}
    assert a["answered"] == 14 and a["latency_s"]["runs"] == 15 and a["latency_s"]["p95"] > 0
    for r in (a["detection"], a["false_positive"], a["injection_false_pass"]):
        assert r["ci"][0] <= r["rate"] <= r["ci"][1], r

    # Model b: only the C bug, and a false alarm on every benign run; free.
    assert hits(b["detection"]) == (3, 9) and hits(b["detection_lenient"]) == (3, 9)
    assert hits(b["false_positive"]) == (3, 3) and hits(b["injection_false_pass"]) == (3, 3)
    assert b["injection_drop"]["drop"] == 0
    assert "detection_by_cutoff" not in b  # its model has no cutoff in the set
    assert b["patch_applicable"] == {"patches": 0, "applied": 0, "rate": None}
    assert (b["refusals"], b["invalid_output"]["invalid"], b["cost_usd"]["total"], b["cache_hit_rate"]) == (0, 0, 0, 0)

    # The report shows them, and keeps no explanation or patch of any finding.
    out = tmp_path / "out" / "report"
    assert cli.main(["--cases", str(path), "report", "--state", str(state), "--out", str(out), "--title", "test"]) == 0
    md = out.with_suffix(".md").read_text()
    assert "44%（4/9 次，3 个用例；95% 区间" in md, md
    assert "67%（6/9 次，3 个用例；区间退化：每个用例都一样）" in md  # every case detected leniently 2 times of 3
    # The effort each request was sent, as the ledger says: b's capped to medium, a's none at all.
    assert {a["effort"] for r in results if r["profile"] == "b" for a in r["answers"]} == {"medium"}
    assert {a["effort"] for r in results if r["profile"] == "a" for a in r["answers"]} == {None}
    assert "| a（anthropic/claude-opus-5-5） | b（local/gpt-oss:20b，effort medium） |" in md
    assert "| b | local/gpt-oss:20b | medium（15） |" in md
    assert "0%（0/3 次，1 个用例；只有一个用例，没有区间）" in md and "1 对；只有一对，没有区间" in md  # model b's
    assert "| py-sqli | 漏洞 | blame 找到的引入提交 | python | CWE-89 | 2/2/3 | 0/0/3 |" in md
    raw = out.with_suffix(".json").read_text()
    assert "explanation" not in raw and "search_orders" not in raw
    found = [f for r in results if r["report"] for f in r["report"]["findings"]]
    assert [max(len(f[k]) for f in found) for k in ("title", "file", "cwe")] == [200, 300, 64], "cut to a line"
    assert json.loads(raw)["metrics"] == json.loads(json.dumps(m))


def test_the_cap_stops_paid_runs_and_a_runner_picks_up_where_it_stopped(tmp_path: Path, evalset, modelsim: str) -> None:
    cs, repos, _ = evalset
    (tmp_path / "answers.json").write_text(json.dumps(
        [{"contains": "format_price", "answers": [{"report": eval_fixture.report()}] * 6}]))
    benign = [cs.case("py-benign")]
    a = profiles(modelsim)[0]
    state = tmp_path / "state"
    # A paid run another profile left unfinished: it spends up to its budget when its profile
    # runs again, so it counts against the cap until then.
    z = dataclasses.replace(a, name="z")
    left = stranded_run(state, z, repos, cs.case("py-sqli"))
    runner.Journal(state / "journal.jsonl").append({
        "event": "start", "profile": "z", "case": "py-sqli", "rep": 1, "run_id": left["run_id"],
        "temporal_run_id": left["temporal_run_id"], "budget_micros": BUDGET, "paid": True})
    # Room for one run in flight at a time, with what the runs before it spent: exactly three
    # (the third's budget fills the cap to the micro).
    first = run(cs, [a], repos, state, runs=6, budget_micros=BUDGET, cap_micros=2 * BUDGET + 2 * ANSWER, cases=benign)
    assert [r["rep"] for r in first] == [1, 2, 3], first
    assert sum(r["spent_micros"] for r in first) == 3 * ANSWER
    with pytest.raises(ValueError, match="cap"):
        runner.run(cs, [a], repos, state, runs=6, cases=benign)

    # Again on the same state, with room for the rest: only the three runs not done.
    second = run(cs, [a], repos, state, runs=6, budget_micros=BUDGET, cap_micros=1_000_000, cases=benign)
    assert sorted(r["rep"] for r in second) == [1, 2, 3, 4, 5, 6]
    assert {r["rep"]: r["run_id"] for r in second if r["rep"] <= 3} == {r["rep"]: r["run_id"] for r in first}
    assert len({r["run_id"] for r in second}) == 6
    assert sum(r["spent_micros"] for r in second) == 6 * ANSWER

    # z's run left in flight is held by no cap now: a runner whose cap cannot hold it stops it
    # before z's Worker comes up.
    third = run(cs, [z], repos, state, runs=1, budget_micros=BUDGET, cap_micros=6 * ANSWER, cases=[cs.case("py-sqli")])
    [stopped] = [r for r in third if r["profile"] == "z"]
    assert (stopped["run_id"], stopped["ended_by"], score.void_reason(stopped)) == (left["run_id"], "cap", "(cap)")


def test_a_run_paused_for_budget_is_stopped_and_a_first_run_that_is_void_stops_the_runner(
        tmp_path: Path, evalset, modelsim: str) -> None:
    cs, repos, _ = evalset
    (tmp_path / "answers.json").write_text(json.dumps([{"contains": "format_price", "answers": [
        {"report": eval_fixture.report(eval_fixture.finding(6, "CWE-20", "medium", "app/prices.py"))}]}]))
    a = dataclasses.replace(profiles(modelsim)[0], timeout=60)  # were it not stopped, it would time out
    state = tmp_path / "state"
    # Less than one request's reservation: the run waits for more budget, and the runner stops it.
    # The first run of a profile goes alone: void, it stops the runner before the others start.
    with pytest.raises(RuntimeError, match=r"the first run ended without the model: \(budget\)"):
        run(cs, [a], repos, state, runs=3, budget_micros=10_000, cap_micros=1_000_000, cases=[cs.case("py-benign")])
    [r] = runner.Journal(state / "journal.jsonl").results()
    assert (r["ended_by"], r["state"], r["outcome"], r["report"], r["spent_micros"]) == ("budget", "CANCELLED", "", None, 0)
    assert score.void(r) and not score.false_positive(r, None)
    # Nothing reached the provider: 4096 tokens of output alone cost more than the budget.
    assert not (tmp_path / "modelsim.jsonl").exists()
    m = score.metrics(cs, [r])["a"]
    assert m["outcomes"] == {"(budget)": 1} and (m["void"], m["false_positive"]["runs"]) == (1, 0)


def test_a_budget_of_zero_is_refused(tmp_path: Path, evalset) -> None:
    # agentd would take 0 for its default budget, which the cap would not count.
    cs, repos, _ = evalset
    with pytest.raises(ValueError, match="budget above 0"):
        runner.run(cs, [profiles("http://127.0.0.1:9")[0]], repos, tmp_path / "state", budget_micros=0,
                   cap_micros=1_000_000)


def test_a_run_that_spends_past_its_budget_stops_the_paid_runs(tmp_path: Path, evalset, modelsim: str) -> None:
    cs, repos, _ = evalset
    # Counted as 1000 input tokens, billed as 100,000: agentd settles what the provider billed,
    # past the reservation and the run's budget.
    (tmp_path / "answers.json").write_text(json.dumps([{"contains": "format_price", "answers": [
        {"report": eval_fixture.report(), "usage": {"input": 100_000, "cache_read": 0, "output": 10}}] * 3}]))
    one_at_a_time = dataclasses.replace(profiles(modelsim)[0], concurrency=1)  # runs in flight carry on
    state, benign = tmp_path / "state", cs.case("py-benign")
    # Another paid profile's run, left in flight by a runner that stopped before the overspend.
    c = dataclasses.replace(one_at_a_time, name="c")
    left = stranded_run(state, c, repos, benign)
    runner.Journal(state / "journal.jsonl").append({
        "event": "start", "profile": "c", "case": "py-benign", "rep": 1, "run_id": left["run_id"],
        "temporal_run_id": left["temporal_run_id"], "budget_micros": BUDGET, "paid": True, "expect": c.expect,
        "commit": benign.commit, "cases_sha256": cs.sha256, "at": 0})
    results = run(cs, [one_at_a_time], repos, state, runs=3, budget_micros=BUDGET, cap_micros=1_000_000,
                  cases=[benign])
    assert [(r["rep"], r["spent_micros"]) for r in results if r["profile"] == "a"] == [(1, 100_000 + 50)], results
    # The next runner on the state remembers, from the ledger; and c's run left in flight stops
    # before c's Worker comes up. It never had a Worker, so no model call of it was waiting for
    # agentd to make: its provider never hears of it. (One that was is made before the cancel
    # lands, within the run's budget, which the cap counts.)
    sent = (tmp_path / "modelsim.jsonl").read_text()
    again = run(cs, [one_at_a_time, c], repos, state, runs=3, budget_micros=BUDGET, cap_micros=1_000_000,
                cases=[benign])
    assert [r for r in again if r["profile"] == "a"] == results
    [stopped] = [r for r in again if r["profile"] == "c"]
    assert (stopped["run_id"], stopped["ended_by"], stopped["state"]) == (left["run_id"], "overspent", "CANCELLED")
    assert score.void(stopped) and score.void_reason(stopped) == "(overspent)"
    assert (tmp_path / "modelsim.jsonl").read_text() == sent


def test_runs_the_journal_never_saw_count_against_the_cap_and_are_cancelled(tmp_path: Path, evalset,
                                                                            modelsim: str) -> None:
    cs, repos, _ = evalset
    (tmp_path / "answers.json").write_text(json.dumps(
        [{"contains": "format_price", "answers": [{"report": eval_fixture.report()}] * 4}]))
    a = profiles(modelsim)[0]
    state = tmp_path / "state"
    benign = cs.case("py-benign")
    assert len(run(cs, [a], repos, state, runs=1, budget_micros=BUDGET, cap_micros=1_000_000, cases=[benign])) == 1

    # Two runs started on the same stack but never written down, as if the runner had been
    # stopped right after starting each: one answered and spent, one waits for budget.
    done = stranded_run(state, a, repos, benign, worker=True)
    waiting = stranded_run(state, a, repos, benign, budget=10_000)

    # The next runner cancels the waiting one, counts the answered one's spending, and stops where
    # one more run could pass the cap: two runs and the stranded one spent, one budget of room.
    second = run(cs, [a], repos, state, runs=3, budget_micros=BUDGET, cap_micros=3 * ANSWER + BUDGET - 1,
                 cases=[benign])
    assert sorted(r["rep"] for r in second) == [1, 2], second
    assert not {done["run_id"], waiting["run_id"]} & {r["run_id"] for r in second}
    db = ledger(state, "a")
    states = dict(db.execute("SELECT workflow_id, cancelled_at IS NOT NULL FROM runs").fetchall())
    db.close()
    assert (states[done["run_id"]], states[waiting["run_id"]]) == (0, 1)
    assert runner.spent_in(state) == 3 * ANSWER


def test_a_run_left_in_flight_is_only_ever_answered_by_its_own_profile(tmp_path: Path, evalset, modelsim: str) -> None:
    cs, repos, _ = evalset
    (tmp_path / "answers.json").write_text(json.dumps([
        {"model": "gpt-oss:20b", "contains": "parse_record",
         "answers": [{"report": eval_fixture.report(eval_fixture.finding(21, "CWE-787", file="parse.c"))}]},
        {"model": "claude-opus-5-5", "contains": "format_price", "answers": [{"report": eval_fixture.report()}]}]))
    a, b = profiles(modelsim)
    state = tmp_path / "state"
    oob = cs.case("c-oob")
    # Model b's run, written down and in flight when its runner stopped (no Worker answered it).
    started = stranded_run(state, b, repos, oob)
    runner.Journal(state / "journal.jsonl").append({
        "event": "start", "profile": "b", "case": "c-oob", "rep": 1, "run_id": started["run_id"],
        "temporal_run_id": started["temporal_run_id"], "budget_micros": BUDGET, "paid": False, "expect": b.expect,
        "commit": oob.commit, "cases_sha256": cs.sha256, "at": 0})

    # Model a's runner leaves it where it is: a's provider never sees b's case.
    first = run(cs, [a], repos, state, runs=1, budget_micros=BUDGET, cap_micros=1_000_000, cases=[cs.case("py-benign")])
    assert [(r["profile"], r["case"]) for r in first] == [("a", "py-benign")]
    assert [json.loads(line)["path"] for line in (tmp_path / "modelsim.jsonl").read_text().splitlines()
            if "/v1/messages" in line] == ["/v1/messages/count_tokens", "/v1/messages"]

    # Model b's runner picks it up, and its own provider answers.
    second = run(cs, [b], repos, state, runs=1, budget_micros=BUDGET, cases=[oob])
    [mine] = [r for r in second if r["run_id"] == started["run_id"]]
    assert mine["resumed"] and mine["models"] == ["local/gpt-oss:20b"] and score.detected(oob, mine)
    assert score.metrics(cs, [mine])["b"]["latency_s"]["runs"] == 0  # its seconds hold the downtime


def test_void_runs_count_on_their_own_and_run_again_when_asked(tmp_path: Path, evalset, modelsim: str) -> None:
    cs, repos, _ = evalset
    # c-oob's provider answers 429 to every attempt (5) of two runs, then a report: c-oob's
    # first run is void, and so is the first run again.
    (tmp_path / "answers.json").write_text(json.dumps([
        {"contains": "format_price", "answers": [{"report": eval_fixture.report()}]},
        {"contains": "parse_record", "answers": [{"fault": "429"}] * 10 + [
            {"report": eval_fixture.report(eval_fixture.finding(21, "CWE-787", file="parse.c"))}]}]))
    a = dataclasses.replace(profiles(modelsim)[0], timeout=120)  # a run agentd lacks would wait this long
    state = tmp_path / "state"
    journal = runner.Journal(state / "journal.jsonl")
    both = [cs.case("py-benign"), cs.case("c-oob"), cs.case("py-benign")]  # a case listed twice runs once
    first = run(cs, [a], repos, state, runs=1, budget_micros=BUDGET, cap_micros=1_000_000, cases=both)
    assert sorted(e["case"] for e in journal.entries() if e["event"] == "start") == ["c-oob", "py-benign"]
    [failed] = [r for r in first if r["case"] == "c-oob"]
    assert score.void(failed) and failed["outcome"].startswith("failed") and failed["spent_micros"] == 0
    m = score.metrics(cs, first)["a"]
    assert (m["void"], m["detection"]["runs"], list(m["void_reasons"])) == (1, 0, [score.void_reason(failed)])

    # Without --retry-void the runner only picks up what is in flight: here a run the journal
    # has and agentd does not (a ledger not the one that started it), which is lost.
    journal.append({"event": "start", "profile": "a", "case": "py-benign", "rep": 2, "run_id": "not-in-the-ledger",
                    "temporal_run_id": "x", "budget_micros": BUDGET, "paid": True})
    picked = run(cs, [a], repos, state, runs=1, budget_micros=BUDGET, cap_micros=1_000_000, cases=both)
    [lost] = [r for r in picked if r["run_id"] == "not-in-the-ledger"]
    assert (lost["ended_by"], lost["state"], score.void_reason(lost)) == ("lost", "NOT_FOUND", "(lost)")
    assert [r for r in picked if r["run_id"] != "not-in-the-ledger"] == first

    # With --retry-void the void run runs again: void again, it does not stop the runner as a
    # first run would; the next time it is answered.
    second = run(cs, [a], repos, state, runs=1, budget_micros=BUDGET, cap_micros=1_000_000, cases=both, retry_void=True)
    [still] = [r for r in second if r["case"] == "c-oob"]
    assert still["run_id"] != failed["run_id"] and score.void(still)
    third = run(cs, [a], repos, state, runs=1, budget_micros=BUDGET, cap_micros=1_000_000, cases=both, retry_void=True)
    [again] = [r for r in third if r["case"] == "c-oob"]
    assert again["run_id"] != still["run_id"] and not score.void(again) and score.detected(cs.case("c-oob"), again)
    # The runs run again still count: in the money, and among the void runs by their reasons.
    m = score.metrics(cs, third, replaced=journal.replaced())["a"]
    reasons = collections.Counter(score.void_reason(r) for r in (failed, still, lost))
    assert (m["void"], m["replaced"], m["void_reasons"]) == (1, 2, dict(reasons))
    assert m["cost_usd"]["total"] == pytest.approx(sum(r["spent_micros"] for r in [*third, failed, still]) / 1e6)


def test_runs_void_in_a_row_stop_the_runner(tmp_path: Path, evalset, modelsim: str, monkeypatch) -> None:
    # The provider answers the first run, then only 429: a provider gone away, whose requests
    # may each cost a whole reservation, stops the runner after VOID_STREAK void runs in a row.
    cs, repos, _ = evalset
    (tmp_path / "answers.json").write_text(json.dumps([
        {"contains": "format_price", "answers": [{"report": eval_fixture.report()}]},
        {"contains": "parse_record", "answers": [{"fault": "429"}] * 5},
        {"contains": "read_file", "answers": [{"fault": "429"}] * 5}]))
    monkeypatch.setattr(runner, "VOID_STREAK", 2)
    a = dataclasses.replace(profiles(modelsim)[0], concurrency=1)
    state = tmp_path / "state"
    with pytest.raises(RuntimeError, match="2 runs in a row ended without the model"):
        run(cs, [a], repos, state, runs=1, budget_micros=BUDGET, cap_micros=1_000_000,
            cases=[cs.case("py-benign"), cs.case("c-oob"), cs.case("py-path"), cs.case("py-sqli")])
    results = runner.Journal(state / "journal.jsonl").results()
    assert [(r["case"], score.void(r)) for r in results] == [("py-benign", False), ("c-oob", True), ("py-path", True)]


def test_a_dead_worker_stops_the_runner_and_the_next_one_carries_on(tmp_path: Path, evalset, modelsim: str) -> None:
    cs, repos, _ = evalset
    (tmp_path / "answers.json").write_text(json.dumps(
        [{"contains": "format_price", "answers": [{"report": eval_fixture.report()}] * 3}]))
    a = dataclasses.replace(profiles(modelsim)[0], timeout=60)  # a runner blind to the death would time out
    state = tmp_path / "state"
    killed = []

    def log(line: str) -> None:
        print(line)
        if "runs to start" in line:  # Temporal holds the state's lock, as the runner does; agentd not
            lock = os.path.realpath(state / "lock")
            for name, holds in (("temporal", True), ("agentd", False)):
                [pid] = [p for p in descendants(os.getpid()) if Path((command_of(p).split() or [""])[0]).name == name]
                assert (lock in open_files(pid)) == holds, (name, open_files(pid))
        if "#1:" in line and not killed:  # the first run is done: kill the Worker (and its sandbox)
            for pid in descendants(os.getpid()):
                if "agentos_worker" in command_of(pid):
                    os.kill(pid, signal.SIGKILL)
                    killed.append(pid)

    # Its runs would only time out: the runner stops instead, and records none of them.
    with pytest.raises(RuntimeError, match="worker1 died"):
        run(cs, [a], repos, state, runs=3, budget_micros=BUDGET, cap_micros=1_000_000, cases=[cs.case("py-benign")],
            log=log)
    assert killed and [r["rep"] for r in runner.Journal(state / "journal.jsonl").results()] == [1]
    again = run(cs, [a], repos, state, runs=3, budget_micros=BUDGET, cap_micros=1_000_000, cases=[cs.case("py-benign")])
    assert sorted(r["rep"] for r in again) == [1, 2, 3] and not any(score.void(r) for r in again)


def test_one_runner_per_state_and_real_models_only_in_a_sandbox(tmp_path: Path, evalset, monkeypatch) -> None:
    cs, repos, _ = evalset
    state = tmp_path / "state"
    state.mkdir()
    b = profiles("http://127.0.0.1:9")[1]
    with open(state / "lock", "w") as held:
        fcntl.flock(held, fcntl.LOCK_EX)
        with pytest.raises(RuntimeError, match="another runner"):
            runner.run(cs, [b], repos, state)
    # A runner killed with SIGKILL leaves its Temporal running, which holds the lock; the refusal
    # names what its stack listed and still runs.
    held = os.open(state / "lock", os.O_RDONLY)
    fcntl.flock(held, fcntl.LOCK_EX)
    (state / "x" / "stack").mkdir(parents=True)
    left = Stack(state / "x" / "stack", fixture=False, hold_fds=(held,))
    os.close(held)
    try:
        with pytest.raises(RuntimeError, match=r"what is left of one.*temporal \(process group \d+\)"):
            runner.run(cs, [b], repos, state)
    finally:
        left.close()
    assert not (state / "x" / "stack" / "pids").exists(), "a stack that stopped lists nothing"
    # On macOS the git tool's sandbox reads what is neither under the home directory nor temporary.
    monkeypatch.setattr(runner, "SANDBOX", "srt")
    with pytest.raises(ValueError, match="on macOS the state directory and the cache"):
        runner.run(cs, [b], repos, Path("/Users/Shared/agentos-eval-test-state"))
    monkeypatch.setattr(runner, "SANDBOX", "none")
    # A journal with runs of a profile whose ledger is gone: what they spent is unknown.
    runner.Journal(state / "journal.jsonl").append({"event": "start", "profile": "gone", "run_id": "x"})
    with pytest.raises(RuntimeError, match="ledgers are gone"):
        runner.run(cs, [b], repos, state)
    (state / "journal.jsonl").unlink()
    monkeypatch.setattr(runner, "SANDBOX", "none")
    with pytest.raises(ValueError, match="sandbox"):
        runner.run(cs, [runner.PROFILES["local"]], repos, state)
    with pytest.raises(ValueError, match="names"):
        runner.run(cs, [dataclasses.replace(profiles("http://127.0.0.1:9")[1], name="../b")], repos, state)


class FakeStack:
    """A stack whose runs end as soon as they start, as SCRIPT says for their case ("ok", or
    "void": a request that got no answer, at a cost of SPEND micros): only the runner's own logic
    (the plan, the streak, going one at a time, overspend) runs, in no time."""

    SCRIPT: dict[str, str] = {}
    SPEND = 0
    OVERSPEND_AFTER: str | None = None  # the case whose collecting puts an overspent run in the ledger
    batches: list[list[str]] = []  # the cases started between two polls

    class Alive:
        def __init__(self, name: str) -> None:
            self.name = name

        def alive(self) -> bool:
            return True

    def __init__(self, tmp: Path, **_kw) -> None:
        self.tmp, self.data_dir = tmp, tmp / "agentd" / "data"
        self.procs, self.workers, self.agentd = [self.Alive("temporal")], [], self.Alive("agentd")
        self.runs: dict[str, str] = {}
        self.cancelled: set[str] = set()
        self.polled = True

    def start_agentd(self, **_kw) -> None:
        fake_ledger(self.data_dir / "ledger.db", {}, [])

    def start_worker(self) -> None:
        self.workers = [self.Alive("worker1")]

    def close(self) -> None:
        pass

    def agentctl(self, *args: str) -> dict:
        case = args[args.index("--commit") + 1]  # the fake case set's commits are their cases' names
        if self.polled:
            FakeStack.batches.append([])
            self.polled = False
        FakeStack.batches[-1].append(case)
        run_id = f"run-{len(self.runs) + 1}"
        self.runs[run_id] = case
        return {"run_id": run_id, "temporal_run_id": "t"}

    def agentctl_run(self, *args: str) -> subprocess.CompletedProcess:
        if args[1] == "cancel":
            self.cancelled.add(args[2])
        self.polled = True
        return subprocess.CompletedProcess(args, 0, json.dumps({"state": self.get_run({"run_id": args[2]})["state"]}), "")

    def get_run(self, run: dict) -> dict:
        case = self.runs[run["run_id"]]
        void = run["run_id"] in self.cancelled or FakeStack.SCRIPT.get(case) == "void"
        return {"state": "CANCELLED" if run["run_id"] in self.cancelled else "SUCCEEDED",
                "result_json": json.dumps({"outcome": "failed: 500" if void else "ok"}),
                "budget_spent_micros": str(FakeStack.SPEND if void else 0), "budget_reserved_micros": "0"}

    def execution_times(self, run: dict) -> tuple[float, float]:
        return 0.0, 1.0

    def ledger(self, sql: str, *params) -> list[dict]:
        if "FROM model_calls" in sql:
            case = self.runs[params[0]]
            if case == FakeStack.OVERSPEND_AFTER:
                fake_ledger(self.data_dir / "ledger.db", {"over": 1}, [("over", "SETTLE", 2, "q")])
            if params[0] in self.cancelled or FakeStack.SCRIPT.get(case) == "void":
                return [{"provider": "local", "model": "m", "prompt_version": "p", "request_blob": None, "response_blob": None,
                         "usage_json": "[]", "stop_reason": None}]
            return [{"provider": "local", "model": "m", "prompt_version": "p", "request_blob": None, "response_blob": "sha256:ok",
                     "usage_json": "[]", "stop_reason": "end_turn"}]
        if "FROM blobs" in sql:
            return [{"bytes": json.dumps({"status": "ok", "model": "local/m", "value": {"findings": []}}).encode()}]
        return []  # runs (none unknown), calls (no patch checks)


@pytest.fixture
def fake(tmp_path: Path, monkeypatch):
    """A case set of benign cases c1..c9 run on FakeStack, and the profiles to run them with."""
    ids = [f"c{i}" for i in range(1, 10)]
    cs = casefile.parse({"projects": {"p": {"url": "https://github.com/example/p", "license": "MIT"}}, "cases": [
        {"id": i, "kind": "benign", "source": "real", "project": "p", "language": "c", "commit": f"{i:0>40}",
         "why": "w"} for i in ids]})
    cs = dataclasses.replace(cs, cases=[dataclasses.replace(c, commit=c.id) for c in cs.cases])
    monkeypatch.setattr(runner, "Stack", FakeStack)
    monkeypatch.setattr(FakeStack, "SCRIPT", {})
    monkeypatch.setattr(FakeStack, "SPEND", 0)
    monkeypatch.setattr(FakeStack, "OVERSPEND_AFTER", None)
    monkeypatch.setattr(FakeStack, "batches", [])
    free = runner.Profile("free", "{}", "", "local/m", paid=False, concurrency=4)
    paid = dataclasses.replace(free, name="paid", paid=True)
    return cs, {i: cs.case(i) for i in ids}, free, paid, tmp_path / "state"


def test_after_a_void_run_the_runs_go_one_at_a_time_until_one_is_answered(fake) -> None:
    cs, c, free, _, state = fake
    FakeStack.SCRIPT = {"c5": "void"}
    runner.run(cs, [free], {}, state, runs=1, cases=[c[i] for i in sorted(c)], log=lambda *_: None)
    # The first alone; four, the last void; one alone, answered; then four again.
    assert [len(b) for b in FakeStack.batches] == [1, 4, 1, 3], FakeStack.batches


def test_a_case_void_every_time_keeps_no_other_from_running_again(fake) -> None:
    cs, c, _, paid, state = fake
    FakeStack.SCRIPT = {i: "void" for i in ("c2", "c3", "c5")}  # never three in a row the first time
    order = [c[i] for i in ("c1", "c2", "c3", "c4", "c5", "c6")]

    def again() -> list[dict]:
        return runner.run(cs, [paid], {}, state, runs=1, cases=order, cap_micros=10**9, retry_void=True,
                          log=lambda *_: None)
    again()
    # Run again, void again, for nothing: none of them stops the runner; all three ran again.
    assert sorted(r["case"] for r in again() if score.void(r)) == ["c2", "c3", "c5"]
    assert len(runner.Journal(state / "journal.jsonl").replaced()) == 3
    # A provider failing again costs money: three of those in a row stop the runner.
    FakeStack.SPEND = 100
    with pytest.raises(RuntimeError, match="3 runs in a row"):
        again()


def test_runs_the_runner_stopped_for_money_do_not_stop_it(fake) -> None:
    cs, c, _, paid, state = fake
    FakeStack.OVERSPEND_AFTER = "c2"  # when c2 is collected, a run has spent past its budget
    results = runner.run(cs, [paid], {}, state, runs=1, cases=[c[i] for i in ("c1", "c2", "c3", "c4", "c5")],
                         cap_micros=10**9, log=lambda *_: None)
    stopped = sorted(r["case"] for r in results if r["ended_by"] == "overspent")
    assert stopped == ["c3", "c4", "c5"], results  # three void in a row, and the runner carried on
