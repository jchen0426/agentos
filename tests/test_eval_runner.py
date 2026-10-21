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
import types
from pathlib import Path

import pytest

from evals import __main__ as cli
from evals import cases as casefile
from evals import runner, score
from tests import eval_fixture
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
    def task(model: str) -> dict:
        return {"model": model, "max_tokens": 4096, "output_schema": str(AUDIT_SCHEMA), "blobs": ["patch"]}
    a = runner.Profile("a", json.dumps({"providers": {"anthropic": {"adapter": "anthropic", "base_url": url}},
                                        "tasks": {"audit_v1": task("anthropic/claude-opus-5-5")}}),
                       PRICES, "anthropic/claude-opus-5-5", paid=True)
    b = runner.Profile("b", json.dumps({"providers": {"local": {"adapter": "openai_compat", "api": "chat",
                                                                "base_url": url + "/v1", "context_tokens": 65536}},
                                        "tasks": {"audit_v1": task("local/gpt-oss:20b")}}),
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
        return {"provider": provider, "model": model, "prompt_version": "audit_v1", "response_blob": "sha256:" + blob,
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
    assert "44%（4/9；95% 区间" in md, md
    assert "67%（6/9；区间退化：每个用例都一样）" in md  # every case detected leniently 2 times of 3
    assert "| a（anthropic/claude-opus-5-5） | b（local/gpt-oss:20b） |" in md
    assert "0%（0/3；区间退化：每个用例都一样）" in md and "区间退化：每一对都一样" in md  # model b's
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
    # before c's Worker comes up, so its provider never hears of it.
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


def test_a_dead_worker_stops_the_runner_and_the_next_one_carries_on(tmp_path: Path, evalset, modelsim: str) -> None:
    cs, repos, _ = evalset
    (tmp_path / "answers.json").write_text(json.dumps(
        [{"contains": "format_price", "answers": [{"report": eval_fixture.report()}] * 3}]))
    a = dataclasses.replace(profiles(modelsim)[0], timeout=60)  # a runner blind to the death would time out
    state = tmp_path / "state"
    killed = []

    def log(line: str) -> None:
        print(line)
        if "runs to start" in line:  # Temporal and agentd hold the state's lock, as the runner does
            lock = os.path.realpath(state / "lock")
            for name in ("temporal", "agentd"):
                [pid] = [p for p in descendants(os.getpid()) if Path((command_of(p).split() or [""])[0]).name == name]
                assert lock in open_files(pid), (name, open_files(pid))
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
    # A runner killed with SIGKILL leaves its Temporal (and agentd) running: they hold the lock.
    held = os.open(state / "lock", os.O_RDONLY)
    fcntl.flock(held, fcntl.LOCK_EX)
    (tmp_path / "left").mkdir()
    left = Stack(tmp_path / "left", fixture=False, hold_fds=(held,))
    os.close(held)
    try:
        with pytest.raises(RuntimeError, match="what is left of one"):
            runner.run(cs, [b], repos, state)
    finally:
        left.close()
    # A journal with runs of a profile whose ledger is gone: what they spent is unknown.
    runner.Journal(state / "journal.jsonl").append({"event": "start", "profile": "gone", "run_id": "x"})
    with pytest.raises(RuntimeError, match="ledgers are gone"):
        runner.run(cs, [b], repos, state)
    (state / "journal.jsonl").unlink()
    # The sandbox's list of paths the Worker may not read is separated by ':'.
    with pytest.raises(ValueError, match="':'"):
        runner.run(cs, [b], repos, state, deny_read=[tmp_path / "a:b"])
    assert not (state / "b").exists(), "refused before any stack started"
    monkeypatch.setattr(runner, "SANDBOX", "none")
    with pytest.raises(ValueError, match="sandbox"):
        runner.run(cs, [runner.PROFILES["local"]], repos, state)
    with pytest.raises(ValueError, match="names"):
        runner.run(cs, [dataclasses.replace(profiles("http://127.0.0.1:9")[1], name="../b")], repos, state)
