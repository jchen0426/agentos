"""The evaluation end to end, with simulated models (no money): python -m evals's runner brings
up Temporal, agentd and a sandboxed Worker, audits a tiny case set (tests/eval_fixture.py) in
dry-run under two profiles, and the metrics come out as the scripted answers make them."""

import json
import sqlite3
from pathlib import Path

import pytest

from evals import cases as casefile
from evals import mirrors, report, runner, score
from tests import eval_fixture
from tests.harness import AUDIT_SCHEMA, BIN, Proc, wait_for

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
def evalset(tmp_path: Path) -> tuple[casefile.CaseSet, dict[str, Path]]:
    root = tmp_path / "repos"
    doc = eval_fixture.cases_doc(root, eval_fixture.make_repos(root))
    built = mirrors.build(casefile.parse(doc), tmp_path / "cache", log=lambda *_: None)
    for c in doc["cases"]:
        c["commit"] = built[c["id"]]["commit"]
    return casefile.parse(doc, "test"), {"shop": root / "shop", "cparse": root / "cparse"}


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
    """runner.run, printing the stack's logs if it fails."""
    try:
        return runner.run(*args, **kwargs)
    except BaseException:
        for log in sorted((args[3] / "stack").glob("*.log")):
            print(f"===== {log}\n{log.read_text(errors='replace')[-6000:]}")
        raise


def test_the_metrics_are_what_the_answers_make_them(tmp_path: Path, evalset, modelsim: str) -> None:
    cs, repos = evalset
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

    # Dry runs: every write was only recorded, and none asked for an approval.
    ledger = sqlite3.connect(f"file:{state / 'stack/agentd/data/ledger.db'}?mode=ro", uri=True)
    assert {s for (s,) in ledger.execute("SELECT DISTINCT status FROM effects")} == {"DRY_RUN"}
    assert ledger.execute("SELECT count(*) FROM approvals").fetchone() == (0,)
    assert ledger.execute("SELECT count(*) FROM runs WHERE dry_run = 0").fetchone() == (0,)
    ledger.close()

    m = score.metrics(cs, results)
    a, b = m["a"], m["b"]
    hits = lambda r: (r["hits"], r["runs"])  # noqa: E731
    # The labels are the ledger's, call by call.
    assert a["models"] == ["anthropic/claude-opus-5-5"] and b["models"] == ["local/gpt-oss:20b"]

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
    assert a["patch_applicable"] == {"patches": 2, "applied": 1, "rate": 0.5}
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
    data = report.write(cs, results, tmp_path / "out" / "report", {"title": "test", "commit": "x", "runs": 3})
    md = (tmp_path / "out" / "report.md").read_text()
    assert "44%（4/9；95% 区间" in md and "67%（6/9；95% 区间" in md, md
    assert "| anthropic/claude-opus-5-5 | local/gpt-oss:20b |" in md
    assert "| py-sqli | 漏洞 | blame 找到的引入提交 | python | CWE-89 | 2/2/3 | 0/0/3 |" in md
    raw = (tmp_path / "out" / "report.json").read_text()
    assert "explanation" not in raw and "search_orders" not in raw
    assert data["metrics"]["a"]["detection"]["hits"] == 4


def test_the_cap_stops_paid_runs_and_a_runner_picks_up_where_it_stopped(tmp_path: Path, evalset, modelsim: str) -> None:
    cs, repos = evalset
    (tmp_path / "answers.json").write_text(json.dumps(
        [{"contains": "format_price", "answers": [{"report": eval_fixture.report()}] * 6}]))
    benign = [cs.case("py-benign")]
    a = profiles(modelsim)[0]
    state = tmp_path / "state"
    # Room for one run in flight at a time, with what the runs before it spent: three.
    first = run(cs, [a], repos, state, runs=6, budget_micros=BUDGET, cap_micros=BUDGET + 3 * ANSWER - 1, cases=benign)
    assert [r["rep"] for r in first] == [1, 2, 3], first
    assert sum(r["spent_micros"] for r in first) == 3 * ANSWER
    with pytest.raises(ValueError, match="cap"):
        runner.run(cs, [a], repos, state, runs=6, cases=benign)

    # Again on the same state, with room for the rest: only the three runs not done.
    second = run(cs, [a], repos, state, runs=6, budget_micros=BUDGET, cap_micros=1_000_000, cases=benign)
    assert sorted(r["rep"] for r in second) == [1, 2, 3, 4, 5, 6]
    assert len({r["run_id"] for r in second}) == 6
    assert sum(r["spent_micros"] for r in second) == 6 * ANSWER
