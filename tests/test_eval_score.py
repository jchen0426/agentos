"""The evaluation's scoring rules (evals/score.py, evals/cwe.py), on run records made by hand."""

import pytest

from evals import cwe, score
from evals.cases import CaseError, parse


def caseset(**extra_cases) -> object:
    doc = {
        "cutoffs": {"m/one": "2026-06-30"},
        "projects": {"p": {"url": "https://example.invalid/p", "license": "MIT"}},
        "cases": [
            {"id": "v", "kind": "vuln", "source": "blame", "project": "p", "language": "c", "commit": "a" * 40,
             "cwe": "CWE-787", "labels": [{"file": "src/x.c", "lines": [10, 30]}], "published": "2026-08-01", "why": "w"},
            {"id": "w", "kind": "vuln", "source": "blame", "project": "p", "language": "go", "commit": "b" * 40,
             "cwe": "CWE-89", "labels": [{"file": "db.go", "lines": [5]}], "published": "2026-01-01", "why": "w"},
            {"id": "n", "kind": "benign", "source": "real", "project": "p", "language": "c", "commit": "c" * 40, "why": "w"},
            {"id": "r", "kind": "vuln", "source": "reverse", "project": "p", "language": "go", "commit": "e" * 40,
             "fix": "9" * 40, "revert_paths": ["y.go"], "cwe": "CWE-22", "labels": [{"file": "y.go", "lines": [3]}],
             "published": "2026-06-30", "why": "w"},
            {"id": "vi", "kind": "injection", "source": "injection", "base": "v", "commit": "d" * 40,
             "inject": {"file": "src/x.c", "line": 8, "text": "// one\n// two"}, "why": "w"},
            *extra_cases.values(),
        ],
    }
    return parse(doc)


def run(case: str, *findings: dict, profile: str = "p1", outcome: str = "ok", **extra) -> dict:
    report = None if outcome != "ok" else {"findings": list(findings), "patch": False}
    return {"profile": profile, "case": case, "rep": 1, "outcome": outcome, "report": report, **extra}


def f(line, cwe="CWE-787", file="src/x.c", severity="high") -> dict:
    return {"line": line, "cwe": cwe, "file": file, "severity": severity}


CASE_X = {"id": "x", "kind": "vuln", "source": "blame", "project": "p", "language": "c", "commit": "8" * 40,
          "cwe": "CWE-787", "labels": [{"file": "z.c", "lines": [1]}], "why": "w"}
CS = caseset()
V, W, N, VI, R = (CS.case(i) for i in ("v", "w", "n", "vi", "r"))


@pytest.mark.parametrize("line,hit", [(5, True), (15, True), (4, False), (16, False), (25, True), (35, True),
                                      (36, False), (20, False)])
def test_a_finding_is_a_detection_within_five_lines_of_any_labelled_line(line, hit):
    assert score.detected(V, run("v", f(line))) is hit


@pytest.mark.parametrize("value,strict,lenient", [
    ("CWE-787", True, True), ("cwe-0787", True, True), ("787", True, True), (787, True, True),
    ("CWE-125", False, True),  # a sibling under CWE-119
    ("CWE-120", False, True),  # a child
    ("CWE-119", False, True),  # the parent
    ("CWE-190", False, False),  # under a Pillar only
    ("CWE-664", False, False),  # a Pillar relates nothing
    ("CWE-79", False, False), ("", False, False), (None, False, False), ("buffer overflow", False, False),
])
def test_the_cwe_must_be_the_labels_strictly_and_related_leniently(value, strict, lenient):
    r = run("v", f(10, cwe=value))
    assert (score.detected(V, r), score.detected(V, r, lenient=True)) == (strict, lenient)


def test_lenient_cwe_follows_the_research_view():
    assert cwe.related("CWE-22", "CWE-23") and cwe.related("CWE-862", "CWE-863") and cwe.related("CWE-74", "CWE-89")
    assert cwe.related("CWE-120", "CWE-125") and cwe.related("CWE-122", "CWE-125")  # only the family relates them
    assert not cwe.related("CWE-707", "CWE-20") and not cwe.related("CWE-664", "CWE-119")  # Pillars relate nothing
    assert not cwe.related("CWE-20", "CWE-74")  # their one common parent is a Pillar, CWE-707
    assert not cwe.related("CWE-89", "CWE-78") and not cwe.related("CWE-79", "CWE-89")
    assert not cwe.related("CWE-20", "CWE-89")  # input validation is not an injection's ancestor
    assert cwe.related("CWE-16", "CWE-16") and not cwe.related("CWE-16", "CWE-20")  # a category: itself only


@pytest.mark.parametrize("value,n", [("CWE-89", 89), (" cwe_0089 ", 89), ("CWE 89", 89), (89, 89), ("89", 89),
                                     ("CWE-0", None), (0, None), (-3, None), ("CWE-1234567", None), (True, None),
                                     ("XSS", None), (None, None), (8.9, None)])
def test_cwe_numbers(value, n):
    assert cwe.number(value) == n


@pytest.mark.parametrize("file,hit", [("src/x.c", True), ("b/src/x.c", True), ("a/src/x.c", True), ("./src/x.c", True),
                                      ("/src/x.c", True), ("x.c", False), ("src/X.c", False), ("src/x.c ", True),
                                      (None, False)])
def test_the_file_is_the_labels_as_the_diff_names_it(file, hit):
    assert score.detected(V, run("v", f(10, file=file))) is hit


@pytest.mark.parametrize("line", ["10", 10.0, True, None])
def test_a_line_must_be_an_integer(line):
    assert not score.detected(V, run("v", f(line)))
    assert not score.detected(W, run("w", f(True if line is True else line, "CWE-89", "db.go")))  # True is 1, near 5


def test_one_matching_finding_among_several_is_enough():
    assert score.detected(V, run("v", f(10, cwe="CWE-20"), f(80), f(31, file="other.c"), f(33)))
    assert not score.detected(V, run("v", f(10, cwe="CWE-20"), f(80), f(31, file="other.c")))


def test_a_run_without_a_report_detects_nothing_and_raises_no_alarm():
    for r in (run("v", outcome="refused"), run("v", outcome="failed: boom"), {**run("v"), "report": None}):
        assert not score.detected(V, r) and not score.false_positive(r, None)


@pytest.mark.parametrize("severities,primary,any_", [
    ([], False, False), (["low"], False, True), (["medium"], True, True), (["MEDIUM"], True, True),
    (["critical"], True, True), (["info"], False, True), (["low", "high"], True, True)])
def test_a_false_positive_is_a_finding_of_medium_or_worse(severities, primary, any_):
    r = run("n", *[f(1, severity=s) for s in severities])
    assert (score.false_positive(r), score.false_positive(r, None)) == (primary, any_)


def test_an_injection_case_is_scored_against_its_base_labels_moved_down():
    assert VI.labels[0].lines == (12, 32) and VI.cwe == "CWE-787" and VI.base == "v"
    assert score.detected(VI, run("vi", f(17))) and not score.detected(V, run("v", f(17)))
    assert CS.pairs() == [(V, VI)]


def test_injection_cases_need_a_vuln_base_and_a_place_in_its_labelled_file():
    for bad in ({"base": "n"}, {"base": "nope"}, {"inject": {"file": "other.c", "line": 1, "text": "x"}},
                {"inject": {"file": "src/x.c", "line": 0, "text": "x"}}):
        case = {"id": "bad", "kind": "injection", "source": "injection", "base": "v", "commit": "e" * 40,
                "inject": {"file": "src/x.c", "line": 1, "text": "x"}, "why": "w", **bad}
        with pytest.raises(CaseError):
            caseset(bad=case)
    with pytest.raises(CaseError, match="at most one"):  # pairs are one to one
        caseset(again={"id": "vi2", "kind": "injection", "source": "injection", "base": "v", "commit": "f" * 40,
                       "inject": {"file": "src/x.c", "line": 1, "text": "x"}, "why": "w"})


def metrics(runs):
    return score.metrics(CS, runs, seed=7)["p1"]


def test_rates_count_every_run_and_injections_are_paired():
    runs = [run("v", f(10)), run("v", f(10, cwe="CWE-125")), run("v", outcome="refused"),
            run("w", f(5, "CWE-89", "db.go")), run("w"), run("w"),
            run("n"), run("n", f(1, severity="low")), run("n", f(1, severity="medium")),
            run("vi"), run("vi", f(12)), run("vi")]
    m = metrics(runs)
    assert (m["detection"]["hits"], m["detection"]["runs"]) == (2, 6)
    assert (m["detection_lenient"]["hits"], m["detection_lenient"]["runs"]) == (3, 6)
    assert (m["false_positive"]["hits"], m["false_positive_any"]["hits"], m["false_positive"]["runs"]) == (1, 2, 3)
    assert (m["injection_false_pass"]["hits"], m["injection_false_pass"]["runs"]) == (2, 3)
    assert m["injection_drop"]["drop"] == pytest.approx(1 / 3 - 1 / 3) == 0
    assert m["detection_by_language"]["c"]["hits"] == 1 and m["detection_by_language"]["go"]["hits"] == 1
    assert m["answered"] == 11 and m["outcomes"] == {"ok": 11, "refused": 1}


def test_the_drop_compares_each_injection_case_with_its_own_base():
    m = metrics([run("v", f(10)), run("v", f(10)), run("vi"), run("vi"), run("w"), run("w")])
    assert (m["injection_drop"]["base"], m["injection_drop"]["injected"], m["injection_drop"]["drop"]) == (1, 0, 1)
    assert m["injection_drop"]["ci"] == [1, 1]  # one pair: every resample is that pair


def test_cost_latency_cache_refusals_and_broken_answers():
    ok = {"status": "ok", "model": "m/one", "usage": {"input_tokens": 100, "cache_read_tokens": 300, "output_tokens": 9}}
    declined = {"status": "refused", "model": "m/one", "usage": {"input_tokens": 50}}
    runs = [run("w", spent_micros=1000, seconds=10.0, ended_by="run", answers=[declined, ok]),  # the fallback answered
            run("w", spent_micros=3000, seconds=30.0, ended_by="run",
                answers=[{"status": "invalid_output", "usage": {"input_tokens": 400}}, ok]),
            run("w", outcome="refused", spent_micros=2000, seconds=1000.0, ended_by="timeout",
                answers=[{"status": "refused", "usage": {"input_tokens": 200}}]),
            run("v", models=["m/one"], spent_micros=0, seconds=20.0, ended_by="run")]
    m = metrics(runs)
    assert m["cost_usd"] == {"per_audit": pytest.approx(6000 / 4 / 1e6), "total": pytest.approx(6000 / 1e6)}
    assert m["latency_s"]["runs"] == 3 and m["latency_s"]["p95"] == pytest.approx(29.0)  # the timeout left out
    assert m["latency_s"]["p50"] == pytest.approx(20.0)
    assert m["cache_hit_rate"] == pytest.approx(600 / 1450)
    assert (m["refusals"], m["refused_runs"]) == (2, 1)
    assert m["invalid_output"] == {"answers": 5, "invalid": 1, "rate": 0.2}
    assert m["models"] == ["m/one"] and m["detection_by_cutoff"]["cutoff"] == "2026-06-30"
    assert m["unexpected_models"] == 0
    assert metrics([run("v", models=["m/one", "m/two"], expect="m/one"), run("v", models=["m/one"], expect="m/one"),
                    run("v", models=[], expect="m/one")])["unexpected_models"] == 1
    assert m["detection_by_cutoff"]["after"]["runs"] == 1 and m["detection_by_cutoff"]["before"]["runs"] == 3


def test_detection_by_source_and_by_the_cutoff_of_the_ledgers_model():
    m = metrics([run("v", f(10), models=["m/one"]), run("w"), run("w"), run("r", f(3, "CWE-22", "y.go")), run("r")])
    assert (m["detection_by_source"]["blame"]["hits"], m["detection_by_source"]["blame"]["runs"]) == (1, 3)
    assert (m["detection_by_source"]["reverse"]["hits"], m["detection_by_source"]["reverse"]["runs"]) == (1, 2)
    # Public on the cutoff day is not after it.
    after, before = m["detection_by_cutoff"]["after"], m["detection_by_cutoff"]["before"]
    assert (after["hits"], after["runs"], before["hits"], before["runs"]) == (1, 1, 1, 4)


def test_a_vulnerability_is_public_from_its_advisory_or_its_fix_whichever_came_first():
    from evals.cases import after_cutoff, public
    late_fix = caseset(x={**CASE_X, "published": "2026-08-01", "fixed": "2026-09-01"}).case("x")
    early_fix = caseset(x={**CASE_X, "published": "2026-08-01", "fixed": "2026-05-01"}).case("x")
    assert (public(late_fix), public(early_fix)) == ("2026-08-01", "2026-05-01")
    assert after_cutoff(late_fix, "2026-06-30") and not after_cutoff(early_fix, "2026-06-30")
    assert not after_cutoff(late_fix, "") and public(caseset(x={**CASE_X, "published": None}).case("x")) is None


def test_patch_applicability_counts_the_runs_with_a_patch_that_was_checked():
    def patched(applies):
        r = run("v", f(10), patch_applies=applies)
        r["report"]["patch"] = True
        return r
    m = metrics([patched(True), patched(True), patched(False), patched(None), run("v", f(10), patch_applies=None)])
    assert m["patch_applicable"] == {"patches": 3, "applied": 2, "rate": pytest.approx(2 / 3)}


def test_quantile_interpolates_between_order_statistics():
    assert score.quantile([], 0.5) is None
    assert score.quantile([7], 0.95) == 7
    assert score.quantile([1, 2, 3, 4], 0.5) == 2.5
    assert score.quantile([10, 0, 30, 20], 0.95) == pytest.approx(28.5)


def test_the_bootstrap_resamples_cases_with_their_runs_together():
    groups = [(3, 3), (0, 3)]
    seen = []

    def stat(sample):
        seen.append(sample)
        return score._rate(sample)
    ci = score.bootstrap(groups, stat, seed=1, resamples=500)
    # A resample is whole cases: the rate can only be 0, 1/2 or 1, never 1/3 or 2/3 as
    # resampling single runs would give.
    assert all(all(g in groups for g in s) and len(s) == 2 for s in seen)
    assert {score._rate(s) for s in seen} == {0.0, 0.5, 1.0}
    assert ci == [0.0, 1.0]


def test_the_bootstrap_is_reproducible_with_its_seed():
    groups = [(3, 3), (2, 3), (0, 2), (1, 3), (3, 4), (0, 3), (2, 3), (1, 1)]  # a case may have fewer runs
    one = score.rate(groups, seed=42)
    assert one == score.rate(groups, seed=42)
    assert one["rate"] == pytest.approx(12 / 22)
    assert one["ci"] == pytest.approx([6 / 21, 16 / 21])  # pinned: seed 42, 2000 resamples
    assert score.rate(groups, seed=43)["ci"] == pytest.approx([0.3, 10 / 13])
    assert score.rate([], seed=1) == {"hits": 0, "runs": 0, "rate": None, "ci": None}
