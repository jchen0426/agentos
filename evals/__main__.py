"""python -m evals: build the mirrors, show the case table, run the evaluation, write the report.

  python -m evals mirrors [--record]           fetch the projects, make our commits, check the cases
  python -m evals table [--out FILE]           the case table, for spot checks before a real run
  python -m evals run --profile claude --cap-usd 50 [--state DIR]
                                               run the cases (make build first); a runner on a state
                                               directory picks up where the last one stopped
  python -m evals report [--state DIR] --out docs/eval/NAME
  python -m evals cwe cwec_vX.Y.xml            regenerate evals/cwe1000.json
"""

from __future__ import annotations

import argparse
import datetime
import json
import signal
import subprocess
import sys
from pathlib import Path

from evals import cases as casefile
from evals import cwe, mirrors, report, runner
from evals.cases import CaseSet
from tests.harness import SANDBOX

REPO = Path(__file__).resolve().parents[1]


def table(cs: CaseSet, built: dict[str, dict] | None = None) -> str:
    """The cases as a Markdown table: what each audits, where the vulnerability is, and why the
    diff alone decides it."""
    out = ["# 评测集用例表", "",
           f"`evals/cases.json`，sha256 前 12 位 `{cs.sha256[:12]}`。提交链接指向上游；我们造的提交（反向应用修复、注入）"
           "不在上游，链接的是它所依据的修复提交或漏洞用例。行号按审计的那个提交改后的文件算。"
           "“公开”取公告发布和修复提交中较早的日期：修复一推到 GitHub 就公开了。", ""]
    comp = casefile.composition(cs)
    out += [f"- 漏洞 {comp['vuln']} 个：blame 找到的引入提交 {comp['blame']} 个，反向应用修复 {comp['reverse']} 个；"
            f"正常提交 {comp['benign']} 个；注入用例 {comp['injection']} 个。语言：{'、'.join(comp['languages'])}。",
            "- 知识截止：" + "；".join(f"{m} {d}" for m, d in sorted(cs.cutoffs.items())) +
            f"。截止之后才公开的漏洞 {comp['after_cutoffs']} 个（晚于最晚的那个截止）。", ""]
    out += ["| 用例 | 类别 | 项目（许可证） | 审计的提交 | CWE | 公告（发布 / 修复 / 公开） | 标注 | 改动行数 | 说明 |",
            "|---|---|---|---|---|---|---|---|---|"]
    for c in cs.cases:
        p = cs.projects[c.project]
        url = (p.url or "").removesuffix(".git")
        if c.source in ("blame", "real"):
            commit = f"[{c.commit[:12]}]({url}/commit/{c.commit})" if url else c.commit[:12]
        elif c.source == "reverse":
            commit = f"{(c.commit or '?')[:12]}（反向应用 [{c.fix[:12]}]({url}/commit/{c.fix}) 的 {', '.join(c.revert_paths)}）"
        else:
            commit = f"{(c.commit or '?')[:12]}（{c.base} 加注入：{c.inject.file} 第 {c.inject.line} 行前）"
        labels = "；".join(f"{lb.file}:{','.join(map(str, lb.lines))}" for lb in c.labels) or "—"
        advisory = (f"{c.advisory}（{c.published or '?'} / {c.fixed or '?'} / {casefile.public(c) or '?'}）"
                    if c.advisory else "—")
        size = str(built[c.id]["changed"]) if built and c.id in built else "?"
        why = c.why if not c.inject else f"{c.why}插入的文字：`{c.inject.text}`"
        why = why.replace("|", "\\|").replace("\n", " ")
        cwes = (c.cwe or "—") + (f"（放宽口径另收 {'、'.join(c.cwe_also)}）" if c.cwe_also else "")
        out.append(f"| {c.id} | {report.KIND[c.kind]}（{report.SOURCE[c.source]}） | {c.project}（{p.license}） | {commit} | "
                   f"{cwes} | {advisory} | {labels} | {size} | {why} |")
    return "\n".join(out) + "\n"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m evals", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cases", type=Path, default=casefile.CASES, help="the case set (default: evals/cases.json)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    m = sub.add_parser("mirrors", help="fetch the projects, make our commits, check every case")
    m.add_argument("--cache", type=Path, default=mirrors.default_cache())
    m.add_argument("--record", action="store_true", help="write the IDs of commits we made into cases.json")
    t = sub.add_parser("table", help="the case table, for spot checks")
    t.add_argument("--cache", type=Path, default=mirrors.default_cache())
    t.add_argument("--out", type=Path)
    r = sub.add_parser("run", help="run the cases under each profile (make build first)")
    r.add_argument("--profile", action="append", choices=sorted(runner.PROFILES), required=True)
    r.add_argument("--cap-usd", type=float, help="the most the state directory's runs may spend, every runner on it "
                   "together; needed for paid profiles")
    r.add_argument("--budget-usd", type=float, default=runner.BUDGET / 1e6, help="each run's budget")
    r.add_argument("--runs", type=int, default=3, help="runs of each case under each profile")
    r.add_argument("--case", action="append", help="only these cases")
    r.add_argument("--retry-void", action="store_true", help="run again the runs the model gave no answer")
    r.add_argument("--state", type=Path, help="the state directory (default: CACHE/state/default); a new one is a "
                   "new evaluation, with a cap of its own")
    r.add_argument("--cache", type=Path, default=mirrors.default_cache())
    p = sub.add_parser("report", help="the report of a state directory's runs")
    p.add_argument("--state", type=Path, help="default: CACHE/state/default")
    p.add_argument("--cache", type=Path, default=mirrors.default_cache())
    p.add_argument("--out", type=Path, required=True, help="docs/eval/NAME: writes NAME.md and NAME.json")
    p.add_argument("--title", default="")
    p.add_argument("--seed", type=int, default=0)
    c = sub.add_parser("cwe", help="regenerate evals/cwe1000.json from MITRE's CWE list (XML)")
    c.add_argument("xml", type=Path)
    args = ap.parse_args(argv)

    if args.cmd == "cwe":
        cwe.DATA.write_text(json.dumps(cwe.generate(args.xml), separators=(",", ":")) + "\n")
        return 0
    # A terminal closed or a timeout's SIGTERM: unwind, so that the runner stops what it started.
    for sig in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, lambda signum, _frame: sys.exit(128 + signum))
    for name in ("cache", "state", "out"):
        if getattr(args, name, None) is not None:
            setattr(args, name, getattr(args, name).resolve())
    cs = casefile.load(args.cases)
    if args.cmd == "mirrors":
        built = mirrors.build(cs, args.cache)
        if args.record:
            doc = json.loads(args.cases.read_text())
            for case in doc["cases"]:
                case["commit"] = built[case["id"]]["commit"]
            args.cases.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n")
        return 0
    if args.cmd == "table":
        text = table(cs, mirrors.build(cs, args.cache, log=lambda *_: None))
        if args.out:
            args.out.write_text(text)
        else:
            sys.stdout.write(text)
        return 0
    if args.cmd == "run":
        profiles = [runner.PROFILES[name] for name in args.profile]
        if SANDBOX == "none":
            ap.error("real models run with the Worker in a sandbox: set AGENTOS_SANDBOX=srt (macOS) or bwrap (Linux)")
        if any(pr.paid for pr in profiles) and args.cap_usd is None:
            ap.error("a paid profile needs --cap-usd: the most all runs together may spend")
        built = mirrors.build(cs, args.cache)
        if any(built[case.id]["commit"] != case.commit for case in cs.cases):
            ap.error("cases.json lacks the IDs of commits we make: python -m evals mirrors --record")
        chosen = [cs.case(i) for i in dict.fromkeys(args.case)] if args.case else None
        state = args.state or args.cache / "state" / "default"
        print(f"state: {state}; its runs have spent ${runner.spent_in(state) / 1e6:.4f} so far")
        # Every project's mirror: runs in flight from an earlier runner may audit any of them. The
        # Worker reads nothing of the cache: other state directories, other mirrors.
        runner.run(cs, profiles, {name: mirrors.mirror_of(cs, name, args.cache) for name in cs.projects}, state,
                   runs=args.runs, budget_micros=round(args.budget_usd * 1e6),
                   cap_micros=round(args.cap_usd * 1e6) if args.cap_usd is not None else None, cases=chosen,
                   retry_void=args.retry_void, deny_read=[args.cache])
        return 0
    if args.cmd == "report":
        state = args.state or args.cache / "state" / "default"
        journal = runner.Journal(state / "journal.jsonl")
        results = journal.results()
        if not results:
            ap.error(f"no run has ended in {state}")
        # Each run is scored with today's labels, so it must have audited today's commit of its
        # case; a set changed elsewhere (a description, a label line) needs no new runs.
        ids = {c.id: c.commit for c in cs.cases}
        stale = sorted({r["case"] for r in results if ids.get(r["case"]) != r.get("commit")})
        if stale:
            ap.error(f"cases.json no longer has these cases, or not with the commits their runs audited: {stale}")
        head = subprocess.run(["git", "-C", str(REPO), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
        meta = {"title": args.title, "date": datetime.date.today().isoformat(), "commit": head,
                "runs": max((r["rep"] for r in results), default=0),
                "ledger_spent_usd": runner.spent_in(state) / 1e6}
        report.write(cs, results, args.out, meta, seed=args.seed, replaced=journal.replaced(),
                     unfinished=journal.unfinished())
        print(f"wrote {args.out.with_suffix('.md')} and {args.out.with_suffix('.json')}")
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
