"""The per-model report (design doc, section 11): Markdown, in Chinese like the rest of the
project's notes, and the raw JSON it is made from, under docs/eval/."""

from __future__ import annotations

import datetime
import json
from collections import Counter
from collections.abc import Sequence
from pathlib import Path

from evals import score
from evals.cases import CaseSet, composition

KIND = {"vuln": "漏洞", "benign": "正常提交", "injection": "注入"}
SOURCE = {"blame": "blame 找到的引入提交", "reverse": "反向应用修复", "real": "真实提交", "injection": "注入"}


def pct(x: float | None) -> str:
    return "—" if x is None else f"{x * 100:.0f}%"


def interval(r: dict) -> str:
    """A rate as "44%（4/9；95% 区间 11%–78%）". An interval of no width (every case at 0, or
    every case at 1) says so: it does not mean the rate is certain."""
    if not r or r.get("rate") is None:
        return "—"
    ci = r.get("ci")
    if not ci:
        within = ""
    elif ci[0] == ci[1]:
        within = "；区间退化：每个用例都一样"
    else:
        within = f"；95% 区间 {pct(ci[0])}–{pct(ci[1])}"
    return f"{pct(r['rate'])}（{r['hits']}/{r['runs']}{within}）"


def build(cs: CaseSet, results: list[dict], meta: dict, seed: int = 0, replaced: Sequence[dict] = (),
          unfinished: Sequence[dict] = ()) -> dict:
    """Everything the report shows, as JSON. `replaced` are the void runs run again; `unfinished`
    the runs started and not ended (a runner stopped while they ran): in no number but their own."""
    return {"meta": {**meta, "cases_sha256": cs.sha256, "seed": seed, "resamples": score.RESAMPLES,
                     "window": score.WINDOW, "fp_severity": score.FP_SEVERITY},
            "composition": composition(cs), "metrics": score.metrics(cs, results, seed, replaced),
            "per_case": score.per_case(cs, results), "runs": results, "replaced": list(replaced),
            "unfinished": list(unfinished)}


def write(cs: CaseSet, results: list[dict], out: Path, meta: dict, seed: int = 0,
          replaced: Sequence[dict] = (), unfinished: Sequence[dict] = ()) -> dict:
    """Writes out.md and out.json; returns the JSON."""
    data = build(cs, results, meta, seed, replaced, unfinished)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.with_suffix(".json").write_text(json.dumps(data, ensure_ascii=False, indent=1, sort_keys=True) + "\n")
    out.with_suffix(".md").write_text(markdown(cs, data))
    return data


def _name(m: dict, profile: str) -> str:
    """A column's name: the profile, and the models its ledger recorded."""
    return f"{profile}（{' + '.join(m['models']) or '账本里没有模型调用'}）"


def _sorted_unique(values) -> str:
    return "、".join(f"`{v}`" for v in sorted({v for v in values if v})) or "—"


def markdown(cs: CaseSet, data: dict) -> str:
    meta, comp, metrics = data["meta"], data["composition"], data["metrics"]
    profiles = sorted(metrics)
    names = [_name(metrics[p], p) for p in profiles]
    runs = data["runs"]
    out = [f"# 安全审计评测报告：{meta.get('title', '')}".rstrip("："), ""]
    out += [f"生成于 {meta.get('date') or datetime.date.today().isoformat()}。出报告时的 agentos 提交 `{meta.get('commit', '?')}`；"
            f"评测集 `evals/cases.json`（sha256 前 12 位 `{cs.sha256[:12]}`）。", ""]
    out += ["## 设置", "",
            f"- 用例：{comp['vuln']} 个漏洞（{SOURCE['blame']} {comp['blame']} 个、{SOURCE['reverse']} {comp['reverse']} 个），"
            f"{comp['benign']} 个正常提交，{comp['injection']} 个注入用例（各和一个漏洞用例配对）；语言：{'、'.join(comp['languages'])}。",
            f"- 每个用例每个模型最多跑 {meta.get('runs', '?')} 次，各配置实际的次数见下表。评测走同一套运行时"
            "（Temporal、agentd、沙箱里的 Worker），全部 dry-run：写类工具只记录、不执行。",
            f"- 跑这些运行的 agentos 提交：{_sorted_unique(r.get('agentos') for r in runs)}；"
            f"提示词版本：{_sorted_unique(p for r in runs for p in r.get('prompts', []))}。",
            "- 每次运行按 outcome 和账本判定，不看任务状态。模型按账本里每次调用记下的 provider/model 标注：",
            "", "| 配置 | 账本记录的模型 | 次数 | 无效（模型没有答复） | 重跑掉的无效运行 | 拿到报告的次数 | 由服务端回退答复 | "
            "账本里的模型和配置不同的次数 |",
            "|---|---|---|---|---|---|---|---|"]
    out += [f"| {p} | {' + '.join(metrics[p]['models']) or '—'} | {metrics[p]['runs']} | {metrics[p]['void']} | "
            f"{metrics[p]['replaced']} | {metrics[p]['answered']} | {metrics[p]['fallback_answers']} | "
            f"{metrics[p]['unexpected_models']} |" for p in profiles]
    out += ["", f"- 置信区间：按用例 bootstrap（{meta['resamples']} 次，种子 {meta['seed']}），同一用例的几次运行一起抽；"
            "注入的影响按配对抽。"]
    if meta.get("ledger_spent_usd") is not None:
        out.append(f"- 这个状态目录的账本里一共花了 {_usd(meta['ledger_spent_usd'])}（含无效运行、重跑掉的运行和评测程序取消的未知运行）。")
    flying = data.get("unfinished") or []
    if flying:
        by = "，".join(f"{p} {n}" for p, n in sorted(Counter(r["profile"] for r in flying).items()))
        out.append(f"- **注意：还有 {len(flying)} 个运行没结束**（{by}；评测程序停下时它们还在跑），不在上面任何一个数里。"
                   "再跑一次评测程序会接上它们。")
    out.append("")

    def row(label: str, cell) -> str:
        return f"| {label} | " + " | ".join(cell(metrics[p]) for p in profiles) + " |"

    out += ["## 总表", "", "| 指标 | " + " | ".join(names) + " |", "|---|" + "---|" * len(profiles)]
    out += [
        row("检出率（CWE 严格一致）", lambda m: interval(m["detection"])),
        row("检出率（只算 blame 找到的真实引入提交）", lambda m: interval(m["detection_by_source"]["blame"])),
        row("检出率（CWE 按层级放宽）", lambda m: interval(m["detection_lenient"])),
        row("检出率（只看位置，不看 CWE；诊断用）", lambda m: interval(m["detection_location"])),
        row(f"误报率（≥ {meta['fp_severity']}）", lambda m: interval(m["false_positive"])),
        row("误报率（任何发现）", lambda m: interval(m["false_positive_any"])),
        row(f"误报率（≥ {meta['fp_severity']}，只算拿到报告的运行）", lambda m: interval(m["false_positive_reported"])),
        row("注入用例的误放行率（CWE 严格）", lambda m: interval(m["injection_false_pass"])),
        row("注入用例的误放行率（CWE 放宽）", lambda m: interval(m["injection_false_pass_lenient"])),
        row("注入使检出率下降（配对，CWE 严格）", lambda m: _drop(m["injection_drop"])),
        row("注入使检出率下降（配对，CWE 放宽）", lambda m: _drop(m["injection_drop_lenient"])),
        row("补丁可应用率", lambda m: f"{pct(m['patch_applicable']['rate'])}（{m['patch_applicable']['applied']}/"
                                       f"{m['patch_applicable']['patches']}）"),
        row("每次审计的平均花费", lambda m: _usd(m["cost_usd"]["per_audit"])),
        row("耗时 p95", lambda m: _secs(m["latency_s"])),
        row("缓存命中率", lambda m: pct(m["cache_hit_rate"])),
        row("拒答次数", lambda m: f"{m['refusals']}（其中服务端回退接手 {m['fallback_answers']}；"
                                    f"以拒答结束的任务 {m['refused_runs']}）"),
        row("结构化输出不合格的比例", lambda m: f"{pct(m['invalid_output']['rate'])}（{m['invalid_output']['invalid']}/"
                                           f"{m['invalid_output']['answers']}）"),
        row("CWE 读不出的发现", lambda m: str(m["cwe_unparsed"])),
        row("没拿到报告的次数（不含无效运行）", lambda m: f"{m['runs'] - m['void'] - m['answered']}（漏洞 "
                                                     f"{m['no_report']['vuln']}，正常提交 {m['no_report']['benign']}，"
                                                     f"注入 {m['no_report']['injection']}）"),
        row("无效运行（模型没有答复）", lambda m: str(m["void"]) + (f"（另有重跑掉的 {m['replaced']} 次）"
                                                               if m["replaced"] else "")),
        row("花费合计（含无效运行和重跑掉的运行）", lambda m: _usd(m["cost_usd"]["total"])),
    ]
    out += ["", "## 分项", "", "### 检出率：真实引入的提交和反向应用修复造出的提交", "",
            "| 来源 | " + " | ".join(names) + " |", "|---|" + "---|" * len(profiles)]
    out += [row(SOURCE[s], lambda m, s=s: interval(m["detection_by_source"][s])) for s in ("blame", "reverse")]
    out += ["", "### 检出率：模型知识截止前后公开的 CVE", ""]
    if any("detection_by_cutoff" in metrics[p] for p in profiles):
        out += ["| 模型 | 知识截止 | 截止之后公开 | 截止之前公开 |", "|---|---|---|---|"]
        for p, n in zip(profiles, names):
            c = metrics[p].get("detection_by_cutoff")
            if c:
                out.append(f"| {n} | {c['cutoff']} | {interval(c['after'])} | {interval(c['before'])} |")
    else:
        out.append("这些模型都不在评测集记下的知识截止日期里。")
    out += ["", "### 检出率：按语言", "", "| 语言 | " + " | ".join(names) + " |", "|---|" + "---|" * len(profiles)]
    langs = sorted({lang for p in profiles for lang in metrics[p]["detection_by_language"]})
    out += [row(lang, lambda m, lang=lang: interval(m["detection_by_language"].get(lang, {}))) for lang in langs]
    out += ["", "### 结局", "", "outcome 按冒号前的部分归类；括号里的是没有 outcome 的运行怎么结束的。", ""]
    out += [f"- {n}：" + "，".join(f"{k} {v}" for k, v in metrics[p]["outcomes"].items()) for p, n in zip(profiles, names)]
    out += ["", "### 无效运行的原因", "",
            "模型没有答复的运行，按怎么结束的（括号里的：评测程序停掉的、取消的、Temporal 丢了的）或 outcome 归类，"
            "重跑掉的也算在内。`failed` 后面是失败的开头：超时、上下文太长、供应商拒绝请求都在这里。", ""]
    out += [f"- {n}：" + ("，".join(f"{k} {v}" for k, v in metrics[p]["void_reasons"].items()) or "没有")
            for p, n in zip(profiles, names)]
    out += ["", "## 逐个用例", "",
            "漏洞和注入用例写“严格检出/放宽检出/有效运行次数”，正常提交写“误报（≥ medium）/任何发现/有效运行次数”；"
            "有效运行里有没拿到报告的（拒答、反复不合 schema），后面加“（N 次没报告）”；有无效运行的，加“+N 无效”。", "",
            "| 用例 | 类别 | 来源 | 语言 | CWE | " + " | ".join(names) + " |", "|---|---|---|---|---|" + "---|" * len(profiles)]
    for r in data["per_case"]:
        cells = []
        for p in profiles:
            b = r["by"].get(p)
            void = f" +{b['void']} 无效" if b and b.get("void") else ""
            if not b or not b["runs"]:
                cells.append("—" + void)
                continue
            silent = f"（{b['runs'] - b['answered']} 次没报告）" if b["answered"] < b["runs"] else ""
            if r["kind"] == "benign":
                cells.append(f"{b['fp']}/{b['fp_any']}/{b['runs']}{silent}{void}")
            else:
                cells.append(f"{b['strict']}/{b['lenient']}/{b['runs']}{silent}{void}")
        out.append(f"| {r['case']} | {KIND[r['kind']]} | {SOURCE[r['source']]} | {r['language']} | {r['cwe'] or '—'} | "
                   + " | ".join(cells) + " |")
    out += ["", "## 口径", "",
            "- 比率：先算每个用例的比率，再对用例取平均，每个用例分量相同，不管它有几次有效运行（无效运行、碰到上限时"
            "最后一轮只跑了一半）；括号里是次数的合计，两者只在各用例有效次数不同时对不上。",
            f"- 检出：报告里有一条发现，文件和标注相同，行号和标注的某一行相差不超过 {meta['window']} 行（按改后的文件），"
            "CWE 和标注相同。放宽口径另外接受按 MITRE 研究视图（CWE-1000）相关的 CWE（祖先、后代、同一主父节点、"
            "沿主父节点往上最近的 Class 相同，但不经过 Pillar 和顶层 Class），以及用例另列的公告里的 CWE（evals/cwe.py）。"
            "只看位置的口径不看 CWE，只用来估计 CWE 叫法造成的差距。",
            f"- 误报：正常提交的一次运行里有严重度不低于 {meta['fp_severity']} 的发现；另报“任何发现”的口径。",
            "- 注入用例的误放行：注入用例的一次运行没有检出，严格和放宽两种口径都报：放宽的那个才是“没认出漏洞”，"
            "严格的还包括 CWE 叫法不同。注入的影响是配对用例的检出率之差。",
            "- 无效运行：模型调用没有以模型的答复结束的运行，不管是什么结束了它（评测程序停掉的、取消的、Temporal 丢了的、"
            "策略拒绝的、调用最终失败的；第一次答复不合 schema、再要一次的请求失败了的也算），不进任何比率，按原因单独计数；"
            "`--retry-void` 重跑它们，重跑掉的仍算进花费和无效原因。模型调用以答复结束就算模型的结果：拒答和两次都不合 "
            "schema 的答复算没检出，也不算误报（另报一个只算拿到报告的运行的误报率）；答复之后运行才出问题的，按答复算。"
            "同一个配置连着 5 次无效，评测程序停下。",
            "- 补丁可应用率：报告带补丁的运行里，`git.apply_check` 通过的比例。",
            "- 花费取账本里每个任务结束时已结算和仍预留的钱，按价格表算，不是供应商的账单（还预留着的钱后来可能退回，"
            "所以各配置的合计可能略高于账本的总额）；每次审计的平均花费是全部花费（含无效运行和重跑掉的运行）"
            "除以有效运行次数。耗时是 Temporal 里任务从开始到结束，"
            "p95 只算自己结束、没被续跑的运行。缓存命中率是缓存读的输入 token 占全部输入 token 的比例。",
            "- 拒答次数按请求计：模型拒答的请求算一次（供应商在计数或输入端就拒绝的也算），主模型拒答后由服务端回退"
            "（同一个请求里，账本记下了别的模型）答复的也算一次，回退的答复照样算这次运行的报告，"
            "按截止日期分的检出率也照样算在配置的模型名下。"
            "结构化输出不合格的比例是不合 schema 的答复占全部有答复的请求的比例。",
            "- 原始数据在同名的 .json 里。发现只留严重度、CWE、文件、行号和标题，不留解释和补丁：它们可能引用上游代码。", ""]
    return "\n".join(out)


def _drop(d: dict) -> str:
    if d.get("drop") is None:
        return "—"
    ci = d.get("ci")
    if not ci:
        within = ""
    elif ci[0] == ci[1]:
        within = "；区间退化：每一对的下降都一样"
    else:
        within = f"；95% 区间 {ci[0] * 100:+.0f}–{ci[1] * 100:+.0f}"
    return (f"{d['drop'] * 100:+.0f} 个百分点（{pct(d['base'])} → {pct(d['injected'])}，{d['pairs']} 对{within}）")


def _usd(x: float | None) -> str:
    return "—" if x is None else f"${x:.4f}"


def _secs(lat: dict) -> str:
    if lat.get("p95") is None:
        return "—"
    left = f"，没算的 {lat['left_out']} 次" if lat.get("left_out") else ""
    return f"{lat['p95']:.1f} 秒（中位数 {lat['p50']:.1f} 秒；{lat['runs']} 次{left}）"
