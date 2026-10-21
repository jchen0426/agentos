"""The case set (design doc, section 11): which commit each case audits and what a correct
audit of it finds.

cases.json holds commit IDs, labels and injection instructions, never the projects' code: the
projects keep their own licenses, and evals/mirrors.py fetches the commits from them. A case is
one of:
  - vuln: a commit that introduces a known vulnerability. Its source says how we have it:
    "blame", the commit that blame on the CVE fix points to; or "reverse", a commit we make by
    reverse-applying the fix's source files on top of the fix.
  - benign: an ordinary commit of the same projects, with nothing to find.
  - injection: a vuln case's commit with text inserted to talk the auditor out of its finding.
    It is paired with that case and scored against the same labels, moved down by the lines
    the text adds.
A label is a file and its lines in the commit's version of the file (after the change), with
the CWE of the vulnerability.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path

CASES = Path(__file__).with_name("cases.json")
SOURCES = {"vuln": {"blame", "reverse"}, "benign": {"real"}, "injection": {"injection"}}
_ID = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")
_SHA = re.compile(r"^[0-9a-f]{40}$")
_CWE = re.compile(r"^CWE-[1-9][0-9]*$")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass(frozen=True)
class Label:
    file: str
    lines: tuple[int, ...]


@dataclass(frozen=True)
class Injection:
    file: str
    line: int  # the text goes in before this line of the base case's commit's version of the file
    text: str  # one or more lines; each gets that line's indentation

    @property
    def added(self) -> int:
        return len(self.text.splitlines())


@dataclass(frozen=True)
class Case:
    id: str
    kind: str  # vuln, benign or injection
    source: str  # blame or reverse (vuln), real (benign), injection
    project: str
    language: str
    commit: str  # the audited commit; for one we make, the ID making it must give ("" until made)
    why: str  # why the diff alone decides it (vuln), what the change is (benign), the injection's ploy
    cwe: str | None = None
    labels: tuple[Label, ...] = ()
    advisory: str | None = None  # CVE or GHSA ID
    published: str | None = None  # the advisory's publication date
    fix: str | None = None  # the commit that fixed it upstream
    revert_paths: tuple[str, ...] = ()  # reverse: the fix's files to take back to before it
    base: str | None = None  # injection: the vuln case it is made from
    inject: Injection | None = None

    @property
    def vulnerable(self) -> bool:
        return self.kind in ("vuln", "injection")


@dataclass(frozen=True)
class Project:
    name: str
    license: str
    base: str  # its default branch, the runs' --base: a dry run sends nothing to it
    url: str | None = None  # fetched from here...
    path: str | None = None  # ...or a repository on this machine (the tests')


@dataclass(frozen=True)
class CaseSet:
    projects: dict[str, Project]
    cases: list[Case]
    cutoffs: dict[str, str] = field(default_factory=dict)  # model -> knowledge cutoff, YYYY-MM-DD
    sha256: str = ""  # of the file, so that a report names the set it scored

    def case(self, case_id: str) -> Case:
        [c] = [c for c in self.cases if c.id == case_id]
        return c

    def pairs(self) -> list[tuple[Case, Case]]:
        """Each injection case with the vuln case it is made from."""
        return [(self.case(c.base), c) for c in self.cases if c.kind == "injection" and c.base]


class CaseError(ValueError):
    pass


def load(path: Path = CASES) -> CaseSet:
    raw = path.read_bytes()
    return parse(json.loads(raw), hashlib.sha256(raw).hexdigest())


def parse(doc: dict, sha256: str = "") -> CaseSet:
    projects = {}
    for name, p in doc["projects"].items():
        if not _ID.match(name) or bool(p.get("url")) == bool(p.get("path")):
            raise CaseError(f"project {name!r}: a lowercase name, and either a url or a path")
        projects[name] = Project(name=name, license=p["license"], base=p.get("base", "main"),
                                 url=p.get("url"), path=p.get("path"))
    raw = {c["id"]: c for c in doc["cases"]}
    if len(raw) != len(doc["cases"]):
        raise CaseError("case IDs must be unique")
    cases = [_case(c, raw, projects) for c in doc["cases"]]
    bases = [c.base for c in cases if c.kind == "injection"]
    if len(bases) != len(set(bases)):
        raise CaseError("a vuln case has at most one injection case")
    return CaseSet(projects=projects, cases=cases, cutoffs=dict(doc.get("cutoffs", {})), sha256=sha256)


def _case(c: dict, raw: dict, projects: dict[str, Project]) -> Case:
    cid, kind, source = c["id"], c["kind"], c.get("source", "")
    where = f"case {cid!r}"
    if not _ID.match(cid):
        raise CaseError(f"{where}: IDs are lowercase letters, digits and dashes")
    if source not in SOURCES.get(kind, ()):
        raise CaseError(f"{where}: kind {kind!r} takes sources {sorted(SOURCES.get(kind, ()))}, not {source!r}")
    commit = c.get("commit", "")
    if commit and not _SHA.match(commit):
        raise CaseError(f"{where}: commit must be a full 40-character SHA")
    if not commit and source not in ("reverse", "injection"):
        raise CaseError(f"{where}: only a commit we make may be missing its ID")
    why = c.get("why", "").strip()
    if not why:
        raise CaseError(f"{where}: say why (field why)")
    if kind == "injection":
        base = raw.get(c.get("base", ""))
        if base is None or base["kind"] != "vuln":
            raise CaseError(f"{where}: base must name a vuln case")
        inj = c.get("inject") or {}
        inject = Injection(file=inj.get("file", ""), line=int(inj.get("line", 0)), text=inj.get("text", ""))
        if not inject.file or inject.line < 1 or not inject.text.strip() or "\r" in inject.text:
            raise CaseError(f"{where}: inject needs a file, a line from 1, and text")
        if not any(lb["file"] == inject.file for lb in base["labels"]):
            raise CaseError(f"{where}: inject into a file the base case's labels name")
        labels = tuple(Label(lb["file"], tuple(n + inject.added if lb["file"] == inject.file and n >= inject.line else n
                                               for n in lb["lines"]))
                       for lb in base["labels"])
        return Case(id=cid, kind=kind, source=source, project=base["project"], language=base["language"],
                    commit=commit, why=why, cwe=base["cwe"], labels=labels, advisory=base.get("advisory"),
                    published=base.get("published"), fix=base.get("fix"), base=base["id"], inject=inject)
    project = c.get("project", "")
    if project not in projects:
        raise CaseError(f"{where}: unknown project {project!r}")
    case = Case(id=cid, kind=kind, source=source, project=project, language=c["language"], commit=commit, why=why,
                cwe=c.get("cwe"), labels=tuple(Label(lb["file"], tuple(lb["lines"])) for lb in c.get("labels", [])),
                advisory=c.get("advisory"), published=c.get("published"), fix=c.get("fix"),
                revert_paths=tuple(c.get("revert_paths", [])))
    if kind == "benign":
        if case.cwe or case.labels or case.fix:
            raise CaseError(f"{where}: a benign case has no CWE, labels or fix")
        return case
    if not (case.cwe and _CWE.match(case.cwe)):
        raise CaseError(f"{where}: cwe must look like CWE-89")
    if not case.labels or any(not lb.file or not lb.lines or min(lb.lines) < 1 for lb in case.labels):
        raise CaseError(f"{where}: labels need a file and lines from 1")
    if case.published is not None and not _DATE.match(case.published):
        raise CaseError(f"{where}: published is YYYY-MM-DD")
    if source == "reverse" and not (case.fix and _SHA.match(case.fix) and case.revert_paths):
        raise CaseError(f"{where}: a reverse case needs its fix commit and the paths to revert")
    return case


def composition(cs: CaseSet) -> dict:
    """What the set is made of, for the report and the checks."""
    vulns = [c for c in cs.cases if c.kind == "vuln"]
    return {
        "vuln": len(vulns),
        "blame": sum(c.source == "blame" for c in vulns),
        "reverse": sum(c.source == "reverse" for c in vulns),
        "benign": sum(c.kind == "benign" for c in cs.cases),
        "injection": sum(c.kind == "injection" for c in cs.cases),
        "languages": sorted({c.language for c in cs.cases}),
        "after_cutoffs": sum(after_cutoff(c, max(cs.cutoffs.values(), default="")) for c in vulns),
    }


def after_cutoff(case: Case, cutoff: str) -> bool:
    """Whether the case's advisory came out after a knowledge cutoff."""
    return bool(cutoff and case.published and case.published > cutoff)
