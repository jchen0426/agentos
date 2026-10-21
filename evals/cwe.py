"""The CWE hierarchy behind the lenient detection rate (design doc, section 11).

The strict rate wants the finding's CWE to be the label's. The lenient one also takes a CWE
related to it in MITRE's research view (CWE-1000): an ancestor or a descendant of it (Pillars
left out), a sibling (the same parent), or one of the same family, the nearest Class at or above
a weakness. Siblings and families do not go through a broad node: a Pillar, or a top-level
Class (one right under the Pillars, such as CWE-74 Injection or CWE-610). So CWE-787, CWE-125,
CWE-120 and CWE-119 go together (CWE-119 is under another Class), and CWE-22 and CWE-23 do
(ancestry); CWE-89 and CWE-78 do not, nor CWE-79 and CWE-77 (only CWE-74 joins them), CWE-611
and CWE-601, or CWE-862 and CWE-863 (siblings under the top-level CWE-285). A CWE outside the
view (a category, a view, an ID that does not exist) relates only to itself.

cwe1000.json holds each weakness's abstraction and parents in the view, extracted from MITRE's
CWE list by `python -m evals cwe <cwec_vX.Y.xml>`.
"""

from __future__ import annotations

import functools
import json
import re
from pathlib import Path
from xml.etree import ElementTree

DATA = Path(__file__).with_name("cwe1000.json")
NOTICE = ("Extracted from the CWE List (https://cwe.mitre.org/). Copyright (c) 2006-2026, The MITRE Corporation. "
          "CWE, CWSS, CWRAF, and the CWE logo are trademarks of The MITRE Corporation. Used under the CWE Terms of Use "
          "(https://cwe.mitre.org/about/termsofuse.html), which allow use and copies for research, development and "
          "commercial purposes provided MITRE's copyright designation and the license are reproduced.")
_BARE = re.compile(r"^0*([1-9][0-9]{0,5})$")
# The first "CWE-89" in a string, with any dash (Unicode's too), colon, underscore or space.
_FIRST = re.compile(r"CWE\s?[-_:\u2010-\u2015\u2212 ]?\s?0*([1-9][0-9]{0,5})(?![0-9])", re.IGNORECASE)


def number(value: object) -> int | None:
    """The CWE number in 89, "89", "CWE-89", "cwe_089", "CWE 89", "CWE-89: SQL Injection" or
    "[CWE-89, CWE-564]" (the first); None for anything else. Only the first 64 characters
    count: a model's text can be any length."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if 0 < value < 10**6 else None
    if not isinstance(value, str):
        return None
    s = value.strip()[:64]
    m = _BARE.match(s) or _FIRST.search(s)
    return int(m.group(1)) if m else None


def generate(xml_path: Path) -> dict:
    """Each weakness of the research view, with its abstraction and its parents there. MITRE's
    file has no DTD; one that does is refused, so no entity is ever expanded."""
    ns = {"c": "http://cwe.mitre.org/cwe-7"}
    raw = xml_path.read_bytes()
    if b"<!DOCTYPE" in raw or b"<!ENTITY" in raw:
        raise ValueError(f"{xml_path}: a DTD or an entity; MITRE's CWE list has neither")
    root = ElementTree.fromstring(raw)
    weaknesses = {}
    for w in root.iterfind("c:Weaknesses/c:Weakness", ns):
        parents = sorted({int(r.get("CWE_ID")) for r in w.iterfind("c:Related_Weaknesses/c:Related_Weakness", ns)
                          if r.get("Nature") == "ChildOf" and r.get("View_ID") == "1000"})
        weaknesses[w.get("ID")] = {"abstraction": w.get("Abstraction"), "parents": parents}
    return {"version": root.get("Version"), "date": root.get("Date"), "notice": NOTICE,
            "weaknesses": dict(sorted(weaknesses.items(), key=lambda kv: int(kv[0])))}


@functools.cache
def _view() -> dict[int, tuple[str, tuple[int, ...]]]:
    data = json.loads(DATA.read_text())
    return {int(k): (v["abstraction"], tuple(v["parents"])) for k, v in data["weaknesses"].items()}


def _broad(n: int) -> bool:
    """A Pillar, or a top-level Class: too wide to relate its children to each other."""
    view = _view()
    kind, parents = view.get(n, ("", ()))
    return kind == "Pillar" or (kind == "Class" and all(view.get(p, ("",))[0] == "Pillar" for p in parents))


def _ancestors(n: int) -> set[int]:
    """Every ancestor of n in the view, Pillars left out."""
    view, seen, todo = _view(), set(), [n]
    while todo:
        for p in view.get(todo.pop(), ("", ()))[1]:
            if p not in seen and view.get(p, ("",))[0] != "Pillar":
                seen.add(p)
                todo.append(p)
    return seen


def _parents(n: int) -> set[int]:
    return {p for p in _view().get(n, ("", ()))[1] if not _broad(p)}


def _family(n: int) -> set[int]:
    """The nearest Classes at or above n, unless they are broad; n itself when there is none."""
    view = _view()
    found, todo, seen = set(), [n], {n}
    while todo:
        m = todo.pop()
        kind, parents = view.get(m, ("", ()))
        if kind == "Class":
            if not _broad(m):
                found.add(m)
            continue
        for p in parents:  # a Pillar has no parents: the walk ends there without a Class
            if p not in seen:
                seen.add(p)
                todo.append(p)
    return found or {n}


def related(a: object, b: object) -> bool:
    """Whether two CWEs are the same, or related in the research view (the module comment)."""
    x, y = number(a), number(b)
    if x is None or y is None:
        return False
    if x == y:
        return True
    return (x in _ancestors(y) or y in _ancestors(x) or bool(_parents(x) & _parents(y))
            or bool(_family(x) & _family(y)))
