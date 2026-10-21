"""The CWE hierarchy behind the lenient detection rate (design doc, section 11).

The strict rate wants the finding's CWE to be the label's. The lenient one also takes a CWE
related to it in MITRE's research view (CWE-1000): an ancestor or a descendant of it, a sibling
(the same parent), or one of the same family, which is the nearest Class at or above a
weakness. So CWE-787, CWE-125, CWE-120 and CWE-119 go together (Class CWE-119), as do CWE-22 and
CWE-23 (Class CWE-706), and CWE-862 and CWE-863 (siblings under CWE-285); CWE-89 and CWE-78 do
not (Classes CWE-943 and CWE-77), nor CWE-79 and CWE-89. Pillars are too broad to relate
anything. A CWE outside the view (a category, a view, an ID that does not exist) relates only
to itself.

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
_NUMBER = re.compile(r"^\s*(?:CWE\s*[-_: ]?\s*)?0*([1-9][0-9]{0,5})\s*$", re.IGNORECASE)


def number(value: object) -> int | None:
    """The CWE number in "CWE-89", "cwe-089", "CWE 89" or 89; None for anything else."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    m = _NUMBER.match(str(value)) if isinstance(value, str) else None
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
    view = _view()
    return {p for p in view.get(n, ("", ()))[1] if view.get(p, ("",))[0] != "Pillar"}


def _family(n: int) -> set[int]:
    """The nearest Classes at or above n; n itself when there is none."""
    view = _view()
    found, todo, seen = set(), [n], {n}
    while todo:
        m = todo.pop()
        kind, parents = view.get(m, ("", ()))
        if kind == "Class":
            found.add(m)
            continue
        for p in parents:
            if p not in seen and view.get(p, ("",))[0] != "Pillar":
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
    view = _view()
    if x not in view or y not in view or "Pillar" in (view[x][0], view[y][0]):
        return False
    return (x in _ancestors(y) or y in _ancestors(x) or bool(_parents(x) & _parents(y))
            or bool(_family(x) & _family(y)))
