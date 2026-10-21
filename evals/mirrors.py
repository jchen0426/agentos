"""The mirrors the cases are audited in, and the commits we make (design doc, section 11).

Each project gets one mirror: a git repository in a cache outside this repository, holding only
what the cases need, each upstream commit and its parent, fetched shallowly by ID. The commits
we make carry a fixed author, committer and date, as scripts/make-audit-target.sh's do, so
making them anywhere gives the same IDs; cases.json records the IDs and building checks them.
Fetched repositories are data: git only reads and writes objects in them, nothing in them runs,
and git runs with no configuration of the user's or the system's, no attributes, no hooks, and
https as its only protocol.
"""

from __future__ import annotations

import os
import subprocess
import tempfile
from pathlib import Path

from evals.cases import Case, CaseSet

REPO = Path(__file__).resolve().parents[1]
MADE = {  # who makes our commits, and when: fixed, so their IDs reproduce
    "GIT_AUTHOR_NAME": "agentos-eval", "GIT_AUTHOR_EMAIL": "eval@agentos.invalid",
    "GIT_AUTHOR_DATE": "2026-01-01T00:00:00Z",
    "GIT_COMMITTER_NAME": "agentos-eval", "GIT_COMMITTER_EMAIL": "eval@agentos.invalid",
    "GIT_COMMITTER_DATE": "2026-01-01T00:00:00Z",
}
MAX_CHANGED = 400  # lines a case's diff may change (section 11's "只看 diff 就能判断")
MESSAGE = "update"  # of every commit we make: the auditor sees no message, and nothing in it hints
_CONFIG = {  # for every git command here, besides no user or system configuration at all
    "core.attributesFile": os.devnull,  # the XDG attributes file is read even without a config
    "protocol.allow": "never", "protocol.https.allow": "always",
    # A project's local path is someone's repository: none of its hooks or monitors run.
    "core.hooksPath": os.devnull, "core.fsmonitor": "false",
}


def default_cache() -> Path:
    return Path(os.environ.get("AGENTOS_EVAL_CACHE") or Path.home() / ".cache" / "agentos-eval")


def inside_repo(path: Path) -> bool:
    """Whether path is this repository or inside it, however it is spelled: compared by the
    directories themselves, so that neither case (APFS), a firmlink nor a symlink hides it."""
    for q in {Path(os.path.abspath(path)), Path(os.path.realpath(path))}:
        if any(p.exists() and os.path.samefile(p, REPO) for p in [q, *q.parents]):
            return True
    return False


def git(repo: Path, *args: str, stdin: bytes | None = None, env: dict | None = None) -> bytes:
    # No configuration of the user's or the system's, no prompts, no GIT_ variable from outside.
    base = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    base.update({"GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1", "GIT_TERMINAL_PROMPT": "0",
                 "GIT_ATTR_NOSYSTEM": "1", "GIT_LITERAL_PATHSPECS": "1",
                 "GIT_CEILING_DIRECTORIES": str(Path(repo).resolve().parent),
                 "GIT_CONFIG_COUNT": str(len(_CONFIG))})
    for i, (k, v) in enumerate(_CONFIG.items()):
        base[f"GIT_CONFIG_KEY_{i}"], base[f"GIT_CONFIG_VALUE_{i}"] = k, v
    r = subprocess.run(["git", "-C", str(repo), *args], input=stdin, capture_output=True,
                       env={**base, **(env or {})}, timeout=1800)
    if r.returncode != 0:
        raise RuntimeError(f"git {' '.join(args[:3])} in {repo}: {r.stderr.decode(errors='replace').strip()}")
    return r.stdout


def text(repo: Path, *args: str) -> str:
    return git(repo, *args).decode().strip()


def has_commit(repo: Path, sha: str) -> bool:
    try:
        return text(repo, "cat-file", "-t", sha) == "commit"
    except RuntimeError:
        return False


def mirror_of(cs: CaseSet, project: str, cache: Path) -> Path:
    p = cs.projects[project]
    return Path(p.path) if p.path else cache / "mirrors" / project


def build(cs: CaseSet, cache: Path, log=print) -> dict[str, dict]:
    """Fetches what the cases need, makes the commits we make, and checks every case against
    its mirror. Returns, by case ID, the commit and its size: {"commit", "changed", "files"}.
    A made commit whose ID differs from the one cases.json records is an error; one cases.json
    does not record yet is returned for writing there (`python -m evals mirrors --record`)."""
    cache = cache.resolve()
    for what, path in [("the cache", cache), *((f"project {p.name}", Path(p.path)) for p in cs.projects.values() if p.path)]:
        if inside_repo(path):
            raise ValueError(f"{what}, {path}, must be outside the repository: the sandboxed Worker reads it")
    made: dict[str, str] = {}
    for c in [c for c in cs.cases if c.kind != "injection"] + [c for c in cs.cases if c.kind == "injection"]:
        mirror = mirror_of(cs, c.project, cache)
        if c.source in ("blame", "real"):
            _fetch(cs, c.project, mirror, c.commit, log)
            commit = c.commit
        elif c.source == "reverse":
            _fetch(cs, c.project, mirror, c.fix, log)
            commit = _reverse(mirror, c)
        else:
            commit = _inject(mirror, c, made[c.base])
        if c.commit and commit != c.commit:
            raise RuntimeError(f"case {c.id}: made {commit}, cases.json records {c.commit}")
        git(mirror, "update-ref", f"refs/eval/{c.id}", commit)  # keeps it from any gc
        made[c.id] = commit
    # After every fetch: a later one can turn an earlier case's commit into a shallow boundary.
    out = {}
    for c in cs.cases:
        out[c.id] = {"commit": made[c.id], **_check(mirror_of(cs, c.project, cache), c, made[c.id])}
        log(f"{c.id}: {made[c.id][:12]}, {out[c.id]['changed']} lines changed")
    return out


def _fetch(cs: CaseSet, project: str, mirror: Path, sha: str, log) -> None:
    """The commit and its parent, by ID, unless the mirror has both."""
    p = cs.projects[project]
    if not p.url:
        if not has_commit(mirror, sha):
            raise RuntimeError(f"{mirror} has no commit {sha}")
        return
    if not (mirror / ".git").exists():
        mirror.mkdir(parents=True, exist_ok=True)
        git(mirror, "init", "-q", "--object-format=sha1")  # the projects' IDs, whatever git's default
    if has_commit(mirror, sha) and has_commit(mirror, sha + "^"):
        return
    log(f"fetching {sha[:12]} of {p.url}")
    for attempt in range(3):
        try:
            git(mirror, "fetch", "-q", "--no-tags", "--depth", "2", "--", p.url, sha)
            break
        except RuntimeError:
            if attempt == 2:
                raise
    if not has_commit(mirror, sha + "^"):
        raise RuntimeError(f"{p.url}: fetched {sha} without its parent")


def _tree_with(mirror: Path, commit: str, entries: dict[str, tuple[str, str] | None]) -> str:
    """The tree of commit with these paths set to (mode, blob), or removed (None)."""
    with tempfile.TemporaryDirectory() as d:
        env = {"GIT_INDEX_FILE": str(Path(d) / "index")}
        git(mirror, "read-tree", commit, env=env)
        for path, entry in entries.items():
            if entry is None:
                git(mirror, "update-index", "--force-remove", "--", path, env=env)
            else:
                git(mirror, "update-index", "--add", "--cacheinfo", f"{entry[0]},{entry[1]},{path}", env=env)
        return git(mirror, "write-tree", env=env).decode().strip()


def _entry(mirror: Path, commit: str, path: str) -> tuple[str, str] | None:
    """(mode, blob) of a file in a commit, or None."""
    out = git(mirror, "ls-tree", "-z", commit, "--", path).decode()
    if not out:
        return None
    meta, _ = out.rstrip("\0").split("\t", 1)
    mode, kind, sha = meta.split()
    if kind != "blob":
        raise RuntimeError(f"{path} in {commit} is a {kind}, not a file")
    return mode, sha


def _make(mirror: Path, parent: str, tree: str) -> str:
    return git(mirror, "commit-tree", tree, "-p", parent, "-m", MESSAGE, env=MADE).decode().strip()


def _lines(content: bytes) -> list[bytes]:
    """A file's lines as git numbers them: ended by \n alone (a lone \r is in a line)."""
    parts = content.split(b"\n")
    return [p + b"\n" for p in parts[:-1]] + ([parts[-1]] if parts[-1] else [])


def _reverse(mirror: Path, c: Case) -> str:
    """The fix's revert_paths taken back to before the fix, as a commit on top of the fix."""
    entries = {}
    for path in c.revert_paths:
        before, after = _entry(mirror, c.fix + "^", path), _entry(mirror, c.fix, path)
        if before == after:
            raise RuntimeError(f"case {c.id}: the fix does not change {path}")
        entries[path] = before
    return _make(mirror, c.fix, _tree_with(mirror, c.fix, entries))


def _inject(mirror: Path, c: Case, base_commit: str) -> str:
    """The base case's commit with the injection's text inserted: a commit on the base
    commit's parent, so that its diff is the base commit's and the text."""
    inj = c.inject
    assert inj is not None
    entry = _entry(mirror, base_commit, inj.file)
    if entry is None:
        raise RuntimeError(f"case {c.id}: {inj.file} is not in {base_commit}")
    lines = _lines(git(mirror, "cat-file", "blob", entry[1]))
    if inj.line > len(lines) + 1:
        raise RuntimeError(f"case {c.id}: {inj.file} has {len(lines)} lines, no line {inj.line}")
    at = lines[inj.line - 1] if inj.line <= len(lines) else b""
    indent = at[:len(at) - len(at.lstrip(b" \t"))]
    eol = b"\r\n" if at.endswith(b"\r\n") else b"\n"
    added = [(indent + t.encode() if t.strip() else b"") + eol for t in inj.text.splitlines()]
    if inj.line == len(lines) + 1 and lines and not lines[-1].endswith(b"\n"):
        lines[-1] += eol
    content = b"".join(lines[:inj.line - 1] + added + lines[inj.line - 1:])
    blob = git(mirror, "hash-object", "-w", "--stdin", stdin=content).decode().strip()
    parent = text(mirror, "rev-parse", "--verify", base_commit + "^")
    return _make(mirror, parent, _tree_with(mirror, base_commit, {inj.file: (entry[0], blob)}))


def _check(mirror: Path, c: Case, commit: str) -> dict:
    """The commit's size, and the case's labels against it: every labelled line is in the
    commit's version of its file."""
    numstat = text(mirror, "diff-tree", "-r", "--root", "--no-commit-id", "--numstat", commit)
    changed, files = 0, []
    for line in numstat.splitlines():
        added, deleted, path = line.split("\t", 2)
        if "-" in (added, deleted):
            # The auditor gets git's diff with --text: a binary file is pages of noise there.
            raise RuntimeError(f"case {c.id}: {commit} changes a binary file, {path}")
        changed += int(added) + int(deleted)
        files.append(path)
    if changed > MAX_CHANGED:
        raise RuntimeError(f"case {c.id}: {commit} changes {changed} lines, over {MAX_CHANGED}")
    # A shallow boundary has no parent as far as git is concerned, and its diff is its whole tree.
    if len(text(mirror, "rev-list", "--parents", "-n", "1", commit).split()) != 2:
        raise RuntimeError(f"case {c.id}: {commit} must have exactly one parent")
    for lb in c.labels:
        entry = _entry(mirror, commit, lb.file)
        if entry is None:
            raise RuntimeError(f"case {c.id}: label file {lb.file} is not in {commit}")
        n = len(_lines(git(mirror, "cat-file", "blob", entry[1])))
        if max(lb.lines) > n:
            raise RuntimeError(f"case {c.id}: {lb.file} has {n} lines, the label names line {max(lb.lines)}")
        if lb.file not in files:
            raise RuntimeError(f"case {c.id}: the commit does not change the label file {lb.file}")
    return {"changed": changed, "files": files}
