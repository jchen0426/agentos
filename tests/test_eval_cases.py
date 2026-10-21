"""The case set (evals/cases.json) and the mirrors its cases are audited in (evals/mirrors.py)."""

import json
import re
import subprocess
from pathlib import Path

import pytest

from evals import __main__ as cli
from evals import cases as casefile
from evals import mirrors
from evals.cases import CaseError, parse
from tests import eval_fixture

# The commits evals/mirrors.py makes from tests/eval_fixture.py's repositories: fixed authors and
# dates make the same IDs anywhere.
MADE = {"py-path": "f87aacae121aba58b8669881f07e6cd905fafffa", "py-sqli-inj": "03ca12995a55e68c033249e9622b6c92634e816c"}
PERMISSIVE = {"MIT", "BSD-2-Clause", "BSD-3-Clause", "Apache-2.0", "ISC", "Zlib", "curl", "PSF-2.0", "Unlicense",
              "0BSD", "MIT-0", "BSL-1.0", "libtiff", "X11"}


def build(root: Path, cache: Path) -> tuple[dict, dict]:
    doc = eval_fixture.cases_doc(root, eval_fixture.make_repos(root))
    return doc, mirrors.build(parse(doc), cache, log=lambda *_: None)


def git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, text=True).stdout


def test_made_commits_have_the_same_ids_in_two_places(tmp_path: Path) -> None:
    _, one = build(tmp_path / "one", tmp_path / "cache1")
    _, two = build(tmp_path / "two", tmp_path / "cache2")
    assert {k: v["commit"] for k, v in one.items()} == {k: v["commit"] for k, v in two.items()}
    assert {k: one[k]["commit"] for k in MADE} == MADE


def test_a_reverse_case_takes_the_fixs_files_back(tmp_path: Path) -> None:
    doc, built = build(tmp_path / "r", tmp_path / "cache")
    shop, made = tmp_path / "r/shop", built["py-path"]["commit"]
    fix = doc["cases"][1]["fix"]
    assert git(shop, "rev-parse", made + "^").strip() == fix
    # Its diff is the fix's, the other way round.
    assert git(shop, "diff", fix, made) == git(shop, "diff", fix, fix + "^", "--", "app/files.py")
    assert built["py-path"]["files"] == ["app/files.py"] and built["py-path"]["changed"] == 4


def test_an_injection_case_is_its_base_commit_plus_the_text(tmp_path: Path) -> None:
    doc, built = build(tmp_path / "r", tmp_path / "cache")
    shop, base, made = tmp_path / "r/shop", built["py-sqli"]["commit"], built["py-sqli-inj"]["commit"]
    assert git(shop, "rev-parse", made + "^").strip() == git(shop, "rev-parse", base + "^").strip()
    diff = git(shop, "diff", base, made)
    added = [line for line in diff.splitlines() if line.startswith("+") and not line.startswith("+++")]
    assert added == ["+    " + eval_fixture.INJECTION], diff  # indented like the line it goes before
    lines = git(shop, "show", f"{made}:app/db.py").splitlines()
    assert lines[12].strip() == eval_fixture.INJECTION and "query =" in lines[13]  # labels 13, 14 moved to 14, 15


@pytest.mark.parametrize("change,message", [
    (lambda d: d["cases"][1].update(commit="0" * 40), "cases.json records"),
    (lambda d: d["cases"][0]["labels"][0].update(lines=[99]), "has 14 lines"),
    (lambda d: d["cases"][1]["labels"][0].update(file="app/db.py"), "does not change the label file"),
    (lambda d: d["cases"][1].update(revert_paths=["app/db.py"]), "does not change app/db.py"),
    (lambda d: d["cases"][4]["inject"].update(line=40), "no line 40"),
])
def test_building_refuses_what_does_not_add_up(tmp_path: Path, change, message) -> None:
    root = tmp_path / "r"
    doc = eval_fixture.cases_doc(root, eval_fixture.make_repos(root))
    change(doc)
    with pytest.raises(RuntimeError, match=message):
        mirrors.build(parse(doc), tmp_path / "cache", log=lambda *_: None)


def test_a_case_may_not_change_a_binary_file(tmp_path: Path) -> None:
    # git diff-tree --text puts a binary file's bytes in the diff the auditor reads.
    root = tmp_path / "r"
    commits = eval_fixture.make_repos(root)
    (root / "shop/logo.png").write_bytes(bytes(range(256)) * 4)
    subprocess.run(["git", "-C", str(root / "shop"), "add", "logo.png"], check=True)
    subprocess.run(["git", "-C", str(root / "shop"), "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "logo"],
                   check=True)
    doc = eval_fixture.cases_doc(root, commits)
    doc["cases"][3]["commit"] = git(root / "shop", "rev-parse", "HEAD").strip()
    with pytest.raises(RuntimeError, match="changes a binary file, logo.png"):
        mirrors.build(parse(doc), tmp_path / "cache", log=lambda *_: None)


def test_the_cache_stays_out_of_the_repository(tmp_path: Path) -> None:
    root = tmp_path / "r"
    doc = eval_fixture.cases_doc(root, eval_fixture.make_repos(root))
    with pytest.raises(ValueError, match="outside the repository"):
        mirrors.build(parse(doc), mirrors.REPO / "build-cache", log=lambda *_: None)


def test_inside_the_repository_however_it_is_spelled(tmp_path: Path) -> None:
    link = tmp_path / "link"
    link.symlink_to(mirrors.REPO / "evals")
    assert mirrors.inside_repo(link / "cache") and mirrors.inside_repo(mirrors.REPO / "docs" / ".." / "x")
    shouted = Path(str(mirrors.REPO).upper())
    if shouted.exists():  # a case-insensitive file system (macOS)
        assert mirrors.inside_repo(shouted / "cache")
    assert not mirrors.inside_repo(tmp_path / "cache") and not mirrors.inside_repo(mirrors.REPO.parent / "x")


def _repo(root: Path, before: dict[str, bytes], after: dict[str, bytes]) -> tuple[str, str]:
    """A repository with two commits: files as `before`, then as `after`. Returns both IDs."""
    root.mkdir(parents=True)
    git(root, "init", "-q", "-b", "main")
    ids = []
    for files in (before, after):
        for name, content in files.items():
            (root / name).write_bytes(content)
        git(root, "add", "-A")
        git(root, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "c")
        ids.append(git(root, "rev-parse", "HEAD").strip())
    return ids[0], ids[1]


def _injected(tmp_path: Path, content: bytes, line: int, text: str, labels: list[int]) -> tuple[bytes, list[int]]:
    """The file an injection case makes from a vuln commit that writes `content`, and its labels."""
    _, vuln = _repo(tmp_path / "r", {"x.c": b"int a;\n"}, {"x.c": content})
    doc = {"projects": {"p": {"path": str(tmp_path / "r"), "license": "MIT"}},
           "cases": [{"id": "v", "kind": "vuln", "source": "blame", "project": "p", "language": "c", "commit": vuln,
                      "cwe": "CWE-787", "labels": [{"file": "x.c", "lines": labels}], "why": "w"},
                     {"id": "i", "kind": "injection", "source": "injection", "base": "v",
                      "inject": {"file": "x.c", "line": line, "text": text}, "why": "w"}]}
    built = mirrors.build(parse(doc), tmp_path / "cache", log=lambda *_: None)
    out = subprocess.run(["git", "-C", str(tmp_path / "r"), "cat-file", "blob", f"{built['i']['commit']}:x.c"],
                         check=True, capture_output=True).stdout
    return out, list(parse({**doc, "cases": [doc["cases"][0], {**doc["cases"][1], "commit": built["i"]["commit"]}]})
                     .case("i").labels[0].lines)


def test_an_injection_keeps_the_files_line_endings_and_moves_the_labels(tmp_path: Path) -> None:
    # Windows line endings, two lines of text, a lone \r inside a line (not a line ending).
    content, labels = _injected(tmp_path, b"int a;\r\n  b = 1;\r\nc\rd;\r\n", 2, "// one\n// two", [2, 3])
    assert content == b"int a;\r\n  // one\r\n  // two\r\n  b = 1;\r\nc\rd;\r\n" and labels == [4, 5]


def test_an_injection_at_the_end_of_a_file_without_a_final_newline(tmp_path: Path) -> None:
    content, labels = _injected(tmp_path, b"int a;\nint b = x[9];", 3, "// three", [2])
    assert content == b"int a;\nint b = x[9];\n// three\n" and labels == [2]


def test_a_case_may_change_at_most_400_lines(tmp_path: Path) -> None:
    _, big = _repo(tmp_path / "r", {"x.c": b"int a;\n"}, {"x.c": b"int a;\n" + b"int b;\n" * 401})
    doc = {"projects": {"p": {"path": str(tmp_path / "r"), "license": "MIT"}},
           "cases": [{"id": "n", "kind": "benign", "source": "real", "project": "p", "language": "c", "commit": big,
                      "why": "w"}]}
    with pytest.raises(RuntimeError, match="changes 401 lines, over 400"):
        mirrors.build(parse(doc), tmp_path / "cache", log=lambda *_: None)


def test_a_commit_at_a_shallow_boundary_is_refused(tmp_path: Path) -> None:
    # Its diff would be its whole tree: a later shallow fetch can leave a case's commit there.
    _, commit = _repo(tmp_path / "r", {"x.c": b"int a;\n"}, {"x.c": b"int b;\n"})
    (tmp_path / "r/.git/shallow").write_text(commit + "\n")
    doc = {"projects": {"p": {"path": str(tmp_path / "r"), "license": "MIT"}},
           "cases": [{"id": "n", "kind": "benign", "source": "real", "project": "p", "language": "c", "commit": commit,
                      "why": "w"}]}
    with pytest.raises(RuntimeError, match="exactly one parent"):
        mirrors.build(parse(doc), tmp_path / "cache", log=lambda *_: None)


def test_a_project_url_is_github_and_nothing_else() -> None:
    good = {"id": "n", "kind": "benign", "source": "real", "project": "p", "language": "c", "commit": "a" * 40,
            "why": "w"}
    for url in ("--upload-pack=touch /tmp/x;true", "https://github.com/a/b --x", "http://github.com/a/b",
                "https://github.com/a/b/c", "ext::sh -c x", "https://evil.example/a/b", "file:///tmp/r"):
        with pytest.raises(CaseError, match="url"):
            parse({"projects": {"p": {"url": url, "license": "MIT"}}, "cases": [good]})
    assert parse({"projects": {"p": {"url": "https://github.com/a-b/c.d_e", "license": "MIT"}}, "cases": [good]})


def test_the_case_table_names_each_case_and_why(tmp_path: Path) -> None:
    root = tmp_path / "r"
    path = tmp_path / "cases.json"
    path.write_text(json.dumps(eval_fixture.cases_doc(root, eval_fixture.make_repos(root))))
    assert cli.main(["--cases", str(path), "mirrors", "--cache", str(tmp_path / "cache"), "--record"]) == 0
    assert all(re.fullmatch(r"[0-9a-f]{40}", c["commit"]) for c in json.loads(path.read_text())["cases"])
    out = tmp_path / "table.md"
    assert cli.main(["--cases", str(path), "table", "--cache", str(tmp_path / "cache"), "--out", str(out)]) == 0
    table = out.read_text()
    assert "| py-sqli | 漏洞（blame 找到的引入提交） | shop（Apache-2.0） |" in table
    assert f"{MADE['py-path'][:12]}（反向应用" in table and "app/files.py:7,8" in table
    assert f"{MADE['py-sqli-inj'][:12]}（py-sqli 加注入：app/db.py 第 13 行前）" in table and "app/db.py:14,15" in table
    assert "| 6 | A comment claims" in table  # the injection case's diff: py-sqli's 5 lines and the comment


def test_the_cli_spends_nothing_without_a_cap_or_unsandboxed(tmp_path: Path, monkeypatch, capsys) -> None:
    path = tmp_path / "cases.json"
    path.write_text(json.dumps(eval_fixture.cases_doc(tmp_path, {k: "a" * 40 for k in ("shop1", "shop2", "shop3",
                                                                                       "cparse1")})))
    monkeypatch.setattr(cli, "SANDBOX", "srt")
    with pytest.raises(SystemExit):
        cli.main(["--cases", str(path), "run", "--profile", "claude"])
    assert "needs --cap-usd" in capsys.readouterr().err
    monkeypatch.setattr(cli, "SANDBOX", "none")
    with pytest.raises(SystemExit):
        cli.main(["--cases", str(path), "run", "--profile", "local"])
    assert "AGENTOS_SANDBOX" in capsys.readouterr().err


def test_case_file_errors() -> None:
    project = {"p": {"url": "https://github.com/example/p", "license": "MIT"}}
    good = {"id": "v", "kind": "vuln", "source": "blame", "project": "p", "language": "c", "commit": "a" * 40,
            "cwe": "CWE-787", "labels": [{"file": "x.c", "lines": [1]}], "why": "w"}
    for bad in ({"source": "reverse"}, {"cwe": "787"}, {"labels": []}, {"labels": [{"file": "x.c", "lines": [0]}]},
                {"commit": "abc"}, {"commit": ""}, {"why": " "}, {"project": "q"}, {"id": "Bad_ID"},
                {"published": "2026/01/01"}, {"kind": "benign", "source": "real"}):
        with pytest.raises(CaseError):
            parse({"projects": project, "cases": [{**good, **bad}]})
    with pytest.raises(CaseError, match="unique"):
        parse({"projects": project, "cases": [good, good]})
    assert parse({"projects": project, "cases": [good]}).case("v").labels[0].lines == (1,)


# --- the case set itself -------------------------------------------------------------------------

@pytest.fixture(scope="module")
def caseset() -> casefile.CaseSet:
    return casefile.load()


def test_the_case_set_is_the_agreed_one(caseset) -> None:
    comp = casefile.composition(caseset)
    assert (comp["vuln"], comp["benign"], comp["injection"]) == (24, 12, 12), comp
    assert comp["blame"] >= 12, "at least half the vulnerabilities from real introducing commits"
    assert 4 <= len(comp["languages"]) <= 5, comp["languages"]
    assert comp["after_cutoffs"] >= 6, "a subset published after every model's knowledge cutoff"
    # Each injection case has a vuln case of its own.
    assert len({base.id for base, _ in caseset.pairs()}) == 12


def test_the_case_set_records_ids_and_sources(caseset) -> None:
    for c in caseset.cases:
        assert re.fullmatch(r"[0-9a-f]{40}", c.commit), f"{c.id}: no commit ID (python -m evals mirrors --record)"
        if c.kind == "vuln":
            assert c.advisory and c.published and c.fix, c.id
            assert re.fullmatch(r"(CVE-\d{4}-\d{4,}|GHSA(-[23456789cfghjmpqrvwx]{4}){3})", c.advisory), c.advisory
    for p in caseset.projects.values():
        assert p.url and p.url.startswith("https://github.com/"), p
        assert p.license in PERMISSIVE, p
    assert set(caseset.cutoffs) == {"anthropic/claude-opus-5-5", "openai/gpt-6.1-sol", "local/gpt-oss:20b"}


def test_the_case_set_holds_no_upstream_code(caseset) -> None:
    # Only IDs, labels, our own injection text and our own words: no line longer than a sentence
    # of ours, nothing that looks like a diff.
    text = casefile.CASES.read_text()
    assert "\n+" not in text and "@@ " not in text
    for c in caseset.cases:
        assert len(c.why) < 600, c.id
