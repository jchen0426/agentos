"""github.open_draft_pr checks on GitHub, before it writes anything, that the run's commit is in
the base branch's history and that the files its patch touches are what the patch was made for
(design doc, sections 3 and 6): the git tool server resolved the commit and read the files from
the mirror, and a crafted repository could have subverted it. Here the commit is not in the base
branch, or GitHub and the mirror disagree about the file the fake model's patch fixes."""

import json
import os
import subprocess
from pathlib import Path

import pytest

from tests.harness import TERMINAL, Stack

PR = "github.open_draft_pr"


@pytest.mark.parametrize("entry", [{"sha": "1" * 40}, {"mode": "100755"}], ids=["other content", "executable"])
def test_a_patch_github_disagrees_with_is_denied(stack: Stack, entry: dict) -> None:
    stack.change_github_entry("db.py", **entry)
    stack.start_agentd()
    stack.start_worker()
    run = stack.start_run()
    final = stack.wait_state(run, TERMINAL)
    # The run ends as it does for any denied write: with the report, and no pull request.
    assert final["state"] == "SUCCEEDED", final
    assert json.loads(final["result_json"]) == {"outcome": "denied", "pr_url": None}, final
    effect = stack.effect(run, PR)
    assert effect["status"] == "DENIED" and effect["reason"].startswith("app/db.py: "), dict(effect)
    # GitHub was asked about the commit and its trees, and nothing was written but the report.
    asked = [e["action"] for e in stack.github_requests() if e["action"] in ("get_commit", "get_tree")]
    assert asked == ["get_commit", "get_tree", "get_tree"], asked
    writes = [e["action"] for e in stack.github_requests() if e["method"] == "POST"]
    assert writes == ["create_comment"], writes


def git(repo: Path, *args: str) -> str:
    env = {**os.environ, "GIT_AUTHOR_NAME": "a contributor", "GIT_AUTHOR_EMAIL": "c@example.com",
           "GIT_COMMITTER_NAME": "a contributor", "GIT_COMMITTER_EMAIL": "c@example.com",
           "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1"}
    return subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, text=True,
                          env=env).stdout.strip()


def test_a_commit_outside_the_base_branch_gets_only_the_report(stack: Stack) -> None:
    # A pull request's head from a fork: the mirror holds it, and GitHub serves it through the
    # repository, but it is not in main's history. A branch on it would put its commits into
    # the repository, where a push runs their CI workflows with the repository's secrets.
    mirror = stack.tmp / "mirror"
    head = git(mirror, "commit-tree", "-p", stack.commit, "-m", "a contribution", f"{stack.commit}^{{tree}}")
    git(mirror, "update-ref", "refs/pull/1/head", head)
    stack.start_agentd()
    stack.start_worker()
    run = stack.agentctl("runs", "start", "security-audit", "--repo", "acme/payments", "--commit", head[:7],
                         "--base", "main")
    final = stack.wait_state(run, TERMINAL)
    # The commit is audited and reported on as any other; only the pull request is denied.
    assert final["state"] == "SUCCEEDED", final
    assert json.loads(final["result_json"]) == {"outcome": "denied", "pr_url": None}, final
    effect = stack.effect(run, PR)
    assert effect["status"] == "DENIED" and f"commit {head} is not in the history of branch main" in effect["reason"], \
        dict(effect)
    # GitHub was asked about the commit, the branch and their comparison, and (to refuse) about
    # the pull request's branch; no tree was listed, and nothing was written but the report.
    asked = [e["action"] for e in stack.github_requests() if e["action"] in ("get_commit", "get_ref", "compare", "get_tree")]
    assert asked == ["get_commit", "get_ref", "compare", "get_ref"], asked
    writes = [(e["action"], e["path"]) for e in stack.github_requests() if e["method"] == "POST"]
    assert writes == [("create_comment", f"/repos/acme/payments/commits/{head}/comments")], writes
