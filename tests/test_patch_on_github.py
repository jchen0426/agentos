"""github.open_draft_pr checks on GitHub, before it writes anything, that the run's commit is in
the base branch's history and that the files its patch touches are what the patch was made for
(design doc, sections 3 and 6): the git tool server resolved the commit and read the files from
the mirror, and a crafted repository could have subverted it. Here the commit is not in the base
branch, or GitHub and the mirror disagree about the file the fake model's patch fixes. And the
pull request's commit is the patch on the tip of the base branch, not on the audited commit."""

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


def test_the_pull_request_is_built_on_the_tip_of_the_base_branch(stack: Stack) -> None:
    # main has moved on from the audited commit: a fork's pull request, merged with its own
    # commits, added a workflow in one commit and took it out in the next, and the operator
    # audits the first (following git blame, say). A branch on that commit would run the
    # workflow when pushed; the pull request's commit is the patch on main's tip instead.
    mirror = stack.tmp / "mirror"
    (mirror / ".github" / "workflows").mkdir(parents=True)
    (mirror / ".github" / "workflows" / "evil.yml").write_text("on: push\n")
    git(mirror, "add", ".github")
    git(mirror, "commit", "-q", "-m", "a contribution")
    planted = git(mirror, "rev-parse", "HEAD")
    git(mirror, "rm", "-q", "-r", ".github")
    git(mirror, "commit", "-q", "-m", "take it out")
    tip = git(mirror, "rev-parse", "HEAD")
    stack.start_agentd()
    stack.start_worker()
    run = stack.agentctl("runs", "start", "security-audit", "--repo", "acme/payments", "--commit", planted[:7],
                         "--base", "main")
    final = stack.wait_state(run, TERMINAL)
    assert final["state"] == "SUCCEEDED", final
    [pr] = stack.external(run, PR)
    assert json.loads(final["result_json"])["pr_url"].endswith(f"/pull/{pr['number']}"), final
    # The branch's one commit: the patch's tree, made on the tip's tree, on the tip.
    [commit] = [e for e in stack.github_requests("create_commit") if e["status"] == 201]
    [tree] = [e for e in stack.github_requests("create_tree") if e["status"] == 201]
    assert commit["parents"] == [tip] and commit["tree"] == tree["sha"], commit
    assert tree["base_tree"] == git(mirror, "rev-parse", f"{tip}^{{tree}}"), tree
