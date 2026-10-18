"""github.open_draft_pr checks the files its patch touches against GitHub before it writes
anything (design doc, sections 3 and 6): the git tool server read them from the mirror, and a
crafted repository could have subverted it. Here GitHub and the mirror disagree about the file
the fake model's patch fixes."""

import json

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
