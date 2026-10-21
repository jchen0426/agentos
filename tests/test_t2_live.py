"""T2 once against the real test repository, before release (design doc, section 11): the draft
PR is open on GitHub and agentd is killed before it records the result. When agentd starts
again it finds the PR by its head branch and records it; GitHub ends up with one PR, and one
report on the commit. The second test does the same with the PR waiting for a person's approval
first, as the development policy has it (M4).

Run it by hand, with the fake model (the first test's policy opens the PR without approval, which
only the fake model may do) and the Worker in its sandbox (agentd holds a real token). The token
for the test repository lives in its own keychain item, agentos-github-test-token:
  AGENTOS_GITHUB=live AGENTOS_GITHUB_REPO=<owner>/<name> AGENTOS_SANDBOX=srt uv run pytest tests/test_t2_live.py -s
(-k approval runs the second test alone.) Each test leaves its PR open on the test repository,
for a look; the report comment is public when the repository is.
"""

import json
import subprocess

from tests.harness import REAL_MODEL, SANDBOX, TERMINAL, Stack, approval_policy, wait_for

PR = "github.open_draft_pr"


def test_t2_live_the_pr_was_opened_before_the_result_was_recorded(live_stack: Stack) -> None:
    stack = live_stack
    assert not REAL_MODEL, "a real model must not open real PRs without approval, as the test policy does"
    assert SANDBOX != "none", "agentd holds a real token: run the Worker in its sandbox (AGENTOS_SANDBOX=srt)"
    hold = stack.tmp / "hold"
    hold.touch()
    stack.start_agentd(faults=[{"name": PR, "attempt": 1, "hold_before_record_while_exists": str(hold)}])
    stack.start_worker()
    run = stack.start_run()
    wait_for(lambda: stack.external(run, PR), "the PR to open on GitHub", timeout=120, interval=3)
    stack.restart_agentd(kill=True)  # GitHub has the PR; agentd's ledger does not
    hold.unlink()

    final = stack.wait_state(run, TERMINAL, timeout=240)
    assert final["state"] == "SUCCEEDED", final
    [pr] = stack.external(run, PR)  # opened once
    assert json.loads(final["result_json"])["pr_url"] == pr["url"], final
    assert stack.effect(run, PR)["status"] == "SUCCEEDED"
    assert len(stack.external(run, "github.comment_commit")) == 1
    print(f"\nthe PR: {pr['url']}")


def test_t2_live_with_approval(live_stack: Stack) -> None:
    """The PR waits for a person's approval. The card shows what GitHub holds now, nothing is
    written before the approval, and once it is given the PR opens and survives agentd's death
    before the result is recorded, as above."""
    stack = live_stack
    assert SANDBOX != "none", "agentd holds a real token: run the Worker in its sandbox (AGENTOS_SANDBOX=srt)"
    hold = stack.tmp / "hold"
    hold.touch()
    stack.start_agentd(policy=approval_policy(),
                       faults=[{"name": PR, "attempt": 1, "hold_before_record_while_exists": str(hold)}])
    stack.start_worker()
    run = stack.start_run()
    item = stack.wait_approval(run, timeout=120)
    stack.wait_state(run, {"WAITING_APPROVAL"}, timeout=60)

    # The card is what agentd observed on GitHub, read with the real token: the base branch's tip
    # is the audited commit, and the patch's files are as the audit saw them.
    card = stack.approval(item["id"])
    assert card["status"] == "PENDING" and card["payload"]["base"] == "main", card
    write = card["context"]["write"]
    assert write["commit"] == stack.commit and write["apply_check"] == {"applies": True}, write
    gh = write["github"]
    assert gh["audited_commit"] == {"status": "identical", "behind_by": 0, "ahead_by": 0, "in_base_branch": True}, gh
    assert (gh["files_at_commit"], gh["files_at_tip"]) == ("ok", "ok"), gh
    assert [f["cwe"] for f in card["context"]["task"]["findings"]] == ["CWE-89"], card["context"]["task"]
    assert stack.external(run, PR) == []  # nothing opened before the approval

    # Approved under a name of its own: the report on the public repository names the reviewer.
    approved = stack.decide(item, "approve", "--as", "t2-live")
    assert approved["status"] == "APPROVED", approved
    wait_for(lambda: stack.external(run, PR), "the PR to open on GitHub", timeout=180, interval=3)
    stack.restart_agentd(kill=True)  # GitHub has the PR; agentd's ledger does not
    hold.unlink()

    final = stack.wait_state(run, TERMINAL, timeout=240)
    assert final["state"] == "SUCCEEDED", final
    [pr] = stack.external(run, PR)  # opened once
    assert json.loads(final["result_json"])["pr_url"] == pr["url"], final
    assert stack.effect(run, PR)["status"] == "SUCCEEDED"
    [comment] = stack.external(run, "github.comment_commit")
    body = json.loads(subprocess.run(["gh", "api", f"repos/{stack.live_repo}/comments/{comment['id']}"],
                                     check=True, capture_output=True, text=True).stdout)["body"]
    assert "Approval of github.open_draft_pr: approved\n" in body, body
    assert "by t2-live at" in body, body
    print(f"\nthe PR: {pr['url']}\nthe report: {comment['url']}")
