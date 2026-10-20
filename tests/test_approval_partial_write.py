"""An approval holds until its write takes effect (design doc, section 10). github.open_draft_pr
pushes a branch, then opens the pull request: here its branch went out in the approval's time,
GitHub's answer came too late, and the next attempt came after the approval had expired. The
pull request is not opened then, and its record and the run's report say that part of it went
out, rather than that nothing did: the branch stays on GitHub."""

import time
from datetime import datetime, timezone

from tests.harness import TERMINAL, Stack, approval_policy, wait_for

PR = "github.open_draft_pr"


def test_a_pull_request_whose_branch_went_out_expires_and_says_so(stack: Stack) -> None:
    hold_retry = stack.tmp / "hold-retry"
    hold_retry.touch()
    # The call's second attempt waits for the approval's time to run out.
    stack.start_agentd(policy=approval_policy(ttl="20s"),
                       faults=[{"name": PR, "attempt": 2, "hold_while_exists": str(hold_retry)}])
    stack.start_worker()
    run = stack.start_run()
    item = stack.wait_approval(run)
    hold_github = stack.tmp / "hold-create-ref"
    hold_github.touch()
    stack.hold_github("create_ref", hold_github)  # GitHub makes the branch, and answers too late
    stack.decide(item, "approve")

    # agentd's request times out (request_timeout 2s): the tool says this attempt did nothing
    # that a retry would not find, and the effect is given back.
    wait_for(lambda: [e for e in stack.effects(run) if e["tool"] == PR and e["status"] == "PENDING"
                      and e["last_request_at"] is not None and e["claimed_by"] is None] or None,
             "the effect to be given back after its branch went out", timeout=60)
    hold_github.unlink()
    assert len([e for e in stack.github_requests("create_ref") if e["status"] == 201]) == 1
    expires = datetime.fromisoformat(item["expires_at"].replace("Z", "+00:00"))
    time.sleep(max(0.0, (expires - datetime.now(timezone.utc)).total_seconds()) + 1)
    hold_retry.unlink()

    final = stack.wait_state(run, TERMINAL, timeout=90)
    assert final["state"] == "SUCCEEDED", final
    eff = stack.effect(run, PR)
    assert eff["status"] == "ABANDONED" and "had not taken effect" in eff["reason"], dict(eff)
    assert "A request of it went out at" in eff["reason"], dict(eff)
    [approval] = stack.ledger("SELECT status, decided_by, reason FROM approvals WHERE approval_id = ?", item["id"])
    assert approval["status"] == "EXPIRED" and approval["decided_by"] == "agentd", dict(approval)
    assert "A request of it went out at" in approval["reason"], dict(approval)
    # The branch stays, and no pull request was opened after the approval's time.
    assert len([e for e in stack.github_requests("create_ref") if e["status"] == 201]) == 1
    assert not [e for e in stack.github_requests("create_pull") if e["status"] == 201]
    [report] = [c["body"] for c in stack.github_comments()]
    assert "Approval of github.open_draft_pr: expired\n" in report, report
    assert "A request of it went out at" in report, report
