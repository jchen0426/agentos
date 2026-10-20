"""A write the policy wants approved is never sent unapproved, whenever it was recorded (design doc,
section 10). Here agentd had no GitHub when the run's PR was recorded, so the write could only
dry-run and asked for no approval; agentd stopped before it settled, and the next agentd has
GitHub. The write waits for a person like any other. (The same holds for a write recorded under a
policy that did not ask for approval: internal/gateway's TestAWriteRecordedWithoutAnApprovalWaitsForOne.)"""

from tests.harness import TERMINAL, Stack, approval_policy, wait_for

PR = "github.open_draft_pr"


def test_a_write_recorded_while_it_could_only_dry_run_waits_for_approval(stack: Stack) -> None:
    hold = stack.tmp / "hold"
    hold.touch()
    stack.start_agentd(policy=approval_policy(), extra={"github": None},
                       faults=[{"name": PR, "attempt": 1, "hold_while_exists": str(hold)}])
    stack.start_worker()
    run = stack.start_run()
    wait_for(lambda: [e for e in stack.effects(run) if e["tool"] == PR], "the PR's write to be recorded")
    assert stack.effect(run, PR)["status"] == "PENDING" and stack.ledger("SELECT * FROM approvals") == []

    stack.agentd.kill()
    stack.start_agentd(policy=approval_policy())  # with GitHub this time
    item = stack.wait_approval(run, timeout=90)
    stack.wait_state(run, {"WAITING_APPROVAL"}, timeout=60)
    assert stack.external(run, PR) == [] and stack.effect(run, PR)["status"] == "AWAITING_APPROVAL"

    stack.decide(item, "approve")
    final = stack.wait_state(run, TERMINAL, timeout=60)
    assert final["state"] == "SUCCEEDED", final
    assert len(stack.external(run, PR)) == 1
