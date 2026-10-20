"""T14: agentd records a decision, or a budget extension, and stops before it rings the doorbell
(design doc, sections 5, 10 and 11). The doorbell was written in the same transaction, to the
outbox: the next agentd delivers it, and the run goes on at once, not when its own timer would
wake it (24 hours for an approval, an hour for budget)."""

import json
import time

from temporalio.api.enums.v1 import EventType

from tests.harness import TERMINAL, Stack, approval_policy

PR = "github.open_draft_pr"


def signals(stack: Stack, run: dict) -> int:
    return sum(e.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED for e in stack.history(run))


def held(stack: Stack, run: dict, why: str) -> None:
    """The run's one doorbell is in the outbox, undelivered, and the run still asleep."""
    [bell] = stack.doorbells(run)
    assert (bell["why"], bell["delivered_at"]) == (why, None), dict(bell)
    time.sleep(1)
    assert signals(stack, run) == 0


def delivered(stack: Stack, run: dict) -> dict:
    final = stack.wait_state(run, TERMINAL, timeout=60)
    assert final["state"] == "SUCCEEDED", final
    assert [b["outcome"] for b in stack.doorbells(run)] == ["delivered"] and signals(stack, run) == 1
    return final


def test_t14_an_approvals_doorbell_survives_a_crash(stack: Stack) -> None:
    hold = stack.tmp / "hold-doorbells"
    stack.start_agentd(policy=approval_policy(), hold_doorbells=hold)
    stack.start_worker()
    run = stack.start_run()
    item = stack.wait_approval(run)
    stack.wait_state(run, {"WAITING_APPROVAL"})
    hold.touch()
    stack.decide(item, "approve")
    held(stack, run, "approval")
    assert stack.get_run(run)["state"] == "WAITING_APPROVAL"

    stack.agentd.kill()  # kill -9: the decision is recorded, its doorbell never went out
    hold.unlink()
    stack.restart_agentd()
    final = delivered(stack, run)
    [pr] = stack.external(run, PR)
    assert json.loads(final["result_json"])["pr_url"].endswith(f"/pull/{pr['number']}"), final


def test_t14_a_budget_doorbell_survives_a_crash(stack: Stack) -> None:
    hold = stack.tmp / "hold-doorbells"
    stack.start_agentd(hold_doorbells=hold)
    stack.start_worker()
    run = stack.start_run(budget_micros=1000)  # not enough for the model call's reservation
    stack.wait_state(run, {"PAUSED_BUDGET"})
    hold.touch()
    stack.agentctl("budget", "extend", run["run_id"], "--usd", "1")
    held(stack, run, "budget")

    stack.agentd.kill()
    hold.unlink()
    stack.restart_agentd()
    delivered(stack, run)
