"""T4: a run waits for approval while agentd and Temporal restart (design doc, sections 10 and 11).
The approval is agentd's record, not the workflow's: it is still there afterwards, nothing is
opened before a person approves it, and the run goes on once one does. An approval nobody
decides in time expires as a rejection, at the time agentd's record says, not when the
workflow's timer would wake it."""

import json
import time
from datetime import datetime, timezone

from temporalio.api.enums.v1 import EventType

from tests.harness import TERMINAL, Stack, approval_policy, wait_for

PR = "github.open_draft_pr"


def writes(stack: Stack) -> list[dict]:
    """Every write the simulated GitHub took: a branch, a commit, a tree, a pull request."""
    return [e for e in stack.github_requests() if e["method"] == "POST" and e["status"] < 300]


def signals(stack: Stack, run: dict) -> list:
    return [e for e in stack.history(run) if e.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED]


def report(stack: Stack) -> str:
    [comment] = stack.github_comments()
    return comment["body"]


def test_t4_the_approval_outlives_restarts_of_agentd_and_temporal(restartable_stack: Stack) -> None:
    stack = restartable_stack
    stack.start_agentd(policy=approval_policy())
    stack.start_worker()
    run = stack.start_run()
    stack.wait_state(run, {"WAITING_APPROVAL"})
    item = stack.wait_approval(run)
    assert (item["run_id"], item["tool"]) == (run["run_id"], PR), item

    # agentd dies, Temporal restarts, then agentd starts again; the Worker carries on.
    stack.agentd.kill()
    stack.restart_temporal()
    stack.restart_agentd()

    assert stack.wait_state(run, {"WAITING_APPROVAL"})["state"] == "WAITING_APPROVAL"
    [after] = stack.approvals()
    assert (after["id"], after["payload_sha256"]) == (item["id"], item["payload_sha256"]), after
    assert writes(stack) == [] and stack.external(run, PR) == []  # nothing written before the approval

    # The card is agentd's own: what GitHub will receive, and what agentd observed.
    card = stack.approval(item["id"])
    assert card["status"] == "PENDING" and card["payload"]["base"] == "main" and card["payload"]["parent"] == stack.commit
    write, task = card["context"]["write"], card["context"]["task"]
    assert write["commit"] == stack.commit and write["files"] == [{"path": "app/db.py", "change": "modify"}], write
    assert "+    return conn.execute(\"SELECT * FROM users WHERE name = ?\", (name,))" in write["patch"], write
    assert write["apply_check"] == {"applies": True}, write
    gh = write["github"]
    assert gh["audited_commit"] == {"status": "identical", "behind_by": 0, "ahead_by": 0, "in_base_branch": True}, gh
    assert (gh["files_at_commit"], gh["files_at_tip"]) == ("ok", "ok"), gh
    assert [f["cwe"] for f in task["findings"]] == ["CWE-89"], task
    assert int(card["budget_spent_micros"]) > 0 and int(card["budget_limit_micros"]) == 5_000_000, card

    approved = stack.decide(item, "approve", "--as", "alice")
    assert (approved["status"], approved["decided_by"]) == ("APPROVED", "alice"), approved
    final = stack.wait_state(run, TERMINAL, timeout=60)
    assert final["state"] == "SUCCEEDED", final
    [pr] = stack.external(run, PR)  # opened once
    assert json.loads(final["result_json"])["pr_url"].endswith(f"/pull/{pr['number']}"), final
    assert stack.effect(run, PR)["status"] == "SUCCEEDED"
    assert "Approval of github.open_draft_pr: approved" in report(stack)
    # The doorbell woke the run, not its 24-hour timer.
    assert len(signals(stack, run)) == 1 and [d["outcome"] for d in stack.doorbells(run)] == ["delivered"]


def test_t4_an_approval_nobody_gives_expires_as_a_rejection(stack: Stack) -> None:
    stack.start_agentd(policy=approval_policy(ttl="8s"))
    stack.start_worker()
    run = stack.start_run()
    item = stack.wait_approval(run)
    stack.wait_state(run, {"WAITING_APPROVAL"})

    # agentd is not running when the approval's time is up; it expires it when it starts.
    stack.agentd.stop()
    expires = datetime.fromisoformat(item["expires_at"].replace("Z", "+00:00"))
    time.sleep(max(0.0, (expires - datetime.now(timezone.utc)).total_seconds()) + 0.5)
    stack.restart_agentd()

    final = stack.wait_state(run, TERMINAL, timeout=60)
    assert final["state"] == "SUCCEEDED", final  # the report went out
    assert json.loads(final["result_json"]) == {"outcome": "expired", "pr_url": None}, final
    assert stack.external(run, PR) == [] and stack.effect(run, PR)["status"] == "ABANDONED"
    [row] = stack.ledger("SELECT * FROM approvals WHERE approval_id = ?", item["id"])
    assert (row["status"], row["decided_by"]) == ("EXPIRED", "agentd") and "nobody decided" in row["reason"], dict(row)
    body = report(stack)
    assert "Approval of github.open_draft_pr: expired" in body and "nobody decided" in body, body
    # The doorbell woke the run, long before the workflow's own 24-hour timer.
    assert len(signals(stack, run)) == 1

    # Too late now: the expiry was the first decision.
    late = stack.agentctl_run("approvals", "approve", item["id"], "--payload-sha256", item["payload_sha256"])
    assert late.returncode != 0 and "already decided: EXPIRED by agentd" in late.stderr, late.stderr
    assert stack.approvals() == []
    wait_for(lambda: stack.external(run, PR) == [], "nothing to open")
