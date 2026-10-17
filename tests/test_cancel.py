"""agentctl runs cancel: a cancelled run ends CANCELLED and posts no report (section 3), and
a workflow cannot get out of a cancel by ignoring it (section 6)."""

from temporalio.api.enums.v1 import EventType

from tests.harness import TERMINAL, TEST_POLICY, Stack, wait_for

# The design doc's own rule for opening the PR: approval required. M1 has no approvals
# table, so the run waits in WAITING_APPROVAL until someone cancels it.
APPROVAL_POLICY = TEST_POLICY.replace(
    "github.open_draft_pr:  {decision: allow, effect: lookup}",
    "github.open_draft_pr:  {decision: require_approval, effect: lookup}",
)


def test_cancel_while_waiting_for_approval_posts_no_report(stack: Stack) -> None:
    assert APPROVAL_POLICY != TEST_POLICY
    stack.start_agentd(policy=APPROVAL_POLICY)
    stack.start_worker()
    run = stack.start_run()
    stack.wait_state(run, {"WAITING_APPROVAL"})
    assert stack.external(run, "github.open_draft_pr") == []  # nothing was opened

    cancelled = stack.agentctl("runs", "cancel", run["run_id"])
    assert cancelled["run_id"] == run["run_id"]
    assert stack.wait_state(run, {"CANCELLED", "SUCCEEDED", "FAILED"})["state"] == "CANCELLED"
    assert stack.external(run, "github.comment_commit") == []


def test_cancel_is_enforced_when_the_workflow_ignores_it(stack: Stack) -> None:
    stack.start_agentd(extra={"cancel_grace": "3s"})
    stack.start_worker(workflows="tests.rogue_workflows:IgnoresCancel")
    run = stack.start_run()
    wait_for(lambda: any(e.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED for e in stack.history(run)),
             "the workflow to start waiting")

    stack.agentctl("runs", "cancel", run["run_id"])
    final = stack.wait_state(run, TERMINAL, timeout=60)
    assert final["state"] == "CANCELLED", final

    # The workflow swallowed the cancel; agentd terminated it once the grace period ran out.
    [ended] = [e for e in stack.history(run) if e.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED]
    assert "cancel not honoured" in ended.workflow_execution_terminated_event_attributes.reason
    # The write it tried after the cancel was refused at the gateway; nothing went out.
    refused = [e for e in stack.agentd_log()
               if e["msg"] == "invoke refused: run cancelled" and e.get("workflow_id") == run["run_id"]]
    assert refused, "expected the gateway to refuse the cancelled run's call"
    assert stack.external(run, "github.comment_commit") == []
