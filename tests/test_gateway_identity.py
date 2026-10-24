"""The gateway serves only the executions agentd started (design doc, section 7: the caller's
identity comes from Temporal's activity info). Matching the workflow ID is not enough: once a
run has finished, Temporal lets a new execution take the same workflow ID, so the gateway
also checks the run ID that StartRun recorded.

A Worker cannot start such an execution itself (the Runtime proxy refuses child workflows,
see internal/runtimeproxy), so here the trusted test harness starts it directly.

StartRun records the run ID once Temporal has started the run, and a Worker that is already
polling can send the run's first call before that: the gateway waits for the run ID.
"""

import pytest

from tests.harness import Stack, TERMINAL, gateway_calls, wait_for


@pytest.mark.audit_verify(exit=1)
def test_gateway_refuses_an_execution_that_reuses_a_finished_runs_workflow_id(stack: Stack) -> None:
    stack.start_agentd()
    stack.start_worker()
    first = stack.start_run()
    assert stack.wait_state(first, TERMINAL)["state"] == "SUCCEEDED"
    writes_before = [e for e in stack.github_requests() if e["method"] == "POST"]

    other = stack.start_workflow_directly(first["run_id"], {
        "repo": "acme/payments", "commit": stack.commit, "base_branch": "main"})
    assert other and other != first["temporal_run_id"]
    wait_for(lambda: stack.execution_closed(first["run_id"], other), "the second execution to end", timeout=60)

    refused = [e for e in stack.agentd_log()
               if e["msg"] == "invoke from an unknown run" and e.get("workflow_id") == first["run_id"]]
    assert refused and all(e["run_id"] == other for e in refused), refused
    # Nothing ran under the finished run's name.
    assert [e for e in stack.github_requests() if e["method"] == "POST"] == writes_before


def test_a_runs_first_call_waits_for_the_run_id(stack: Stack) -> None:
    # agentd records the run ID 2 s after Temporal has started the run, and the Worker is
    # already polling: the run's first call comes before the run ID. Refused, it would come
    # back as attempt 2; waited for, it keeps attempt 1, which the fault holds.
    hold = stack.tmp / "hold"
    hold.touch()
    stack.start_agentd(faults=[{"name": "git.show_diff", "attempt": 1, "hold_while_exists": str(hold)}],
                       run_id_delay="2s")
    stack.start_worker()
    run = stack.start_run()

    def logged(msg: str) -> list[dict]:
        return [e for e in stack.agentd_log() if e["msg"] == msg and e.get("workflow_id") == run["run_id"]]
    [started] = wait_for(lambda: logged("invoke started"), "the run's first call to start")
    assert (started["name"], started["attempt"]) == ("git.show_diff", 1), started
    assert [e["attempt"] for e in logged("invoke waited for the run ID")] == [1], \
        "the first call came after the run ID was recorded: nothing raced"
    assert stack.get_run(run)["state"] == "RUNNING"

    hold.unlink()
    assert stack.wait_state(run, TERMINAL)["state"] == "SUCCEEDED"
    assert not logged("invoke refused: the run ID is still not recorded")
    # Temporal's record: the call started once, as attempt 1, with no failed attempt before it.
    [call] = [c for c in gateway_calls(stack.history(run)) if c["name"] == "git.show_diff"]
    started_event = call["started"].activity_task_started_event_attributes
    assert started_event.attempt == 1 and not started_event.HasField("last_failure"), started_event
