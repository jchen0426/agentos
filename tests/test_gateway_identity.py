"""The gateway serves only the executions agentd started (design doc, section 7: the caller's
identity comes from Temporal's activity info). Matching the workflow ID is not enough: once a
run has finished, Temporal lets a new execution take the same workflow ID, so the gateway
also checks the run ID that StartRun recorded.

A Worker cannot start such an execution itself (the Runtime proxy refuses child workflows,
see internal/runtimeproxy), so here the trusted test harness starts it directly.
"""

from tests.harness import Stack, TERMINAL, wait_for


def test_gateway_refuses_an_execution_that_reuses_a_finished_runs_workflow_id(stack: Stack) -> None:
    stack.start_agentd()
    stack.start_worker()
    first = stack.start_run()
    assert stack.wait_state(first, TERMINAL)["state"] == "SUCCEEDED"
    writes_before = stack.external(first)

    other = stack.start_workflow_directly(first["run_id"], {
        "repo": "acme/payments", "commit": stack.commit, "base_branch": "main"})
    assert other and other != first["temporal_run_id"]
    wait_for(lambda: stack.execution_closed(first["run_id"], other), "the second execution to end", timeout=60)

    refused = [e for e in stack.agentd_log()
               if e["msg"] == "invoke from an unknown run" and e.get("workflow_id") == first["run_id"]]
    assert refused and all(e["run_id"] == other for e in refused), refused
    assert stack.external(first) == writes_before  # nothing ran under the finished run's name
