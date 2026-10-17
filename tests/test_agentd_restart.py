"""agentd restarts while a Worker is connected (the design doc's "守护进程重启" case, in the
M1 form): the Worker keeps reaching Temporal through the new agentd. On Linux this needs the
sandbox to mount the Runtime socket's directory, not the socket file, because agentd
re-creates the socket on start."""

from tests.harness import Stack, TERMINAL


def test_worker_keeps_working_across_an_agentd_restart(stack: Stack) -> None:
    stack.start_agentd()
    worker = stack.start_worker()
    first = stack.start_run()
    assert stack.wait_state(first, TERMINAL)["state"] == "SUCCEEDED"

    stack.restart_agentd()
    second = stack.start_run()
    assert stack.wait_state(second, TERMINAL, timeout=90)["state"] == "SUCCEEDED"
    assert worker.alive()
    assert len(stack.workers) == 1  # the same Worker served both runs
