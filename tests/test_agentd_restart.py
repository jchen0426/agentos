"""agentd restarts (the design doc's "守护进程重启" case), in both of the Worker's modes.

- agentd's own Worker goes when agentd goes, and the new agentd starts a Worker of its own, which
  serves the next run.
- A Worker run by hand (--no-worker), as in M1, outlives agentd and keeps reaching Temporal through
  the new agentd. On Linux this needs the sandbox to mount the Runtime socket's directory, not the
  socket file, because agentd re-creates the socket on start.
"""

import pytest

from tests.harness import Stack, TERMINAL


@pytest.mark.parametrize("worker", ["agentd's own", "run by hand"])
def test_worker_keeps_working_across_an_agentd_restart(stack: Stack, worker: str) -> None:
    own = worker == "agentd's own"
    stack.start_agentd(worker=own)
    hand = None if own else stack.start_worker()
    before = stack.worker().pid() if own else None
    first = stack.start_run()
    assert stack.wait_state(first, TERMINAL)["state"] == "SUCCEEDED"

    stack.restart_agentd()
    second = stack.start_run()
    assert stack.wait_state(second, TERMINAL, timeout=90)["state"] == "SUCCEEDED"
    if own:  # the first agentd's Worker went with it; the second agentd's, of its own, served the run
        assert stack.worker_processes() == [stack.worker().pid()] != [before]
        assert [e["generation"] for e in stack.worker().log("worker ready")] == [1]
    else:
        assert hand is not None and hand.alive()
        assert len(stack.workers) == 1  # the same Worker served both runs
