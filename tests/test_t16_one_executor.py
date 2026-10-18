"""T16: two executors handle the same effect; only one claims it and sends its request. One data
directory has one agentd at a time (design doc, section 6), so the executors that can meet are
those of one agentd: an attempt that is stuck, the attempt Temporal starts when its heartbeats
stop, and the recovery a restarted agentd runs. The claim's compare-and-swap itself is tested in
Go, with two executors that bypass the in-process merge (internal/gateway: TestOnlyTheClaimantSends)."""

import subprocess

from tests.harness import BIN, TERMINAL, Stack, gateway_calls, wait_for

PR = "github.open_draft_pr"


def test_t16_a_second_agentd_on_the_same_data_directory_does_not_start(stack: Stack) -> None:
    hold = stack.tmp / "hold"
    hold.touch()
    # The model has answered and agentd holds the answer: its reservation is still open.
    stack.start_agentd(faults=[{"name": "audit_v1", "attempt": 1, "hold_before_record_while_exists": str(hold)}])
    stack.start_worker()
    run = stack.start_run()
    wait_for(lambda: stack.model_answered(run), "the model to answer")

    second = subprocess.run([str(a) for a in stack._agentd_args], capture_output=True, text=True, timeout=30)
    assert second.returncode != 0 and "another agentd is using the data directory" in second.stderr, second.stderr
    assert stack.agentd.alive()
    assert stack.get_run(run)["state"] == "RUNNING"  # the first agentd still has its control socket

    hold.unlink()
    final = stack.wait_state(run, TERMINAL, timeout=60)
    assert final["state"] == "SUCCEEDED", final
    # The second agentd charged nothing: the open reservation was settled by its own request.
    settled = [e for e in stack.budget_entries(run) if e["kind"] == "SETTLE"]
    assert settled and all(e["reason"] == "usage" for e in settled), [dict(e) for e in settled]


def test_t16_a_stuck_attempt_its_retry_and_a_restart_send_once(stack: Stack) -> None:
    hold = stack.tmp / "hold"
    hold.touch()
    # Attempt 1 claims the PR, then sticks without heartbeats: Temporal times it out after 30s
    # and starts attempt 2, which meets the stuck execution.
    stack.start_agentd(faults=[{"name": PR, "attempt": 1, "hold_after_claim_while_exists": str(hold),
                                "no_heartbeat": True}])
    stack.start_worker()
    run = stack.start_run()
    wait_for(lambda: any(e["claimed_by"] for e in stack.effects(run)), "the PR to be claimed", timeout=60)
    wait_for(lambda: [e for e in stack.agentd_log() if e["msg"] == "invoke started" and e.get("name") == PR
                      and e.get("attempt") == 2], "Temporal to start attempt 2", timeout=60)
    # Both die with agentd; the new one recovers the claim while Temporal sends attempt 3.
    stack.restart_agentd(kill=True)
    hold.unlink()

    final = stack.wait_state(run, TERMINAL, timeout=150)
    assert final["state"] == "SUCCEEDED", final
    assert len(stack.external(run, PR)) == 1
    [call] = [c for c in gateway_calls(stack.history(run)) if c["name"] == PR]
    assert call["started"].activity_task_started_event_attributes.attempt >= 3
