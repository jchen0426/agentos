"""T8: a gateway attempt returns only after its heartbeat timed out. Temporal discards that
late result, and agentd's ledgers hold no duplicate: the external write happened once, and
a model call is neither repeated nor paid twice."""

from datetime import datetime

from temporalio.api.enums.v1 import TimeoutType

from tests.harness import Stack, TERMINAL, gateway_calls


def at(entry: dict) -> datetime:
    return datetime.fromisoformat(entry["time"])


def run_with_stuck_first_attempt(stack: Stack, name: str) -> dict:
    """Attempt 1 of the call sends no heartbeats and takes 45s, past the workflow's 30s
    heartbeat timeout: a stuck gateway attempt. Returns the finished run."""
    stack.start_agentd(faults=[{"name": name, "attempt": 1, "delay": "45s", "no_heartbeat": True}])
    stack.start_worker()
    run = stack.start_run()
    final = stack.wait_state(run, TERMINAL, timeout=150)
    assert final["state"] == "SUCCEEDED", final

    # Temporal recorded attempt 2's completion; attempt 1 timed out on its heartbeat.
    [call] = [c for c in gateway_calls(stack.history(run)) if c["name"] == name]
    started = call["started"].activity_task_started_event_attributes
    assert started.attempt == 2
    assert started.last_failure.timeout_failure_info.timeout_type == TimeoutType.TIMEOUT_TYPE_HEARTBEAT
    assert call["completed"] is not None

    # Attempt 1 really did return, after attempt 2 had started, and Temporal refused it
    # (the Go SDK logs the server's NotFound answer to the stale completion).
    log = [e for e in stack.agentd_log() if e.get("name") == name]
    a1_done = next(e for e in log if e["msg"] == "invoke finished" and e["attempt"] == 1)
    a2_start = next(e for e in log if e["msg"] == "invoke started" and e["attempt"] == 2)
    assert at(a1_done) > at(a2_start)
    refused = [e for e in stack.agentd_log() if e["msg"] == "Task processing failed with error"
               and ("timed out" in e.get("Error", "") or "not found" in e.get("Error", "").lower())]
    assert refused, "expected Temporal to refuse attempt 1's late completion"
    return run


def test_t8_late_result_of_timed_out_attempt_is_discarded(stack: Stack) -> None:
    tool = "github.comment_commit"
    run = run_with_stuck_first_attempt(stack, tool)
    effects = [e for e in stack.effects(run) if e["tool"] == tool]
    assert len(effects) == 1 and effects[0]["status"] == "SUCCEEDED", [dict(e) for e in effects]
    assert len(stack.external(run, tool)) == 1


def test_t8_late_model_call_is_not_repeated(stack: Stack) -> None:
    # Attempt 2 joins attempt 1's call in flight instead of asking the provider again.
    run = run_with_stuck_first_attempt(stack, "audit_v1")
    rows = stack.model_calls(run)
    assert len(rows) == 1 and rows[0]["attempt"] == 1 and rows[0]["response_blob"], [dict(r) for r in rows]
