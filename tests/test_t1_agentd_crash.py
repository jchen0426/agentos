"""T1, the agentd half: kill -9 agentd around the moment a model result is recorded.

- After the result is in the ledger: the retry is served from the ledger and the provider
  was asked once (design doc, section 5: recorded results are reused).
- After the provider answered but before the ledger recorded it: agentd cannot know the
  outcome, so the retry asks again, and the first request's reservation is charged in full
  when agentd starts (section 9). The run is not lost and the second request is not hidden.
"""

from tests.harness import Stack, TERMINAL, gateway_calls, wait_for

MODEL = "audit_v1"


def model_attempt(stack: Stack, run: dict) -> int:
    [call] = [c for c in gateway_calls(stack.history(run)) if c["kind"] == "model"]
    return call["started"].activity_task_started_event_attributes.attempt


def test_t1_kill_agentd_after_the_model_result_is_recorded(stack: Stack) -> None:
    hold = stack.tmp / "hold-after-record"
    hold.touch()
    stack.start_agentd(faults=[{"name": MODEL, "attempt": 1, "hold_after_record_while_exists": str(hold)}])
    stack.start_worker()
    run = stack.start_run()
    wait_for(lambda: (rows := stack.model_calls(run)) and rows[0]["response_blob"] is not None,
             "the model result to be recorded")
    stack.restart_agentd(kill=True)  # the result is in the ledger; Temporal never heard of it
    hold.unlink()

    final = stack.wait_state(run, TERMINAL, timeout=120)
    assert final["state"] == "SUCCEEDED", final
    assert model_attempt(stack, run) == 2  # Temporal retried the activity...
    assert [r["attempt"] for r in stack.model_calls(run)] == [1]  # ...and the ledger answered it
    assert len(stack.provider_calls(run)) == 1  # the provider was asked once


def test_t1_kill_agentd_after_the_provider_answers_before_the_result_is_recorded(stack: Stack) -> None:
    hold = stack.tmp / "hold-before-record"
    hold.touch()
    stack.start_agentd(faults=[{"name": MODEL, "attempt": 1, "hold_before_record_while_exists": str(hold)}])
    stack.start_worker()
    run = stack.start_run()
    wait_for(lambda: len(stack.provider_calls(run)) == 1, "the provider to answer")
    stack.restart_agentd(kill=True)  # the provider answered; the ledger has no result
    hold.unlink()

    final = stack.wait_state(run, TERMINAL, timeout=120)
    assert final["state"] == "SUCCEEDED", final
    assert len(stack.provider_calls(run)) == 2  # asked again: agentd could not know
    rows = [(r["attempt"], r["response_blob"] is not None) for r in stack.model_calls(run)]
    assert rows == [(1, False), (2, True)], rows  # the unanswered attempt stays visible
    # Unrecorded, so charged in full when agentd started again; the retry, what it cost.
    entries = stack.budget_entries(run)
    reserved = {e["request_id"]: e for e in entries if e["kind"] == "RESERVE"}
    settled = {reserved[e["request_id"]]["attempt"]: e for e in entries if e["kind"] == "SETTLE"}
    assert sorted(settled) == [1, 2], [dict(e) for e in entries]
    first, second = settled[1], settled[2]
    assert first["reason"] == "open_at_start"
    assert first["amount_micros"] == reserved[first["request_id"]]["amount_micros"]
    assert second["reason"] == "usage"
    assert 0 < second["amount_micros"] < reserved[second["request_id"]]["amount_micros"]
