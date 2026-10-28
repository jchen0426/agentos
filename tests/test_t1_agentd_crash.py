"""T1, the agentd half: kill -9 agentd, its own Worker with it, around the moment a model result is
recorded; the next agentd starts a Worker of its own.

- After the result is in the ledger: the retry is served from the ledger and the provider
  was asked once (design doc, section 5: recorded results are reused).
- After the provider answered but before the ledger recorded it: agentd cannot know the
  outcome, so the retry asks again, and the first request's reservation is charged in full
  when agentd starts (section 9). The run is not lost and the second request is not hidden.
"""

from tests.harness import MODEL_TIMEOUT, Stack, TERMINAL, gateway_calls, wait_for

MODEL = "audit_v1"


def model_attempt(stack: Stack, run: dict) -> int:
    [call] = [c for c in gateway_calls(stack.history(run)) if c["kind"] == "model"]
    return call["started"].activity_task_started_event_attributes.attempt


def test_t1_kill_agentd_after_the_model_result_is_recorded(stack: Stack) -> None:
    hold = stack.tmp / "hold-after-record"
    hold.touch()
    stack.start_agentd(faults=[{"name": MODEL, "attempt": 1, "hold_after_record_while_exists": str(hold)}], worker=True)
    run = stack.start_run()
    # The answer the call returns: a real model's first answer may break the schema and be
    # asked for again, and agentd holds only after the one that counts.
    wait_for(lambda: any(r["response_blob"] is not None and r["stop_reason"] != "invalid_output"
                         for r in stack.model_calls(run)),
             "the model result to be recorded", timeout=MODEL_TIMEOUT)
    stack.restart_agentd(kill=True)  # the result is in the ledger; Temporal never heard of it
    hold.unlink()

    final = stack.wait_state(run, TERMINAL, timeout=120)
    assert final["state"] == "SUCCEEDED", final
    assert stack.worker_processes() == [stack.worker().pid()]  # the next agentd's own Worker
    assert model_attempt(stack, run) == 2  # Temporal retried the activity...
    rows = stack.model_calls(run)
    assert {r["attempt"] for r in rows} == {1}, [dict(r) for r in rows]  # ...and the ledger answered it
    assert stack.provider_requests(run) == len(rows)  # the provider was asked once (twice if it broke the schema)


def test_t1_kill_agentd_after_the_provider_answers_before_the_result_is_recorded(stack: Stack) -> None:
    hold = stack.tmp / "hold-before-record"
    hold.touch()
    stack.start_agentd(faults=[{"name": MODEL, "attempt": 1, "hold_before_record_while_exists": str(hold)}], worker=True)
    run = stack.start_run()
    wait_for(lambda: stack.model_answered(run), "the provider to answer", timeout=MODEL_TIMEOUT)
    stack.restart_agentd(kill=True)  # the provider answered; the ledger has no result
    hold.unlink()

    final = stack.wait_state(run, TERMINAL, timeout=MODEL_TIMEOUT + 120)
    assert final["state"] == "SUCCEEDED", final
    assert stack.worker_processes() == [stack.worker().pid()]  # the next agentd's own Worker
    rows = stack.model_calls(run)
    assert stack.provider_requests(run) == len(rows) >= 2  # asked again: agentd could not know
    # The unanswered attempt stays visible; the retry got the one accepted answer.
    answered = {r["attempt"] for r in rows if r["response_blob"] is not None and r["stop_reason"] != "invalid_output"}
    assert {r["attempt"] for r in rows} == {1, 2} and answered == {2}, [dict(r) for r in rows]
    # Unrecorded, so charged in full when agentd started again; the retry, what it cost. A
    # model priced at 0 (the local one) reserves and settles nothing.
    entries = stack.budget_entries(run)
    reserved = {e["request_id"]: e for e in entries if e["kind"] == "RESERVE"}
    settled = [(reserved[e["request_id"]], e) for e in entries if e["kind"] == "SETTLE"]
    first = [s for r, s in settled if r["attempt"] == 1]
    second = [(r, s) for r, s in settled if r["attempt"] == 2]
    assert first and all(s["reason"] == "open_at_start" and s["amount_micros"] == reserved[s["request_id"]]["amount_micros"]
                         for s in first), [dict(e) for e in entries]
    assert second and all(s["reason"] == "usage" and (0 < s["amount_micros"] < r["amount_micros"]
                                                      or s["amount_micros"] == r["amount_micros"] == 0)
                          for r, s in second), [dict(e) for e in entries]
