"""The gateway checks a model's answer against the task's output schema (design doc, section
9). An answer that breaks it is asked for once more, and both requests are paid for; the
workflow only ever sees the answer that passed, with its large parts stored as blobs."""

from tests.harness import TERMINAL, Stack, gateway_calls, wait_for


def test_an_answer_that_breaks_the_schema_is_asked_for_again(stack: Stack) -> None:
    stack.start_agentd(faults=[{"name": "audit_v1", "attempt": 1, "provider_bad_output": True}])
    stack.start_worker()
    run = stack.start_run()
    final = stack.wait_state(run, TERMINAL, timeout=120)
    assert final["state"] == "SUCCEEDED", final

    # Two requests within the first attempt: the first answer broke the schema.
    rows = [(r["attempt"], r["request_seq"], r["stop_reason"]) for r in stack.model_calls(run)]
    assert rows == [(1, 1, "invalid_output"), (1, 2, "end_turn")], rows
    # Each was reserved for and charged.
    settled = [e for e in stack.budget_entries(run) if e["kind"] == "SETTLE"]
    assert len(settled) == 2 and all(e["reason"] == "usage" and e["amount_micros"] > 0 for e in settled), settled
    assert len(stack.provider_calls(run)) == 2

    # The workflow got the passing answer: the report and the patch as blobs, which went on
    # into a PR and a report on the commit.
    args = {c["name"]: c["args"] for c in gateway_calls(stack.history(run))}
    assert args["github.comment_commit"]["report_ref"].startswith("sha256:")
    assert args["github.open_draft_pr"]["patch_ref"].startswith("sha256:")
    assert len(stack.external(run, "github.comment_commit")) == len(stack.external(run, "github.open_draft_pr")) == 1


def test_the_answer_asked_for_again_is_served_from_the_ledger_after_agentd_dies(stack: Stack) -> None:
    # T1's kill after the result is recorded, when the first answer broke the schema, as a real
    # model's may: agentd holds once the answer asked for again is recorded, not before, and
    # the retry is served that answer. Nobody is asked a third time.
    hold = stack.tmp / "hold-after-record"
    hold.touch()
    stack.start_agentd(faults=[{"name": "audit_v1", "attempt": 1, "provider_bad_output": True,
                                "hold_after_record_while_exists": str(hold)}])
    stack.start_worker()
    run = stack.start_run()
    wait_for(lambda: any(r["stop_reason"] == "end_turn" for r in stack.model_calls(run)),
             "the answer asked for again to be recorded")
    stack.restart_agentd(kill=True)
    hold.unlink()
    final = stack.wait_state(run, TERMINAL, timeout=120)
    assert final["state"] == "SUCCEEDED", final

    rows = [(r["attempt"], r["request_seq"], r["stop_reason"]) for r in stack.model_calls(run)]
    assert rows == [(1, 1, "invalid_output"), (1, 2, "end_turn")], rows
    assert len(stack.provider_calls(run)) == 2
    [call] = [c for c in gateway_calls(stack.history(run)) if c["kind"] == "model"]
    assert call["started"].activity_task_started_event_attributes.attempt == 2  # Temporal retried
