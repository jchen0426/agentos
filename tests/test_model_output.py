"""The gateway checks a model's answer against the task's output schema (design doc, section
9). An answer that breaks it is asked for once more, and both requests are paid for; the
workflow only ever sees the answer that passed, with its large parts stored as blobs."""

from tests.harness import TERMINAL, Stack


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

    # The workflow got the passing answer: the report and the patch as blobs.
    [comment] = stack.external(run, "github.comment_commit")
    assert comment["args"]["report_ref"].startswith("sha256:")
    [pr] = stack.external(run, "github.open_draft_pr")
    assert pr["args"]["patch_ref"].startswith("sha256:")
