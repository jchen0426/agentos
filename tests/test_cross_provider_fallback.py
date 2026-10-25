"""A model that declines to answer hands the call to another provider's, when the task lists
one in cross_provider_fallback and the policy allows its provider (design doc, section 9).
The fallback is a request of its own: reserved for, charged and recorded on its own."""

import json

import pytest

from tests.harness import AUDIT_SCHEMA, TERMINAL, TEST_POLICY, TEST_PRICES, Stack, wait_for

MODELS = f"""
providers:
  fake:  {{adapter: fake}}
  other: {{adapter: fake}}
tasks:
  audit_v1:
    model: fake/audit-fixture
    max_tokens: 4096
    inputs: [diff_ref]
    output_schema: {AUDIT_SCHEMA}
    blobs: [patch]
    cross_provider_fallback: [other/audit-fixture]
"""
PRICES = TEST_PRICES + "  other/audit-fixture: {input: 3.00, output: 15.00}\n"
POLICY = TEST_POLICY.replace("allowed_providers: [fake, anthropic]", "allowed_providers: [fake, other]")
REFUSES = [{"name": "audit_v1", "provider_refuses": "fake"}]


def test_a_refusal_goes_to_the_next_provider(stack: Stack) -> None:
    stack.start_agentd(models=MODELS, prices=PRICES, policy=POLICY, faults=REFUSES)
    stack.start_worker()
    run = stack.start_run()
    final = stack.wait_state(run, TERMINAL, timeout=120)
    assert final["state"] == "SUCCEEDED" and json.loads(final["result_json"])["outcome"] == "ok", final

    # Two requests in the one call: the refusal, then the other provider's answer.
    rows = [(r["request_seq"], r["provider"], r["model"], r["stop_reason"]) for r in stack.model_calls(run)]
    assert rows == [(1, "fake", "audit-fixture", "refusal"), (2, "other", "audit-fixture", "end_turn")], rows
    # Each reserved for alone, at its own prices, and charged what it was billed.
    entries = stack.budget_entries(run)
    reserved = [(e["model"], e["amount_micros"]) for e in entries if e["kind"] == "RESERVE"]
    settled = [(e["model"], e["reason"], e["amount_micros"]) for e in entries if e["kind"] == "SETTLE"]
    assert [m for m, _ in reserved] == ["fake/audit-fixture", "other/audit-fixture"], reserved
    assert reserved[1][1] > reserved[0][1] * 2, reserved  # three times the prices
    assert [(m, r) for m, r, _ in settled] == [("fake/audit-fixture", "usage"), ("other/audit-fixture", "usage")], settled
    assert settled[1][2] == settled[0][2] * 3, settled
    assert [c["model"] for c in stack.provider_calls(run)] == ["fake/audit-fixture", "other/audit-fixture"]


def sent(run: dict, stack: Stack) -> list[tuple]:
    """Each request of the run as the ledger has it: attempt, provider, stop reason."""
    return [(r["attempt"], r["provider"], r["stop_reason"]) for r in stack.model_calls(run)]


def test_a_retry_picks_up_at_the_fallback(stack: Stack) -> None:
    # The fallback answers 429 the first time: the call is retried, and the retry asks the
    # fallback again, never the model that declined (it would be paid for a second time).
    faults = REFUSES + [{"name": "audit_v1", "attempt": 1, "provider_status": 429, "provider_status_on": "other"}]
    stack.start_agentd(models=MODELS, prices=PRICES, policy=POLICY, faults=faults)
    stack.start_worker()
    run = stack.start_run()
    final = stack.wait_state(run, TERMINAL, timeout=120)
    assert final["state"] == "SUCCEEDED" and json.loads(final["result_json"])["outcome"] == "ok", final
    assert sent(run, stack) == [(1, "fake", "refusal"), (1, "other", None), (2, "other", "end_turn")]
    settled = [(e["model"], e["reason"]) for e in stack.budget_entries(run) if e["kind"] == "SETTLE"]
    assert settled == [("fake/audit-fixture", "usage"), ("other/audit-fixture", "rejected"),
                       ("other/audit-fixture", "usage")], settled
    assert [(c["model"], c["status"]) for c in stack.provider_calls(run)] == [
        ("fake/audit-fixture", 200), ("other/audit-fixture", 429), ("other/audit-fixture", 200)]


def test_a_lost_answer_of_the_fallback_is_served_from_the_ledger(stack: Stack) -> None:
    # agentd dies once the fallback's answer is recorded, before Temporal hears of it: the
    # retry is served that answer, not the refusal before it, and nobody is asked again.
    hold = stack.tmp / "hold-after-record"
    hold.touch()
    faults = REFUSES + [{"name": "audit_v1", "attempt": 1, "hold_after_record_while_exists": str(hold),
                         "hold_record_on": "other"}]
    stack.start_agentd(models=MODELS, prices=PRICES, policy=POLICY, faults=faults)
    stack.start_worker()
    run = stack.start_run()
    wait_for(lambda: any(r["provider"] == "other" and r["response_blob"] for r in stack.model_calls(run)),
             "the fallback's answer to be recorded", timeout=60)
    stack.restart_agentd(kill=True)
    hold.unlink()
    final = stack.wait_state(run, TERMINAL, timeout=120)
    assert final["state"] == "SUCCEEDED" and json.loads(final["result_json"])["outcome"] == "ok", final
    assert sent(run, stack) == [(1, "fake", "refusal"), (1, "other", "end_turn")]
    assert [c["model"] for c in stack.provider_calls(run)] == ["fake/audit-fixture", "other/audit-fixture"]


def test_a_budget_pause_picks_up_at_the_fallback(stack: Stack) -> None:
    # Room for the model's request, not for the fallback's at three times the prices: the call
    # waits for budget, and is sent again, under a new activity, once the budget is extended.
    stack.start_agentd(models=MODELS, prices=PRICES, policy=POLICY, faults=REFUSES)
    stack.start_worker()
    run = stack.start_run(budget_micros=30_000)
    stack.wait_state(run, {"PAUSED_BUDGET"}, timeout=120)
    assert sent(run, stack) == [(1, "fake", "refusal")]
    stack.agentctl("budget", "extend", run["run_id"], "--usd", "0.1")
    final = stack.wait_state(run, TERMINAL, timeout=120)
    assert final["state"] == "SUCCEEDED" and json.loads(final["result_json"])["outcome"] == "ok", final
    # The model that declined was asked once, in the first activity; the fallback in the next.
    rows = stack.model_calls(run)
    assert [(r["provider"], r["stop_reason"]) for r in rows] == [("fake", "refusal"), ("other", "end_turn")], rows
    assert rows[0]["activity_id"] != rows[1]["activity_id"], [dict(r) for r in rows]
    assert [c["model"] for c in stack.provider_calls(run)] == ["fake/audit-fixture", "other/audit-fixture"]
    reserved = [e["model"] for e in stack.budget_entries(run) if e["kind"] == "RESERVE"]
    assert reserved == ["fake/audit-fixture", "other/audit-fixture"], reserved


def test_a_fallback_that_cannot_answer_leaves_the_refusal(stack: Stack) -> None:
    # The fallback turns the request away for good (400): the call ends with the refusal, not
    # with the fallback's error.
    faults = REFUSES + [{"name": "audit_v1", "provider_status": 400, "provider_status_on": "other"}]
    stack.start_agentd(models=MODELS, prices=PRICES, policy=POLICY, faults=faults)
    stack.start_worker()
    run = stack.start_run()
    final = stack.wait_state(run, TERMINAL, timeout=120)
    assert final["state"] == "SUCCEEDED" and json.loads(final["result_json"])["outcome"] == "refused", final
    assert sent(run, stack) == [(1, "fake", "refusal"), (1, "other", None)]
    settled = [(e["model"], e["reason"]) for e in stack.budget_entries(run) if e["kind"] == "SETTLE"]
    assert settled == [("fake/audit-fixture", "usage"), ("other/audit-fixture", "rejected")], settled
    wait_for(lambda: any("not falling back: the request failed" in e.get("msg", "") for e in stack.agentd_log()),
             "agentd to log why it did not fall back")


def test_without_a_fallback_a_refusal_ends_the_call(stack: Stack) -> None:
    stack.start_agentd(faults=REFUSES)
    stack.start_worker()
    run = stack.start_run()
    final = stack.wait_state(run, TERMINAL, timeout=120)
    assert final["state"] == "SUCCEEDED" and json.loads(final["result_json"])["outcome"] == "refused", final
    assert [r["stop_reason"] for r in stack.model_calls(run)] == ["refusal"]


@pytest.mark.audit_verify(skip="agentd refuses to start, by design")
def test_agentd_refuses_a_fallback_the_policy_never_allows(stack: Stack) -> None:
    # The test policy allows only fake and anthropic for audit_v1: the fallback could never run.
    with pytest.raises(AssertionError, match="cross_provider_fallback other/audit-fixture"):
        stack.start_agentd(models=MODELS, prices=PRICES)
