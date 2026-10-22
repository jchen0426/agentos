"""T3: an unsafe write, one that neither dedupes nor can be looked up, crashes where T2's does:
agentd stops after claiming it, maybe after sending it, before recording what came of it. On
restart nobody can tell what happened, so the run waits in WAITING_RECONCILE until a person
says; then it goes on, and the write happened exactly once (design doc, section 6)."""

import json
from pathlib import Path

from tests.harness import TERMINAL, TEST_POLICY, Stack, wait_for

TOOL = "test.unsafe_write"
# The policy states no effect class: the tool's own, unsafe, applies.
POLICY = TEST_POLICY.replace("models:", f"  {TOOL}:     {{decision: allow}}\nmodels:")
WORKFLOWS = "tests.t3_workflows:UnsafeWrite"


def crash_after(stack: Stack, fault: str) -> tuple[dict, Path]:
    """Starts a run whose write holds at `fault` until the returned file is removed."""
    assert POLICY != TEST_POLICY
    hold = stack.tmp / "hold"
    hold.touch()
    stack.start_agentd(policy=POLICY, test_tools=True, faults=[{"name": TOOL, "attempt": 1, fault: str(hold)}])
    stack.start_worker(workflows=WORKFLOWS)
    return stack.start_run(), hold


def waiting_for_a_person(stack: Stack, run: dict) -> dict:
    stack.wait_state(run, {"WAITING_RECONCILE"}, timeout=90)
    [item] = stack.agentctl("reconcile", "list")["items"]
    effect = stack.effect(run, TOOL)
    assert (item["id"], item["run_id"], item["tool"]) == (effect["effect_id"], run["run_id"], TOOL), item
    assert effect["status"] == "UNKNOWN" and json.loads(item["payload_json"])["note"] == "t3", item
    return item


def finished(stack: Stack, run: dict) -> None:
    final = stack.wait_state(run, TERMINAL, timeout=60)
    assert final["state"] == "SUCCEEDED", final
    # The workflow returns the call's value as it got it: the write's result, and the record of the
    # call in agentd's audit log (_audit).
    result = json.loads(final["result_json"])
    value = result.pop("value")
    assert result == {"status": "ok"} and value.pop("_audit")["seq"] > 0 and value == {"written": True}, final
    assert stack.agentctl("reconcile", "list")["items"] == []


def test_t3_the_write_happened(stack: Stack) -> None:
    run, hold = crash_after(stack, "hold_before_record_while_exists")
    wait_for(lambda: stack.external(run, TOOL), "the write to reach the outside world")
    stack.restart_agentd(kill=True)
    hold.unlink()
    item = waiting_for_a_person(stack, run)

    # The person looks at the outside world: the write is there.
    resolved = stack.agentctl("reconcile", "resolve", item["id"], "--executed", "--result", '{"written": true}')
    assert resolved["status"] == "SUCCEEDED"
    finished(stack, run)
    assert len(stack.external(run, TOOL)) == 1  # not written again


def test_t3_the_write_never_went_out(stack: Stack) -> None:
    run, hold = crash_after(stack, "hold_after_claim_while_exists")
    wait_for(lambda: any(e["claimed_by"] for e in stack.effects(run)), "the write to be claimed")
    stack.restart_agentd(kill=True)
    hold.unlink()
    item = waiting_for_a_person(stack, run)
    assert stack.external(run, TOOL) == []

    # The person looks: nothing there. The run's call sends it again, under its original key.
    resolved = stack.agentctl("reconcile", "resolve", item["id"], "--not-executed")
    assert resolved["status"] == "PENDING"
    finished(stack, run)
    [write] = stack.external(run, TOOL)
    assert write["idempotency_key"] == "agentos-" + item["id"][:32]


def test_t3_a_result_that_breaks_the_tools_schema_is_refused(stack: Stack) -> None:
    run, hold = crash_after(stack, "hold_before_record_while_exists")
    wait_for(lambda: stack.external(run, TOOL), "the write to reach the outside world")
    stack.restart_agentd(kill=True)
    hold.unlink()
    item = waiting_for_a_person(stack, run)
    try:
        stack.agentctl("reconcile", "resolve", item["id"], "--executed", "--result", '{"written": "maybe"}')
    except AssertionError as e:
        assert "bad result" in str(e), e
    else:
        raise AssertionError("a result that breaks the tool's result schema was accepted")
    assert stack.effect(run, TOOL)["status"] == "UNKNOWN"
