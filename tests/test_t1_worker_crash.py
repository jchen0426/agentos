"""T1: kill -9 the Worker at different points; the run finishes and every model call appears
exactly once in the gateway's log (the model_calls ledger). The Worker is agentd's own, which agentd
starts again; where a test needs no Worker running, agentd's fault script holds the next one back.
Runs on the fake model, or on a real one with AGENTOS_MODEL (tests/harness.py)."""

import json

import pytest
from temporalio.api.enums.v1 import EventType

from tests.harness import KEYED_MODEL, MODEL_TIMEOUT, REAL_MODEL, SANDBOX, Stack, TERMINAL, gateway_calls, wait_for

MODEL = "audit_v1"

pytestmark = pytest.mark.skipif(KEYED_MODEL and SANDBOX == "none", reason=(
    "a real model's key is a real secret, beside which agentd runs no Worker of its own without a sandbox "
    "(--insecure-dev): run it under AGENTOS_SANDBOX=srt"))


def model_activities(stack: Stack, run: dict) -> list[dict]:
    return [c for c in gateway_calls(stack.history(run)) if c["kind"] == "model"]


def assert_finished_once(stack: Stack, run: dict) -> None:
    final = stack.wait_state(run, TERMINAL, timeout=MODEL_TIMEOUT + 60)
    assert final["state"] == "SUCCEEDED", final
    result = json.loads(final["result_json"])
    # A real model's patch may not apply to the fixture; the run still ends normally.
    assert result["outcome"] in ({"ok", "patch rejected"} if REAL_MODEL else {"ok"}), result
    rows = stack.model_calls(run)
    # One call, one attempt, one accepted answer. (A real model may take a second request
    # within the attempt, when its first answer broke the schema.)
    assert {r["attempt"] for r in rows} == {1}, [dict(r) for r in rows]
    accepted = [r for r in rows if r["response_blob"] is not None and r["stop_reason"] != "invalid_output"]
    assert len(accepted) == 1, [dict(r) for r in rows]
    assert len(model_activities(stack, run)) == 1  # and Temporal scheduled it once
    # The rest of the run went on normally: a PR when the model's patch applied, one report.
    assert len(stack.external(run, "github.open_draft_pr")) == (1 if result["pr_url"] else 0)
    assert len(stack.external(run, "github.comment_commit")) == 1


def test_t1_kill_worker_during_model_call(stack: Stack) -> None:
    hold, no_worker = stack.tmp / "hold-model", stack.tmp / "hold-worker"
    hold.touch()
    stack.start_agentd(faults=[{"name": MODEL, "hold_while_exists": str(hold)}], worker=True, hold_worker=no_worker)
    w = stack.worker()
    run = stack.start_run()

    wait_for(lambda: len(stack.model_calls(run)) == 1, "the model call to start")
    no_worker.touch()  # agentd starts no Worker again while it is there
    w.kill()  # kill -9 while agentd is in the middle of the model call
    wait_for(lambda: w.log("the fault script holds the Worker's next start"), "agentd to hold its next Worker back")
    hold.unlink()
    wait_for(lambda: stack.model_calls(run)[0]["response_blob"] is not None,
             "the model call to finish with no Worker alive", timeout=MODEL_TIMEOUT)
    assert stack.worker_processes() == [], "a Worker was alive"

    no_worker.unlink()
    w.wait_ready(2)
    assert_finished_once(stack, run)
    assert stack.worker_processes() == [w.pid()]  # agentd's second generation served the rest


def test_t1_kill_worker_after_model_returns_before_task_completes(stack: Stack) -> None:
    hold = stack.tmp / "hold-model"
    hold.touch()
    stack.start_agentd(faults=[{"name": MODEL, "hold_while_exists": str(hold)}], worker=True)
    w = stack.worker()
    run = stack.start_run()

    wait_for(lambda: len(stack.model_calls(run)) == 1, "the model call to start")
    # Freeze the Worker: it keeps its connection and its outstanding polls but does nothing, and
    # agentd, which has no liveness probe, leaves it so.
    w.pause()
    hold.unlink()

    # The model's result is recorded and the next workflow task is handed to the frozen
    # Worker, which will never complete it: the window between "call returned" and
    # "workflow task committed".
    def task_started_after_result() -> bool:
        events = stack.history(run)
        done = [i for i, e in enumerate(events) if e.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED
                and e.activity_task_completed_event_attributes.scheduled_event_id
                == model_activities(stack, run)[0]["scheduled"].event_id]
        return bool(done) and any(e.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_STARTED
                                  for e in events[done[0]:])
    wait_for(task_started_after_result, "the frozen Worker to receive the workflow task", timeout=MODEL_TIMEOUT + 30)
    w.kill()

    w.wait_ready(2)  # agentd starts it again
    assert_finished_once(stack, run)
    # The task the dead Worker held timed out and was handed to the new Worker.
    events = stack.history(run)
    assert any(e.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT for e in events)
    assert stack.worker_processes() == [w.pid()]
