"""T1 (fake model): kill -9 the Worker at different points; the run finishes and every model
call appears exactly once in the gateway's log (the model_calls ledger)."""

import json

from temporalio.api.enums.v1 import EventType

from tests.harness import Stack, TERMINAL, gateway_calls, wait_for

MODEL = "audit_v1"


def model_activities(stack: Stack, run: dict) -> list[dict]:
    return [c for c in gateway_calls(stack.history(run)) if c["kind"] == "model"]


def assert_finished_once(stack: Stack, run: dict) -> None:
    final = stack.wait_state(run, TERMINAL, timeout=90)
    assert final["state"] == "SUCCEEDED", final
    assert json.loads(final["result_json"])["outcome"] == "ok"
    rows = stack.model_calls(run)
    assert len(rows) == 1, [dict(r) for r in rows]  # one call, one attempt, one provider request
    assert rows[0]["response_blob"] is not None
    assert len(model_activities(stack, run)) == 1  # and Temporal scheduled it once
    # The rest of the run went on normally: one PR, one report.
    assert len(stack.external(run, "github.open_draft_pr")) == 1
    assert len(stack.external(run, "github.comment_commit")) == 1


def test_t1_kill_worker_during_model_call(stack: Stack) -> None:
    hold = stack.tmp / "hold-model"
    hold.touch()
    stack.start_agentd(faults=[{"name": MODEL, "hold_while_exists": str(hold)}])
    w1 = stack.start_worker()
    run = stack.start_run()

    wait_for(lambda: len(stack.model_calls(run)) == 1, "the model call to start")
    w1.kill()  # kill -9 while agentd is in the middle of the model call
    hold.unlink()
    wait_for(lambda: stack.model_calls(run)[0]["response_blob"] is not None,
             "the model call to finish with no Worker alive")

    stack.start_worker()
    assert_finished_once(stack, run)


def test_t1_kill_worker_after_model_returns_before_task_completes(stack: Stack) -> None:
    hold = stack.tmp / "hold-model"
    hold.touch()
    stack.start_agentd(faults=[{"name": MODEL, "hold_while_exists": str(hold)}])
    w1 = stack.start_worker()
    run = stack.start_run()

    wait_for(lambda: len(stack.model_calls(run)) == 1, "the model call to start")
    # Freeze the Worker: it keeps its connection and its outstanding polls but does nothing.
    w1.pause()
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
    wait_for(task_started_after_result, "the frozen Worker to receive the workflow task", timeout=30)
    w1.kill()

    stack.start_worker()
    assert_finished_once(stack, run)
    # The task the dead Worker held timed out and was handed to the new Worker.
    events = stack.history(run)
    assert any(e.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT for e in events)
