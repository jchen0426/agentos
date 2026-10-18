"""T15: a run is cancelled and agentd restarted. The run writes nothing more, and what it had
already sent is reconciled as usual: a write that reached GitHub is found and recorded, and one
that was never sent is abandoned (design doc, section 6)."""

from datetime import datetime, timezone

from tests.harness import Stack, wait_for

PR = "github.open_draft_pr"


def writes_since(stack: Stack, since: datetime) -> list[dict]:
    return [e for e in stack.github_requests()
            if e["method"] == "POST" and e["status"] == 201 and datetime.fromisoformat(e["at"]) > since]


def cancelled(stack: Stack, run: dict) -> None:
    final = stack.wait_state(run, {"CANCELLED", "SUCCEEDED", "FAILED"}, timeout=180)
    assert final["state"] == "CANCELLED", final


def test_t15_a_sent_write_is_reconciled_after_cancel_and_restart(stack: Stack) -> None:
    hold = stack.tmp / "hold"
    hold.touch()
    stack.start_agentd(faults=[{"name": PR, "attempt": 1, "hold_before_record_while_exists": str(hold)}],
                       extra={"cancel_grace": "20s"})
    stack.start_worker()
    run = stack.start_run()
    wait_for(lambda: stack.external(run, PR), "the PR to open on GitHub", timeout=60)

    stack.agentctl("runs", "cancel", run["run_id"])
    at = datetime.now(timezone.utc)
    stack.restart_agentd(kill=True)
    hold.unlink()
    cancelled(stack, run)

    # The PR that reached GitHub is on record, found rather than sent again ...
    wait_for(lambda: stack.effect(run, PR)["status"] == "SUCCEEDED", "the PR to be recorded")
    assert len(stack.external(run, PR)) == 1
    # ... and nothing was written after the cancel: no second PR, no report.
    assert stack.external(run, "github.comment_commit") == []
    assert writes_since(stack, at) == []


def test_t15_an_unsent_write_of_a_cancelled_run_is_abandoned(stack: Stack) -> None:
    hold = stack.tmp / "hold"
    hold.touch()
    # Holds the write once its effect is recorded, before it is claimed.
    stack.start_agentd(faults=[{"name": PR, "attempt": 1, "hold_while_exists": str(hold)}],
                       extra={"cancel_grace": "20s"})
    stack.start_worker()
    run = stack.start_run()
    wait_for(lambda: [e for e in stack.effects(run) if e["tool"] == PR], "the PR's effect to be recorded", timeout=60)

    stack.agentctl("runs", "cancel", run["run_id"])
    stack.restart_agentd(kill=True)
    hold.unlink()
    cancelled(stack, run)
    effect = stack.effect(run, PR)
    assert effect["status"] == "ABANDONED" and effect["claimed_by"] is None, dict(effect)
    assert stack.external(run, PR) == []
    assert stack.external(run, "github.comment_commit") == []


def test_t15_a_cancel_stops_a_write_that_was_about_to_be_claimed(stack: Stack) -> None:
    """No restart: the cancel lands while the write waits between being recorded and being
    claimed. The claim checks the cancel in the same transaction, so the request never goes out."""
    hold = stack.tmp / "hold"
    hold.touch()
    stack.start_agentd(faults=[{"name": PR, "attempt": 1, "hold_while_exists": str(hold)}],
                       extra={"cancel_grace": "20s"})
    stack.start_worker()
    run = stack.start_run()
    wait_for(lambda: [e for e in stack.effects(run) if e["tool"] == PR], "the PR's effect to be recorded", timeout=60)
    stack.agentctl("runs", "cancel", run["run_id"])
    hold.unlink()
    wait_for(lambda: stack.effect(run, PR)["status"] == "ABANDONED", "the write to be abandoned")
    cancelled(stack, run)
    assert stack.external(run, PR) == []
