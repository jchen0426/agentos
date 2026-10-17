"""T10: new workflow code ships while a run is suspended. The replay test that CI runs stops
an incompatible change; a change guarded by workflow.patched() replays cleanly, and the
suspended run continues on the patched branch."""

import asyncio
import os

import pytest
from temporalio.api.enums.v1 import EventType
from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer
from temporalio.workflow import NondeterminismError

from agentos_worker.workflows import SecurityAudit
from tests.harness import REPO, Stack, gateway_calls
from tests.t10_workflows import PATCH_ID, BreakingV2, PatchedV2

HISTORIES = REPO / "tests" / "histories"


def replay(workflow: type, run: dict, history_json: str) -> None:
    history = WorkflowHistory.from_json(run["run_id"], history_json)
    asyncio.run(Replayer(workflows=[workflow]).replay_workflow(history))


def test_t10_new_code_while_a_run_is_suspended(stack: Stack) -> None:
    # The first draft-PR call of each run waits for approval; the doorbell lets it through.
    stack.start_agentd(faults=[{"name": "github.open_draft_pr", "occurrence": 1, "status": "needs_approval"}])
    v1 = stack.start_worker()

    # Run A goes all the way through on v1, including one wait.
    a = stack.start_run()
    stack.wait_state(a, {"WAITING_APPROVAL"})
    stack.signal(a, "doorbell")
    assert stack.wait_state(a, {"SUCCEEDED", "FAILED"})["state"] == "SUCCEEDED"
    history_a = stack.history_json(a)

    # Run B is suspended, waiting for approval, holding no Worker.
    b = stack.start_run()
    stack.wait_state(b, {"WAITING_APPROVAL"})
    history_b = stack.history_json(b)
    if os.environ.get("AGENTOS_WRITE_HISTORIES"):
        HISTORIES.mkdir(exist_ok=True)
        (HISTORIES / "security-audit-v1-completed.json").write_text(history_a)
        (HISTORIES / "security-audit-v1-waiting-approval.json").write_text(history_b)

    # The replay test CI runs: the shipped code replays both, the unguarded change is stopped,
    # the guarded one passes.
    replay(SecurityAudit, a, history_a)
    replay(SecurityAudit, b, history_b)
    with pytest.raises(NondeterminismError, match="6:"):
        replay(BreakingV2, a, history_a)
    replay(PatchedV2, a, history_a)
    replay(PatchedV2, b, history_b)

    # Ship the guarded version while B waits, then let B go on.
    v1.stop()
    stack.start_worker(workflows="tests.t10_workflows:PatchedV2")
    stack.signal(b, "doorbell")
    assert stack.wait_state(b, {"SUCCEEDED", "FAILED"})["state"] == "SUCCEEDED"

    # B took the patched branch: the patch marker, then the new step after the PR.
    events = stack.history(b)
    markers = [e.marker_recorded_event_attributes for e in events
               if e.event_type == EventType.EVENT_TYPE_MARKER_RECORDED]
    assert any(m.marker_name == "core_patch" and PATCH_ID.encode() in m.SerializeToString()
               for m in markers), markers
    ids = [c["activity_id"] for c in gateway_calls(events)]
    assert ids == ["1:git.show_diff", "2:audit_v1", "3:git.apply_check", "4:github.open_draft_pr",
                   "5:github.open_draft_pr", "6:git.show_diff", "7:github.comment_commit"], ids
    assert len(stack.external(b, "git.show_diff")) == 2

    # From now on B's history replays only under the new code; going back to v1 is caught too.
    history_b_done = stack.history_json(b)
    replay(PatchedV2, b, history_b_done)
    with pytest.raises(NondeterminismError):
        replay(SecurityAudit, b, history_b_done)
