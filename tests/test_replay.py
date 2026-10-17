"""The replay gate CI runs on every commit (design doc, section 5.8): every recorded history
must replay under the current workflow code. A change that alters which gateway calls a
recorded run makes fails here, before it can reach a run in flight.

Record new samples with: AGENTOS_WRITE_HISTORIES=1 pytest tests/test_t10_versioning.py
"""

import asyncio
import json
from pathlib import Path

import pytest
from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer

from agentos_worker.workflows import WORKFLOWS

HISTORIES = sorted((Path(__file__).parent / "histories").glob("*.json"))


@pytest.mark.parametrize("path", HISTORIES, ids=[p.stem for p in HISTORIES])
def test_recorded_history_replays_under_current_code(path: Path) -> None:
    text = path.read_text()
    first = json.loads(text)["events"][0]
    workflow_id = first.get("workflowExecutionStartedEventAttributes", {}).get("workflowId") or path.stem
    asyncio.run(Replayer(workflows=WORKFLOWS).replay_workflow(WorkflowHistory.from_json(workflow_id, text)))


def test_there_are_histories_to_replay() -> None:
    assert HISTORIES, "tests/histories is empty: the replay gate would check nothing"
