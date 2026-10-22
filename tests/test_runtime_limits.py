"""The Runtime proxy reads no request larger than Temporal takes (4 MiB): a larger one could
never succeed, and agentd would hold it in memory to check it (internal/runtimeproxy/limits.go,
docs/m1-notes.md). agentd refuses it with ResourceExhausted, as Temporal did, so the SDK
handles it the same way: it fails the workflow task as GRPC_MESSAGE_TOO_LARGE, Temporal ends
the run, and the Worker keeps serving. The SDK gzips its requests: the limit is on the
decompressed size. A request with more values than agentd decodes is refused in the same
words, so it ends the same way instead of failing the workflow task for ever."""

import json

import grpc
from temporalio.api.common.v1 import WorkflowExecution
from temporalio.api.enums.v1 import EventType
from temporalio.api.workflowservice.v1 import GetWorkflowExecutionHistoryRequest
from temporalio.api.workflowservice.v1.service_pb2_grpc import WorkflowServiceStub
from temporalio.client import Client

from tests.harness import TERMINAL, Stack


def refused_completion(stack: Stack, workflows: str) -> tuple[str, list]:
    """Runs a workflow whose task completion agentd refuses; returns the refusal's reason and
    the run's history."""
    stack.start_agentd()
    worker = stack.start_worker(workflows=workflows)
    run = stack.start_run()
    assert stack.wait_state(run, TERMINAL)["state"] == "FAILED"
    refused = [e for e in stack.agentd_log() if e["msg"] == "runtime call refused"]
    assert [e["method"].rsplit("/", 1)[1] for e in refused] == ["RespondWorkflowTaskCompleted"], refused
    assert worker.alive() and "GrpcMessageTooLarge" in worker.text()
    # Temporal ended the run, not agentd: no change to the ledger. verify finds nothing amiss, and lists
    # the run as terminated outside agentd, with the reason its terminator gave, in its own word.
    r = stack.agentctl_run("audit", "verify")
    rep = json.loads(r.stdout)
    assert r.returncode == 0, (r.returncode, rep, r.stderr)
    assert [(x["run_id"], x["claimed_reason"]) for x in rep["terminated_outside_agentd"]] == [
        (run["run_id"], "GrpcMessageTooLarge")], rep
    return refused[0]["reason"], stack.history(run)


def test_agentd_refuses_a_completion_temporal_could_never_take(stack: Stack) -> None:
    reason, events = refused_completion(stack, "tests.rogue_workflows:OversizedCompletion")
    assert reason.startswith("grpc: received message after decompression larger than max"), reason
    assert_ended_as_too_large(events)


def test_agentd_refuses_a_completion_with_too_many_values(stack: Stack) -> None:
    reason, events = refused_completion(stack, "tests.rogue_workflows:ManyCalls")
    assert reason == "grpc: received message larger than max (more than 50000 values)", reason
    assert_ended_as_too_large(events)


def assert_ended_as_too_large(events: list) -> None:
    # Temporal terminates the run on the SDK's GRPC_MESSAGE_TOO_LARGE, as it did before agentd
    # refused the request itself.
    assert events[-1].event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED
    assert events[-1].workflow_execution_terminated_event_attributes.reason == "GrpcMessageTooLarge"
    # Nothing in the refused completion took effect: no gateway call was scheduled.
    assert not [e for e in events if e.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED]


def test_a_long_history_pages_through_the_runtime_entry(stack: Stack) -> None:
    """GetWorkflowExecutionHistory is one of the methods whose requests agentd caps at 64 KiB.
    Its requests carry Temporal's own next_page_token: paging a long history a few events at a
    time through the Runtime entry, as a Worker replaying it does, every page must come
    through, with tokens far under the cap."""
    stack.start_agentd()
    workflow_id = "long-history"

    async def build(c: Client) -> None:
        h = await c.start_workflow("nobody-runs-this", id=workflow_id, task_queue="nobody-polls-this")
        for i in range(300):
            await h.signal("pad", i)
    stack._temporal(build)

    listen = stack.runtime_listen()
    target = "unix:" + listen.removeprefix("unix://") if listen.startswith("unix://") else listen.removeprefix("tcp://")
    with grpc.insecure_channel(target) as channel:
        service = WorkflowServiceStub(channel)
        events, token, tokens = 0, b"", []
        while True:
            page = service.GetWorkflowExecutionHistory(GetWorkflowExecutionHistoryRequest(
                namespace="default", execution=WorkflowExecution(workflow_id=workflow_id),
                maximum_page_size=5, next_page_token=token), timeout=30)
            events += len(page.history.events)
            token = page.next_page_token
            if not token:
                break
            tokens.append(len(token))
    assert events >= 302, events  # started, a workflow task scheduled, 300 signals
    assert len(tokens) >= 60 and max(tokens) < 4096, (len(tokens), max(tokens))
    print(f"history of {events} events in {len(tokens) + 1} pages; tokens up to {max(tokens)} bytes")
