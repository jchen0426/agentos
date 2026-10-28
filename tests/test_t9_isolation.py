"""T9 (design doc, section 11): from inside the Worker, nothing but its own run, through the
gateway. The Worker gets no environment variable it was not given, reads nothing of the home
directory, finds no key, and reaches neither the network, nor the Control socket, nor
Temporal's own port. Through the Runtime entry it can signal, cancel or terminate no run, and
the workflow-task completions agentd lets through carry no command that reaches another run,
starts a workflow of its own, or schedules work anywhere but the gateway.

What the Runtime entry refuses is agentd's own doing, the same in every sandbox mode, so those
tests run unsandboxed too: there agentd still stands between a Worker that uses the Runtime
entry and Temporal's other methods (an unsandboxed Worker could dial Temporal's port itself).
What the operating system refuses needs AGENTOS_SANDBOX=srt (macOS) or bwrap (Linux).

Not covered: runs on the Worker's own task queue. Every Worker is handed their workflow tasks,
histories included, so a subverted Worker reads every such run; the victim here sits on a queue
nobody polls. And the outcome a workflow reports is its own text, which agentd shows in the
report it posts (docs/m5-notes.md).
"""

import base64
import json
import os
import secrets
from collections.abc import Callable
from pathlib import Path

import grpc
import pytest
from temporalio.api.common.v1 import WorkflowExecution, WorkflowType
from temporalio.api.deployment.v1 import WorkerDeploymentOptions
from temporalio.api.enums.v1 import EventType, IndexedValueType, WorkerVersioningMode
from temporalio.api.operatorservice.v1 import AddSearchAttributesRequest
from temporalio.api.operatorservice.v1.service_pb2_grpc import OperatorServiceStub
from temporalio.api.query.v1 import WorkflowQuery
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.workflowservice import v1 as ws
from temporalio.api.workflowservice.v1.service_pb2_grpc import WorkflowServiceStub
from temporalio.client import Client
from temporalio.service import RPCError

from tests.harness import KEYCHAIN_ITEMS, LOGIN_KEYCHAIN, SANDBOX, Stack, in_keychain, user_temp_dir, wait_for
from tests.sandbox_probe import REPORT_LINE
from tests.rogue_workflows import T9_VICTIM

NOBODY_POLLS = "t9-nobody-polls"
AUDIT_ANCHORS = "agentos-audit-anchor"  # the anchor workflows' type and task queue (internal/audit)

# The run's base branch, which names the step StepsOutOfLine takes, and what agentd refuses it with.
STEPS = {
    "signal": ("SignalExternalWorkflowExecution", "SignalExternalWorkflowExecution is not available to workers"),
    "cancel": ("RequestCancelExternalWorkflowExecution", "RequestCancelExternalWorkflowExecution is not available"),
    "child": ("StartChildWorkflowExecution", "StartChildWorkflowExecution is not available to workers"),
    "continue-as-new": ("ContinueAsNewWorkflowExecution", "ContinueAsNewWorkflowExecution is not available to workers"),
    "activity-queue": ("ScheduleActivityTask", 'activities go to "gateway" only, not "t9-elsewhere"'),
    "activity-type": ("ScheduleActivityTask", 'activity type "NotInvoke" is not available to workers'),
    "search-attribute": ("UpsertWorkflowSearchAttributes", 'search attribute "T9Elsewhere" is not available to workers'),
}

# Events that would show a step out of line took effect in the run that took it.
STEPPED_OUT = {
    EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
    EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
    EventType.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED,
    EventType.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
    EventType.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
    EventType.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES,
    EventType.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
}


def start_victim(stack: Stack) -> None:
    """A run no other may reach: on a task queue nobody polls, it sits at its first workflow task,
    and its history shows whatever reaches it."""
    async def start(c: Client) -> None:
        await c.start_workflow("t9-victim", id=T9_VICTIM, task_queue=NOBODY_POLLS)
    stack._temporal(start)


def history_of(stack: Stack, workflow_id: str) -> list:
    async def fetch(c: Client) -> list:
        return list((await c.get_workflow_handle(workflow_id).fetch_history()).events)
    return stack._temporal(fetch)


def assert_untouched(stack: Stack) -> None:
    """The victim has its first two events and nothing more: no signal, no cancel request, no
    termination. And no workflow was started under the name a child would have had."""
    assert [e.event_type for e in history_of(stack, T9_VICTIM)] == [
        EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, EventType.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED]

    async def child(c: Client) -> str:
        try:
            await c.get_workflow_handle("t9-child").describe()
            return "started"
        except RPCError as e:
            return e.status.name
    assert stack._temporal(child) == "NOT_FOUND"


def refusals(stack: Stack) -> list[dict]:
    return [e for e in stack.agentd_log() if e.get("msg") == "runtime call refused"]


def test_t9_a_workflow_task_reaches_no_other_run(stack: Stack) -> None:
    """Each run of StepsOutOfLine sends one command no Worker may send, in the completion of its
    first workflow task: signal or cancel another run, start a child workflow, continue as new,
    schedule an activity on another task queue or of another type, upsert a search attribute
    agentd does not list. agentd refuses each completion whole, so nothing in it takes effect:
    the run never gets past its first workflow task, which keeps failing, and the victim's history
    holds nothing from any of them."""
    stack.start_agentd()
    start_victim(stack)

    async def register(c: Client) -> None:
        # Temporal itself would refuse an attribute it does not know: this one it takes.
        await c.operator_service.add_search_attributes(AddSearchAttributesRequest(
            namespace="default", search_attributes={"T9Elsewhere": IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD}))
    stack._temporal(register)
    stack.start_worker(workflows="tests.rogue_workflows:StepsOutOfLine")
    runs = {step: stack.start_run(base=step) for step in STEPS}

    def refused_all() -> bool:
        seen = {(e.get("command"), e["reason"]) for e in refusals(stack)}
        return all(any(c == command and why in reason for c, reason in seen) for command, why in STEPS.values())
    wait_for(refused_all, "agentd to refuse every step out of line", timeout=60)
    # agentd logs a refusal before it answers. Each run's first workflow task then ends without a
    # completion (it times out); a completion agentd had passed on would show in its place.
    ended = {EventType.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT, EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED}
    for step, run in runs.items():
        wait_for(lambda: any(e.event_type in ended for e in stack.history(run)), f"{step}'s first workflow task to end",
                 timeout=60, interval=1)
    print(json.dumps(refusals(stack), indent=2))
    assert {e["method"].rsplit("/", 1)[1] for e in refusals(stack)} == {"RespondWorkflowTaskCompleted"}

    assert_untouched(stack)
    for step, run in runs.items():
        events = stack.history(run)
        took = sorted(EventType.Name(e.event_type) for e in events if e.event_type in STEPPED_OUT)
        assert not took, (step, took)
        assert stack.get_run(run)["state"] == "RUNNING", (step, stack.get_run(run))
    assert not stack.ledger("SELECT * FROM calls"), "a step out of line reached the gateway"


def runtime_channel(stack: Stack) -> grpc.Channel:
    listen = stack.runtime_listen()
    if listen.startswith("unix://"):
        return grpc.insecure_channel("unix:" + listen.removeprefix("unix://"))
    return grpc.insecure_channel(listen.removeprefix("tcp://"))


# A Worker Deployment a poll or a completion would register, starting its system workflows.
ROGUE_DEPLOYMENT = WorkerDeploymentOptions(deployment_name="t9-rogue", build_id="b1",
                                           worker_versioning_mode=WorkerVersioningMode.WORKER_VERSIONING_MODE_VERSIONED)


def forbidden_calls(channel: grpc.Channel) -> dict[str, Callable[[], object]]:
    """Calls a Worker must not be able to make through the Runtime entry, each aimed at the
    victim where it names a run: what would steer, end or read another run, start one, answer
    the gateway's activities, poll another task queue, or reach another of Temporal's services."""
    s, op = WorkflowServiceStub(channel), OperatorServiceStub(channel)
    victim = WorkflowExecution(workflow_id=T9_VICTIM)
    audit = WorkflowType(name="security-audit")
    queue = TaskQueue(name="agentos-workflows")
    return {
        "SignalWorkflowExecution": lambda: s.SignalWorkflowExecution(ws.SignalWorkflowExecutionRequest(
            namespace="default", workflow_execution=victim, signal_name="doorbell")),
        "SignalWithStartWorkflowExecution": lambda: s.SignalWithStartWorkflowExecution(
            ws.SignalWithStartWorkflowExecutionRequest(namespace="default", workflow_id=T9_VICTIM, workflow_type=audit,
                                                       task_queue=queue, signal_name="doorbell")),
        "RequestCancelWorkflowExecution": lambda: s.RequestCancelWorkflowExecution(
            ws.RequestCancelWorkflowExecutionRequest(namespace="default", workflow_execution=victim)),
        "TerminateWorkflowExecution": lambda: s.TerminateWorkflowExecution(
            ws.TerminateWorkflowExecutionRequest(namespace="default", workflow_execution=victim)),
        "ResetWorkflowExecution": lambda: s.ResetWorkflowExecution(ws.ResetWorkflowExecutionRequest(
            namespace="default", workflow_execution=victim, workflow_task_finish_event_id=2)),
        "UpdateWorkflowExecution": lambda: s.UpdateWorkflowExecution(ws.UpdateWorkflowExecutionRequest(
            namespace="default", workflow_execution=victim)),
        "StartWorkflowExecution": lambda: s.StartWorkflowExecution(ws.StartWorkflowExecutionRequest(
            namespace="default", workflow_id="t9-started", workflow_type=audit, task_queue=queue)),
        "PollWorkflowTaskQueue(another queue)": lambda: s.PollWorkflowTaskQueue(ws.PollWorkflowTaskQueueRequest(
            namespace="default", task_queue=TaskQueue(name=NOBODY_POLLS)), timeout=5),
        # The workflows that hold the audit log's heads (agentd pushes them): a Worker that ran them,
        # or signalled them, could answer for heads agentd never pushed.
        "PollWorkflowTaskQueue(the audit log's anchors)": lambda: s.PollWorkflowTaskQueue(ws.PollWorkflowTaskQueueRequest(
            namespace="default", task_queue=TaskQueue(name=AUDIT_ANCHORS)), timeout=5),
        "SignalWorkflowExecution(the audit log's anchors)": lambda: s.SignalWorkflowExecution(ws.SignalWorkflowExecutionRequest(
            namespace="default", workflow_execution=WorkflowExecution(workflow_id=f"{AUDIT_ANCHORS}-2026-10-22"),
            signal_name="anchor")),
        "SignalWithStartWorkflowExecution(the audit log's anchors)": lambda: s.SignalWithStartWorkflowExecution(
            ws.SignalWithStartWorkflowExecutionRequest(namespace="default", workflow_id=f"{AUDIT_ANCHORS}-2026-10-22",
                                                       workflow_type=WorkflowType(name=AUDIT_ANCHORS),
                                                       task_queue=TaskQueue(name=AUDIT_ANCHORS), signal_name="anchor")),
        "PollWorkflowTaskQueue(deployment options)": lambda: s.PollWorkflowTaskQueue(ws.PollWorkflowTaskQueueRequest(
            namespace="default", task_queue=queue, deployment_options=ROGUE_DEPLOYMENT), timeout=5),
        "RespondWorkflowTaskCompleted(deployment options)": lambda: s.RespondWorkflowTaskCompleted(
            ws.RespondWorkflowTaskCompletedRequest(namespace="default", task_token=b"t9", deployment_options=ROGUE_DEPLOYMENT)),
        "PollActivityTaskQueue": lambda: s.PollActivityTaskQueue(ws.PollActivityTaskQueueRequest(
            namespace="default", task_queue=TaskQueue(name="gateway")), timeout=5),
        "RespondActivityTaskCompleted": lambda: s.RespondActivityTaskCompleted(
            ws.RespondActivityTaskCompletedRequest(namespace="default", task_token=b"t9")),
        "RespondActivityTaskCompletedById": lambda: s.RespondActivityTaskCompletedById(
            ws.RespondActivityTaskCompletedByIdRequest(namespace="default", workflow_id=T9_VICTIM, activity_id="1")),
        "DescribeWorkflowExecution": lambda: s.DescribeWorkflowExecution(ws.DescribeWorkflowExecutionRequest(
            namespace="default", execution=victim)),
        "ListWorkflowExecutions": lambda: s.ListWorkflowExecutions(ws.ListWorkflowExecutionsRequest(namespace="default")),
        "QueryWorkflow": lambda: s.QueryWorkflow(ws.QueryWorkflowRequest(
            namespace="default", execution=victim, query=WorkflowQuery(query_type="__stack_trace")), timeout=5),
        "OperatorService.AddSearchAttributes": lambda: op.AddSearchAttributes(AddSearchAttributesRequest(
            namespace="default", search_attributes={"T9Elsewhere": IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD})),
    }


def test_t9_the_runtime_entry_refuses_what_steers_another_run(stack: Stack) -> None:
    """The methods a Worker never needs are refused before Temporal sees them, whatever the
    sandbox: signalling, cancelling, terminating, resetting or updating a run, starting one,
    reading another run's state, answering or polling the gateway's activities, polling another
    task queue, Temporal's other services. So is a poll or a completion with deployment options,
    which would register a Worker Deployment under a name the Worker picks and start its system
    workflows. Temporal would refuse the completion's made-up task token: PERMISSION_DENIED is
    agentd's answer, before it."""
    stack.start_agentd()
    start_victim(stack)
    with runtime_channel(stack) as channel:
        got = {}
        for name, call in forbidden_calls(channel).items():
            try:
                call()
                got[name] = "allowed"
            except grpc.RpcError as e:
                got[name] = f"{e.code().name}: {e.details()[:120]}"
    print(json.dumps(got, indent=2))
    assert all(v.startswith("PERMISSION_DENIED") for v in got.values()), got
    assert_untouched(stack)

    async def deployment(c: Client) -> str:
        try:
            await c.get_workflow_handle("temporal-sys-worker-deployment:t9-rogue").describe()
            return "started"
        except RPCError as e:
            return e.status.name
    assert stack._temporal(deployment) == "NOT_FOUND"


sandboxed = pytest.mark.skipif(SANDBOX == "none", reason="needs AGENTOS_SANDBOX=bwrap or srt")

# What the Worker's environment holds, by sandbox: what the launcher sets (and the shell it starts
# the Worker with: PWD), and on macOS what
# sandbox-runtime (0.0.79) hands every command it runs (its proxy, with the session's credential
# the Worker dials the Runtime entry through, spelled for each tool that reads one; its git
# settings), what the shell it runs the command with sets, and what macOS adds to every process.
# Nothing of the caller's environment: a variable srt adds in another version fails this test.
SRT_ENV = {"SANDBOX_RUNTIME", "GIT_SSH_COMMAND", "GIT_CONFIG_PARAMETERS", "JAVA_TOOL_OPTIONS",
           "HTTPS_PROXY", "HTTP_PROXY", "ALL_PROXY", "NO_PROXY", "FTP_PROXY", "GRPC_PROXY", "RSYNC_PROXY",
           "https_proxy", "http_proxy", "all_proxy", "no_proxy", "ftp_proxy", "grpc_proxy",
           "DOCKER_HTTPS_PROXY", "DOCKER_HTTP_PROXY", "CLOUDSDK_PROXY_TYPE", "CLOUDSDK_PROXY_ADDRESS",
           "CLOUDSDK_PROXY_PORT", "CLOUDSDK_PROXY_USERNAME", "CLOUDSDK_PROXY_PASSWORD"}
WORKER_ENV = {
    "bwrap": {"PATH", "HOME", "LANG", "PYTHONDONTWRITEBYTECODE", "PWD"},
    "srt": {"PATH", "HOME", "LANG", "PYTHONDONTWRITEBYTECODE", "TMPDIR", "CLAUDE_CODE_TMPDIR",
            "PWD", "SHLVL", "_", "__CF_USER_TEXT_ENCODING", *SRT_ENV},
}


# agentd's environment holds it, its Worker's must not.
CANARY_ENV = "AGENTOS_TEST_CANARY"


def start_probing_agentd(stack: Stack, monkeypatch: pytest.MonkeyPatch) -> None:
    """agentd, with a secret in its environment, running its own Worker on tests/probe_workflows.py."""
    monkeypatch.setenv(CANARY_ENV, "a secret the Worker must not see")
    stack.start_agentd(worker=True, workflows="tests.probe_workflows:SandboxProbe")


def probe(stack: Stack, canary: Path, extra: dict) -> dict:
    """tests/sandbox_probe.py inside agentd's own Worker (start_probing_agentd), in its sandbox and
    with its environment: a run of SandboxProbe gets what to probe by a signal, and the Worker reports
    in its output, which agentd logs."""
    args = ["--temporal-port", stack.temporal_address.rsplit(":", 1)[1],
            "--control-socket", stack.control_socket, "--home", Path.home(), "--home-canary", canary,
            "--ledger", stack.data_dir / "ledger.db", "--task-queue", stack.workflow_queue, "--extra", json.dumps(extra)]
    if SANDBOX == "bwrap":
        args += ["--runtime", "127.0.0.1:7233"]
    else:
        args += ["--runtime", f"127.0.0.1:{stack.runtime_port}", "--proxy", "env"]
    run = stack.start_run()
    stack.signal(run, "probe", [str(a) for a in args])

    def report() -> dict | None:
        parts: dict[int, str] = {}
        n = 0
        for line in stack.worker().output():
            if line.startswith(REPORT_LINE + " "):
                _, count, part = (line + " ").split(" ", 2)
                i, n = map(int, count.split("/"))
                parts[i] = part.strip()
        if not n or len(parts) < n:
            return None
        return json.loads(base64.b64decode("".join(parts[i] for i in range(1, n + 1))))
    found = wait_for(report, "the probe's report in agentd's log of its Worker", timeout=180, interval=1)
    assert "failed" not in found, found
    return found


@sandboxed
def test_t9_the_worker_sandbox_reaches_temporal_only_through_agentd(stack: Stack, monkeypatch: pytest.MonkeyPatch) -> None:
    """Inside agentd's own Worker's sandbox the Temporal SDK reaches Temporal through agentd's Runtime
    entry and nothing else: not Temporal's own port, the Control socket, the network, the home
    directory, agentd's ledger, the keychain (macOS) or the session bus (Linux), where a desktop's
    Secret Service would hand out keys. Through the entry it starts no workflow, not even on its own
    task queue. On macOS it reads nothing in the shared temporary directories either (/tmp, /var/tmp,
    the user's own under /var/folders), which hold other programs' files. Its environment holds only
    what agentd and the launcher gave it: nothing of agentd's own."""
    start_probing_agentd(stack, monkeypatch)
    tag = secrets.token_hex(4)
    canary = Path.home() / f".agentos-canary-{tag}"
    planted = []
    extra: dict = {"reads": {}, "sockets": {}}
    if SANDBOX == "srt":
        for key, where in {"tmp": Path("/tmp"), "var_tmp": Path("/var/tmp"), "user_tmp": user_temp_dir()}.items():
            p = where / f"agentos-canary-{tag}"
            planted.append(p)
            extra["reads"][key] = str(p)
    else:
        bus = Path(f"/run/user/{os.getuid()}/bus")
        if bus.exists():
            extra["sockets"]["session_bus"] = str(bus)
    for p in [canary, *planted]:
        p.write_text("a file the Worker must not read")
    try:
        report = probe(stack, canary, extra)
    finally:
        for p in [canary, *planted]:
            p.unlink()
    print(json.dumps(report, indent=2))

    assert report["runtime_sdk"].startswith("connected"), report
    assert report["forbidden"] and all(v.startswith("refused: PERMISSION_DENIED")
                                       for v in report["forbidden"].values()), report["forbidden"]
    for key in ("temporal_sdk", "temporal_tcp", "internet_tcp", "control_socket", "home_canary", "ssh_dir", "ledger",
                "keychains", "keychain_items"):
        assert report[key].startswith("refused"), (key, report[key])
    for group in ("reads", "sockets"):
        for key, result in report[group].items():
            assert result.startswith("refused"), (group, key, result)
    if SANDBOX == "bwrap":
        assert report["dev"] == ["full", "null", "random", "shm", "urandom", "zero"], report["dev"]
    assert set(report["env"]) <= WORKER_ENV[SANDBOX], sorted(set(report["env"]) - WORKER_ENV[SANDBOX])


@pytest.mark.skipif(SANDBOX != "srt", reason="the macOS keychain")
def test_t9_the_worker_finds_no_key_in_the_keychain(stack: Stack, monkeypatch: pytest.MonkeyPatch) -> None:
    """agentd's keys live in the login keychain, which the Worker's sandbox cannot read. The
    sandbox lets it reach securityd all the same (sandbox-runtime allows those Mach services, and
    has no setting to stop it): asked for by name, the way agentd reads them, the items must still
    not be found. Attributes only, never a secret. Without an agentos item in the keychain there
    is nothing to look for: the test is skipped, not passed."""
    items = [s for s in KEYCHAIN_ITEMS if in_keychain(s)]
    if not items:
        pytest.skip("no agentos item in the login keychain")
    start_probing_agentd(stack, monkeypatch)
    canary = Path.home() / f".agentos-canary-{secrets.token_hex(4)}"
    canary.write_text("a file the Worker must not read")
    try:
        report = probe(stack, canary, {"keychain_file": str(LOGIN_KEYCHAIN), "keychain_items": items})
    finally:
        canary.unlink()
    print(json.dumps(report["keychain"], indent=2))
    assert set(report["keychain"]) == set(items), report["keychain"]
    for service, result in report["keychain"].items():
        assert result.startswith("refused"), (service, result)
