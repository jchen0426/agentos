"""Runs inside the Worker sandbox and reports, as one JSON object on stdout, what the
sandbox lets through. tests/test_t9_isolation.py has agentd's own Worker run it, as a child of its
own (report_from_worker, which tests/probe_workflows.py calls)."""

import argparse
import asyncio
import base64
import json
import os
import socket
import subprocess
import sys
import time

from temporalio.api import workflowservice
from temporalio.api.common.v1 import WorkflowExecution, WorkflowType
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.client import Client
from temporalio.service import RPCError

from agentos_worker.__main__ import proxy_from


def tcp(host: str, port: int) -> str:
    try:
        with socket.create_connection((host, port), timeout=3):
            return "connected"
    except OSError as e:
        return f"refused: {e.__class__.__name__}: {e}"


def unix(path: str) -> str:
    try:
        with socket.socket(socket.AF_UNIX) as s:
            s.settimeout(3)
            s.connect(path)
            return "connected"
    except OSError as e:
        return f"refused: {e.__class__.__name__}: {e}"


def read(path: str) -> str:
    try:
        with open(path) as f:
            return f"read {len(f.read())} bytes"
    except OSError as e:
        return f"refused: {e.__class__.__name__}: {e}"


def listdir_names(path: str) -> list[str] | str:
    try:
        return sorted(os.listdir(path))
    except OSError as e:
        return f"refused: {e.__class__.__name__}: {e}"


def listdir(path: str) -> str:
    try:
        return f"listed {len(os.listdir(path))} entries"
    except OSError as e:
        return f"refused: {e.__class__.__name__}: {e}"


def keychain_item(service: str, keychain: str) -> str:
    """Whether an item agentd may keep a secret in can be found: its attributes only, never
    the secret (no -w, no -g)."""
    try:
        r = subprocess.run(["/usr/bin/security", "find-generic-password", "-s", service, keychain],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=20)
    except (OSError, subprocess.TimeoutExpired) as e:
        return f"refused: {type(e).__name__}"
    return "found" if r.returncode == 0 else f"refused: exit {r.returncode}"


def keychain_items(home: str) -> str:
    """Whether the user's login keychain, where agentd's model keys live, can be listed: item
    names only, never a secret. The system keychain is out of scope: it holds no user keys."""
    login = os.path.join(home, "Library", "Keychains", "login.keychain-db")
    try:
        r = subprocess.run(["/usr/bin/security", "dump-keychain", login], capture_output=True, text=True, timeout=20)
    except (OSError, subprocess.TimeoutExpired) as e:
        return f"refused: {type(e).__name__}"
    items = r.stdout.count("class:")
    return f"readable: {items} items" if r.returncode == 0 and items else f"refused: no items listed (exit {r.returncode})"


async def sdk(endpoint: str, proxy: str | None) -> tuple[str, Client | None]:
    try:
        c = await Client.connect(endpoint, http_connect_proxy_config=proxy_from(proxy))
        info = await c.service_client.workflow_service.get_system_info(
            workflowservice.v1.GetSystemInfoRequest())
        return f"connected: server {info.server_version}", c
    except Exception as e:
        return f"refused: {e.__class__.__name__}: {str(e)[:300]}", None


# The lines a Worker writes the probe's report in, for agentd to log (report_from_worker).
REPORT_LINE = "agentos-probe-report"


def report_from_worker(args: list[str]) -> None:
    """Runs this probe with args as a child of the calling process, a Worker: in its sandbox, with its
    environment. The report goes to the Worker's stdout, which agentd logs: in base64, in lines of
    REPORT_LINE <i>/<n> <part>, short and slow enough for agentd's log (2 KiB a line, 20 lines a
    second)."""
    r = subprocess.run([sys.executable, os.path.abspath(__file__), *args], stdin=subprocess.DEVNULL,
                       capture_output=True, text=True, timeout=300)
    out = r.stdout.strip().splitlines()
    body = out[-1] if r.returncode == 0 and out else json.dumps({"failed": r.returncode, "stderr": r.stderr[-3000:]})
    data = base64.b64encode(body.encode()).decode()
    parts = [data[i:i + 1500] for i in range(0, len(data), 1500)]
    for i, part in enumerate(parts, 1):
        print(f"{REPORT_LINE} {i}/{len(parts)} {part}", flush=True)
        time.sleep(0.1)


async def forbidden(c: Client, task_queue: str) -> dict[str, str]:
    """Calls the Worker must not be able to make through the Runtime entry."""
    ws = c.service_client.workflow_service
    wf = WorkflowExecution(workflow_id="any-run")
    v1 = workflowservice.v1
    calls = {
        "SignalWorkflowExecution": lambda: ws.signal_workflow_execution(
            v1.SignalWorkflowExecutionRequest(namespace="default", workflow_execution=wf, signal_name="doorbell")),
        "TerminateWorkflowExecution": lambda: ws.terminate_workflow_execution(
            v1.TerminateWorkflowExecutionRequest(namespace="default", workflow_execution=wf)),
        "StartWorkflowExecution": lambda: ws.start_workflow_execution(
            v1.StartWorkflowExecutionRequest(namespace="default", workflow_id="x", workflow_type=WorkflowType(name="security-audit"),
                                             task_queue=TaskQueue(name=task_queue))),
        "PollActivityTaskQueue": lambda: ws.poll_activity_task_queue(
            v1.PollActivityTaskQueueRequest(namespace="default", task_queue=TaskQueue(name="gateway"))),
        "RespondActivityTaskCompletedById": lambda: ws.respond_activity_task_completed_by_id(
            v1.RespondActivityTaskCompletedByIdRequest(namespace="default", workflow_id="any-run", activity_id="1")),
    }
    out = {}
    for name, call in calls.items():
        try:
            await call()
            out[name] = "allowed"
        except RPCError as e:
            out[name] = f"refused: {e.status.name}: {e.message[:120]}"
    return out


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--runtime", required=True)
    p.add_argument("--proxy", default=None)
    p.add_argument("--temporal-port", type=int, required=True)
    p.add_argument("--control-socket", required=True)
    p.add_argument("--home", required=True)
    p.add_argument("--home-canary", required=True, help="a file in the home directory")
    p.add_argument("--ledger", required=True, help="agentd's ledger file")
    p.add_argument("--task-queue", required=True, help="the Worker's own, where agentd starts runs")
    p.add_argument("--extra", default="{}", help="JSON: more reads, sockets and keychain items to try")
    a = p.parse_args()
    extra = json.loads(a.extra)

    report: dict = {}
    report["runtime_sdk"], client = await sdk(a.runtime, a.proxy)
    report["forbidden"] = await forbidden(client, a.task_queue) if client else {}
    report["temporal_sdk"], _ = await sdk(f"127.0.0.1:{a.temporal_port}", a.proxy)
    report["temporal_tcp"] = tcp("127.0.0.1", a.temporal_port)
    report["internet_tcp"] = tcp("1.1.1.1", 443)
    report["control_socket"] = unix(a.control_socket)
    report["home_canary"] = read(a.home_canary)
    report["ledger"] = read(a.ledger)
    report["ssh_dir"] = listdir(os.path.join(a.home, ".ssh"))
    # agentd's model keys live in the login keychain (macOS): out of the Worker's reach.
    report["keychains"] = listdir(os.path.join(a.home, "Library", "Keychains"))
    report["keychain_items"] = keychain_items(a.home)
    report["reads"] = {k: read(path) for k, path in extra.get("reads", {}).items()}
    report["sockets"] = {k: unix(path) for k, path in extra.get("sockets", {}).items()}
    report["keychain"] = {s: keychain_item(s, extra["keychain_file"]) for s in extra.get("keychain_items", [])}
    report["dev"] = listdir_names("/dev")
    report["env"] = sorted(os.environ)
    print(json.dumps(report))


if __name__ == "__main__":
    asyncio.run(main())
