"""Runs inside the Worker sandbox and reports, as one JSON object on stdout, what the
sandbox lets through. tests/test_sandbox.py launches it the way Workers are launched."""

import argparse
import asyncio
import json
import os
import socket
import subprocess

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


def listdir(path: str) -> str:
    try:
        return f"listed {len(os.listdir(path))} entries"
    except OSError as e:
        return f"refused: {e.__class__.__name__}: {e}"


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


async def forbidden(c: Client) -> dict[str, str]:
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
                                             task_queue=TaskQueue(name="agentos-workflows"))),
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
    a = p.parse_args()

    report: dict = {}
    report["runtime_sdk"], client = await sdk(a.runtime, a.proxy)
    report["forbidden"] = await forbidden(client) if client else {}
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
    report["env"] = sorted(os.environ)
    print(json.dumps(report))


if __name__ == "__main__":
    asyncio.run(main())
