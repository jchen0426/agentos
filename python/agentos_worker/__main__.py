"""Runs the Agent OS Worker: task workflows only, no activities, no keys.

The Worker reaches Temporal only through agentd's Runtime entry. Inside the Linux sandbox
that is 127.0.0.1:7233, which socat forwards to the bind-mounted Runtime socket; on macOS
it is agentd's loopback port, reached through sandbox-runtime's HTTP CONNECT proxy.
"""

import argparse
import asyncio
import importlib
import io
import logging
import os
import select
import signal
import sys
from urllib.parse import unquote, urlsplit

from temporalio.client import Client
from temporalio.service import HttpConnectProxyConfig
from temporalio.worker import Worker

# Workflow tasks at once, and so the threads the SDK runs them on: unset, its executor may grow to
# 500 threads (its slots are 100), on top of one thread a CPU for the SDK's core. The sandbox's
# task limit (M5 C3) counts threads.
MAX_WORKFLOW_TASKS = 100


class _Waiting(io.RawIOBase):
    """A file descriptor written as if it were in blocking mode, whether it is or not."""

    def __init__(self, fd: int) -> None:
        self._fd = fd

    def writable(self) -> bool:
        return True

    def fileno(self) -> int:
        return self._fd

    def write(self, b) -> int:  # noqa: ANN001 - a bytes-like object
        while True:
            try:
                return os.write(self._fd, b)
            except BlockingIOError:
                select.select([], [self._fd], [])


def wait_on_output() -> None:
    """Make the Worker's stdout and stderr wait while agentd has not read what came before, as a
    pipe in blocking mode does. Under sandbox-runtime they need not be in blocking mode: srt hands
    the Worker its own stdout and stderr, and node puts them into non-blocking mode once srt
    proxies a connection. A write to a full pipe would then fail (EAGAIN): a workflow's print(),
    or the Worker as it imports workflows that write a lot."""
    sys.stdout = io.TextIOWrapper(io.BufferedWriter(_Waiting(1)), encoding=sys.stdout.encoding,
                                  errors=sys.stdout.errors)
    sys.stderr = io.TextIOWrapper(io.BufferedWriter(_Waiting(2)), encoding=sys.stderr.encoding,
                                  errors=sys.stderr.errors, line_buffering=True)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="agentos-worker", description=__doc__.splitlines()[0])
    p.add_argument("--endpoint", default="127.0.0.1:7233", help="Runtime entry, host:port")
    p.add_argument("--namespace", default="default")
    p.add_argument("--task-queue", default="agentos-workflows")
    p.add_argument(
        "--workflows", default="agentos_worker.workflows:WORKFLOWS", metavar="MODULE:ATTR",
        help="workflow classes to serve: a list, or one class (a new version, for example)",
    )
    p.add_argument(
        "--http-connect-proxy", default=None, metavar="HOST:PORT",
        help='HTTP CONNECT proxy to reach the endpoint through; "env" reads HTTPS_PROXY/HTTP_PROXY',
    )
    return p.parse_args()


def load_workflows(spec: str) -> list[type]:
    module, _, attr = spec.partition(":")
    found = getattr(importlib.import_module(module), attr)
    return list(found) if isinstance(found, (list, tuple)) else [found]


def proxy_from(arg: str | None) -> HttpConnectProxyConfig | None:
    if not arg:
        return None
    if arg == "env":
        # sandbox-runtime sets these, with a per-session credential in the userinfo.
        arg = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY") or ""
        if not arg:
            raise SystemExit("--http-connect-proxy env: neither HTTPS_PROXY nor HTTP_PROXY is set")
    u = urlsplit(arg if "://" in arg else "http://" + arg)
    if not u.hostname or not u.port:
        raise SystemExit(f"--http-connect-proxy: cannot parse {arg!r}")
    auth = (unquote(u.username), unquote(u.password or "")) if u.username else None
    return HttpConnectProxyConfig(target_host=f"{u.hostname}:{u.port}", basic_auth=auth)


async def main() -> None:
    args = parse_args()
    wait_on_output()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    client = await Client.connect(
        args.endpoint, namespace=args.namespace, http_connect_proxy_config=proxy_from(args.http_connect_proxy),
    )
    # Sticky execution off: the Runtime proxy refuses sticky queues, whose names the Worker
    # picks and Temporal does not tie to a task queue. Each workflow task replays the history.
    worker = Worker(client, task_queue=args.task_queue, workflows=load_workflows(args.workflows),
                    max_cached_workflows=0, max_concurrent_workflow_tasks=MAX_WORKFLOW_TASKS)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    async with worker:
        logging.info("worker ready: endpoint=%s task_queue=%s", args.endpoint, args.task_queue)
        await stop.wait()


if __name__ == "__main__":
    asyncio.run(main())
