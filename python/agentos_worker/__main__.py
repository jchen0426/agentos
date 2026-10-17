"""Runs the Agent OS Worker: task workflows only, no activities, no keys.

The Worker reaches Temporal only through agentd's Runtime entry. Inside the Linux sandbox
that is 127.0.0.1:7233, which socat forwards to the bind-mounted Runtime socket; on macOS
it is agentd's loopback port, reached through sandbox-runtime's HTTP CONNECT proxy.
"""

import argparse
import asyncio
import importlib
import logging
import os
import signal
from urllib.parse import unquote, urlsplit

from temporalio.client import Client
from temporalio.service import HttpConnectProxyConfig
from temporalio.worker import Worker


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
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    client = await Client.connect(
        args.endpoint, namespace=args.namespace, http_connect_proxy_config=proxy_from(args.http_connect_proxy),
    )
    # Sticky execution off: the Runtime proxy refuses sticky queues, whose names the Worker
    # picks and Temporal does not tie to a task queue. Each workflow task replays the history.
    worker = Worker(client, task_queue=args.task_queue, workflows=load_workflows(args.workflows),
                    max_cached_workflows=0)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    async with worker:
        logging.info("worker ready: endpoint=%s task_queue=%s", args.endpoint, args.task_queue)
        await stop.wait()


if __name__ == "__main__":
    asyncio.run(main())
