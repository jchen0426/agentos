"""The workflow that has agentd's own Worker go over its resource limits (tests/test_resource_limits.py,
M5 C3a): from a thread of its own, once a signal says how, then waits for another to end."""

import threading
import time

from temporalio import workflow

from agentos_worker.workflows.security_audit import AuditInput, SecurityAudit

# What the Worker writes of what it did, which agentd logs: MARK <nonce> <what it found>.
MARK = "agentos-limits-test"


def allocate(nonce: str, mib: int) -> None:
    """Holds mib MiB more, 16 MiB at a time, each page written: past the Worker's memory limit the
    kernel kills it. First it waits a while, for the workflow task that started it to complete: a
    replay of that task starts nothing (OverTheLimits), so the next generation of the Worker, which
    replays it, carries the run on to its end."""
    with workflow.unsafe.sandbox_unrestricted():
        time.sleep(3)
        print(f"{MARK} {nonce} allocating {mib} MiB", flush=True)
        held = [b"\x01" * (16 << 20) for _ in range(mib // 16)]
        print(f"{MARK} {nonce} allocated {len(held) * 16} MiB", flush=True)


def spawn(nonce: str, most: int) -> None:
    """Starts threads that wait, until a new one cannot start (or most have), says how many did and
    why the next did not, and lets them all end."""
    with workflow.unsafe.sandbox_unrestricted():
        release = threading.Event()
        started, why = 0, "none"
        try:
            while started < most:
                threading.Thread(target=release.wait, daemon=True).start()
                started += 1
        except RuntimeError as e:
            why = str(e)
        release.set()
        print(f"{MARK} {nonce} started {started} threads; the next: {why}", flush=True)


@workflow.defn(name="security-audit")
class OverTheLimits(SecurityAudit):
    """Waits for the go signal ({"nonce": ..., "allocate_mib": n} or {"nonce": ..., "threads": n}), and
    has the Worker that runs it do that in a thread of its own; then waits for the finish signal, and
    ends. A replay of the go signal's workflow task starts nothing."""

    def __init__(self) -> None:
        super().__init__()
        self.request: dict | None = None
        self.finished = False

    @workflow.run
    async def run(self, a: AuditInput) -> dict:
        await workflow.wait_condition(lambda: self.request is not None)
        r = self.request
        if not workflow.unsafe.is_replaying():
            with workflow.unsafe.sandbox_unrestricted():
                if "allocate_mib" in r:
                    threading.Thread(target=allocate, args=(r["nonce"], r["allocate_mib"]), daemon=True).start()
                else:
                    threading.Thread(target=spawn, args=(r["nonce"], r["threads"]), daemon=True).start()
        await workflow.wait_condition(lambda: self.finished)
        return {}

    @workflow.signal
    def go(self, request: dict) -> None:
        self.request = request

    @workflow.signal
    def finish(self) -> None:
        self.finished = True

    @workflow.signal
    def doorbell(self) -> None:
        super().doorbell()


WORKFLOWS = [OverTheLimits]
