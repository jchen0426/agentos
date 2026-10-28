"""The workflow that has agentd's own Worker run tests/sandbox_probe.py (tests/test_t9_isolation.py)."""

import threading

from temporalio import workflow

from agentos_worker.workflows.security_audit import AuditInput, SecurityAudit

with workflow.unsafe.imports_passed_through():
    from tests import sandbox_probe


def probe_in_the_background(args: list[str]) -> None:
    """The probe, in a thread of the Worker's own: the workflow sandbox restricts each thread until
    it says otherwise, this one included."""
    with workflow.unsafe.sandbox_unrestricted():
        sandbox_probe.report_from_worker(args)


@workflow.defn(name="security-audit")
class SandboxProbe(SecurityAudit):
    """Waits for the probe signal, which says what to probe, then has the Worker that runs it run
    tests/sandbox_probe.py, as a child of its own, and ends. The report goes to the Worker's output,
    which agentd logs (sandbox_probe.report_from_worker)."""

    def __init__(self) -> None:
        super().__init__()
        self.args: list[str] | None = None

    @workflow.run
    async def run(self, a: AuditInput) -> dict:
        await workflow.wait_condition(lambda: self.args is not None)
        if not workflow.unsafe.is_replaying():  # once, not again in a replay
            with workflow.unsafe.sandbox_unrestricted():
                threading.Thread(target=probe_in_the_background, args=(self.args,), daemon=True).start()
        return {}

    @workflow.signal
    def probe(self, args: list[str]) -> None:
        self.args = args

    @workflow.signal
    def doorbell(self) -> None:
        super().doorbell()


WORKFLOWS = [SandboxProbe]
