"""A workflow for T3 (tests/test_t3_reconcile.py): one write through the unsafe test tool. When
its outcome is unknown the call waits in WAITING_RECONCILE, like any call of the security
audit, and asks again when the doorbell rings."""

from temporalio import workflow

from agentos_worker.workflows.security_audit import AuditInput, SecurityAudit


@workflow.defn(name="security-audit")
class UnsafeWrite(SecurityAudit):
    @workflow.run
    async def run(self, a: AuditInput) -> dict:
        status, value = await self.call("tool", "test.unsafe_write", repo=a.repo, note="t3")
        return {"status": status, "value": value}

    @workflow.signal
    def doorbell(self) -> None:
        super().doorbell()
