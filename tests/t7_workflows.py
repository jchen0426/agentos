"""A workflow for T7 (tests/test_t7_budget.py): many model calls at once, sharing one budget."""

import asyncio

from temporalio import workflow

from agentos_worker.workflows.security_audit import AuditInput, SecurityAudit

CALLS = 50


@workflow.defn(name="security-audit")
class FanOut(SecurityAudit):
    """Sends CALLS model calls at once. A call the budget cannot cover waits in PAUSED_BUDGET
    for the doorbell, like any call of the security audit, then asks again."""

    @workflow.run
    async def run(self, a: AuditInput) -> dict:
        results = await asyncio.gather(*(self.call("model", "audit_v1", diff_ref=f"fan-out-{i}") for i in range(CALLS)))
        return {"statuses": [status for status, _ in results]}

    @workflow.signal
    def doorbell(self) -> None:
        super().doorbell()
