"""A workflow for T7 (tests/test_t7_budget.py): many model calls at once, sharing one budget."""

import asyncio

from temporalio import workflow

from agentos_worker.workflows.security_audit import AuditInput, SecurityAudit

CALLS = 50


@workflow.defn(name="security-audit")
class FanOut(SecurityAudit):
    """Reads the run's diff, then sends CALLS model calls at once, all of them with that diff: a
    model task takes only the inputs models.yaml declares, each a blob of the run's own, and the
    run has this one, so the calls are alike, argument for argument. A call the budget cannot
    cover waits in PAUSED_BUDGET for the doorbell, like any call of the security audit, then asks
    again."""

    @workflow.run
    async def run(self, a: AuditInput) -> dict:
        _, diff = await self.call("tool", "git.show_diff", repo=a.repo, commit=a.commit)
        results = await asyncio.gather(*(self.call("model", "audit_v1", diff_ref=diff["blob"]) for _ in range(CALLS)))
        return {"statuses": [status for status, _ in results]}

    @workflow.signal
    def doorbell(self) -> None:
        super().doorbell()
