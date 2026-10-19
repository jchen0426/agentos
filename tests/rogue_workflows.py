"""Workflows that misbehave, for the tests of agentd's own enforcement (tests/test_cancel.py,
tests/test_write_limits.py)."""

import asyncio

from temporalio import workflow

from agentos_worker.workflows.security_audit import AuditInput, SecurityAudit, invoke


@workflow.defn(name="security-audit")
class IgnoresCancel(SecurityAudit):
    """Swallows the cancel request, tries a write afterwards and never ends on its own."""

    @workflow.run
    async def run(self, a: AuditInput) -> dict:
        try:
            await workflow.wait_condition(lambda: False)
        except asyncio.CancelledError:
            pass  # nothing makes a workflow honour a cancel request
        status, _ = await invoke("1:github.comment_commit", "tool", "github.comment_commit",
                                 repo=a.repo, commit=a.commit, outcome="ignored the cancel",
                                 report_ref=None, pr_url=None)
        await workflow.wait_condition(lambda: False)
        return {"after_cancel": status}

    @workflow.signal
    def doorbell(self) -> None:
        super().doorbell()


@workflow.defn(name="security-audit")
class ManyReports(SecurityAudit):
    """Posts three reports on the commit, each a little different, so each is a new effect."""

    @workflow.run
    async def run(self, a: AuditInput) -> dict:
        statuses = []
        for i in range(3):
            status, _ = await self.call("tool", "github.comment_commit", repo=a.repo, commit=a.commit,
                                        outcome=f"report {i}", report_ref=None, pr_url=None)
            statuses.append(status)
        return {"statuses": statuses}

    @workflow.signal
    def doorbell(self) -> None:
        super().doorbell()


@workflow.defn(name="security-audit")
class OversizedCompletion(SecurityAudit):
    """Schedules three gateway calls at once, each with 1.5 MiB of arguments: every payload is
    under Temporal's 2 MiB limit, which the SDK checks itself, but the workflow task's completion
    is over the 4 MiB Temporal takes in a request, so it can never be delivered."""

    @workflow.run
    async def run(self, a: AuditInput) -> dict:
        await asyncio.gather(*(invoke(f"{i}:test.big", "tool", "test.big", padding="x" * (3 << 19))
                               for i in range(1, 4)))
        return {}

    @workflow.signal
    def doorbell(self) -> None:
        super().doorbell()


@workflow.defn(name="security-audit")
class ManyCalls(SecurityAudit):
    """Schedules 5,000 gateway calls at once: well under 4 MiB, but over 100,000 values in the
    workflow task's completion, more than the Runtime proxy decodes."""

    @workflow.run
    async def run(self, a: AuditInput) -> dict:
        await asyncio.gather(*(invoke(f"{i}:test.x", "tool", "test.x") for i in range(1, 5001)))
        return {}

    @workflow.signal
    def doorbell(self) -> None:
        super().doorbell()
