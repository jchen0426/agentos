"""A workflow for T13 (tests/test_t13_model_errors.py): one whose model call carries an argument of
its own besides its task's input."""

from temporalio import workflow

from agentos_worker.workflows.security_audit import AuditInput, SecurityAudit

# Words of the workflow's own that its model call would carry to the model, whose answer is the run's
# public report.
CARRIED = "carried-to-the-model-0123456789abcdef"


@workflow.defn(name="security-audit")
class AddsToTheModelCall(SecurityAudit):
    """Sends the security audit's model call with the run's diff and an argument of its own."""

    async def audit(self, a: AuditInput) -> tuple[str, str | None, str | None]:
        _, diff = await self.call("tool", "git.show_diff", repo=a.repo, commit=a.commit)
        status, _ = await self.call("model", "audit_v1", diff_ref=diff["blob"], note=CARRIED)
        return status, None, None

    @workflow.run
    async def run(self, a: AuditInput) -> dict:
        return await super().run(a)

    @workflow.signal
    def doorbell(self) -> None:
        super().doorbell()
