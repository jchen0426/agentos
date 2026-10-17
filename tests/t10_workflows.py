"""Two candidate next versions of the security-audit workflow, used by T10.

Both add one step after the draft PR is opened: read the diff again to attach it to the
report. PatchedV2 guards the new step with workflow.patched(); BreakingV2 does not.
"""

from temporalio import workflow

from agentos_worker.workflows.security_audit import AuditInput, SecurityAudit

PATCH_ID = "attach-diff-to-report"


class _V2(SecurityAudit):
    guarded = True

    async def audit(self, a: AuditInput) -> tuple[str, str | None, str | None]:
        status, diff = await self.call("tool", "git.show_diff", repo=a.repo, commit=a.commit)
        if status != "ok":
            return status, None, None
        status, report = await self.call("model", "audit_v1", diff_ref=diff["blob"])
        if status != "ok":
            return status, None, None
        self.report_ref = report["report_ref"]
        patch_ref = report.get("patch_ref")
        while patch_ref:
            status, _ = await self.call(
                "tool", "git.apply_check", repo=a.repo, commit=a.commit, patch_ref=patch_ref,
            )
            if status != "ok":
                return "patch rejected", report["report_ref"], None
            status, pr = await self.call(
                "tool", "github.open_draft_pr", repo=a.repo, commit=a.commit,
                base_branch=a.base_branch, patch_ref=patch_ref,
            )
            if status == "edited":
                patch_ref = pr["patch_ref"]
                continue
            if status != "ok":
                return status, report["report_ref"], None
            if not self.guarded or workflow.patched(PATCH_ID):
                await self.call("tool", "git.show_diff", repo=a.repo, commit=a.commit)  # the new step
            return "ok", report["report_ref"], pr["url"]
        return "ok", report["report_ref"], None


@workflow.defn(name="security-audit")
class PatchedV2(_V2):
    @workflow.run
    async def run(self, a: AuditInput) -> dict:
        return await super().run(a)

    @workflow.signal
    def doorbell(self) -> None:
        super().doorbell()


@workflow.defn(name="security-audit")
class BreakingV2(_V2):
    guarded = False

    @workflow.run
    async def run(self, a: AuditInput) -> dict:
        return await super().run(a)

    @workflow.signal
    def doorbell(self) -> None:
        super().doorbell()
