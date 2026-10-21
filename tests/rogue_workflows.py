"""Workflows that misbehave, for the tests of agentd's own enforcement (tests/test_cancel.py,
tests/test_write_limits.py, tests/test_t5_payload_binding.py, tests/test_t9_isolation.py)."""

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import SearchAttributeKey

from agentos_worker.workflows.security_audit import GATEWAY, AuditInput, SecurityAudit, invoke

with workflow.unsafe.imports_passed_through():
    from agentos.v1.gateway_pb2 import Call

# A run no workflow task of another run may reach: tests/test_t9_isolation.py starts it, on a
# task queue nobody polls, before any run of StepsOutOfLine.
T9_VICTIM = "t9-victim"


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


@workflow.defn(name="security-audit")
class SwapsThePatch(SecurityAudit):
    """Has its patch approved through an edit, then sends its own patch instead of the approved one:
    it asks for approval of the reviewer's patch, and once the reviewer approves that, calls the
    pull request again with the patch the model wrote."""

    @workflow.run
    async def run(self, a: AuditInput) -> dict:
        _, diff = await self.call("tool", "git.show_diff", repo=a.repo, commit=a.commit)
        _, report = await self.call("model", "audit_v1", diff_ref=diff["blob"])
        pr = dict(repo=a.repo, commit=a.commit, base_branch=a.base_branch)
        mine = report["patch_ref"]
        status, value = await self.call("tool", "github.open_draft_pr", patch_ref=mine, **pr)
        if status != "edited":
            return {"statuses": [status]}
        seen = self.rings
        asked, _ = await invoke(self.activity_id("github.open_draft_pr"), "tool", "github.open_draft_pr",
                                patch_ref=value["patch_ref"], **pr)
        await workflow.wait_condition(lambda: self.rings > seen)  # the reviewer approved the edit
        swapped, _ = await invoke(self.activity_id("github.open_draft_pr"), "tool", "github.open_draft_pr",
                                  patch_ref=mine, **pr)
        return {"statuses": [status, asked, swapped]}

    @workflow.signal
    def doorbell(self) -> None:
        super().doorbell()


@workflow.defn(name="security-audit")
class StepsOutOfLine(SecurityAudit):
    """Sends, in its first workflow task, one command no Worker may send (T9), named by the run's
    base branch. Each would reach past the gateway: another run, a workflow of its own, or a task
    queue or activity agentd does not serve. The Runtime entry refuses the whole completion, so
    the workflow task fails, again and again, and nothing in it takes effect."""

    @workflow.run
    async def run(self, a: AuditInput) -> dict:
        victim = workflow.get_external_workflow_handle(T9_VICTIM)
        call = Call(kind="tool", name="git.show_diff", args_json="{}")
        if a.base_branch == "signal":
            await victim.signal("doorbell")
        elif a.base_branch == "cancel":
            await victim.cancel()
        elif a.base_branch == "child":
            await workflow.execute_child_workflow("security-audit", a, id="t9-child", task_queue=workflow.info().task_queue)
        elif a.base_branch == "continue-as-new":
            workflow.continue_as_new(a)
        elif a.base_branch == "activity-queue":
            await workflow.execute_activity("Invoke", call, activity_id="1:git.show_diff", task_queue="t9-elsewhere",
                                            start_to_close_timeout=timedelta(minutes=1))
        elif a.base_branch == "activity-type":
            await workflow.execute_activity("NotInvoke", call, activity_id="1:git.show_diff", task_queue=GATEWAY,
                                            start_to_close_timeout=timedelta(minutes=1))
        elif a.base_branch == "search-attribute":
            workflow.upsert_search_attributes([SearchAttributeKey.for_keyword("T9Elsewhere").value_set("x")])
            await workflow.wait_condition(lambda: False)
        else:
            raise ValueError(f"no such step out of line: {a.base_branch}")
        return {}

    @workflow.signal
    def doorbell(self) -> None:
        super().doorbell()
