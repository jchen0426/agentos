"""The first task: a security audit of one commit (design doc, sections 3 and 7).

The workflow only orchestrates. Every model and tool call is one gateway activity that
agentd executes; the workflow cannot reach the outside world any other way.
"""

import asyncio
import json
import re
from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy, SearchAttributeKey
from temporalio.exceptions import ActivityError, ApplicationError, CancelledError

with workflow.unsafe.imports_passed_through():
    from agentos.v1.gateway_pb2 import Call, Result  # generated from proto/agentos/v1/gateway.proto

GATEWAY = "gateway"  # task queue served by agentd (Go)
AGENT_STATE = SearchAttributeKey.for_keyword("AgentState")
WAIT_STATES = {  # gateway status -> AgentState while waiting
    "needs_approval": "WAITING_APPROVAL",
    "needs_reconcile": "WAITING_RECONCILE",
    "budget_exhausted": "PAUSED_BUDGET",
}
# The run's result keeps this much of its outcome: an error's text can be any length. (The report
# says what agentd's records say of the run, not this: the workflow tells it only why it reports.)
MAX_OUTCOME = 4000
# What github.comment_commit takes as an exception's type: an identifier.
ERROR_TYPE = re.compile(r"[A-Za-z_][A-Za-z0-9_.]{0,63}")


@dataclass
class AuditInput:
    repo: str  # logical repo id, e.g. "acme/payments"
    commit: str  # full 40-char SHA, resolved by agentd at start
    base_branch: str  # target branch of the draft PR


async def invoke(activity_id: str, kind: str, name: str, **args) -> tuple[str, dict]:
    # Every model and tool call is one gateway activity, recorded in the workflow history.
    r = await workflow.execute_activity(
        "Invoke",
        Call(kind=kind, name=name, args_json=json.dumps(args, sort_keys=True)),
        activity_id=activity_id,
        result_type=Result,
        task_queue=GATEWAY,
        start_to_close_timeout=timedelta(minutes=10),
        heartbeat_timeout=timedelta(seconds=30),
        retry_policy=RetryPolicy(maximum_attempts=5),  # agentd marks non-retryable errors
    )
    return r.status, json.loads(r.value_json or "{}")


@workflow.defn(name="security-audit")
class SecurityAudit:
    def __init__(self) -> None:
        self.rings = 0  # doorbell count: waiters compare it, nobody clears it
        self.calls = 0
        self.report_ref: str | None = None  # paid for: reported even if a later step fails

    @workflow.signal
    def doorbell(self) -> None:
        # Wake-up only: agentd's own tables hold the decision, the resolution or the budget.
        self.rings += 1

    def activity_id(self, name: str) -> str:
        # Every call has the same activity type, Invoke, and replay compares only activity
        # types and IDs. IDs that name the call let the replayer catch added, removed or
        # reordered calls; default IDs ("1", "2", ...) would hand recorded results to the
        # wrong calls without any error.
        self.calls += 1
        return f"{self.calls}:{name}"

    async def call(self, kind: str, name: str, **args) -> tuple[str, dict]:
        # Approval, reconciliation and budget all block the same way: wait for the doorbell,
        # then send the same call again. Each call notes the doorbell count before it sends, so
        # a doorbell that rings while the call is in flight is not lost, and concurrent calls
        # never clear each other's wake-ups.
        seen = self.rings
        status, value = await invoke(self.activity_id(name), kind, name, **args)
        while status in WAIT_STATES:
            workflow.upsert_search_attributes([AGENT_STATE.value_set(WAIT_STATES[status])])
            # Every wait has a bound: a lost doorbell delays a run but never strands it.
            timeout = timedelta(hours=24) if status == "needs_approval" else timedelta(hours=1)
            try:
                await workflow.wait_condition(lambda: self.rings > seen, timeout=timeout)
            except asyncio.TimeoutError:
                pass  # agentd reports "expired" on the next call
            workflow.upsert_search_attributes([AGENT_STATE.value_set("RUNNING")])
            seen = self.rings
            status, value = await invoke(self.activity_id(name), kind, name, **args)
        return status, value

    @workflow.run
    async def run(self, a: AuditInput) -> dict:
        raised: Exception | None = None
        try:
            outcome, report_ref, pr_url = await self.audit(a)
        except ActivityError as e:
            if isinstance(e.cause, CancelledError):
                raise  # a cancelled run posts no report
            outcome, report_ref, pr_url = f"failed: {e.cause}", self.report_ref, None
        except (asyncio.CancelledError, CancelledError):
            raise  # nor does one cancelled while its own code ran
        except Exception as e:  # noqa: BLE001 - the workflow's own code failed: report it, then fail the run
            # Left to the SDK, it would fail every workflow task, and the run would stay RUNNING with
            # no report: a result the code cannot read (a 5,000-digit integer) is enough.
            raised = e
            outcome = f"failed: the workflow raised {type(e).__name__}" + (f": {e}" if str(e) else "")
            report_ref, pr_url = self.report_ref, None
        outcome = outcome[:MAX_OUTCOME]
        # Every other end path posts a report (section 3). agentd writes its outcome from its own
        # records; the workflow says only why it reports.
        why = {"reason": "done"}
        if raised is not None:
            why = {"reason": "workflow_error"}
            if ERROR_TYPE.fullmatch(type(raised).__name__):
                why["error_type"] = type(raised).__name__
        # From the report on, whatever goes wrong ends the run FAILED, with why: left to the SDK, an
        # exception would fail every workflow task, and the run would stay RUNNING for good (a report's
        # result the code cannot read is enough).
        try:
            status, _ = await self.call(
                "tool", "github.comment_commit", repo=a.repo, commit=a.commit,
                report_ref=report_ref, pr_url=pr_url, **why,
            )
        except ActivityError as e:
            if isinstance(e.cause, CancelledError):
                raise  # cancelled while it reported
            raise ApplicationError(f"report not posted: {e.cause}"[:MAX_OUTCOME], non_retryable=True) from e
        except (asyncio.CancelledError, CancelledError):
            raise
        except Exception as e:  # noqa: BLE001 - the workflow's own code failed on the report's result
            raise ApplicationError(
                (f"the workflow raised {type(e).__name__} on its report's result" + (f": {e}" if str(e) else ""))[:MAX_OUTCOME],
                non_retryable=True) from e
        if status != "ok":  # a report that was not posted fails the run
            raise ApplicationError(f"report not posted: {status}", non_retryable=True)
        if raised is not None:  # reported; the run still failed
            raise ApplicationError(outcome.removeprefix("failed: "), type=type(raised).__name__,
                                   non_retryable=True) from raised
        return {"outcome": outcome, "pr_url": pr_url}

    async def audit(self, a: AuditInput) -> tuple[str, str | None, str | None]:
        status, diff = await self.call("tool", "git.show_diff", repo=a.repo, commit=a.commit)
        if status != "ok":
            return status, None, None
        status, report = await self.call("model", "audit_v1", diff_ref=diff["blob"])
        if status != "ok":
            return status, None, None  # e.g. "refused" or "denied"
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
                patch_ref = pr["patch_ref"]  # the reviewer's patch is validated again
                continue
            if status != "ok":
                return status, report["report_ref"], None  # "rejected" or "expired"
            return "ok", report["report_ref"], pr["url"]
        return "ok", report["report_ref"], None
