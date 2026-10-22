from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class StartRunRequest(_message.Message):
    __slots__ = ("task", "params", "budget_limit_micros", "dry_run")
    class ParamsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    TASK_FIELD_NUMBER: _ClassVar[int]
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    BUDGET_LIMIT_MICROS_FIELD_NUMBER: _ClassVar[int]
    DRY_RUN_FIELD_NUMBER: _ClassVar[int]
    task: str
    params: _containers.ScalarMap[str, str]
    budget_limit_micros: int
    dry_run: bool
    def __init__(self, task: _Optional[str] = ..., params: _Optional[_Mapping[str, str]] = ..., budget_limit_micros: _Optional[int] = ..., dry_run: _Optional[bool] = ...) -> None: ...

class GetRunRequest(_message.Message):
    __slots__ = ("run_id",)
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    def __init__(self, run_id: _Optional[str] = ...) -> None: ...

class ListRunsRequest(_message.Message):
    __slots__ = ("limit",)
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    limit: int
    def __init__(self, limit: _Optional[int] = ...) -> None: ...

class ListRunsResponse(_message.Message):
    __slots__ = ("runs",)
    RUNS_FIELD_NUMBER: _ClassVar[int]
    runs: _containers.RepeatedCompositeFieldContainer[Run]
    def __init__(self, runs: _Optional[_Iterable[_Union[Run, _Mapping]]] = ...) -> None: ...

class CancelRunRequest(_message.Message):
    __slots__ = ("run_id",)
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    def __init__(self, run_id: _Optional[str] = ...) -> None: ...

class Run(_message.Message):
    __slots__ = ("run_id", "temporal_run_id", "task", "state", "params", "budget_limit_micros", "created_at", "result_json", "failure", "budget_spent_micros", "budget_reserved_micros", "dry_run")
    class ParamsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    TEMPORAL_RUN_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    BUDGET_LIMIT_MICROS_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    RESULT_JSON_FIELD_NUMBER: _ClassVar[int]
    FAILURE_FIELD_NUMBER: _ClassVar[int]
    BUDGET_SPENT_MICROS_FIELD_NUMBER: _ClassVar[int]
    BUDGET_RESERVED_MICROS_FIELD_NUMBER: _ClassVar[int]
    DRY_RUN_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    temporal_run_id: str
    task: str
    state: str
    params: _containers.ScalarMap[str, str]
    budget_limit_micros: int
    created_at: str
    result_json: str
    failure: str
    budget_spent_micros: int
    budget_reserved_micros: int
    dry_run: bool
    def __init__(self, run_id: _Optional[str] = ..., temporal_run_id: _Optional[str] = ..., task: _Optional[str] = ..., state: _Optional[str] = ..., params: _Optional[_Mapping[str, str]] = ..., budget_limit_micros: _Optional[int] = ..., created_at: _Optional[str] = ..., result_json: _Optional[str] = ..., failure: _Optional[str] = ..., budget_spent_micros: _Optional[int] = ..., budget_reserved_micros: _Optional[int] = ..., dry_run: _Optional[bool] = ...) -> None: ...

class ListPendingRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListPendingResponse(_message.Message):
    __slots__ = ("items",)
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[PendingItem]
    def __init__(self, items: _Optional[_Iterable[_Union[PendingItem, _Mapping]]] = ...) -> None: ...

class PendingItem(_message.Message):
    __slots__ = ("kind", "id", "run_id", "expires_at", "tool", "reason", "since", "payload_json", "payload_sha256")
    KIND_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    TOOL_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    SINCE_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_JSON_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_SHA256_FIELD_NUMBER: _ClassVar[int]
    kind: str
    id: str
    run_id: str
    expires_at: str
    tool: str
    reason: str
    since: str
    payload_json: str
    payload_sha256: str
    def __init__(self, kind: _Optional[str] = ..., id: _Optional[str] = ..., run_id: _Optional[str] = ..., expires_at: _Optional[str] = ..., tool: _Optional[str] = ..., reason: _Optional[str] = ..., since: _Optional[str] = ..., payload_json: _Optional[str] = ..., payload_sha256: _Optional[str] = ...) -> None: ...

class GetApprovalRequest(_message.Message):
    __slots__ = ("approval_id",)
    APPROVAL_ID_FIELD_NUMBER: _ClassVar[int]
    approval_id: str
    def __init__(self, approval_id: _Optional[str] = ...) -> None: ...

class Approval(_message.Message):
    __slots__ = ("approval_id", "run_id", "tool", "status", "created_at", "expires_at", "payload_sha256", "payload_json", "context_json", "decided_by", "decided_at", "reason", "supersedes", "superseded_by", "edited_patch", "budget_spent_micros", "budget_reserved_micros", "budget_limit_micros")
    APPROVAL_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    TOOL_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_SHA256_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_JSON_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_JSON_FIELD_NUMBER: _ClassVar[int]
    DECIDED_BY_FIELD_NUMBER: _ClassVar[int]
    DECIDED_AT_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    SUPERSEDES_FIELD_NUMBER: _ClassVar[int]
    SUPERSEDED_BY_FIELD_NUMBER: _ClassVar[int]
    EDITED_PATCH_FIELD_NUMBER: _ClassVar[int]
    BUDGET_SPENT_MICROS_FIELD_NUMBER: _ClassVar[int]
    BUDGET_RESERVED_MICROS_FIELD_NUMBER: _ClassVar[int]
    BUDGET_LIMIT_MICROS_FIELD_NUMBER: _ClassVar[int]
    approval_id: str
    run_id: str
    tool: str
    status: str
    created_at: str
    expires_at: str
    payload_sha256: str
    payload_json: str
    context_json: str
    decided_by: str
    decided_at: str
    reason: str
    supersedes: str
    superseded_by: str
    edited_patch: str
    budget_spent_micros: int
    budget_reserved_micros: int
    budget_limit_micros: int
    def __init__(self, approval_id: _Optional[str] = ..., run_id: _Optional[str] = ..., tool: _Optional[str] = ..., status: _Optional[str] = ..., created_at: _Optional[str] = ..., expires_at: _Optional[str] = ..., payload_sha256: _Optional[str] = ..., payload_json: _Optional[str] = ..., context_json: _Optional[str] = ..., decided_by: _Optional[str] = ..., decided_at: _Optional[str] = ..., reason: _Optional[str] = ..., supersedes: _Optional[str] = ..., superseded_by: _Optional[str] = ..., edited_patch: _Optional[str] = ..., budget_spent_micros: _Optional[int] = ..., budget_reserved_micros: _Optional[int] = ..., budget_limit_micros: _Optional[int] = ...) -> None: ...

class ResolveApprovalRequest(_message.Message):
    __slots__ = ("approval_id", "payload_sha256", "idempotency_key", "approve", "approve_with_edits", "reject", "decided_by")
    APPROVAL_ID_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_SHA256_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    APPROVE_FIELD_NUMBER: _ClassVar[int]
    APPROVE_WITH_EDITS_FIELD_NUMBER: _ClassVar[int]
    REJECT_FIELD_NUMBER: _ClassVar[int]
    DECIDED_BY_FIELD_NUMBER: _ClassVar[int]
    approval_id: str
    payload_sha256: str
    idempotency_key: str
    approve: Approve
    approve_with_edits: ApproveWithEdits
    reject: Reject
    decided_by: str
    def __init__(self, approval_id: _Optional[str] = ..., payload_sha256: _Optional[str] = ..., idempotency_key: _Optional[str] = ..., approve: _Optional[_Union[Approve, _Mapping]] = ..., approve_with_edits: _Optional[_Union[ApproveWithEdits, _Mapping]] = ..., reject: _Optional[_Union[Reject, _Mapping]] = ..., decided_by: _Optional[str] = ...) -> None: ...

class Approve(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ApproveWithEdits(_message.Message):
    __slots__ = ("edited_patch",)
    EDITED_PATCH_FIELD_NUMBER: _ClassVar[int]
    edited_patch: str
    def __init__(self, edited_patch: _Optional[str] = ...) -> None: ...

class Reject(_message.Message):
    __slots__ = ("reason",)
    REASON_FIELD_NUMBER: _ClassVar[int]
    reason: str
    def __init__(self, reason: _Optional[str] = ...) -> None: ...

class ResolveApprovalResponse(_message.Message):
    __slots__ = ("status", "decided_by", "decided_at")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    DECIDED_BY_FIELD_NUMBER: _ClassVar[int]
    DECIDED_AT_FIELD_NUMBER: _ClassVar[int]
    status: str
    decided_by: str
    decided_at: str
    def __init__(self, status: _Optional[str] = ..., decided_by: _Optional[str] = ..., decided_at: _Optional[str] = ...) -> None: ...

class ResolveReconciliationRequest(_message.Message):
    __slots__ = ("effect_id", "executed", "result_json", "idempotency_key", "lookup")
    EFFECT_ID_FIELD_NUMBER: _ClassVar[int]
    EXECUTED_FIELD_NUMBER: _ClassVar[int]
    RESULT_JSON_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    LOOKUP_FIELD_NUMBER: _ClassVar[int]
    effect_id: str
    executed: bool
    result_json: str
    idempotency_key: str
    lookup: bool
    def __init__(self, effect_id: _Optional[str] = ..., executed: _Optional[bool] = ..., result_json: _Optional[str] = ..., idempotency_key: _Optional[str] = ..., lookup: _Optional[bool] = ...) -> None: ...

class ResolveReconciliationResponse(_message.Message):
    __slots__ = ("status",)
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: str
    def __init__(self, status: _Optional[str] = ...) -> None: ...

class ExtendBudgetRequest(_message.Message):
    __slots__ = ("run_id", "amount_micros", "idempotency_key")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_MICROS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    amount_micros: int
    idempotency_key: str
    def __init__(self, run_id: _Optional[str] = ..., amount_micros: _Optional[int] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class ExplainRunRequest(_message.Message):
    __slots__ = ("run_id",)
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    def __init__(self, run_id: _Optional[str] = ...) -> None: ...

class Explanation(_message.Message):
    __slots__ = ("run_id", "step", "cause", "evidence")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    STEP_FIELD_NUMBER: _ClassVar[int]
    CAUSE_FIELD_NUMBER: _ClassVar[int]
    EVIDENCE_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    step: str
    cause: str
    evidence: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, run_id: _Optional[str] = ..., step: _Optional[str] = ..., cause: _Optional[str] = ..., evidence: _Optional[_Iterable[str]] = ...) -> None: ...

class VerifyAuditRequest(_message.Message):
    __slots__ = ("run_id", "allow_chains", "allow_replaced_ledgers")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    ALLOW_CHAINS_FIELD_NUMBER: _ClassVar[int]
    ALLOW_REPLACED_LEDGERS_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    allow_chains: _containers.RepeatedScalarFieldContainer[str]
    allow_replaced_ledgers: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, run_id: _Optional[str] = ..., allow_chains: _Optional[_Iterable[str]] = ..., allow_replaced_ledgers: _Optional[_Iterable[str]] = ...) -> None: ...

class AuditReport(_message.Message):
    __slots__ = ("records", "head_seq", "head_sha256", "broken_at_seq", "broken_reason", "anchors_checked", "anchors_mismatched", "runs_unavailable", "runs_checked", "runs_mismatched", "state_mismatched", "external_anchors_checked", "last_external_anchor_seq", "records_after_external_anchor", "retention_seconds", "other_anchor_workflows", "runs_before_log", "chain", "other_chains", "attempts_cut_short", "unchecked_tables", "unread", "attempts_unrecorded", "terminated_outside_agentd")
    RECORDS_FIELD_NUMBER: _ClassVar[int]
    HEAD_SEQ_FIELD_NUMBER: _ClassVar[int]
    HEAD_SHA256_FIELD_NUMBER: _ClassVar[int]
    BROKEN_AT_SEQ_FIELD_NUMBER: _ClassVar[int]
    BROKEN_REASON_FIELD_NUMBER: _ClassVar[int]
    ANCHORS_CHECKED_FIELD_NUMBER: _ClassVar[int]
    ANCHORS_MISMATCHED_FIELD_NUMBER: _ClassVar[int]
    RUNS_UNAVAILABLE_FIELD_NUMBER: _ClassVar[int]
    RUNS_CHECKED_FIELD_NUMBER: _ClassVar[int]
    RUNS_MISMATCHED_FIELD_NUMBER: _ClassVar[int]
    STATE_MISMATCHED_FIELD_NUMBER: _ClassVar[int]
    EXTERNAL_ANCHORS_CHECKED_FIELD_NUMBER: _ClassVar[int]
    LAST_EXTERNAL_ANCHOR_SEQ_FIELD_NUMBER: _ClassVar[int]
    RECORDS_AFTER_EXTERNAL_ANCHOR_FIELD_NUMBER: _ClassVar[int]
    RETENTION_SECONDS_FIELD_NUMBER: _ClassVar[int]
    OTHER_ANCHOR_WORKFLOWS_FIELD_NUMBER: _ClassVar[int]
    RUNS_BEFORE_LOG_FIELD_NUMBER: _ClassVar[int]
    CHAIN_FIELD_NUMBER: _ClassVar[int]
    OTHER_CHAINS_FIELD_NUMBER: _ClassVar[int]
    ATTEMPTS_CUT_SHORT_FIELD_NUMBER: _ClassVar[int]
    UNCHECKED_TABLES_FIELD_NUMBER: _ClassVar[int]
    UNREAD_FIELD_NUMBER: _ClassVar[int]
    ATTEMPTS_UNRECORDED_FIELD_NUMBER: _ClassVar[int]
    TERMINATED_OUTSIDE_AGENTD_FIELD_NUMBER: _ClassVar[int]
    records: int
    head_seq: int
    head_sha256: str
    broken_at_seq: int
    broken_reason: str
    anchors_checked: int
    anchors_mismatched: _containers.RepeatedCompositeFieldContainer[AnchorMismatch]
    runs_unavailable: _containers.RepeatedCompositeFieldContainer[UnavailableRun]
    runs_checked: int
    runs_mismatched: _containers.RepeatedCompositeFieldContainer[RunMismatch]
    state_mismatched: _containers.RepeatedCompositeFieldContainer[StateMismatch]
    external_anchors_checked: int
    last_external_anchor_seq: int
    records_after_external_anchor: int
    retention_seconds: int
    other_anchor_workflows: int
    runs_before_log: _containers.RepeatedCompositeFieldContainer[UnavailableRun]
    chain: str
    other_chains: _containers.RepeatedScalarFieldContainer[str]
    attempts_cut_short: _containers.RepeatedCompositeFieldContainer[CutShortAttempt]
    unchecked_tables: _containers.RepeatedCompositeFieldContainer[UncheckedTable]
    unread: _containers.RepeatedCompositeFieldContainer[Unread]
    attempts_unrecorded: _containers.RepeatedCompositeFieldContainer[UnrecordedAttempt]
    terminated_outside_agentd: _containers.RepeatedCompositeFieldContainer[TerminatedExecution]
    def __init__(self, records: _Optional[int] = ..., head_seq: _Optional[int] = ..., head_sha256: _Optional[str] = ..., broken_at_seq: _Optional[int] = ..., broken_reason: _Optional[str] = ..., anchors_checked: _Optional[int] = ..., anchors_mismatched: _Optional[_Iterable[_Union[AnchorMismatch, _Mapping]]] = ..., runs_unavailable: _Optional[_Iterable[_Union[UnavailableRun, _Mapping]]] = ..., runs_checked: _Optional[int] = ..., runs_mismatched: _Optional[_Iterable[_Union[RunMismatch, _Mapping]]] = ..., state_mismatched: _Optional[_Iterable[_Union[StateMismatch, _Mapping]]] = ..., external_anchors_checked: _Optional[int] = ..., last_external_anchor_seq: _Optional[int] = ..., records_after_external_anchor: _Optional[int] = ..., retention_seconds: _Optional[int] = ..., other_anchor_workflows: _Optional[int] = ..., runs_before_log: _Optional[_Iterable[_Union[UnavailableRun, _Mapping]]] = ..., chain: _Optional[str] = ..., other_chains: _Optional[_Iterable[str]] = ..., attempts_cut_short: _Optional[_Iterable[_Union[CutShortAttempt, _Mapping]]] = ..., unchecked_tables: _Optional[_Iterable[_Union[UncheckedTable, _Mapping]]] = ..., unread: _Optional[_Iterable[_Union[Unread, _Mapping]]] = ..., attempts_unrecorded: _Optional[_Iterable[_Union[UnrecordedAttempt, _Mapping]]] = ..., terminated_outside_agentd: _Optional[_Iterable[_Union[TerminatedExecution, _Mapping]]] = ...) -> None: ...

class TerminatedExecution(_message.Message):
    __slots__ = ("run_id", "temporal_run_id", "at", "claimed_identity", "claimed_reason")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    TEMPORAL_RUN_ID_FIELD_NUMBER: _ClassVar[int]
    AT_FIELD_NUMBER: _ClassVar[int]
    CLAIMED_IDENTITY_FIELD_NUMBER: _ClassVar[int]
    CLAIMED_REASON_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    temporal_run_id: str
    at: str
    claimed_identity: str
    claimed_reason: str
    def __init__(self, run_id: _Optional[str] = ..., temporal_run_id: _Optional[str] = ..., at: _Optional[str] = ..., claimed_identity: _Optional[str] = ..., claimed_reason: _Optional[str] = ...) -> None: ...

class UnrecordedAttempt(_message.Message):
    __slots__ = ("run_id", "activity_id", "attempt", "seq", "why")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    ACTIVITY_ID_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    SEQ_FIELD_NUMBER: _ClassVar[int]
    WHY_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    activity_id: str
    attempt: int
    seq: int
    why: str
    def __init__(self, run_id: _Optional[str] = ..., activity_id: _Optional[str] = ..., attempt: _Optional[int] = ..., seq: _Optional[int] = ..., why: _Optional[str] = ...) -> None: ...

class Unread(_message.Message):
    __slots__ = ("what", "count", "why")
    WHAT_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    WHY_FIELD_NUMBER: _ClassVar[int]
    what: str
    count: int
    why: str
    def __init__(self, what: _Optional[str] = ..., count: _Optional[int] = ..., why: _Optional[str] = ...) -> None: ...

class UncheckedTable(_message.Message):
    __slots__ = ("name", "why")
    NAME_FIELD_NUMBER: _ClassVar[int]
    WHY_FIELD_NUMBER: _ClassVar[int]
    name: str
    why: str
    def __init__(self, name: _Optional[str] = ..., why: _Optional[str] = ...) -> None: ...

class CutShortAttempt(_message.Message):
    __slots__ = ("run_id", "activity_id", "attempt", "seq", "restarted_seq")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    ACTIVITY_ID_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    SEQ_FIELD_NUMBER: _ClassVar[int]
    RESTARTED_SEQ_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    activity_id: str
    attempt: int
    seq: int
    restarted_seq: int
    def __init__(self, run_id: _Optional[str] = ..., activity_id: _Optional[str] = ..., attempt: _Optional[int] = ..., seq: _Optional[int] = ..., restarted_seq: _Optional[int] = ...) -> None: ...

class StateMismatch(_message.Message):
    __slots__ = ("kind", "id", "why")
    KIND_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    WHY_FIELD_NUMBER: _ClassVar[int]
    kind: str
    id: str
    why: str
    def __init__(self, kind: _Optional[str] = ..., id: _Optional[str] = ..., why: _Optional[str] = ...) -> None: ...

class AnchorMismatch(_message.Message):
    __slots__ = ("run_id", "activity_id", "seq", "why")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    ACTIVITY_ID_FIELD_NUMBER: _ClassVar[int]
    SEQ_FIELD_NUMBER: _ClassVar[int]
    WHY_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    activity_id: str
    seq: int
    why: str
    def __init__(self, run_id: _Optional[str] = ..., activity_id: _Optional[str] = ..., seq: _Optional[int] = ..., why: _Optional[str] = ...) -> None: ...

class RunMismatch(_message.Message):
    __slots__ = ("run_id", "why")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    WHY_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    why: str
    def __init__(self, run_id: _Optional[str] = ..., why: _Optional[str] = ...) -> None: ...

class UnavailableRun(_message.Message):
    __slots__ = ("run_id", "why")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    WHY_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    why: str
    def __init__(self, run_id: _Optional[str] = ..., why: _Optional[str] = ...) -> None: ...
