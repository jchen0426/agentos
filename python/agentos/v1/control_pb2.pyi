from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class StartRunRequest(_message.Message):
    __slots__ = ("task", "params", "budget_limit_micros")
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
    task: str
    params: _containers.ScalarMap[str, str]
    budget_limit_micros: int
    def __init__(self, task: _Optional[str] = ..., params: _Optional[_Mapping[str, str]] = ..., budget_limit_micros: _Optional[int] = ...) -> None: ...

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
    __slots__ = ("run_id", "temporal_run_id", "task", "state", "params", "budget_limit_micros", "created_at", "result_json", "failure")
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
    run_id: str
    temporal_run_id: str
    task: str
    state: str
    params: _containers.ScalarMap[str, str]
    budget_limit_micros: int
    created_at: str
    result_json: str
    failure: str
    def __init__(self, run_id: _Optional[str] = ..., temporal_run_id: _Optional[str] = ..., task: _Optional[str] = ..., state: _Optional[str] = ..., params: _Optional[_Mapping[str, str]] = ..., budget_limit_micros: _Optional[int] = ..., created_at: _Optional[str] = ..., result_json: _Optional[str] = ..., failure: _Optional[str] = ...) -> None: ...

class ListPendingRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListPendingResponse(_message.Message):
    __slots__ = ("items",)
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[PendingItem]
    def __init__(self, items: _Optional[_Iterable[_Union[PendingItem, _Mapping]]] = ...) -> None: ...

class PendingItem(_message.Message):
    __slots__ = ("kind", "id", "run_id", "expires_at")
    KIND_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    kind: str
    id: str
    run_id: str
    expires_at: str
    def __init__(self, kind: _Optional[str] = ..., id: _Optional[str] = ..., run_id: _Optional[str] = ..., expires_at: _Optional[str] = ...) -> None: ...

class ResolveApprovalRequest(_message.Message):
    __slots__ = ("approval_id", "payload_sha256", "idempotency_key", "approve", "approve_with_edits", "reject")
    APPROVAL_ID_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_SHA256_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    APPROVE_FIELD_NUMBER: _ClassVar[int]
    APPROVE_WITH_EDITS_FIELD_NUMBER: _ClassVar[int]
    REJECT_FIELD_NUMBER: _ClassVar[int]
    approval_id: str
    payload_sha256: str
    idempotency_key: str
    approve: Approve
    approve_with_edits: ApproveWithEdits
    reject: Reject
    def __init__(self, approval_id: _Optional[str] = ..., payload_sha256: _Optional[str] = ..., idempotency_key: _Optional[str] = ..., approve: _Optional[_Union[Approve, _Mapping]] = ..., approve_with_edits: _Optional[_Union[ApproveWithEdits, _Mapping]] = ..., reject: _Optional[_Union[Reject, _Mapping]] = ...) -> None: ...

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
    __slots__ = ("effect_id", "executed", "result_json", "idempotency_key")
    EFFECT_ID_FIELD_NUMBER: _ClassVar[int]
    EXECUTED_FIELD_NUMBER: _ClassVar[int]
    RESULT_JSON_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    effect_id: str
    executed: bool
    result_json: str
    idempotency_key: str
    def __init__(self, effect_id: _Optional[str] = ..., executed: _Optional[bool] = ..., result_json: _Optional[str] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

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
