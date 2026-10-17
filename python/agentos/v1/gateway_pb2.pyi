from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class Call(_message.Message):
    __slots__ = ("kind", "name", "args_json")
    KIND_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    ARGS_JSON_FIELD_NUMBER: _ClassVar[int]
    kind: str
    name: str
    args_json: str
    def __init__(self, kind: _Optional[str] = ..., name: _Optional[str] = ..., args_json: _Optional[str] = ...) -> None: ...

class Result(_message.Message):
    __slots__ = ("status", "value_json")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    VALUE_JSON_FIELD_NUMBER: _ClassVar[int]
    status: str
    value_json: str
    def __init__(self, status: _Optional[str] = ..., value_json: _Optional[str] = ...) -> None: ...
