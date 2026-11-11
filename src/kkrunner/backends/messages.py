"""JSON-compatible arguments and replies of static target operations.

These values cross both local and SSH invocation boundaries. They contain
paths and opaque handles, never client file manifests or live I/O objects.
"""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, TypedDict, Union

if TYPE_CHECKING:
    from kkrunner.runs.base import ExecutionSpec

from kkrunner.runs.states import RunState


class ExecutorOptions(TypedDict, total=False):
    backend: str
    url: Optional[str]


class MpiOptions(TypedDict, total=False):
    required: bool
    autodetect: bool
    launcher: List[str]
    process_count: Optional[int]
    warning: bool


class LaunchOptions(TypedDict, total=False):
    mpi: MpiOptions
    local_root: Optional[str]
    remote_root: Optional[str]
    input_sync: str
    output_sync: str


class _CachePath(TypedDict):
    path: str


class CacheFile(_CachePath, total=False):
    before: Optional[List[int]]


class CacheBaseline(TypedDict):
    path: str
    signature: Optional[List[int]]


class ClientSync(TypedDict, total=False):
    output_file: Optional[str]
    error_file: Optional[str]
    output_files: List[str]
    output_patterns: List[str]
    cache_files: List[CacheFile]
    cache_patterns: List[str]
    cache_before: List[CacheBaseline]


class _DirectoryRequest(TypedDict):
    directory: str


class RecoveryRequest(_DirectoryRequest, total=False):
    executor: str
    executor_options: ExecutorOptions


class _CommandRequest(RecoveryRequest):
    argv: List[str]


class ExecutionRequest(_CommandRequest, total=False):
    id: Optional[str]
    detached: bool
    mpi_argv: Optional[List[str]]
    mpi_uses_stdin: bool
    launch: LaunchOptions
    stdin: Optional[str]
    input_file: Optional[str]
    output_file: Optional[str]
    error_file: Optional[str]
    input_files: List[str]
    output_files: List[str]
    cache_files: List[str]
    client_sync: ClientSync


class StatusReply(TypedDict):
    state: str
    return_code: Optional[int]
    failure_phase: Optional[str]
    error: Optional[str]


class RecoveryReceipt(StatusReply):
    remote_id: str
    directory: str
    output_file: Optional[str]
    error_file: Optional[str]


class SubmissionReply(RecoveryReceipt):
    input_file: Optional[str]


class UnsupportedRecovery(TypedDict):
    unsupported: Literal[True]
    error: str


RecoveryReply = Optional[Union[RecoveryReceipt, UnsupportedRecovery]]


class AcknowledgeReply(TypedDict):
    acknowledged: bool


class FollowOptions(TypedDict, total=False):
    """Offsets are byte positions; None requests only future live bytes."""

    stdout_offset: Optional[int]
    stderr_offset: Optional[int]
    poll_interval: float
    stdout_path: Optional[str]
    stderr_path: Optional[str]


def request_to_spec(request: ExecutionRequest) -> ExecutionSpec:
    """Build a native execution declaration from target pathnames."""
    from kkrunner.runs.base import ExecutionSpec, LaunchSpec, MpiSpec

    sync = request.get("client_sync") or {}
    # LaunchSpec validates the decoded fields; its native mpi field remains
    # an MpiSpec rather than leaking the JSON representation to native code.
    launch: Dict[str, Any] = dict(request.get("launch") or {})
    launch["mpi"] = MpiSpec(**launch.get("mpi", {}))
    return ExecutionSpec(
        argv=tuple(request["argv"]),
        directory=request["directory"],
        input_file=request.get("input_file"),
        stdin=request.get("stdin"),
        output_file=request.get("output_file"),
        error_file=request.get("error_file"),
        input_files=tuple(request.get("input_files", ())),
        output_files=tuple(dict.fromkeys((
            *request.get("output_files", ()),
            *sync.get("output_files", ()), *sync.get("output_patterns", ()),
        ))),
        cache_files=tuple(dict.fromkeys((
            *request.get("cache_files", ()),
            *(item["path"] for item in sync.get("cache_files", ())),
            *sync.get("cache_patterns", ()),
        ))),
        mpi_argv=request.get("mpi_argv"),
        launch=LaunchSpec(**launch),
        mpi_uses_stdin=request.get("mpi_uses_stdin", True),
    )


def status_reply(
    state: RunState, return_code: Optional[int] = None, *,
    failure_phase: Optional[str] = None, error: Optional[str] = None,
) -> StatusReply:
    """Encode pure backend facts without adding client Run information."""
    return {
        "state": state.value,
        "return_code": return_code,
        "failure_phase": failure_phase.value if isinstance(failure_phase, Enum) else failure_phase,
        "error": error,
    }
