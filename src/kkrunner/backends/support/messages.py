"""JSON-compatible arguments and replies of static target operations.

These values cross both local and SSH invocation boundaries. They contain
paths and opaque handles, never client file manifests or live I/O objects.
"""

from __future__ import annotations

import os
from dataclasses import asdict
from enum import Enum
from typing import List, Literal, Optional, TypedDict, Union

from kkrunner.execution import MpiSpec, RunState


class BackendOptions(TypedDict, total=False):
    executor_name: str
    url: Optional[str]


class MpiOptions(TypedDict, total=False):
    required: bool
    autodetect: bool
    launcher: List[str]
    process_count: Optional[int]
    warning: bool


class _DirectoryRequest(TypedDict):
    directory: str


class RecoveryRequest(_DirectoryRequest, total=False):
    backend: str
    backend_options: BackendOptions


class _CommandRequest(RecoveryRequest):
    argv: List[str]


class ExecutionRequest(_CommandRequest, total=False):
    submission_key: Optional[str]
    detached: bool
    mpi_argv: Optional[List[str]]
    mpi_uses_stdin: bool
    mpi: MpiOptions
    stdin: Optional[str]
    input_file: Optional[str]
    output_file: Optional[str]
    error_file: Optional[str]
    input_files: List[str]
    output_files: List[str]
    cache_files: List[str]


class PreparedExecution(TypedDict):
    request: ExecutionRequest
    launcher: Optional[List[str]]
    warning: Optional[str]
    use_stdin: bool


class StatusReply(TypedDict):
    state: str
    return_code: Optional[int]
    failure_phase: Optional[str]
    error: Optional[str]


class ExecutionReply(StatusReply):
    remote_id: Optional[str]


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


def normalize_request(request: ExecutionRequest) -> ExecutionRequest:
    """Validate target-only values and resolve paths relative to its directory.

    This is a JSON request throughout, never a client ExecutionSpec. Validation
    and normalization do not create directories or modify any target files.
    """
    if "launch" in request or "staging" in request:
        raise ValueError("Target requests accept MPI options, not client staging configuration")
    result = dict(request)
    directory = request["directory"]
    if not isinstance(directory, str) or not directory:
        raise ValueError("Execution directory must be a nonempty string")
    directory = os.path.abspath(os.path.expanduser(directory))
    result["directory"] = directory

    def command(value):
        if not isinstance(value, (list, tuple)) or any(not isinstance(arg, str) for arg in value):
            raise TypeError("Execution commands must be lists of strings")
        if not value or not value[0]:
            raise ValueError("Execution commands must not be empty")
        return list(value)

    result["argv"] = command(request["argv"])
    result["mpi_argv"] = None if request.get("mpi_argv") is None else command(request["mpi_argv"])
    result["mpi"] = asdict(MpiSpec(**request.get("mpi", {})))
    for name, default in (("mpi_uses_stdin", True), ("detached", True)):
        value = request.get(name, default)
        if type(value) is not bool:
            raise TypeError(f"{name} must be bool")
        result[name] = value

    def path(value):
        if value is None:
            return None
        if not isinstance(value, str) or not value:
            raise ValueError("Target file paths must be nonempty strings")
        return os.path.abspath(os.path.join(directory, value))

    for name in ("input_file", "stdin", "output_file", "error_file"):
        result[name] = path(request.get(name))
    for name in ("input_files", "output_files", "cache_files"):
        values = request.get(name, ())
        if not isinstance(values, (list, tuple)):
            raise TypeError(f"{name} must be a list of target paths")
        if any(value is None for value in values):
            raise ValueError(f"{name} cannot contain null paths")
        result[name] = list(dict.fromkeys(path(value) for value in values))
    return result


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
