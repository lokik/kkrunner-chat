"""Client-side composition contracts: neither backends nor implementations."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional, Protocol

if TYPE_CHECKING:
    from kkrunner.backends.config import BackendConfig
    from kkrunner.backends.contracts import Backend, JobRef, JobStatus, RecoveryResult
    from kkrunner.client.file_session import FileSession
    from kkrunner.client.run_parts import RunParts
    from kkrunner.client.transports import Transport
    from kkrunner.files.manifest import FileManifest
    from kkrunner.runs.base import ExecutionOutcome, ExecutionSpec, RunStatus
    from kkrunner.runs.model import Execution
    from kkrunner.runs.records import RunRecord


@dataclass(frozen=True)
class JobAttachment:
    """Client-only snapshot for rebuilding a Job and its FileSession."""

    execution: Execution
    files: FileManifest


class RunClientProtocol(Protocol):
    """Create an entire client session, never implement the backend contract."""

    backend: Backend
    transport: Transport
    supports_result_file_retrieval: bool

    @property
    def backend_config(self) -> BackendConfig: ...

    @property
    def default_persistent(self) -> bool: ...

    @property
    def default_detached(self) -> bool: ...

    def create_run_parts(self, spec, *, submission_key=None, callback=None) -> RunParts: ...

    def open_run_parts(self, attachment: JobAttachment, *, callback=None, output_listener=None) -> RunParts: ...

    def submission_directory(self, spec: ExecutionSpec) -> str: ...

    def resolve_submission(self, record: RunRecord, spec: ExecutionSpec) -> RunRecord: ...

    def file_session(self, spec: ExecutionSpec) -> FileSession: ...

    def status(self, *runs: JobAttachment) -> tuple[RunStatus, ...]: ...

    def recover_submission(self, submission_key, submission, *, submitted_at=None) -> RecoveryResult: ...

    def acknowledge(self, run): ...

    def acknowledge_submission(self, run): ...

    def snapshot_file(self, run, file, *, destination=None): ...

    def fetch_result_files(self, run, files, *, destinations=None) -> None: ...


class RunningCommand(Protocol):
    """A running command with independently readable output streams."""

    stdout: asyncio.StreamReader
    stderr: asyncio.StreamReader
    stdin: asyncio.StreamWriter | None
    pid: int
    returncode: int | None

    async def wait(self) -> int: ...

    def terminate(self) -> None: ...

    def kill(self) -> None: ...


@dataclass(frozen=True)
class SubmissionContext:
    """Original submitted execution context, before its JobRef was known."""

    execution: Execution


class JobProtocol(Protocol):
    """Live, per-execution handle.  Only reopenable jobs need a stable ref."""

    @property
    def ref(self) -> Optional[JobRef]: ...

    @property
    def backend_id(self) -> Optional[str]: ...

    @property
    def pid(self) -> Optional[int]: ...

    @property
    def state(self): ...

    # The Job has a runtime specification; the original submission spec
    # is a separate client-side value and may not survive reattachment.
    @property
    def spec(self) -> ExecutionSpec: ...

    @property
    def return_code(self) -> Optional[int]: ...

    @property
    def exception(self) -> Optional[BaseException]: ...

    @property
    def output_error(self) -> Optional[BaseException]: ...

    @property
    def failure_phase(self): ...

    @property
    def failure_message(self) -> Optional[str]: ...

    def add_output_listener(self, callback) -> None: ...

    def add_output_error_listener(self, callback) -> None: ...

    def status(self) -> JobStatus: ...

    def refresh_status(self) -> JobStatus: ...

    def submit(self) -> None: ...

    def wait(self) -> ExecutionOutcome: ...

    def cancel(self) -> None: ...
