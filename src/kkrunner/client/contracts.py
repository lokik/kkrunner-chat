"""Client-side composition contracts: neither backends nor implementations."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional, Protocol

if TYPE_CHECKING:
    from kkrunner.backends.contracts import JobRef, JobStatus
    from kkrunner.runs.base import ExecutionOutcome, ExecutionSpec
    from kkrunner.client.run_parts import RunParts
    from kkrunner.files.manifest import FileManifest
    from kkrunner.runs.model import Execution


@dataclass(frozen=True)
class JobAttachment:
    """Client-only snapshot for rebuilding a Job and its FileSession."""

    execution: Execution
    files: FileManifest


class RunEndpoint(Protocol):
    """Create an entire client session, never implement the backend contract."""

    supports_reattachment: bool

    def create_run_parts(self, spec, *, submission_key=None, callback=None) -> RunParts: ...


class ReopenableRunEndpoint(RunEndpoint, Protocol):
    """Optional ability to reattach client-side resources around an existing Job."""

    def open_run_parts(self, attachment: JobAttachment, *, callback=None, output_listener=None) -> RunParts: ...


class RunningCommand(Protocol):
    """A running command with independently readable output streams."""

    stdout: asyncio.StreamReader
    stderr: asyncio.StreamReader
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
    def failure_phase(self): ...

    @property
    def failure_message(self) -> Optional[str]: ...

    def add_output_listener(self, callback) -> None: ...

    def add_output_error_listener(self, callback) -> None: ...

    def status(self) -> JobStatus: ...

    def submit(self) -> None: ...

    def wait(self) -> ExecutionOutcome: ...

    def cancel(self) -> None: ...
