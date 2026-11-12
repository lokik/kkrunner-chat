"""Client-side composition contracts: neither backends nor implementations."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Optional, Protocol

if TYPE_CHECKING:
    from kkrunner.backends.contracts import JobRef, JobStatus
    from kkrunner.files.manifest import FileManifest
    from kkrunner.runs.base import ExecutionOutcome, ExecutionSpec
    from kkrunner.runs.model import ExecutionMetadata


@dataclass(frozen=True)
class Operation:
    """Reference a backend method or a standalone function for a transport.

    This is a client-side description, not an object reconstructed on the
    target. A transport validates source delivery only when it needs it.
    """

    method: str | None = None
    function: Callable | None = None
    backend: type | None = None

    def __post_init__(self):
        if (self.function is None) == (self.backend is None):
            raise ValueError("Choose a backend method or a standalone function")
        if self.function is not None:
            if self.method is not None:
                raise ValueError("A standalone operation cannot also name a backend method")
            if not callable(self.function):
                raise TypeError("A standalone operation must be callable")
        else:
            from kkrunner.backends.support.class_helper import static_methods
            if self.method not in static_methods(self.backend):
                raise ValueError(f"{self.backend.__name__}.{self.method} is not a static backend operation")

    @classmethod
    def for_backend(cls, backend: type, method: str):
        """Reference one declared static backend method without an instance."""
        return cls(method=method, backend=backend)

    @classmethod
    def standalone(cls, function: Callable):
        return cls(function=function)


@dataclass(frozen=True)
class JobAttachment:
    """Client-only snapshot for rebuilding a Job and its FileSession."""

    execution: ExecutionMetadata
    files: FileManifest


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

    execution: ExecutionMetadata


class BackendExecution(Protocol):
    """One backend execution bound to a location, without client persistence.

    Acceptance and completion return backend facts. An opaque reference is
    not a promise of reattachment. status() may return the live call's last
    facts when the backend cannot query execution independently.
    """

    ref: Optional[JobRef]
    snapshot: JobStatus

    @property
    def requires_stream_retrieval(self) -> bool: ...

    def configure_detached(self, detached: bool) -> None: ...
    def configure_observation(self, *, stream_offsets, stream_paths) -> None: ...

    async def submit(self, request, on_output) -> JobStatus: ...
    async def observe(self, on_output) -> JobStatus: ...
    async def status(self) -> JobStatus: ...
    async def cancel(self): ...
    async def stop_observation(self) -> None: ...
    async def close(self) -> None: ...


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
