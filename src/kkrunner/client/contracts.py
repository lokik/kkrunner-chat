"""Client-side composition contracts: neither backends nor implementations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from kkrunner.backends.contracts import JobRef
    from kkrunner.client.run_parts import RunParts
    from kkrunner.files.manifest import FileManifest
    from kkrunner.runs.model import Execution


@dataclass(frozen=True)
class JobAttachment:
    """Client-only snapshot for rebuilding a Job and its FileSession."""

    execution: Execution
    files: FileManifest


@runtime_checkable
class RunEndpoint(Protocol):
    """Create an entire client session, never implement the backend contract."""

    def create_run_parts(self, spec, *, submission_key=None, callback=None) -> RunParts: ...


@runtime_checkable
class ReopenableRunEndpoint(RunEndpoint, Protocol):
    """Optional ability to reattach client-side resources around an existing Job."""

    def open_run_parts(self, attachment: JobAttachment, *, callback=None, output_listener=None) -> RunParts: ...
