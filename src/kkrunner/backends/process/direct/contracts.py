"""Private protocol for process mechanics shared by local and direct SSH."""

from __future__ import annotations

from typing import Protocol

from kkrunner.runs.base import ExecutionSpec


class DirectProcessAdapter(Protocol):
    """Process mechanism, not a second Job lifecycle or state owner."""

    expose_pid: bool
    uncertain_after_launch: bool
    detached: bool

    def prepare(self, spec: ExecutionSpec) -> None: ...

    async def launch(self, spec: ExecutionSpec): ...

    async def stop(self, process) -> bool: ...

    def output(self, data: bytes, kind: str): ...

    def detached_output_paths(self, spec: ExecutionSpec) -> dict: ...

    def exit_status(self, code: int): ...

    def close(self, process) -> None: ...

