"""Serializable execution identity and working directories."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Optional

from kkrunner.client.endpoint import ExecutionEndpointConfig


@dataclass(frozen=True)
class WorkingDirectory:
    """Local working directory and its optional target-side counterpart."""

    local: str
    remote: Optional[str] = None

    def __post_init__(self):
        import os

        object.__setattr__(self, "local", os.fspath(self.local))
        if self.remote is not None:
            object.__setattr__(self, "remote", os.fspath(self.remote))

    @property
    def target(self):
        return self.remote if self.remote is not None else self.local


@dataclass(frozen=True)
class Execution:
    """Backend identity plus the directory in which the job executes."""

    endpoint: ExecutionEndpointConfig
    directory: WorkingDirectory
    backend_id: Optional[str] = None
    submission_key: Optional[str] = None

    def __post_init__(self):
        if isinstance(self.endpoint, dict):
            object.__setattr__(self, "endpoint", ExecutionEndpointConfig(**self.endpoint))
        if isinstance(self.directory, dict):
            object.__setattr__(self, "directory", WorkingDirectory(**self.directory))
        if not isinstance(self.endpoint, ExecutionEndpointConfig):
            raise TypeError("Execution.endpoint must be an ExecutionEndpointConfig")
        if not isinstance(self.directory, WorkingDirectory):
            raise TypeError("Execution.directory must be a WorkingDirectory")
        if self.submission_key is not None:
            if not isinstance(self.submission_key, str) or not self.submission_key:
                raise ValueError("submission_key must be a nonempty string or None")
        if self.backend_id is not None:
            identifier = str(self.backend_id)
            if not identifier:
                raise ValueError("backend_id cannot be empty")
            object.__setattr__(self, "backend_id", identifier)

    def submitted(self, backend_id: str):
        """Return the execution bound to its immutable backend identity."""
        submitted = replace(self, backend_id=backend_id)
        if self.backend_id is not None and self.backend_id != submitted.backend_id:
            raise ValueError("Cannot replace an assigned backend identity")
        return submitted
