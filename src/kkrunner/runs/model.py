"""Serializable execution metadata and working directories."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Optional

from kkrunner.client.transports import Transport, require_persistable_transport, transport_from_dict
from kkrunner.backends.config import BackendConfig


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
class ExecutionMetadata:
    """Immutable execution configuration, directories and submission identifiers.

    A serializable component of a run, without live backend control or state.
    """

    transport: Transport
    directory: WorkingDirectory
    backend_id: Optional[str] = None
    submission_key: Optional[str] = None
    backend: BackendConfig = field(default_factory=BackendConfig)

    def __post_init__(self):
        if isinstance(self.transport, dict):
            object.__setattr__(self, "transport", transport_from_dict(self.transport))
        if isinstance(self.directory, dict):
            object.__setattr__(self, "directory", WorkingDirectory(**self.directory))
        require_persistable_transport(self.transport)
        if isinstance(self.backend, dict):
            object.__setattr__(self, "backend", BackendConfig(**self.backend))
        if not isinstance(self.backend, BackendConfig):
            raise TypeError("ExecutionMetadata.backend must be a BackendConfig")
        if not isinstance(self.directory, WorkingDirectory):
            raise TypeError("ExecutionMetadata.directory must be a WorkingDirectory")
        if self.submission_key is not None:
            if not isinstance(self.submission_key, str) or not self.submission_key:
                raise ValueError("submission_key must be a nonempty string or None")
        if self.backend_id is not None:
            identifier = str(self.backend_id)
            if not identifier:
                raise ValueError("backend_id cannot be empty")
            object.__setattr__(self, "backend_id", identifier)

    def submitted(self, backend_id: str):
        """Return metadata bound to its immutable backend identity."""
        submitted = replace(self, backend_id=backend_id)
        if self.backend_id is not None and self.backend_id != submitted.backend_id:
            raise ValueError("Cannot replace an assigned backend identity")
        return submitted
