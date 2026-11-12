"""Client-only file mapping and synchronization policy."""

from __future__ import annotations

import os
from dataclasses import dataclass

from kkrunner.files.manifest import FileSyncPolicy


@dataclass(frozen=True)
class StagingSpec:
    """Map client files to a target and select their synchronization policy."""

    local_root: str | None = None
    remote_root: str | None = None
    input_sync: str = FileSyncPolicy.CHANGED.value
    output_sync: str = FileSyncPolicy.CHANGED.value

    def __post_init__(self):
        for name in ("local_root", "remote_root"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, os.fspath(value))
        for name in ("input_sync", "output_sync"):
            try:
                value = FileSyncPolicy(getattr(self, name))
            except ValueError as exc:
                raise ValueError(f"Invalid {name} file synchronization policy") from exc
            object.__setattr__(self, name, value.value)
