"""Shared execution values with no client, transport or persistence dependencies."""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple


class MpiNotFoundWarning(UserWarning):
    """MPI autodetection allowed execution to fall back to the serial command."""


class RunState(str, Enum):
    """Native execution state, independent of client file processing."""

    NEW = "new"
    SUBMITTING = "submitting"
    QUEUED = "queued"
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"
    CANCELLED = "cancelled"
    UNKNOWN = "unknown"

    @property
    def is_terminal(self):
        """Whether this run is finished, failed or cancelled (not UNKNOWN)."""
        return self in (RunState.FINISHED, RunState.FAILED, RunState.CANCELLED)


def merge_terminal_outcome(current, code, observed, observed_code):
    """Refine a confirmed exit without erasing stronger native evidence.

    A liveness-only FINISHED may acquire the real outcome later. Confirmed
    cancellation is retained when the actual exit code arrives.
    """
    if not observed.is_terminal or code is not None:
        return current, code
    if current == RunState.CANCELLED:
        return current, observed_code
    if observed == RunState.CANCELLED or (
        current == RunState.FINISHED and observed_code is not None
    ):
        return observed, observed_code
    return current, observed_code if current == observed else code


class RunFileState(str, Enum):
    """Client file ownership and processing, independent of execution."""

    NEW = "new"
    PREPROCESSING = "preprocessing"
    WAITING = "waiting"
    COLLECTING = "collecting"
    READY = "ready"
    POSTPROCESSING = "postprocessing"
    POSTPROCESSED = "postprocessed"
    DONE = "done"
    FAILED = "failed"

    @property
    def is_terminal(self):
        return self in (self.POSTPROCESSED, self.DONE, self.FAILED)


class FailurePhase(str, Enum):
    """Diagnostic phase of native execution or client file failure."""

    PREPROCESSING = "preprocessing"
    SUBMISSION = "submission"
    LAUNCH = "launch"
    EXECUTION = "execution"
    ARTIFACTS = "artifacts"
    OUTPUT = "output"
    DOWNLOAD = "download"
    POSTPROCESSING = "postprocessing"


@dataclass(frozen=True)
class MpiSpec:
    """Portable MPI launch semantics independent of the execution host."""

    required: bool = False
    autodetect: bool = False
    launcher: Tuple[str, ...] = ()
    process_count: Optional[int] = None
    warning: bool = True

    def __post_init__(self):
        for name in ("required", "autodetect", "warning"):
            if type(getattr(self, name)) is not bool:
                raise TypeError(f"MPI {name} must be bool")
        launcher = (self.launcher,) if isinstance(self.launcher, (str, os.PathLike)) else self.launcher
        launcher = tuple(os.fspath(part) for part in launcher)
        if any(not isinstance(part, str) or not part for part in launcher):
            raise ValueError("MPI launcher must contain nonempty strings")
        object.__setattr__(self, "launcher", launcher)
        if self.process_count is not None and type(self.process_count) is not int:
            raise TypeError("process_count must be an integer")
        if self.process_count is not None and self.process_count < 1:
            raise ValueError("process_count must be positive")
        if self.launcher and self.autodetect:
            raise ValueError("explicit MPI launcher cannot be combined with autodetection")
        if self.launcher and not self.required:
            raise ValueError("explicit MPI launcher must be required")
        if self.required and not self.autodetect and not self.launcher:
            raise ValueError("required MPI needs autodetection or an explicit launcher")
        if not self.requested and self.process_count is not None:
            raise ValueError("process_count requires MPI")

    @classmethod
    def from_value(cls, mpi, *, process_count=None, warning=True):
        if isinstance(mpi, cls):
            if process_count is not None or warning is not True:
                raise ValueError("MPI options cannot override an existing MpiSpec")
            return mpi
        if mpi is None or mpi is False:
            if process_count is not None:
                raise ValueError("process_count requires MPI")
            return cls(warning=warning)
        if isinstance(mpi, bool):
            return cls(
                required=True,
                autodetect=True,
                process_count=process_count,
                warning=warning,
            )
        if isinstance(mpi, int):
            if process_count is not None:
                raise ValueError("MPI process count was specified twice")
            return cls(
                required=True,
                autodetect=True,
                process_count=mpi,
                warning=warning,
            )
        if mpi == "auto":
            return cls(
                autodetect=True,
                process_count=process_count,
                warning=warning,
            )
        launcher = (mpi,) if isinstance(mpi, str) else tuple(os.fspath(part) for part in mpi)
        return cls(
            required=True,
            launcher=launcher,
            process_count=process_count,
            warning=warning,
        )

    @property
    def requested(self) -> bool:
        return self.required or self.autodetect or bool(self.launcher)

    @property
    def allows_serial_fallback(self) -> bool:
        return self.autodetect and not self.required
