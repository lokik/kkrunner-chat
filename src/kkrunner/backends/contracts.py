"""Backend execution contracts, independent of clients, agents, and persistence.

A Job is a live Python handle.  A JobRef, when available, identifies an
execution independently of that handle.  Submission recovery is a *backend*
operation, because the caller may have lost the submit reply and job identity.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Optional, Protocol, runtime_checkable

if TYPE_CHECKING:
    from kkrunner.runs.base import ExecutionSpec


class JobState(str, Enum):
    NEW = "new"
    QUEUED = "queued"
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"
    CANCELLED = "cancelled"
    UNKNOWN = "unknown"

    @property
    def is_terminal(self) -> bool:
        return self in (self.FINISHED, self.FAILED, self.CANCELLED)


class JobFailurePhase(str, Enum):
    SUBMISSION = "submission"
    LAUNCH = "launch"
    EXECUTION = "execution"
    OUTPUT = "output"


@dataclass(frozen=True)
class JobRef:
    """Backend-scoped, serializable identity; never a Run ID or file manifest."""

    backend: str
    token: str

    def __post_init__(self):
        for name in ("backend", "token"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise ValueError(f"JobRef.{name} must be a nonempty string")


@dataclass(frozen=True)
class JobStatus:
    """Execution facts only: no download, postprocessing or observation state."""

    state: JobState
    return_code: Optional[int] = None
    failure_phase: Optional[JobFailurePhase] = None
    message: Optional[str] = None

    def __post_init__(self):
        if not isinstance(self.state, JobState):
            object.__setattr__(self, "state", JobState(self.state))
        if self.failure_phase is not None and not isinstance(self.failure_phase, JobFailurePhase):
            object.__setattr__(self, "failure_phase", JobFailurePhase(self.failure_phase))
        if self.return_code is not None and type(self.return_code) is not int:
            raise TypeError("JobStatus.return_code must be an integer or None")


class RecoveryDisposition(str, Enum):
    FOUND = "found"
    ABSENT = "absent"
    INDETERMINATE = "indeterminate"
    UNSUPPORTED = "unsupported"


@dataclass(frozen=True)
class RecoveryResult:
    """A proof about submission, not a guess from missing receipts.

    ABSENT is reserved for cases in which the backend can prove non-submission.
    INDETERMINATE/UNSUPPORTED must not authorize a second submission.
    """

    disposition: RecoveryDisposition
    ref: Optional[JobRef] = None
    message: Optional[str] = None

    def __post_init__(self):
        if not isinstance(self.disposition, RecoveryDisposition):
            object.__setattr__(self, "disposition", RecoveryDisposition(self.disposition))
        if (self.disposition == RecoveryDisposition.FOUND) != (self.ref is not None):
            raise ValueError("Exactly FOUND requires a JobRef")

    @classmethod
    def found(cls, ref: JobRef) -> RecoveryResult:
        return cls(RecoveryDisposition.FOUND, ref)

    @classmethod
    def absent(cls) -> RecoveryResult:
        return cls(RecoveryDisposition.ABSENT)

    @classmethod
    def indeterminate(cls, message: Optional[str] = None) -> RecoveryResult:
        return cls(RecoveryDisposition.INDETERMINATE, message=message)

    @classmethod
    def unsupported(cls, message: Optional[str] = None) -> RecoveryResult:
        return cls(RecoveryDisposition.UNSUPPORTED, message=message)


class UnsupportedCapability(NotImplementedError):
    """The selected backend/job cannot provide the requested guarantee."""


class ObservationUnavailable(OSError):
    """Observation was lost; this is not a terminal backend failure."""


@runtime_checkable
class Job(Protocol):
    """Live, per-execution handle.  Only reopenable jobs need a stable ref."""

    @property
    def ref(self) -> Optional[JobRef]: ...

    async def status(self) -> JobStatus: ...

    async def wait(self) -> JobStatus: ...

    async def cancel(self) -> None: ...


class Backend(Protocol):
    """Execution technology, independent of how the client reaches it."""

    async def submit(
        self,
        spec: ExecutionSpec,
        *,
        submission_key: Optional[str] = None,
    ) -> Job: ...


class ReopenableBackend(Backend, Protocol):
    async def open(self, ref: JobRef) -> Job: ...


class RecoverableBackend(ReopenableBackend, Protocol):
    async def recover_submission(self, submission_key: str) -> RecoveryResult: ...


__all__ = [
    "Backend", "Job", "JobFailurePhase", "JobRef", "JobState", "JobStatus",
    "ObservationUnavailable", "RecoverableBackend", "RecoveryDisposition",
    "RecoveryResult", "ReopenableBackend", "UnsupportedCapability",
]
