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
    from kkrunner.files.manifest import FileManifest
    from kkrunner.runs.base import ExecutionOutcome, ExecutionSpec
    from kkrunner.runs.model import Execution


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
class JobAttachment:
    """Reattach context without passing a persisted RunRecord to a backend.

    The backend needs the execution endpoint, working directory and declared
    files to reconstruct its observation; client lifecycle state, processing
    attempts and project metadata are deliberately excluded.
    """

    execution: Execution
    files: FileManifest


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
    directory: Optional[str] = None
    output_file: Optional[str] = None
    error_file: Optional[str] = None

    def __post_init__(self):
        if not isinstance(self.disposition, RecoveryDisposition):
            object.__setattr__(self, "disposition", RecoveryDisposition(self.disposition))
        if (self.disposition == RecoveryDisposition.FOUND) != (self.ref is not None):
            raise ValueError("Exactly FOUND requires a JobRef")
        if self.ref is not None and not isinstance(self.ref, JobRef):
            raise TypeError("RecoveryResult.ref must be a JobRef")
        if self.disposition is not RecoveryDisposition.FOUND and any(
            path is not None for path in (self.directory, self.output_file, self.error_file)
        ):
            raise ValueError("Only FOUND may describe recovered execution paths")

    @classmethod
    def found(cls, ref: JobRef, *, directory=None, output_file=None, error_file=None) -> RecoveryResult:
        return cls(
            RecoveryDisposition.FOUND, ref, directory=directory,
            output_file=output_file, error_file=error_file,
        )

    @classmethod
    def absent(cls) -> RecoveryResult:
        return cls(RecoveryDisposition.ABSENT)

    @classmethod
    def indeterminate(cls, message: Optional[str] = None) -> RecoveryResult:
        return cls(RecoveryDisposition.INDETERMINATE, message=message)

    @classmethod
    def unsupported(cls, message: Optional[str] = None) -> RecoveryResult:
        return cls(RecoveryDisposition.UNSUPPORTED, message=message)


def job_status_from_facts(state, *, return_code=None, failure_phase=None, message=None):
    """Translate execution facts into the backend-only status vocabulary.

    Staging and postprocessing are client concerns, not Job states. This
    converter intentionally rejects those states instead of silently mapping
    them to a completed backend execution.
    """
    value = state.value if hasattr(state, "value") else state
    if value == "submitting":
        # An attempted submit without a confirmed identity is uncertainty,
        # not evidence that the job has never been launched.
        value = "unknown"
    phase = failure_phase.value if hasattr(failure_phase, "value") else failure_phase
    if phase == "artifacts":
        phase = "output"
    if phase in ("download", "postprocessing"):
        phase = None
    return JobStatus(
        state=JobState(value),
        return_code=return_code,
        failure_phase=phase,
        message=message,
    )


class UnsupportedCapability(NotImplementedError):
    """The selected backend/job cannot provide the requested guarantee."""


class ObservationUnavailable(OSError):
    """Observation was lost; this is not a terminal backend failure."""


@runtime_checkable
class Job(Protocol):
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


@runtime_checkable
class Backend(Protocol):
    """Native execution mechanism; creates Jobs but never client sessions."""

    def create_job(self, spec: ExecutionSpec, *, submission_key=None, callback=None) -> Job: ...


class ReopenableBackend(Backend, Protocol):
    def open_job(self, attachment: JobAttachment, *, callback=None, output_listener=None, start=False) -> Job: ...


class RecoverableBackend(ReopenableBackend, Protocol):
    def recover_submission(
        self, submission_key: str, submission: JobAttachment, *, submitted_at=None,
    ) -> RecoveryResult: ...


__all__ = [
    "Backend", "Job", "JobAttachment", "JobFailurePhase", "JobRef", "JobState", "JobStatus",
    "ObservationUnavailable", "RecoverableBackend", "RecoveryDisposition",
    "RecoveryResult", "ReopenableBackend", "UnsupportedCapability",
    "job_status_from_facts",
]
