"""Backend execution contracts, independent of clients, agents, and persistence.

A Job is a live Python handle.  A JobRef, when available, identifies an
execution independently of that handle.  Submission recovery is a *backend*
operation, because the caller may have lost the submit reply and job identity.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
import json
from typing import (
    TYPE_CHECKING, Awaitable, BinaryIO, Callable, ClassVar, Mapping, Optional, Protocol, Tuple, Union,
)

if TYPE_CHECKING:
    from typing_extensions import Unpack
    from kkrunner.backends.support.messages import (
        AcknowledgeReply, ExecutionRequest, FollowOptions, RecoveryReply, RecoveryRequest,
        StatusReply, SubmissionReply, PreparedExecution, ExecutionReply,
    )

from kkrunner.execution import RunState
from kkrunner.support.decorators import class_property


class BackendCapability(str, Enum):
    """Native backend guarantees, independent of client transport."""

    REATTACH = "reattach"
    RECOVER = "recover"
    STATUS = "status"
    CANCEL = "cancel"


class SubmissionNotStarted(RuntimeError):
    """The target confirms that the native submission boundary was not crossed."""

    submission_started = False


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

    state: RunState
    return_code: Optional[int] = None
    failure_phase: Optional[JobFailurePhase] = None
    message: Optional[str] = None

    def __post_init__(self):
        state = RunState(self.state)
        if state in (RunState.SUBMITTING, RunState.DOWNLOADING, RunState.POSTPROCESSING):
            raise ValueError(f"{state.value} is a client lifecycle phase, not a JobStatus")
        object.__setattr__(self, "state", state)
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
        state=RunState(value),
        return_code=return_code,
        failure_phase=phase,
        message=message,
    )


def decode_job_status(reply):
    """Decode the shared JSON status reply at a backend invocation boundary."""
    return job_status_from_facts(
        reply.get("state", "unknown"), return_code=reply.get("return_code"),
        failure_phase=reply.get("failure_phase"), message=reply.get("error"),
    )


class UnsupportedCapability(NotImplementedError):
    """The selected backend/job cannot provide the requested guarantee."""


class ObservationUnavailable(OSError):
    """Observation was lost; this is not a terminal backend failure."""


class ByteSink(Protocol):
    """Borrowed binary output; write/flush complete before returning."""

    def write(self, data: bytes, /) -> Optional[int]: ...
    def flush(self) -> None: ...


class AsyncByteSink(Protocol):
    """Borrowed binary output whose write awaits available capacity."""

    def write(self, data: bytes, /) -> Awaitable[Optional[int]]: ...
    def flush(self) -> None: ...


class ControlReader(Protocol):
    async def readline(self) -> bytes: ...


class DirectOperations(Protocol):
    """Static namespace interface for one session-bound execution."""

    @staticmethod
    def prepare_mpi(request: ExecutionRequest, *, resolve_only: bool = False) -> PreparedExecution: ...

    @staticmethod
    def prepare_target_outputs(request: ExecutionRequest) -> None: ...

    @staticmethod
    def status(job_ref: str) -> StatusReply: ...

    @staticmethod
    async def stream(
        request: ExecutionRequest, *,
        stdout: Optional[Union[ByteSink, AsyncByteSink]] = None,
        stderr: Optional[Union[ByteSink, AsyncByteSink]] = None,
        control: Optional[ControlReader] = None,
        files: Optional[Mapping[str, BinaryIO]] = None,
        started: Optional[Callable[[ExecutionReply], Union[None, Awaitable[None]]]] = None,
        stdin: Optional[BinaryIO] = None,
    ) -> ExecutionReply: ...


class ManagedOperations(Protocol):
    """Static namespace interface for durable execution and observation.

    submit returns an opaque identity. status/cancel/follow return native
    facts. Cancelling follow closes only its observer, while cancel requests
    calculation termination. recover never starts a second calculation.
    """

    @staticmethod
    def submit(request: ExecutionRequest) -> SubmissionReply: ...

    @staticmethod
    def status(job_ref: str) -> StatusReply: ...

    @staticmethod
    def cancel(job_ref: str) -> StatusReply: ...

    @staticmethod
    def recover(
        submission_key: str, request: RecoveryRequest, *, submitted_at: Optional[str] = None,
    ) -> RecoveryReply: ...

    @staticmethod
    def acknowledge(job_ref: str) -> AcknowledgeReply: ...

    @staticmethod
    def acknowledge_submission(submission_key: str) -> AcknowledgeReply: ...

    @staticmethod
    async def follow(
        job_ref: str, stdout: ByteSink, stderr: ByteSink, **options: Unpack[FollowOptions],
    ) -> StatusReply: ...

    @staticmethod
    async def stream(job_ref: str, **options: Unpack[FollowOptions]) -> StatusReply: ...

    @staticmethod
    async def follow_framed(job_ref: str, **options: Unpack[FollowOptions]) -> None: ...


class Backend:
    """Configured native backend with a class-level static-operation interface.

    Every backend implements ``stream(subject, **options)``. Session-bound
    backends receive an execution request and return authoritative status. Durable
    backends receive a job reference and return authoritative terminal facts;
    they also declare submit, status, cancel, recover and acknowledgement.
    Short operations are synchronous and exchange JSON-compatible values.
    ``follow(ref, stdout, stderr, **options)`` is asynchronous and borrows its
    sinks; cancelling it releases observation without cancelling execution.
    stream is asynchronous too, using interpreter streams at the SSH boundary.
    recover(key, request, submitted_at=...) returns a receipt, None when
    indeterminate, or an explicit unsupported reply. It never resubmits.
    Each instance owns validated immutable options and its actual capabilities.
    Static native operations and SSH helpers remain methods of its class.
    The client supplies its transport and client resources.
    """

    name: str
    default_options: ClassVar[Tuple[Tuple[str, Optional[str]], ...]] = ()
    resolves_mpi_launcher = False
    execution_mode = "direct"
    supports: frozenset[BackendCapability]
    options: Mapping[str, object]
    default_detached = False

    def __init__(self, **options):
        """Keep backend configuration in one immutable, instance-owned place."""
        defaults = dict(self.default_options)
        unexpected = options.keys() - defaults.keys()
        if unexpected:
            raise ValueError(f"Unknown {self.name} backend options: {sorted(unexpected)}")
        defaults.update(options)
        if any(not isinstance(key, str) or not isinstance(value, (str, int, float, bool, type(None)))
               for key, value in defaults.items()):
            raise TypeError("Backend options require string keys and scalar JSON values")
        json.dumps(defaults, allow_nan=False)
        self.options = MappingProxyType(defaults)
        self.supports = frozenset()

    @class_property
    def SshHelper(cls):
        """Portable implementations are sent unchanged, one method at a time."""
        return cls


__all__ = [
    "Backend", "BackendCapability", "DirectOperations", "ManagedOperations", "ByteSink", "AsyncByteSink", "ControlReader", "JobFailurePhase", "JobRef", "JobStatus",
    "ObservationUnavailable", "RecoveryDisposition", "RecoveryResult",
    "UnsupportedCapability", "SubmissionNotStarted", "job_status_from_facts", "decode_job_status",
]
