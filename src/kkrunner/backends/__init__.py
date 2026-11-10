"""Process and PSI/J execution backends with lazy imports."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kkrunner.backends.contracts import Backend, BackendJobContext, Job, JobRef, JobStatus, SubmissionContext
    from kkrunner.backends.process.backend import DirectProcessBackend, SupervisedProcessBackend
    from kkrunner.backends.psij.backend import PsijBackend

__all__ = ["Backend", "BackendJobContext", "Job", "JobRef", "JobStatus", "SubmissionContext", "DirectProcessBackend", "SupervisedProcessBackend", "PsijBackend"]


def __getattr__(name):
    if name in ("Backend", "BackendJobContext", "Job", "JobRef", "JobStatus", "SubmissionContext"):
        from . import contracts
        value = getattr(contracts, name)
    elif name in ("DirectProcessBackend", "SupervisedProcessBackend"):
        from .process import backend
        value = getattr(backend, name)
    elif name == "PsijBackend":
        from .psij.backend import PsijBackend as value
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))
