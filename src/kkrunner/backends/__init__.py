"""Process and scheduler execution backends with lazy imports."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kkrunner.backends.contracts import Backend, Job, JobAttachment, JobRef, JobStatus
    from kkrunner.backends.process.engine import ProcessBackend
    from kkrunner.backends.scheduler.engine import PsijBackend

__all__ = ["Backend", "Job", "JobAttachment", "JobRef", "JobStatus", "ProcessBackend", "PsijBackend"]


def __getattr__(name):
    if name in ("Backend", "Job", "JobAttachment", "JobRef", "JobStatus"):
        from . import contracts
        value = getattr(contracts, name)
    elif name == "ProcessBackend":
        from .process.engine import ProcessBackend as value
    elif name == "PsijBackend":
        from .scheduler.engine import PsijBackend as value
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))
