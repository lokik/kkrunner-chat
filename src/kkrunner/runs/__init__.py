"""Run contracts, durable records and lifecycle orchestration.

Public conveniences load on demand to avoid importing managers and backends
when only the execution contracts are required.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kkrunner.execution import FailurePhase, MpiNotFoundWarning, MpiSpec, RunState
    from kkrunner.runs.base import ExecutionOutcome, ExecutionSpec, RunStatus
    from kkrunner.runs.manager import (
        DuplicateSubmissionError,
        RunManager,
    )
    from kkrunner.runs.files import HistoricalFileMismatchError
    from kkrunner.runs.model import ExecutionMetadata, WorkingDirectory
    from kkrunner.runs.processing import ProcessingPending
    from kkrunner.runs.records import ActiveRunDeletionError, CorruptRunRecord, ForgottenRunError, RunRecord, RunStore
    from kkrunner.runs.run import Run

__all__ = [
    "ActiveRunDeletionError",
    "CorruptRunRecord",
    "ForgottenRunError",
    "DuplicateSubmissionError",
    "ExecutionMetadata",
    "ExecutionOutcome",
    "ExecutionSpec",
    "FailurePhase",
    "HistoricalFileMismatchError",
    "MpiNotFoundWarning",
    "MpiSpec",
    "Run",
    "ProcessingPending",
    "RunManager",
    "RunRecord",
    "RunState",
    "RunStatus",
    "RunStore",
    "WorkingDirectory",
]


def __getattr__(name):
    if name in (
        "ExecutionOutcome",
        "ExecutionSpec",
        "RunStatus",
    ):
        from . import base
        value = getattr(base, name)
    elif name in ("FailurePhase", "MpiNotFoundWarning", "MpiSpec", "RunState"):
        from kkrunner import execution
        value = getattr(execution, name)
    elif name in (
        "DuplicateSubmissionError",
        "RunManager",
    ):
        from . import manager
        value = getattr(manager, name)
    elif name == "HistoricalFileMismatchError":
        from .files import HistoricalFileMismatchError as value
    elif name in (
        "ExecutionMetadata",
        "WorkingDirectory",
    ):
        from . import model
        value = getattr(model, name)
    elif name == "Run":
        from .run import Run as value
    elif name == "ProcessingPending":
        from .processing import ProcessingPending as value
    elif name in (
        "ActiveRunDeletionError",
        "CorruptRunRecord",
        "ForgottenRunError",
        "RunRecord",
        "RunStore",
    ):
        from . import records
        value = getattr(records, name)
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = value
    return value

def __dir__():
    return sorted(set(globals()) | set(__all__))
