"""Backend-independent execution library with lazy root-level conveniences."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kkrunner._version import __version__
    from kkrunner.execution import RunState
    from kkrunner.files.task_file import TaskFile
    from kkrunner.runs.base import ExecutionOutcome, ExecutionSpec
    from kkrunner.runs.manager import RunManager
    from kkrunner.runs.processing import ProcessingPending
    from kkrunner.runs.records import RunStore
    from kkrunner.runs.run import Run

__all__ = [
    "ExecutionOutcome", "ExecutionSpec", "ProcessingPending",
    "Run", "RunManager", "RunState", "RunStore", "TaskFile", "__version__",
]


def __getattr__(name):
    if name == "__version__":
        from ._version import __version__ as value
    elif name == "TaskFile":
        from .files.task_file import TaskFile as value
    elif name in (
        "ExecutionOutcome",
        "ExecutionSpec",
    ):
        from .runs import base
        value = getattr(base, name)
    elif name == "RunState":
        from .execution import RunState as value
    elif name == "Run":
        from .runs.run import Run as value
    elif name == "RunManager":
        from .runs.manager import RunManager as value
    elif name == "ProcessingPending":
        from .runs.processing import ProcessingPending as value
    elif name == "RunStore":
        from .runs.records import RunStore as value
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = value
    return value

def __dir__():
    return sorted(set(globals()) | set(__all__))
