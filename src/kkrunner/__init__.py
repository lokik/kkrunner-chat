"""Backend-independent execution library with lazy root-level conveniences."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kkrunner._version import __version__
    from kkrunner.execution import MpiSpec, RunState
    from kkrunner.backends import DirectBackend, SupervisedBackend, PsijBackend
    from kkrunner.client import ExecutionClient, StagingSpec
    from kkrunner.targets import Target, LocalTarget, SshTarget
    from kkrunner.projects import ProjectRepository, ProjectRunManager
    from kkrunner._convenience import run, submit
    from kkrunner.files.task_file import TaskFile
    from kkrunner.runs.base import ExecutionOutcome, ExecutionSpec
    from kkrunner.runs.manager import RunManager
    from kkrunner.runs.processing import ProcessingPending
    from kkrunner.runs.records import RunStore
    from kkrunner.runs.memory_store import MemoryRunStore
    from kkrunner.runs.run import Run

__all__ = [
    "ExecutionOutcome", "ExecutionSpec", "ProcessingPending",
    "Run", "RunManager", "RunState", "RunStore", "MemoryRunStore", "TaskFile", "__version__",
    "run", "submit", "ExecutionClient", "StagingSpec", "MpiSpec", "Target", "LocalTarget", "SshTarget",
    "DirectBackend", "SupervisedBackend", "PsijBackend", "ProjectRepository", "ProjectRunManager",
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
    elif name in ("RunState", "MpiSpec"):
        from . import execution
        value = getattr(execution, name)
    elif name in ("run", "submit"):
        from . import _convenience
        value = getattr(_convenience, name)
    elif name in ("ExecutionClient", "StagingSpec"):
        from . import client
        value = getattr(client, name)
    elif name in ("Target", "LocalTarget", "SshTarget"):
        from . import targets
        value = getattr(targets, name)
    elif name in ("DirectBackend", "SupervisedBackend", "PsijBackend"):
        from . import backends
        value = getattr(backends, name)
    elif name in ("ProjectRepository", "ProjectRunManager"):
        from . import projects
        value = getattr(projects, name)
    elif name == "Run":
        from .runs.run import Run as value
    elif name == "RunManager":
        from .runs.manager import RunManager as value
    elif name == "ProcessingPending":
        from .runs.processing import ProcessingPending as value
    elif name == "MemoryRunStore":
        from .runs.memory_store import MemoryRunStore as value
    elif name == "RunStore":
        from .runs.records import RunStore as value
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = value
    return value

def __dir__():
    return sorted(set(globals()) | set(__all__))
