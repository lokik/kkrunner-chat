"""Execution backends and their reusable endpoint identities."""

from typing import TYPE_CHECKING

from kkrunner.backends.endpoint import ExecutorEndpoint

if TYPE_CHECKING:
    from kkrunner.backends.process.engine import LocalExecutor
    from kkrunner.backends.pool import ExecutorPool, default_executor_pool
    from kkrunner.backends.scheduler.engine import PsijExecutor

# Run contracts use endpoint metadata. Load implementations only on request so
# importing those contracts does not load backends or create an import cycle.
__all__ = ["ExecutorEndpoint", "ExecutorPool", "LocalExecutor", "PsijExecutor", "default_executor_pool"]


def __getattr__(name):
    if name == "LocalExecutor":
        from .local import LocalExecutor as value
    elif name == "PsijExecutor":
        from .psij import PsijExecutor as value
    elif name in (
        "ExecutorPool",
        "default_executor_pool",
    ):
        from . import pool
        value = getattr(pool, name)
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = value
    return value

def __dir__():
    return sorted(set(globals()) | set(__all__))
