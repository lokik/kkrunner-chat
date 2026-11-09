"""Execution backend contracts and implementations.

Import implementations lazily to avoid loading process or optional PSI/J code
when only declarations are requested.
"""

from typing import TYPE_CHECKING

from kkrunner.backends.endpoint import ExecutorEndpoint

if TYPE_CHECKING:
    from kkrunner.backends.process.engine import ProcessBackend
    from kkrunner.backends.pool import ExecutorPool, default_executor_pool
    from kkrunner.backends.scheduler.engine import PsijBackend

__all__ = ["ExecutorEndpoint", "ExecutorPool", "ProcessBackend", "PsijBackend", "default_executor_pool"]


def __getattr__(name):
    if name == "ProcessBackend":
        from .process.engine import ProcessBackend as value
    elif name == "PsijBackend":
        from .scheduler.engine import PsijBackend as value
    elif name in ("ExecutorPool", "default_executor_pool"):
        from . import pool
        value = getattr(pool, name)
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))
