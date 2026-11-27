"""Client-side execution composition and endpoint caching."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kkrunner.client.execution_client import ExecutionClient
    from kkrunner.client.contracts import (
        JobAttachment,
        JobProtocol,
        SubmissionContext,
    )
    from kkrunner.client.pool import ClientPool, default_client_pool
    from kkrunner.client.staging import StagingSpec
    from kkrunner.targets import LocalTarget, SshTarget, Target

__all__ = [
    "ExecutionClient", "StagingSpec",
    "ClientPool", "JobAttachment", "JobProtocol",
    "SubmissionContext", "LocalTarget",
    "SshTarget", "Target", "default_client_pool",
]


def __getattr__(name):
    if name == "ExecutionClient":
        from .execution_client import ExecutionClient as value
    elif name == "StagingSpec":
        from .staging import StagingSpec as value
    elif name in ("JobAttachment", "JobProtocol", "SubmissionContext"):
        from . import contracts
        value = getattr(contracts, name)
    elif name in ("ClientPool", "default_client_pool"):
        from . import pool
        value = getattr(pool, name)
    elif name in ("LocalTarget", "SshTarget", "Target"):
        from kkrunner import targets
        value = getattr(targets, name)
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))
