"""Client-side execution composition, pooling, and transports."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kkrunner.client.client import RunClient
    from kkrunner.client.contracts import (
        JobAttachment,
        JobProtocol,
        RunClientProtocol,
        SubmissionContext,
    )
    from kkrunner.client.pool import ClientPool, default_client_pool
    from kkrunner.client.staging import StagingSpec
    from kkrunner.client.transports import LocalTransport, SshTransport, Transport

__all__ = [
    "RunClient", "StagingSpec",
    "ClientPool", "JobAttachment", "JobProtocol",
    "SubmissionContext", "RunClientProtocol", "LocalTransport",
    "SshTransport", "Transport", "default_client_pool",
]


def __getattr__(name):
    if name == "RunClient":
        from .client import RunClient as value
    elif name == "StagingSpec":
        from .staging import StagingSpec as value
    elif name in ("JobAttachment", "JobProtocol", "SubmissionContext", "RunClientProtocol"):
        from . import contracts
        value = getattr(contracts, name)
    elif name in ("ClientPool", "default_client_pool"):
        from . import pool
        value = getattr(pool, name)
    elif name in ("LocalTransport", "SshTransport", "Transport"):
        from . import transports
        value = getattr(transports, name)
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))
