"""Client-side execution endpoints, pooling, and transports."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kkrunner.client.local import LocalRunEndpoint
    from kkrunner.client.ssh import SshRunEndpoint
    from kkrunner.client.contracts import JobAttachment, JobProtocol, SubmissionContext, RunEndpoint, ReopenableRunEndpoint
    from kkrunner.client.endpoint import ExecutionEndpointConfig
    from kkrunner.client.pool import ExecutorPool, default_executor_pool
    from kkrunner.client.transport import LocalTransport, SshTransport, Transport

__all__ = [
    "LocalRunEndpoint", "SshRunEndpoint",
    "ExecutorPool", "ExecutionEndpointConfig", "JobAttachment", "JobProtocol",
    "SubmissionContext", "RunEndpoint", "ReopenableRunEndpoint", "LocalTransport",
    "SshTransport", "Transport", "default_executor_pool",
]


def __getattr__(name):
    if name == "LocalRunEndpoint":
        from .local import LocalRunEndpoint as value
    elif name == "SshRunEndpoint":
        from .ssh import SshRunEndpoint as value
    elif name in ("JobAttachment", "JobProtocol", "SubmissionContext", "RunEndpoint", "ReopenableRunEndpoint"):
        from . import contracts
        value = getattr(contracts, name)
    elif name == "ExecutionEndpointConfig":
        from .endpoint import ExecutionEndpointConfig as value
    elif name in ("ExecutorPool", "default_executor_pool"):
        from . import pool
        value = getattr(pool, name)
    elif name in ("LocalTransport", "SshTransport", "Transport"):
        from . import transport
        value = getattr(transport, name)
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))
