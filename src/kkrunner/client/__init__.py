"""Client-side execution endpoints, pooling, and transports."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kkrunner.client.local import LocalRunEndpoint
    from kkrunner.client.ssh import SshRunEndpoint
    from kkrunner.client.contracts import JobAttachment, JobProtocol, SubmissionContext, RunEndpoint
    from kkrunner.client.endpoint import ExecutionEndpointConfig
    from kkrunner.client.pool import EndpointPool, default_endpoint_pool
    from kkrunner.client.transport import LocalTransport, SshTransport, Transport

__all__ = [
    "LocalRunEndpoint", "SshRunEndpoint",
    "EndpointPool", "ExecutionEndpointConfig", "JobAttachment", "JobProtocol",
    "SubmissionContext", "RunEndpoint", "LocalTransport",
    "SshTransport", "Transport", "default_endpoint_pool",
]


def __getattr__(name):
    if name == "LocalRunEndpoint":
        from .local import LocalRunEndpoint as value
    elif name == "SshRunEndpoint":
        from .ssh import SshRunEndpoint as value
    elif name in ("JobAttachment", "JobProtocol", "SubmissionContext", "RunEndpoint"):
        from . import contracts
        value = getattr(contracts, name)
    elif name == "ExecutionEndpointConfig":
        from .endpoint import ExecutionEndpointConfig as value
    elif name in ("EndpointPool", "default_endpoint_pool"):
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
