"""Client-side execution endpoints, pooling, and transports."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kkrunner.client.agent import AgentRunEndpoint
    from kkrunner.client.contracts import JobAttachment, RunEndpoint, ReopenableRunEndpoint
    from kkrunner.client.endpoint import ExecutionEndpointConfig
    from kkrunner.client.pool import ExecutorPool, default_executor_pool
    from kkrunner.client.ssh_direct import SshDirectRunEndpoint, SshDirectTransport
    from kkrunner.client.transport import LocalTransport, SshTransport, Transport

__all__ = [
    "AgentRunEndpoint", "ExecutorPool", "ExecutionEndpointConfig", "JobAttachment", "RunEndpoint", "ReopenableRunEndpoint", "LocalTransport",
    "SshTransport", "SshDirectRunEndpoint", "SshDirectTransport", "Transport", "default_executor_pool",
]


def __getattr__(name):
    if name == "AgentRunEndpoint":
        from . import agent
        value = getattr(agent, name)
    elif name in ("SshDirectRunEndpoint", "SshDirectTransport"):
        from . import ssh_direct
        value = getattr(ssh_direct, name)
    elif name in ("JobAttachment", "RunEndpoint", "ReopenableRunEndpoint"):
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
