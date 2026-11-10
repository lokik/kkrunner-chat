"""Client-side execution endpoints, pooling, and transports."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kkrunner.client.agent import AgentBackendProxy
    from kkrunner.client.endpoint import ExecutionEndpoint
    from kkrunner.client.pool import BackendPool, default_backend_pool
    from kkrunner.client.ssh_direct import SshDirectBackend, SshDirectTransport
    from kkrunner.client.transport import LocalTransport, SshTransport, Transport

__all__ = [
    "AgentBackendProxy", "BackendPool", "ExecutionEndpoint", "LocalTransport",
    "SshTransport", "SshDirectBackend", "SshDirectTransport", "Transport", "default_backend_pool",
]


def __getattr__(name):
    if name == "AgentBackendProxy":
        from .agent import AgentBackendProxy as value
    elif name in ("SshDirectBackend", "SshDirectTransport"):
        from . import ssh_direct
        value = getattr(ssh_direct, name)
    elif name == "ExecutionEndpoint":
        from .endpoint import ExecutionEndpoint as value
    elif name in ("BackendPool", "default_backend_pool"):
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
