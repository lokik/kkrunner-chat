"""Remote execution clients and transports.

Imports of this package are lightweight; concrete interfaces load on demand.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kkrunner.client.agent import RemoteEndpointExecutor
    from kkrunner.client.transport import LocalTransport, RunningCommand, SshTransport, Transport

__all__ = [
    "LocalTransport",
    "RemoteEndpointExecutor",
    "RunningCommand",
    "SshTransport",
    "Transport",
]


def __getattr__(name):
    if name == "RemoteEndpointExecutor":
        from .executor import RemoteEndpointExecutor as value
    elif name in (
        "LocalTransport",
        "RunningCommand",
        "SshTransport",
        "Transport",
    ):
        from . import transport
        value = getattr(transport, name)
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = value
    return value

def __dir__():
    return sorted(set(globals()) | set(__all__))
