"""Connection implementations, independent of execution backend selection."""

from .base import Transport
from .local import LocalTransport
from .ssh import SshTransport

__all__ = ["Transport", "LocalTransport", "SshTransport"]


def require_persistable_transport(transport):
    """Reject extensions whose behavior cannot be reconstructed from a record."""
    if type(transport) not in (LocalTransport, SshTransport):
        raise TypeError("Persistent runs require a supported LocalTransport or SshTransport")
    return transport


def transport_from_dict(value):
    """Reconstruct a whitelisted transport; never import stored class paths."""
    kind = value.get("kind")
    if kind == "local":
        return LocalTransport(**value)
    if kind == "ssh":
        return SshTransport(**value)
    raise ValueError(f"Unknown transport kind: {kind!r}")
