"""Connection implementations, independent of execution backend selection."""

__all__ = ["Transport", "LocalTransport", "SshTransport"]


def __getattr__(name):
    if name == "Transport":
        from .base import Transport as value
    elif name == "LocalTransport":
        from .local import LocalTransport as value
    elif name == "SshTransport":
        from .ssh import SshTransport as value
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))


def require_persistable_transport(transport):
    """Reject extensions whose behavior cannot be reconstructed from a record."""
    from .local import LocalTransport
    from .ssh import SshTransport

    if type(transport) not in (LocalTransport, SshTransport):
        raise TypeError("Persistent runs require a supported LocalTransport or SshTransport")
    return transport


def transport_from_dict(value):
    """Reconstruct a whitelisted transport; never import stored class paths."""
    kind = value.get("kind")
    if kind == "local":
        from .local import LocalTransport
        return LocalTransport(**value)
    if kind == "ssh":
        from .ssh import SshTransport
        return SshTransport(**value)
    raise ValueError(f"Unknown transport kind: {kind!r}")
