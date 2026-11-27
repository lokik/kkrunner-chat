"""Connection implementations, independent of execution backend selection."""

__all__ = ["Target", "LocalTarget", "SshTarget"]


def __getattr__(name):
    if name == "Target":
        from .base import Target as value
    elif name == "LocalTarget":
        from .implementations.local.target import LocalTarget as value
    elif name == "SshTarget":
        from .implementations.ssh.target import SshTarget as value
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))


def require_persistable_target(transport):
    """Accept built-in targets and read-only unsupported transport settings."""
    from .implementations.local.target import LocalTarget
    from .implementations.ssh.target import SshTarget
    from .implementations.unavailable import UnavailableTarget

    if type(transport) not in (LocalTarget, SshTarget, UnavailableTarget):
        raise TypeError("Persistent runs require a supported LocalTarget or SshTarget")
    return transport


def target_from_dict(value):
    """Decode settings without importing stored class paths or enabling unknown kinds."""
    kind = value.get("kind")
    if kind == "local":
        from .implementations.local.target import LocalTarget
        return LocalTarget(**value)
    if kind == "ssh":
        from .implementations.ssh.target import SshTarget
        # Historical persisted transport declarations used a boolean.
        if "zstd" in value:
            value = dict(value)
            old = value.pop("zstd")
            value.setdefault("compression", "zstd" if old else None)
        return SshTarget(**value)
    if isinstance(kind, str) and kind:
        from .implementations.unavailable import UnavailableTarget
        return UnavailableTarget(value)
    raise ValueError(f"Unknown transport kind: {kind!r}")
