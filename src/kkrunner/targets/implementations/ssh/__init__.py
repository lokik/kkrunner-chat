"""SSH transport, persistent Python sessions and streaming calls."""

__all__ = ["SshTarget"]


def __getattr__(name):
    if name == "SshTarget":
        from .target import SshTarget
        globals()[name] = SshTarget
        return SshTarget
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
