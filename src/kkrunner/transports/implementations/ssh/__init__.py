"""SSH transport, persistent Python sessions and streaming calls."""

__all__ = ["SshTransport"]


def __getattr__(name):
    if name == "SshTransport":
        from .transport import SshTransport
        globals()[name] = SshTransport
        return SshTransport
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
