"""Local subprocess and filesystem transport implementation."""

__all__ = ["LocalTransport"]


def __getattr__(name):
    if name == "LocalTransport":
        from .transport import LocalTransport
        globals()[name] = LocalTransport
        return LocalTransport
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
