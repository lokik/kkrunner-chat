"""Local subprocess and filesystem transport implementation."""

__all__ = ["LocalTarget"]


def __getattr__(name):
    if name == "LocalTarget":
        from .target import LocalTarget
        globals()[name] = LocalTarget
        return LocalTarget
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
