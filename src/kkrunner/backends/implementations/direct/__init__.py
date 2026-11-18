"""Portable direct process backend."""

__all__ = ["DirectBackend"]


def __getattr__(name):
    if name == "DirectBackend":
        from .backend import DirectBackend
        return DirectBackend
    raise AttributeError(name)
