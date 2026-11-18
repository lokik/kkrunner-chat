"""Supervised process backend and its private support modules."""

__all__ = ["SupervisedBackend"]


def __getattr__(name):
    if name == "SupervisedBackend":
        from .backend import SupervisedBackend
        return SupervisedBackend
    raise AttributeError(name)
