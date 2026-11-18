"""PSI/J scheduler backend and its private support modules."""

__all__ = ["PsijBackend"]


def __getattr__(name):
    if name == "PsijBackend":
        from .backend import PsijBackend
        return PsijBackend
    raise AttributeError(name)
