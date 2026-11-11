"""Static backend classes with explicit selection.

No registration framework: the three supported implementations are fixed,
imported lazily only by the target that executes them.
"""
from __future__ import annotations


def backend_class(kind):
    """Static-operation backend type, separate from client Job instances."""
    if kind == "direct":
        from .direct import DirectBackend
        return DirectBackend
    if kind == "process":
        from .supervised import SupervisedBackend
        return SupervisedBackend
    if kind == "psij":
        from .psij import PsijBackend
        return PsijBackend
    raise ValueError(f"Unsupported backend: {kind!r}")


def available_backend_names():
    return frozenset(("direct", "process", "psij"))


def installed_backend_names():
    """Backends the installed agent can submit and reattach."""
    return frozenset(("process", "psij"))


__all__ = ["DirectBackend", "SupervisedBackend", "PsijBackend"]


def __getattr__(name):
    kinds = {"DirectBackend": "direct", "SupervisedBackend": "process", "PsijBackend": "psij"}
    try:
        value = backend_class(kinds[name])
    except KeyError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))
