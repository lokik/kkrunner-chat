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
        from .psij import PsijOperations
        return PsijOperations
    raise ValueError(f"Unsupported backend: {kind!r}")


def available_backend_names():
    return frozenset(("direct", "process", "psij"))


def installed_backend_names():
    """Backends the installed agent can submit and reattach."""
    return frozenset(("process", "psij"))


__all__ = [
    "Backend", "BackendJobContext", "JobProtocol", "JobRef", "JobStatus",
    "SubmissionContext", "DirectProcessBackend", "SupervisedProcessBackend",
    "PsijBackend",
]


def __getattr__(name):
    if name in ("Backend", "BackendJobContext", "JobProtocol", "JobRef", "JobStatus", "SubmissionContext"):
        from . import contracts
        value = getattr(contracts, name)
    elif name == "DirectProcessBackend":
        from .direct import DirectProcessBackend as value
    elif name == "SupervisedProcessBackend":
        from .supervised import SupervisedProcessBackend as value
    elif name == "PsijBackend":
        from .psij import PsijBackend as value
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = value
    return value
