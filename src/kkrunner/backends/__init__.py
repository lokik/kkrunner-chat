"""Execution backend functions and explicit backend selection.

No registration framework: the three supported implementations are fixed,
imported lazily only by the target that executes them.
"""
from __future__ import annotations

from importlib import import_module

def operation_module(kind):
    """Import path for one of the three supported functional implementations."""
    if kind == "direct":
        return "kkrunner.backends.direct"
    if kind == "process":
        return "kkrunner.backends.supervised"
    if kind == "psij":
        return "kkrunner.backends.psij"
    raise ValueError(f"Unsupported backend: {kind!r}")


def backend_module(kind):
    return import_module(operation_module(kind))


def available_backend_names():
    return frozenset(("direct", "process", "psij"))


def installed_backend_names():
    """Backends the installed agent can submit and reattach."""
    return frozenset(("process", "psij"))


def operation_ref(kind, name):
    if not isinstance(name, str) or not name.isidentifier():
        raise ValueError("Operation must be a Python function name")
    return operation_module(kind), name


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
