"""Canonical backend operation module registry.

Module identity is data. Backend implementations expose context-independent
functions and are imported only in the interpreter performing an operation.
The registry is not an instance factory and holds no job state.
"""

from __future__ import annotations

from types import MappingProxyType

_INSTALLED_BACKENDS = MappingProxyType({
    "process": "kkrunner.backends.process.supervised.operations",
    "psij": "kkrunner.backends.psij.agent",
})


def installed_backend_names():
    return frozenset(_INSTALLED_BACKENDS)


def operation_module(backend):
    try:
        return _INSTALLED_BACKENDS[backend]
    except KeyError as exc:
        raise ValueError(f"Unsupported installed backend: {backend!r}") from exc


def operation_ref(backend, function):
    """An immutable import path for an installed backend operation."""
    if not isinstance(function, str) or not function.isidentifier():
        raise ValueError("Operation must be a Python function name")
    return operation_module(backend), function
