"""Select native backend types and construct configured backend instances.

No registration framework: the three supported implementations are fixed,
imported lazily only by the target that executes them.
"""
from __future__ import annotations


def backend_class(kind):
    """Static-operation backend type, separate from client Job instances."""
    if kind == "direct":
        from .direct.backend import DirectBackend
        return DirectBackend
    if kind == "process":
        from .supervised.backend import SupervisedBackend
        return SupervisedBackend
    if kind == "psij":
        from .psij.backend import PsijBackend
        return PsijBackend
    raise ValueError(f"Unsupported backend: {kind!r}")


def configured_backend(backend=None, *, options=None):
    """Construct one configured backend; never duplicate its options in a transport."""
    from .contracts import Backend

    if backend is None:
        backend = backend_class("direct")
    elif isinstance(backend, str):
        backend = backend_class(backend)
    elif isinstance(backend, Backend):
        if options:
            raise ValueError("Options belong to the existing backend instance")
        return backend
    if not isinstance(backend, type) or not issubclass(backend, Backend):
        raise TypeError("Backend must be a backend name, class or instance")
    return backend(**dict(options or ()))


def available_backend_names():
    return frozenset(("direct", "process", "psij"))


def installed_backend_names():
    """Backends the installed agent can submit and reattach."""
    return frozenset(("process", "psij"))


__all__ = ["DirectBackend", "SupervisedBackend", "PsijBackend", "BackendConfig"]


def __getattr__(name):
    if name == "BackendConfig":
        from .config import BackendConfig
        globals()[name] = BackendConfig
        return BackendConfig
    kinds = {"DirectBackend": "direct", "SupervisedBackend": "process", "PsijBackend": "psij"}
    try:
        value = backend_class(kinds[name])
    except KeyError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))
