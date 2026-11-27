"""Select native backend types and construct configured backend instances.

No registration framework: the three supported implementations are fixed,
imported lazily only by the target that executes them.
"""
from __future__ import annotations


# Fixed lazy imports; not a runtime registration mechanism.
_BACKENDS = {
    "direct": (".implementations.direct.backend", "DirectBackend"),
    "kkrunner": (".implementations.supervised.backend", "SupervisedBackend"),
    "psij": (".implementations.psij.backend", "PsijBackend"),
}


def backend_class(kind):
    """Resolve one of the supported backend identifiers."""
    from importlib import import_module

    try:
        module, name = _BACKENDS[kind]
    except KeyError:
        raise ValueError(f"Unsupported backend: {kind!r}") from None
    return getattr(import_module(module, __name__), name)


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
    return frozenset(_BACKENDS)


def installed_backend_names():
    """Backends the installed agent can submit and reattach."""
    return available_backend_names() - {"direct"}


__all__ = [name for _, name in _BACKENDS.values()] + ["BackendConfig"]


def __getattr__(name):
    if name == "BackendConfig":
        from .config import BackendConfig
        globals()[name] = BackendConfig
        return BackendConfig
    for kind, (_, class_name) in _BACKENDS.items():
        if name == class_name:
            value = backend_class(kind)
            globals()[name] = value
            return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(set(globals()) | set(__all__))
