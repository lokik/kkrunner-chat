"""SSH source adapter for stateless backend classes.

Installed backends transmit import-only wrappers; portable backends retain
their own method implementations. Only explicitly declared static operations
are eligible. The helper owns no jobs, streams, connections or registrations.
"""

from __future__ import annotations

import inspect


def identity(backend):
    """Use the backend unchanged when its implementation is portable."""
    return backend


def static_methods(backend):
    if not inspect.isclass(backend) or backend.__qualname__ != backend.__name__:
        raise TypeError("Backend must be a top-level class")
    return tuple(
        name for name, member in backend.__dict__.items()
        if not name.startswith("_") and isinstance(member, staticmethod)
    )


def class_helper(backend):
    """Return a class delegating all public static operations to *backend*."""
    methods = static_methods(backend)
    if not methods:
        raise TypeError(f"{backend.__name__} exposes no static operations")
    module, name = backend.__module__, backend.__name__
    if not all(part.isidentifier() for part in module.split(".")):
        raise ValueError("Backend module is not importable")
    helper_name = name + "Ssh"
    lines = [f"class {helper_name}:"]
    for method in methods:
        lines.extend((
            "    @staticmethod",
            f"    def {method}(*args, **kwargs):",
            f"        from {module} import {name}",
            f"        return {name}.{method}(*args, **kwargs)",
            "",
        ))
    source = "\n".join(lines)
    namespace = {"__name__": module}
    exec(compile(source, f"<{helper_name}>", "exec"), namespace)
    helper = namespace[helper_name]
    helper.__source__ = source
    return helper


def class_source(backend):
    """Source of a helper class for transmission, never an arbitrary object."""
    return backend.__dict__.get("__source__") or inspect.getsource(backend)
