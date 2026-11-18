"""Discover explicit public static backend operations."""

import inspect

def static_methods(backend):
    """Explicit public operations, excluding the helper-selection descriptor."""
    if not inspect.isclass(backend) or backend.__qualname__ != backend.__name__:
        raise TypeError("Backend must be a top-level class")
    return tuple(
        name for name, member in vars(backend).items()
        if not name.startswith("_") and isinstance(member, staticmethod)
    )
