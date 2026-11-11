"""Select a portable backend or build its import-only SSH counterpart.

Helpers are namespaces, just like backends. Source delivery always selects
one method; neither a class definition nor unrelated methods cross SSH.
"""

import inspect
from functools import lru_cache

from kkrunner.support.decorators import class_property

def static_methods(backend):
    """Explicit public operations, excluding the helper-selection descriptor."""
    if not inspect.isclass(backend) or backend.__qualname__ != backend.__name__:
        raise TypeError("Backend must be a top-level class")
    return tuple(
        name for name, member in vars(backend).items()
        if not name.startswith("_") and isinstance(member, staticmethod)
    )


@lru_cache(maxsize=None)
def class_helper(backend):
    """Build a cached namespace whose methods import and call *backend*."""
    methods = static_methods(backend)
    if not methods:
        raise TypeError(f"{backend.__name__} exposes no static operations")
    module, name = backend.__module__, backend.__name__
    if not all(part.isidentifier() for part in module.split(".")):
        raise ValueError("Backend module is not importable")
    members = {"__module__": module}
    for method in methods:
        asynchronous = inspect.iscoroutinefunction(getattr(backend, method))
        prefix = "async " if asynchronous else ""
        awaiting = "await " if asynchronous else ""
        source = (
            f"{prefix}def {method}(*args, **kwargs):\n"
            f"    from {module} import {name}\n"
            f"    return {awaiting}{name}.{method}(*args, **kwargs)\n"
        )
        namespace = {"__name__": module}
        exec(compile(source, f"<{name}.{method} SSH helper>", "exec"), namespace)
        function = namespace[method]
        function.__source__ = source
        members[method] = staticmethod(function)
    return type(name + "Ssh", (), members)


imported_ssh_helper = class_property(class_helper)
