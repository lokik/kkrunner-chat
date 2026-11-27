"""Create source-portable import-only wrappers for managed backend operations."""

import inspect
from functools import lru_cache
from kkrunner.backends.support.class_helper import static_methods
from kkrunner.support.decorators import portable

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
        members[method] = staticmethod(portable(function))
    return type(name + "Ssh", (), members)


def ssh_function(operation):
    """Deliver the operation itself if portable, otherwise import its backend."""
    if operation.function is not None:
        return operation.function
    function = getattr(operation.backend, operation.method)
    if getattr(function, "__kkrunner_portable__", False):
        return function
    return getattr(class_helper(operation.backend), operation.method)
