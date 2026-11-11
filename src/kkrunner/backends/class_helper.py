"""Generate import-only SSH delegates and serialize individual static methods.

Backend classes are client-side references. Only the selected operation and
its explicit portable dependencies are sent to the target.
"""

from __future__ import annotations

import ast
import inspect
import textwrap
from functools import lru_cache


def identity(backend):
    """Keep an already-portable backend unchanged."""
    return backend


def static_methods(backend):
    """Public static operations declared directly by a top-level class."""
    if not inspect.isclass(backend) or backend.__qualname__ != backend.__name__:
        raise TypeError("Backend must be a top-level class")
    return tuple(
        name for name, member in vars(backend).items()
        if not name.startswith("_") and isinstance(member, staticmethod)
    )


@lru_cache(maxsize=None)
def class_helper(backend):
    """Return a class whose static methods delegate to an installed backend."""
    names = static_methods(backend)
    if not names:
        raise TypeError(f"{backend.__name__} declares no static operations")
    module, klass = backend.__module__, backend.__name__
    if not all(part.isidentifier() for part in module.split(".")):
        raise TypeError("Backend must have an importable module")

    methods = {}
    sources = {}
    for name in names:
        code = (
            f"def {name}(*args, **kwargs):\n"
            f"    from {module} import {klass}\n"
            f"    return {klass}.{name}(*args, **kwargs)\n"
        )
        namespace = {}
        exec(compile(code, f"<SSH {klass}.{name}>", "exec"), namespace)
        methods[name] = staticmethod(namespace[name])
        sources[name] = code
    helper = type(f"{klass}Ssh", (), methods)
    helper.__method_sources__ = sources
    return helper


def method_source(backend, name):
    """Return only the selected method's function source, without the class."""
    if name not in static_methods(backend):
        raise ValueError(f"{backend.__name__}.{name} is not a static operation")

    portable = vars(backend).get("__method_sources__", {})
    if name in portable:
        return portable[name]

    function = vars(backend)[name].__func__
    source = textwrap.dedent(inspect.getsource(function))
    tree = ast.parse(source)
    if len(tree.body) != 1 or not isinstance(tree.body[0], ast.FunctionDef):
        raise TypeError("Only synchronous Python static methods may be sent")
    decorators = tree.body[0].decorator_list
    if any(not isinstance(dec, ast.Name) or dec.id != "staticmethod" for dec in decorators):
        raise TypeError("Only @staticmethod decorators may be removed")
    # The transmitted method becomes a top-level function on the target.
    return "".join(source.splitlines(keepends=True)[len(decorators):])
