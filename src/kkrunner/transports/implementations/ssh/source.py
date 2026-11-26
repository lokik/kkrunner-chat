"""Extract explicitly marked portable function sources for SSH delivery.

Only the @portable marker is checked at runtime. Validation of source
dependencies, globals and defaults belongs in the direct-backend tests.
"""

from __future__ import annotations

import ast
import inspect
import textwrap
from functools import lru_cache
from hashlib import sha256


def function_definition(fn, *, static=False, name=None):
    """Assemble an explicitly marked function and its declared dependencies.

    Dependencies are emitted before their users. Source extraction is not
    responsible for verifying portability (which the tests enforce).
    """
    emitted = {}
    visiting = set()
    definitions = []

    def visit(function, *, name=None):
        if not getattr(function, "__kkrunner_portable__", False):
            raise TypeError("Remote operation must be marked with @portable")
        binding = name or function.__name__
        if function in visiting:
            raise TypeError("Cyclic remote source dependencies")
        if binding in emitted:
            if emitted[binding] is not function:
                raise TypeError(f"Conflicting remote dependency name: {binding}")
            return
        emitted[binding] = function
        visiting.add(function)
        for dependency in function.__source_dependencies__:
            visit(dependency)
        definitions.append((function, name))
        visiting.remove(function)

    visit(fn, name=name)
    return "".join(_function_definition(function, name=binding)
                   for function, binding in definitions)


def _function_definition(fn, *, name=None):
    """Extract source, erasing client-side decorators and annotations."""
    source = textwrap.dedent(getattr(fn, "__source__", None) or inspect.getsource(fn))
    node = ast.parse(source).body[0]
    # Client-side decorators have no meaning on the target. Tests reject
    # unsupported ones, avoiding the need to validate on every operation.
    node.decorator_list = []
    node.returns = None
    for parameter in (*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs,
                      node.args.vararg, node.args.kwarg):
        if parameter is not None:
            parameter.annotation = None
    if name is not None:
        node.name = name
    return ast.unparse(node) + "\n"


@lru_cache(maxsize=None)
def portable_bundle(function):
    """Compile a portable function and its dependencies into stable globals.

    Names depend *only* on module and qualname, not on source or dependencies.
    The caller must discard its sessions after reloading portable code.
    Returns (entrypoint, ((name, source), ...)), in dependency order.
    """

    def symbol(fn):
        identity = f"{fn.__module__}.{fn.__qualname__}"
        return "_kkrunner_" + sha256(identity.encode("utf8")).hexdigest()[:24]

    visited = set()
    visiting = set()
    order = []

    def visit(fn):
        if not getattr(fn, "__kkrunner_portable__", False):
            raise TypeError("Remote operation must be marked with @portable")
        if fn in visiting:
            raise TypeError("Cyclic remote source dependencies")
        if fn in visited:
            return
        visiting.add(fn)
        bindings = set()
        for dependency in fn.__source_dependencies__:
            if dependency.__name__ in bindings:
                raise TypeError(f"Duplicate remote dependency name: {dependency.__name__}")
            bindings.add(dependency.__name__)
            visit(dependency)
        visiting.remove(fn)
        visited.add(fn)
        order.append(fn)

    visit(function)
    definitions = []
    for fn in order:
        source = _function_definition(fn, name=symbol(fn))
        node = ast.parse(source).body[0]
        aliases = [
            ast.Assign(
                targets=[ast.Name(id=dep.__name__, ctx=ast.Store())],
                value=ast.Name(id=symbol(dep), ctx=ast.Load()),
            )
            for dep in fn.__source_dependencies__
        ]
        # Preserve the function docstring at the beginning of its body.
        index = int(bool(node.body and isinstance(node.body[0], ast.Expr)
                         and isinstance(node.body[0].value, ast.Constant)
                         and isinstance(node.body[0].value.value, str)))
        node.body[index:index] = aliases
        ast.fix_missing_locations(node)
        definitions.append((symbol(fn), ast.unparse(node) + "\n"))
    return symbol(function), tuple(definitions)
