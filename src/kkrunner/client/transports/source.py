"""Validate and extract one self-contained function for source delivery."""

from __future__ import annotations

import ast
import dis
import inspect
import textwrap
import types


def function_definition(fn, *, static=False, name=None):
    """Extract one function, checking its explicit dependencies.

    Functions may import only target-available modules inside their own body.
    Nonlocal state and module-level referenced objects are rejected rather
    than implicitly copied across the client/target boundary. ``name`` can
    bind an internal bootstrap under a name distinct from the operation.
    """
    if not inspect.isfunction(fn) or (not static and fn.__qualname__ != fn.__name__):
        raise TypeError("Remote operation must be a top-level Python function")
    if fn.__closure__:
        raise TypeError("Remote operation must not depend on module globals or closures")

    def globals_used(code):
        instructions = tuple(dis.get_instructions(code))
        for index, instruction in enumerate(instructions):
            if instruction.opname == "LOAD_NAME" and instruction.argval == "__name__":
                # Class bodies implicitly set __module__ from the interpreter
                # namespace. This compiler-generated metadata is portable;
                # explicit global references in methods remain forbidden.
                following = instructions[index + 1] if index + 1 < len(instructions) else None
                if following is not None and following.opname == "STORE_NAME" and following.argval == "__module__":
                    continue
            if instruction.opname in ("LOAD_GLOBAL", "LOAD_NAME"):
                yield instruction.argval
        for constant in code.co_consts:
            if isinstance(constant, types.CodeType):
                yield from globals_used(constant)

    # Attribute names and function-local imports are not global references.
    # Walk nested code too: a callback may hide a dependency on the client.
    if any(name in fn.__globals__ for name in globals_used(fn.__code__)):
        raise TypeError("Remote operation must not depend on module globals or closures")
    source = textwrap.dedent(getattr(fn, "__source__", None) or inspect.getsource(fn))
    node = ast.parse(source).body[0]
    if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        raise TypeError("Remote operation must be a Python function")
    if static and len(node.decorator_list) == 1 and isinstance(node.decorator_list[0], ast.Name):
        if node.decorator_list[0].id == "staticmethod":
            node.decorator_list = []
    if node.decorator_list:
        raise TypeError("Remote operation must be an undecorated function")
    # Type annotations describe the client API, not dependencies of the target
    # implementation. Erase them from the actual syntax tree; keep the same
    # function body rather than maintaining a separate remote implementation.
    node.returns = None
    for parameter in (*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs,
                      node.args.vararg, node.args.kwarg):
        if parameter is not None:
            parameter.annotation = None
    for default in (*node.args.defaults, *(d for d in node.args.kw_defaults if d is not None)):
        try:
            ast.literal_eval(default)
        except (ValueError, TypeError, SyntaxError) as exc:
            raise TypeError("Portable defaults must be literal values") from exc

    if name is not None:
        node.name = name
    return ast.unparse(node) + "\n"
