"""Transport standalone Python functions, not arbitrary objects or modules."""

from __future__ import annotations

import ast
import inspect
import textwrap


def function_source(fn, *, invocation="call"):
    """Build a remote script from an explicitly standalone function.

    Functions may import only target-available modules inside their own body.
    Nonlocal state and module-level referenced objects are rejected rather
    than implicitly copied across the client/target boundary.
    """
    if not inspect.isfunction(fn) or fn.__qualname__ != fn.__name__:
        raise TypeError("Remote operation must be a top-level Python function")
    closure = inspect.getclosurevars(fn)
    # inspect also labels attribute names (e.g. os.path, Path.resolve)
    # as 'unbound'; they are not external module dependencies.
    if closure.globals or closure.nonlocals:
        raise TypeError("Remote operation must not depend on module globals or closures")
    source = textwrap.dedent(inspect.getsource(fn))
    node = ast.parse(source).body[0]
    if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) or node.decorator_list:
        raise TypeError("Remote operation must be an undecorated function")
    parameters = node.args.posonlyargs + node.args.args + node.args.kwonlyargs
    if (node.returns is not None or any(p.annotation is not None for p in parameters)
            or (node.args.vararg is not None and node.args.vararg.annotation is not None)
            or (node.args.kwarg is not None and node.args.kwarg.annotation is not None)):
        raise TypeError("Portable functions must not have annotations")
    for default in (*node.args.defaults, *(d for d in node.args.kw_defaults if d is not None)):
        try:
            ast.literal_eval(default)
        except (ValueError, TypeError, SyntaxError) as exc:
            raise TypeError("Portable defaults must be literal values") from exc

    name = fn.__name__
    if invocation == "call":
        runner = (
            "import json, sys\n"
            "payload = json.loads(sys.stdin.buffer.read())\n"
            f"result = {name}(*payload['args'], **payload['kwargs'])\n"
            "sys.stdout.write(json.dumps(result))\n"
        )
    elif invocation == "start":
        runner = (
            "import json, sys\n"
            "payload = json.loads(sys.argv[1])\n"
            f"{name}(*payload['args'], **payload['kwargs'])\n"
        )
    elif invocation == "query":
        runner = (
            "import json, sys\n"
            "request = json.loads(sys.argv[1])\n"
            f"print(json.dumps({name}(request['paths'], root=request.get('root'), digest=request.get('digest', False))))\n"
        )
    elif invocation == "finalize":
        runner = (
            "import sys\n"
            f"{name}(*sys.argv[1:])\n"
        )
    else:
        raise ValueError(f"Unknown remote function invocation {invocation!r}")
    return source + "\n\n" + runner
