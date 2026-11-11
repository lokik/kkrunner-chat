"""Target-independent references to backend operations.

The operation is data, not an object to be reconstructed on the target.
An installed operation is imported inside the *target* Python interpreter;
a portable one is a deliberately standalone function with no closures/globals.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class BackendOperation:
    """How an endpoint locates one context-independent Python operation."""

    module: str | None = None
    function: str | None = None
    portable: Callable | None = None
    backend: type | None = None

    def __post_init__(self):
        installed = self.module is not None
        if sum((installed, self.portable is not None, self.backend is not None)) != 1:
            raise ValueError("Choose exactly one of installed, portable or backend operation")
        if installed:
            if not self.module or not self.function:
                raise ValueError("Installed operation needs module and function")
            if any(not part.isidentifier() for part in self.module.split(".")):
                raise ValueError("Invalid Python module path")
            if not self.function.isidentifier():
                raise ValueError("Invalid function name")
        elif self.portable is not None:
            # Validate at declaration time, before any work can be submitted.
            from kkrunner.client.portable_functions import function_source
            function_source(self.portable)

    @classmethod
    def installed(cls, module: str, function: str):
        """Call the named operation by importing it on the target interpreter."""
        return cls(module=module, function=function)

    @classmethod
    def for_backend(cls, backend: type, method: str):
        """Reference one declared static backend method without any instance."""
        from kkrunner.backends.class_helper import static_methods
        if method not in static_methods(backend):
            raise ValueError(f"{backend.__name__}.{method} is not a static backend operation")
        return cls(function=method, backend=backend)

    @classmethod
    def standalone(cls, function: Callable):
        return cls(portable=function)


# Local backend calls import their class; no client objects cross the wire.
_CLASS_CALL = """import importlib, json, sys
from contextlib import redirect_stdout
payload = json.loads(sys.stdin.buffer.read())
backend = getattr(importlib.import_module(sys.argv[1]), sys.argv[2])
with redirect_stdout(sys.stderr):
    result = getattr(backend, sys.argv[3])(*payload['args'], **payload['kwargs'])
sys.stdout.write(json.dumps(result))
"""

_CLASS_START = """import importlib, json, sys
backend = getattr(importlib.import_module(sys.argv[1]), sys.argv[2])
payload = json.loads(sys.argv[4])
result = getattr(backend, sys.argv[3])(*payload['args'], **payload['kwargs'])
if len(sys.argv) > 5:
    with open(sys.argv[5], 'w', encoding='utf8') as receipt:
        json.dump(result, receipt, allow_nan=False)
"""


def _ssh_class_command(operation, python, streaming):
    from kkrunner.backends.class_helper import method_source

    original = operation.backend
    target = original.SshHelper(original)
    source = method_source(target, operation.function)
    # The selected static method can be an imported top-level function
    # (direct backend: execute) or a generated installed delegate.
    name = vars(target)[operation.function].__func__.__name__
    lines = [source, "import json, sys"]
    if streaming:
        lines.extend((
            "payload = json.loads(sys.argv[1])",
            f"result = {name}(*payload['args'], **payload['kwargs'])",
            "if len(sys.argv) > 2:",
            "    with open(sys.argv[2], 'w', encoding='utf8') as receipt:",
            "        json.dump(result, receipt, allow_nan=False)",
        ))
    else:
        lines.extend((
            "from contextlib import redirect_stdout",
            "payload = json.loads(sys.stdin.buffer.read())",
            "with redirect_stdout(sys.stderr):",
            f"    result = {name}(*payload['args'], **payload['kwargs'])",
            "sys.stdout.write(json.dumps(result))",
        ))
    script = "\n".join(lines) + "\n"
    return (*python, "-u", "-c", script) if streaming else (*python, "-c", script)


# In the installed case, this short runner is the same for all backends.
# Backend-specific code is imported on the target; no client globals or
# Python object identity crosses the transport.
_INSTALLED_CALL = """import importlib, json, sys
from contextlib import redirect_stdout
request = json.loads(sys.stdin.buffer.read())
operation = getattr(importlib.import_module(sys.argv[1]), sys.argv[2])
with redirect_stdout(sys.stderr):
    result = operation(*request['args'], **request['kwargs'])
sys.stdout.write(json.dumps(result))
"""

_INSTALLED_START = """import importlib, json, sys
operation = getattr(importlib.import_module(sys.argv[1]), sys.argv[2])
request = json.loads(sys.argv[3])
result = operation(*request['args'], **request['kwargs'])
if len(sys.argv) > 4:
    with open(sys.argv[4], 'w', encoding='utf8') as receipt:
        json.dump(result, receipt, allow_nan=False)
"""


def operation_command(operation: BackendOperation, *, python=("python3",), streaming=False, ssh=False):
    """Return a command to run the operation in a fresh target interpreter."""
    if operation.portable is not None:
        from kkrunner.client.portable_functions import function_source
        script = function_source(operation.portable, invocation="start" if streaming else "call")
        return (*python, "-u", "-c", script) if streaming else (*python, "-c", script)
    if operation.backend is not None:
        if ssh:
            return _ssh_class_command(operation, python, streaming)
        script = _CLASS_START if streaming else _CLASS_CALL
        args = (operation.backend.__module__, operation.backend.__name__, operation.function)
        return (*python, "-u", "-c", script, *args) if streaming else (*python, "-c", script, *args)
    script = _INSTALLED_START if streaming else _INSTALLED_CALL
    return (*python, "-u", "-c", script, operation.module, operation.function) if streaming else (*python, "-c", script, operation.module, operation.function)


def operation_payload(args, kwargs):
    import json
    return json.dumps({"args": args, "kwargs": kwargs}, allow_nan=False)
