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

    def __post_init__(self):
        installed = self.module is not None or self.function is not None
        if installed == (self.portable is not None):
            raise ValueError("Choose exactly one of installed or portable operation")
        if installed:
            if not self.module or not self.function:
                raise ValueError("Installed operation needs module and function")
            if any(not part.isidentifier() for part in self.module.split(".")):
                raise ValueError("Invalid Python module path")
            if not self.function.isidentifier():
                raise ValueError("Invalid function name")
        else:
            # Validate at declaration time, before any work can be submitted.
            from kkrunner.client.portable_functions import function_source
            function_source(self.portable)

    @classmethod
    def installed(cls, module: str, function: str):
        """Call the named operation by importing it on the target interpreter."""
        return cls(module=module, function=function)

    @classmethod
    def standalone(cls, function: Callable):
        return cls(portable=function)


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


def operation_command(operation: BackendOperation, *, python=("python3",), streaming=False):
    """Return a command to run the operation in a fresh target interpreter."""
    if operation.portable is not None:
        from kkrunner.client.portable_functions import function_source
        script = function_source(operation.portable, invocation="start" if streaming else "call")
        return (*python, "-u", "-c", script) if streaming else (*python, "-c", script)
    script = _INSTALLED_START if streaming else _INSTALLED_CALL
    return (*python, "-u", "-c", script, operation.module, operation.function) if streaming else (*python, "-c", script, operation.module, operation.function)


def operation_payload(args, kwargs):
    import json
    return json.dumps({"args": args, "kwargs": kwargs}, allow_nan=False)
