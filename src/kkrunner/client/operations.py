"""Target-independent references to backend operations.

The operation is data, not an object to be reconstructed on the target.
A backend selects its SSH helper; shared file/MPI utilities supply a
standalone function with no closures or module globals.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class BackendOperation:
    """How an endpoint locates one context-independent Python operation."""

    function: str | None = None
    portable: Callable | None = None
    backend: type | None = None

    def __post_init__(self):
        if (self.portable is None) == (self.backend is None):
            raise ValueError("Choose a backend method or a standalone function")
        if self.portable is not None:
            from kkrunner.client.portable_functions import function_definition
            function_definition(self.portable)
        else:
            from kkrunner.backends.class_helper import static_methods
            if self.function not in static_methods(self.backend):
                raise ValueError(f"{self.backend.__name__}.{self.function} is not a static backend operation")

    @classmethod
    def for_backend(cls, backend: type, method: str):
        """Reference one declared static backend method without any instance."""
        return cls(function=method, backend=backend)

    @classmethod
    def standalone(cls, function: Callable):
        return cls(portable=function)


def operation_command(operation: BackendOperation, *, python=("python3",), streaming=False):
    """Build one selected operation and a shared JSON/stream invocation stub."""
    from kkrunner.client.portable_functions import function_definition

    if operation.portable is not None:
        definition = function_definition(operation.portable)
        entrypoint = operation.portable.__name__
    else:
        definition = function_definition(getattr(operation.backend.SshHelper, operation.function), static=True)
        entrypoint = operation.function
    lines = [definition, "import asyncio, inspect, json, sys"]
    if streaming:
        lines += [
            "payload = json.loads(sys.argv[1])",
            f"result = {entrypoint}(*payload['args'], **payload['kwargs'])",
            "if inspect.isawaitable(result): result = asyncio.run(result)",
        ]
        if operation.backend is not None and getattr(operation.backend, "streaming_exit_code", False):
            lines.append("sys.exit(result if result >= 0 else 128 - result)")
    else:
        lines += [
            "from contextlib import redirect_stdout",
            "payload = json.loads(sys.stdin.buffer.read())",
            "with redirect_stdout(sys.stderr):",
            f"    result = {entrypoint}(*payload['args'], **payload['kwargs'])",
            "    if inspect.isawaitable(result): result = asyncio.run(result)",
            "sys.stdout.write(json.dumps(result, allow_nan=False))",
        ]
    script = "\n".join(lines) + "\n"
    return (*python, "-u", "-c", script) if streaming else (*python, "-c", script)


def operation_payload(args, kwargs):
    import json
    return json.dumps({"args": args, "kwargs": kwargs}, allow_nan=False)
