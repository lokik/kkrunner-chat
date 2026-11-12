"""Remote call protocol: source bootstrap, JSON arguments, replies and errors.

The bootstrap functions are ordinary, self-contained Python. SSH sends their
source alongside the selected operation, without requiring an installed
KKRunner for portable calls. Local calls share only the JSON value contract.
"""

from __future__ import annotations

import json

from kkrunner.backends.contracts import SubmissionNotStarted
from kkrunner.client.contracts import Operation


class RemoteOperationError(RuntimeError):
    """The target ran the operation and reported an application failure."""


def _json_call(function):
    """Target bootstrap for a completed JSON call; stdout is protocol-only."""
    import asyncio
    import inspect
    import json
    import sys
    from contextlib import redirect_stdout

    payload = json.loads(sys.stdin.buffer.read())
    try:
        with redirect_stdout(sys.stderr):
            result = function(*payload["args"], **payload["kwargs"])
            if inspect.isawaitable(result):
                result = asyncio.run(result)
        encoded = json.dumps({"version": 1, "result": result}, allow_nan=False)
    except Exception as exc:
        encoded = json.dumps({"version": 1, "error": {
            "type": type(exc).__name__, "message": str(exc),
            "submission_started": getattr(exc, "submission_started", None),
            "errno": getattr(exc, "errno", None),
        }}, allow_nan=False)
    sys.stdout.write(encoded)


def _stream_call(function):
    """Target bootstrap leaving standard streams available to the operation."""
    import asyncio
    import inspect
    import json
    import sys

    payload = json.loads(sys.argv[1])
    result = function(*payload["args"], **payload["kwargs"])
    if inspect.isawaitable(result):
        result = asyncio.run(result)


def _framed_call(function, protocol_factory):
    """Portable invocation carrying accepted identity, output and backend status."""
    import asyncio
    import json
    import os
    import sys

    payload = json.loads(sys.argv[1])
    protocol = protocol_factory()

    async def run():
        descriptor = sys.stdout.fileno()
        blocking = os.get_blocking(descriptor)
        os.set_blocking(descriptor, False)
        lock = asyncio.Lock()
        loop = asyncio.get_running_loop()

        async def write(data):
            async with lock:
                remaining = memoryview(data)
                while remaining:
                    try:
                        count = os.write(descriptor, remaining)
                    except BlockingIOError:
                        ready = loop.create_future()

                        def writable():
                            loop.remove_writer(descriptor)
                            if not ready.done():
                                ready.set_result(None)

                        loop.add_writer(descriptor, writable)
                        try:
                            await ready
                        finally:
                            loop.remove_writer(descriptor)
                    else:
                        remaining = remaining[count:]

        class Sink:
            def __init__(self, kind):
                self.kind = kind

            async def write(self, data):
                await write(protocol.encode(self.kind, data))
                return len(data)

            def flush(self):
                pass

        async def started(facts):
            await write(protocol.encode("started", facts))

        try:
            await write(protocol.magic)
            facts = await function(
                *payload["args"], **payload["kwargs"],
                stdout=Sink("out"), stderr=Sink("err"), started=started,
            )
            await write(protocol.encode("terminal", facts))
        finally:
            os.set_blocking(descriptor, blocking)

    asyncio.run(run())


def operation_command(operation: Operation, *, python=("python3",), streaming=False, framed=False):
    """Compile the selected source and its invocation protocol into Python argv."""
    from .source import function_definition

    if operation.function is not None:
        definition = function_definition(operation.function)
        entrypoint = operation.function.__name__
    else:
        definition = function_definition(getattr(operation.backend.SshHelper, operation.method), static=True)
        entrypoint = operation.method
    bootstrap = _framed_call if framed else _stream_call if streaming else _json_call
    bootstrap_name = "_kkrunner_invoke" if entrypoint != "_kkrunner_invoke" else "_kkrunner_invoke_"
    options = ""
    if framed:
        from kkrunner.remote.stream import stream_protocol
        protocol_name = "_kkrunner_protocol" if entrypoint != "_kkrunner_protocol" else "_kkrunner_protocol_"
        definition += function_definition(stream_protocol, name=protocol_name)
        options = f", {protocol_name}"
    script = (
        definition + function_definition(bootstrap, name=bootstrap_name)
        + f"{bootstrap_name}({entrypoint}{options})\n"
    )
    return (*python, "-u", "-c", script) if streaming or framed else (*python, "-c", script)


def operation_payload(args, kwargs):
    """Encode arguments using the same JSON contract for both transports."""
    return json.dumps({"args": args, "kwargs": kwargs}, allow_nan=False)


def operation_result(payload):
    """Decode a target reply independently of SSH process/connection failures."""
    try:
        reply = json.loads(payload)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise RemoteOperationError("Invalid backend operation reply") from exc
    if not isinstance(reply, dict) or reply.get("version") != 1:
        raise RemoteOperationError("Invalid backend operation reply")
    if "error" not in reply:
        if "result" not in reply:
            raise RemoteOperationError("Backend operation reply has no result")
        return reply["result"]
    error = reply["error"]
    if (
        "result" in reply or not isinstance(error, dict)
        or not isinstance(error.get("type"), str) or not isinstance(error.get("message"), str)
    ):
        raise RemoteOperationError("Invalid backend operation error")
    message = error["message"]
    if error.get("submission_started") is False:
        raise SubmissionNotStarted(message)
    # Reconstruct only explicitly supported errors; never import a remote type.
    errors = {kind.__name__: kind for kind in (
        ValueError, TypeError, FileNotFoundError, PermissionError, IsADirectoryError,
        NotADirectoryError, FileExistsError, NotImplementedError, OSError,
    )}
    kind = errors.get(error.get("type"))
    if kind is not None:
        if issubclass(kind, OSError):
            raise kind(error.get("errno"), message)
        raise kind(message)
    raise RemoteOperationError(f"{error.get('type', 'RemoteError')}: {message}")
