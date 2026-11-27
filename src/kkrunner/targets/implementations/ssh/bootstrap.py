"""Self-contained entry points delivered as Python source over SSH.

The client sends only these function definitions, not this module's imports.
The remote interpreter requires Python's standard library but not KKRunner.
"""

from kkrunner.support.decorators import portable


@portable
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


@portable
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


@portable
def _framed_call(function, protocol_factory, mode):
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

        # The transport owns stdin in both modes. The direct operation
        # consumes control messages; a passive observer only watches EOF.
        control = asyncio.StreamReader()
        pipe = os.fdopen(os.dup(sys.stdin.fileno()), "rb", buffering=0)
        try:
            transport, _ = await loop.connect_read_pipe(
                lambda: asyncio.StreamReaderProtocol(control), pipe,
            )
        except BaseException:
            pipe.close()
            os.set_blocking(descriptor, blocking)
            raise

        async def invoke():
            options = dict(payload["kwargs"], stdout=Sink("out"), stderr=Sink("err"))
            if mode in ("execute", "submit"):
                options["started"] = started
            if mode != "follow":
                options["control"] = control
            return await function(*payload["args"], **options)

        try:
            await write(protocol.magic)
            if mode == "follow":
                running = asyncio.create_task(invoke())
                disconnected = asyncio.create_task(control.read(1))
                try:
                    done, _ = await asyncio.wait(
                        (running, disconnected), return_when=asyncio.FIRST_COMPLETED,
                    )
                    if running not in done:
                        if disconnected.result():
                            raise ValueError("Observation-only call received unexpected input")
                        return
                    facts = running.result()
                finally:
                    for task in (running, disconnected):
                        task.cancel()
                    await asyncio.gather(running, disconnected, return_exceptions=True)
            else:
                facts = await invoke()
            await write(protocol.encode("terminal", facts))
        finally:
            transport.close()
            os.set_blocking(descriptor, blocking)

    asyncio.run(run())

