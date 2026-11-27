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
def _framed_call(function, protocol_factory, observe_only=False, submission=True):
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

        async def invoke():
            sinks = {"stdout": Sink("out"), "stderr": Sink("err")}
            if submission:
                sinks["started"] = started
            return await function(*payload["args"], **payload["kwargs"], **sinks)

        try:
            await write(protocol.magic)
            if observe_only:
                # A passive call receives no input. Readability therefore
                # signals EOF (or invalid input), even if the job is silent.
                disconnected = loop.create_future()
                fd = sys.stdin.fileno()

                def readable():
                    loop.remove_reader(fd)
                    disconnected.set_result(None)

                loop.add_reader(fd, readable)
                running = asyncio.create_task(invoke())
                try:
                    done, _ = await asyncio.wait(
                        (running, disconnected), return_when=asyncio.FIRST_COMPLETED,
                    )
                    if running not in done:
                        if os.read(fd, 1):
                            raise ValueError("Observation-only call received unexpected input")
                        return
                    facts = running.result()
                finally:
                    loop.remove_reader(fd)
                    running.cancel()
                    await asyncio.gather(running, return_exceptions=True)
            else:
                facts = await invoke()
            await write(protocol.encode("terminal", facts))
        finally:
            os.set_blocking(descriptor, blocking)

    asyncio.run(run())

