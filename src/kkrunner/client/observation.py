"""Client output delivery and ownership of a remote FOLLOW command."""

import asyncio

from kkrunner.remote.stream import StreamProtocolError, read_stream_frames


class OutputSink:
    """Expose a binary sink to a backend without changing global streams."""

    def __init__(self, on_output, kind):
        self._on_output = on_output
        self._kind = kind

    def write(self, data):
        self._on_output(data, self._kind)
        return len(data)

    def flush(self):
        pass


async def follow_framed_command(process, on_output):
    """Read calculation frames and diagnostics, then reap the SSH command.

    Cancelling this coroutine terminates only the transport observer. The
    independent supervisor or scheduler remains responsible for the job.
    """
    diagnostics = bytearray()

    async def drain_diagnostics():
        while data := await process.stderr.read(65536):
            diagnostics.extend(data[:max(0, 65536 - len(diagnostics))])

    async def receive():
        terminal = None
        async for kind, data in read_stream_frames(process.stdout):
            if kind == "terminal":
                terminal = data
            else:
                on_output(data, kind)
        if terminal is None:
            raise StreamProtocolError("Remote stream ended without terminal frame")
        return terminal

    readers = [asyncio.create_task(receive()), asyncio.create_task(drain_diagnostics())]
    waiting = asyncio.create_task(process.wait())
    discarding = None
    protocol_error = None
    try:
        # wait() leaves the child tasks alive when observation is cancelled.
        # Cleanup can then drain/reap the helper and retrieve every exception,
        # without abandoned shield futures reporting errors during detach.
        tasks = (*readers, waiting)
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
        for task in tasks:
            if task in done:
                task.result()
        terminal = readers[0].result()
        code = waiting.result()
        if code:
            diagnostic = diagnostics.decode("utf8", errors="replace").strip()
            raise StreamProtocolError(f"Remote stream helper exited with status {code}: {diagnostic}")
    except StreamProtocolError as exc:
        protocol_error = exc
    finally:
        # Close the observer before cancelling readers so a full pipe cannot
        # hold it alive. Reap even after malformed output or a writer error.
        if not waiting.done():
            if readers[0].done():
                # An output sink or decoder may already have failed. Drain
                # remaining bytes so subprocess.wait() can close its pipes.
                async def discard():
                    while await process.stdout.read(65536):
                        pass
                discarding = asyncio.create_task(discard())
            try:
                process.terminate()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(asyncio.shield(waiting), timeout=2)
            except asyncio.TimeoutError:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
                await waiting
        for task in readers:
            if not task.done():
                task.cancel()
        if discarding is not None:
            discarding.cancel()
        await asyncio.gather(*readers, *((discarding,) if discarding is not None else ()), return_exceptions=True)
    if protocol_error is not None:
        diagnostic = diagnostics.decode("utf8", errors="replace").strip()
        detail = f"; remote transport diagnostic: {diagnostic}" if diagnostic else ""
        raise StreamProtocolError(str(protocol_error) + detail) from protocol_error
    return terminal
