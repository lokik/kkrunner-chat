"""FOLLOW output adaptation, framing and ownership of the SSH observer.

These are observation resources for an existing execution. Closing them
does not cancel the calculation; execution calls have a separate runtime.
"""

import asyncio

from kkrunner.execution import RunState
from kkrunner.remote.stream import StreamProtocolError, read_stream_frames


async def follow_framed_command(process, on_output, *, on_started=None, close_input=False):
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
        accepted = False
        async for kind, data in read_stream_frames(process.stdout):
            if kind == "terminal":
                terminal = data
            elif kind == "started":
                if accepted or on_started is None:
                    raise StreamProtocolError("Unexpected execution-start frame")
                accepted = True
                on_started(data)
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
        if code and not RunState(terminal["state"]).is_terminal:
            diagnostic = diagnostics.decode("utf8", errors="replace").strip()
            raise StreamProtocolError(f"Remote stream helper exited with status {code}: {diagnostic}")
        # A valid terminal backend receipt remains authoritative even if the
        # SSH/helper process subsequently exits unsuccessfully during cleanup.
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
            if close_input and process.stdin is not None:
                process.stdin.close()
                try:
                    await asyncio.wait_for(asyncio.shield(waiting), timeout=2)
                except asyncio.TimeoutError:
                    pass
            if not waiting.done():
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
