"""Live operation calls: local coroutines or framed SSH invocations.

These handles own invocation resources, never calculation identity or state
interpretation. The selected backend emits acceptance and completion facts.
"""

from __future__ import annotations

import asyncio

from kkrunner.support.growing_file_tail import follow_growing_files

from .follow import follow_framed_command


class OutputSink:
    """Adapt a binary sink to the caller's output callback."""

    def __init__(self, on_output, kind):
        self._on_output = on_output
        self._kind = kind

    def write(self, data):
        self._on_output(data, self._kind)
        return len(data)

    def flush(self):
        pass


class _Call:
    def __init__(self):
        self._accepted = asyncio.get_running_loop().create_future()
        self._task = None

    def _started(self, facts):
        self._accepted.set_result(facts)

    def _start(self, coroutine):
        self._task = asyncio.create_task(coroutine)
        self._task.add_done_callback(self._completed)

    def _completed(self, task):
        if not self._accepted.done():
            self._accepted.set_result(None)

    async def submission(self):
        """Acceptance facts, or the completed result if launch was rejected."""
        facts = await asyncio.shield(self._accepted)
        return await self.wait() if facts is None else facts

    async def wait(self):
        return await asyncio.shield(self._task)


class LocalCall(_Call):
    """Invoke a backend coroutine with borrowed streams on the caller's loop."""

    def __init__(self, function, request, on_output, *, files=None, stdin=None, tail_paths=None):
        super().__init__()
        self._control = asyncio.StreamReader()
        self._start(self._invoke(function, request, on_output, files, stdin, tail_paths))

    async def _invoke(self, function, request, on_output, files, stdin, tail_paths):
        running = asyncio.create_task(function(
            request, stdout=OutputSink(on_output, "out"), stderr=OutputSink(on_output, "err"),
            control=self._control, started=self._started, files=files, stdin=stdin,
        ))
        tasks = [running]
        if tail_paths:
            async def terminal():
                return running.result() if running.done() else None

            tasks.append(asyncio.create_task(follow_growing_files(
                tail_paths, lambda kind, data: on_output(data, kind), terminal, wakeup=running,
            )))
        try:
            return (await asyncio.gather(*tasks))[0]
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def cancel(self):
        if self._task.done():
            raise ConnectionError("Execution call is closed")
        self._control.feed_data(b"cancel\n")

    async def close(self):
        self._control.feed_eof()
        if not self._task.done():
            self._task.cancel()
        await asyncio.gather(self._task, return_exceptions=True)


class SshCall(_Call):
    """Frame delivery and the SSH command's control/cleanup, without exit-code inference."""

    def __init__(self, process, on_output):
        super().__init__()
        self._process = process
        self._start(follow_framed_command(
            process, on_output, on_started=self._started, close_input=True,
        ))

    async def cancel(self):
        writer = self._process.stdin
        if writer is None or writer.is_closing():
            raise ConnectionError("Execution control channel is closed")
        try:
            writer.write(b"cancel\n")
            await writer.drain()
        except (OSError, ConnectionError):
            await self.close()
            raise

    async def close(self):
        writer = self._process.stdin
        if writer is not None and not writer.is_closing():
            writer.close()
        try:
            await asyncio.wait_for(asyncio.shield(self._task), timeout=3)
        except asyncio.TimeoutError:
            self._task.cancel()
        except Exception:
            pass  # The caller observes the invocation error through wait().
        await asyncio.gather(self._task, return_exceptions=True)
