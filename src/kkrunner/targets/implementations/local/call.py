"""Local invocation with borrowed streams."""

from __future__ import annotations

import asyncio
from kkrunner.support.growing_file_tail import follow_growing_files
from ...calls import _Call, OutputSink


class LocalCall(_Call):
    """Invoke a backend coroutine with borrowed streams on the caller's loop."""

    def __init__(self, function, request, on_output, *, files=None, stdin=None, tail_paths=None):
        super().__init__()
        self._control = asyncio.StreamReader()
        self._start(self._invoke(function, request, on_output, files, stdin, tail_paths))

    async def _invoke(self, function, request, on_output, files, stdin, tail_paths):
        io = {} if files is None else {"files": files, "stdin": stdin}
        running = asyncio.create_task(function(
            request, stdout=OutputSink(on_output, "out"), stderr=OutputSink(on_output, "err"),
            control=self._control, started=self._started, **io,
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
