"""Local invocation with borrowed streams."""

from __future__ import annotations

import asyncio
from ...calls import _Call, OutputSink


class LocalCall(_Call):
    """Invoke a backend coroutine with borrowed streams on the caller's loop."""

    def __init__(self, function, request, on_output, *, files=None, stdin=None,
                 observe_only=False, output=True, follow_options=None):
        super().__init__()
        self._control = asyncio.StreamReader()
        self._observe_only = observe_only
        self._start(self._invoke(function, request, on_output, files, stdin, output, follow_options))

    async def _invoke(self, function, request, on_output, files, stdin, output, follow_options):
        io = {} if files is None else {"files": files, "stdin": stdin}
        io["control"] = self._control
        if follow_options is None:
            io["started"] = self._started
        else:
            io.update(follow_options)
        if not output:
            io["output"] = False
        return await function(
            request, stdout=OutputSink(on_output, "out"), stderr=OutputSink(on_output, "err"),
            **io,
        )

    def _send_control(self, command):
        if self._task.done():
            raise ConnectionError("Execution call is closed")
        self._control.feed_data(command + b"\n")

    async def cancel(self):
        if self._observe_only:
            raise RuntimeError("Observation-only calls do not accept cancellation commands")
        self._send_control(b"cancel")

    async def set_output(self, enabled):
        self._send_control(b"follow" if enabled else b"unfollow")

    async def close(self):
        self._control.feed_eof()
        if not self._task.done():
            self._task.cancel()
        await asyncio.gather(self._task, return_exceptions=True)
