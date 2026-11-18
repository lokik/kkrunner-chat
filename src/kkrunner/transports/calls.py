"""Shared call lifecycle and output adapters for concrete transports.

Local and SSH call implementations live in their respective implementation
packages. They own invocation resources, not calculation state.
"""

from __future__ import annotations

import asyncio


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


