"""Shared call lifecycle and output adapters for concrete targets.

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


    async def wait_with_output(self, subscription, enabled):
        """Forward subscription changes on the existing invocation."""
        waiting = asyncio.create_task(self.wait())
        changed = None
        try:
            while not waiting.done():
                changed = asyncio.create_task(subscription.wait_for_change(enabled))
                done, _ = await asyncio.wait(
                    (waiting, changed), return_when=asyncio.FIRST_COMPLETED,
                )
                if waiting in done:
                    break
                enabled = changed.result()
                try:
                    await self.set_output(enabled)
                except (ConnectionError, OSError):
                    # An execution can finish concurrently with a control write.
                    # Its terminal reply, rather than that write, owns the result.
                    break
            return await waiting
        finally:
            tasks = [waiting] + ([] if changed is None else [changed])
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
