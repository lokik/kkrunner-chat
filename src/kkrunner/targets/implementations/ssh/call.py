"""Live SSH streaming invocation and control."""

from __future__ import annotations
import asyncio
from ...calls import _Call
from .follow import follow_framed_command

class SshCall(_Call):
    """Frame delivery and the SSH command's control/cleanup, without exit-code inference."""

    def __init__(self, process, on_output, *, observe_only=False):
        super().__init__()
        self._process = process
        self._observe_only = observe_only
        self._start(follow_framed_command(
            process, on_output, on_started=self._started, close_input=True,
        ))

    async def _send_control(self, command):
        writer = self._process.stdin
        if writer is None or writer.is_closing():
            raise ConnectionError("Execution control channel is closed")
        writer.write(command + b"\n")
        await writer.drain()

    async def cancel(self):
        if self._observe_only:
            raise RuntimeError("Observation-only calls do not accept cancellation commands")
        try:
            await self._send_control(b"cancel")
        except (OSError, ConnectionError):
            await self.close()
            raise

    async def set_output(self, enabled):
        await self._send_control(b"follow" if enabled else b"unfollow")

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
