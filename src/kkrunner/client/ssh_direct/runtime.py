"""SSH-specific process mechanics for the shared direct Job.

No Job lifecycle is implemented here. The target's one-shot Python worker
runs without installing kkrunner and writes stream files at the target.
"""

from __future__ import annotations

import asyncio
import json

from kkrunner.runs.base import RunState
from .worker import WORKER_SOURCE


class SshProcessAdapter:
    expose_pid = False
    uncertain_after_launch = True

    def __init__(self, context):
        self.context = context
        self._request = None
        self._stream_writer = None
        self._cancel_unconfirmed = False
        self._detached = False

    @property
    def detached(self):
        return self._detached

    def configure_detached(self, detached):
        self._detached = detached

    def configure_submission(self, request, *, stream_offsets, stream_writer, stream_paths):
        if self._request is not None:
            raise RuntimeError("SSH direct submission already configured")
        if any(offset not in (None, 0) for offset in stream_offsets.values()):
            raise ValueError("Direct SSH streaming requires empty local output streams")
        self._request = request
        self._stream_writer = stream_writer

    def prepare(self, spec):
        if self._request is None:
            raise ValueError("SSH direct job is missing its staged request")

    async def launch(self, spec):
        request, self._request = self._request, None
        request["detached"] = self._detached
        python_command = getattr(self.context.transport, "python_command", ("python3",))
        return await self.context.transport.start(
            (*python_command, "-u", "-c", WORKER_SOURCE, json.dumps(request)),
            stdin=asyncio.subprocess.PIPE,
        )

    async def stop(self, process):
        # Ask the target worker to signal its entire process group. A blind
        # local SSH kill is not sufficient proof that the target was stopped.
        if process.returncode is not None:
            return True
        writer = process.stdin
        if writer is not None and not writer.is_closing():
            try:
                writer.write(b"cancel\n")
                await writer.drain()
                await asyncio.wait_for(process.wait(), timeout=5)
                return process.returncode != 255
            except (BrokenPipeError, ConnectionError, OSError, asyncio.TimeoutError):
                pass
        self._cancel_unconfirmed = True
        if writer is not None and not writer.is_closing():
            writer.close()
        try:
            await asyncio.wait_for(process.wait(), timeout=2)
        except asyncio.TimeoutError:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=2)
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
        return False

    def detached_output_paths(self, spec):
        # Target files are retrieved at terminal completion, not tailed on
        # the client as if they were local files.
        return {}

    def output(self, data, kind):
        if self._stream_writer is None:
            return
        try:
            self._stream_writer(data, kind)
        except Exception as exc:
            # A file delivery problem is not a remote Job outcome.
            return exc
        return None

    def exit_status(self, code):
        if code == 255:
            return (RunState.UNKNOWN, None, "SSH disconnected or remote command returned status 255")
        if self._cancel_unconfirmed:
            return (RunState.UNKNOWN, None, "Unable to confirm target cancellation over SSH")
        return None

    def close(self, process):
        if process is not None and process.stdin is not None and not process.stdin.is_closing():
            process.stdin.close()
