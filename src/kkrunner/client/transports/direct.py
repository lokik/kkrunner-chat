"""Connect DirectObservation to the same backend method locally or through SSH.

These channels own client I/O resources. Process execution and process-group
termination belong exclusively to DirectBackend.stream().
"""

from __future__ import annotations

import asyncio
import os
from collections import deque
from dataclasses import replace

from kkrunner.backends.direct import DirectBackend
from kkrunner.client.execution_request import execution_request
from kkrunner.client.operations import BackendOperation
from kkrunner.files.paths import file_path
from kkrunner.files.streams import open_output_streams
from kkrunner.execution import MpiNotFoundWarning, RunState

from .local import LocalTransport


async def prepare_direct_request(transport, backend, request):
    """Invoke native preflight; the channel only owns its transport and sinks."""
    import warnings

    backend_type = backend if isinstance(backend, type) else type(backend)
    prepared = await transport.call(BackendOperation.for_backend(backend_type, "prepare_mpi"), request)
    if prepared["warning"] is not None:
        warnings.warn(prepared["warning"], MpiNotFoundWarning, stacklevel=3)
    request = prepared["request"]
    await transport.call(BackendOperation.for_backend(backend_type, "prepare_target_outputs"), request)
    return request, prepared["use_stdin"]


class _OutputChannel:
    """Bounded byte channel; close discards data and wakes every waiter."""

    def __init__(self):
        self._chunks = deque()
        self._buffer = b""
        self._finished = False
        self._closed = False
        self._changed = asyncio.Event()

    def _wake_waiters(self):
        changed, self._changed = self._changed, asyncio.Event()
        changed.set()

    async def write(self, data):
        while len(self._chunks) >= 8 and not self._closed and not self._finished:
            await self._changed.wait()
        if self._closed or self._finished:
            raise BrokenPipeError("Output channel is closed")
        self._chunks.append(bytes(data))
        self._wake_waiters()

    def flush(self):
        pass

    async def finish(self):
        """Publish EOF after buffered data, without requiring queue capacity."""
        self._finished = True
        self._wake_waiters()

    def close(self):
        self._closed = True
        self._chunks.clear()
        self._buffer = b""
        self._wake_waiters()

    async def read(self, n=-1):
        if n == 0:
            return b""
        chunks = []
        while not self._closed:
            if self._buffer:
                size = len(self._buffer) if n < 0 else min(n, len(self._buffer))
                chunks.append(self._buffer[:size])
                self._buffer = self._buffer[size:]
                if n >= 0:
                    return b"".join(chunks)
            if self._chunks:
                self._buffer = self._chunks.popleft()
                self._wake_waiters()
            elif self._finished:
                break
            else:
                await self._changed.wait()
        return b"" if self._closed else b"".join(chunks)


class _LocalDirectOperation:
    """Own one backend invocation and its bounded client byte channels.

    No worker thread or Python subprocess: the backend task and its readers
    use the Job's existing event loop. Only the backend starts the calculation.
    """

    def __init__(self, backend, request, *, files, stdin):
        self.pid = None
        self.identity = None
        self.returncode = None
        self.stdout = _OutputChannel()
        self.stderr = _OutputChannel()
        self.control = asyncio.StreamReader()
        self._started = asyncio.get_running_loop().create_future()
        self._task = asyncio.create_task(self._execute(backend, request, files, stdin))

    def _on_started(self, pid):
        self.pid = pid
        from kkrunner.support.process_identity import ProcessIdentity
        self.identity = ProcessIdentity.read(pid)
        self._started.set_result(pid)

    async def _execute(self, backend, request, files, stdin):
        try:
            self.returncode = await backend.stream(
                request, stdout=self.stdout, stderr=self.stderr,
                control=self.control, files=files, stdin=stdin, started=self._on_started,
            )
            return self.returncode
        except BaseException as exc:
            if not self._started.done():
                self._started.set_exception(exc)
            raise
        finally:
            await self.stdout.finish()
            await self.stderr.finish()

    async def started(self):
        try:
            await asyncio.shield(self._started)
        except BaseException:
            self._started.cancel()
            self.close_output()
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            raise
        return self

    async def wait(self):
        return await asyncio.shield(self._task)

    def close_output(self):
        self.stdout.close()
        self.stderr.close()

    def cancel(self):
        self.control.feed_data(b"cancel\n")

    def close(self):
        self.close_output()
        self.control.feed_eof()


class LocalDirectChannel:
    """Local stream ownership and a direct call to the portable backend."""

    expose_pid = True
    uncertain_after_launch = False

    def __init__(self, backend=DirectBackend, *, files=None):
        self.backend = backend
        self.files = files
        self._request = None
        self._streams = None
        self.detached = False

    def configure_detached(self, detached):
        self.detached = detached

    def configure_submission(self, request, **observation):
        if self._request is not None:
            raise RuntimeError("Direct submission is already configured")
        self._request = request

    def prepare(self, spec):
        if self.detached and any(
            value is not None and not isinstance(value, (str, os.PathLike))
            for value in (spec.output_file, spec.error_file)
        ):
            raise ValueError("Detached direct output requires a named file or None")

    async def launch(self, spec):
        # Live local handles are explicit arguments; they never enter JSON.
        declaration = replace(spec, **{
            name: file_path(getattr(spec, name), spec.directory)
            for name in ("stdin", "output_file", "error_file")
        })
        request, self._request = self._request, None
        request, use_stdin = await prepare_direct_request(
            LocalTransport(), self.backend, execution_request(declaration) if request is None else request)
        request["detached"] = self.detached
        self._streams = self.files.open_direct_streams() if self.files is not None else open_output_streams(spec)
        files = {kind: stream for kind, stream in (
            ("out", self._streams.output_file), ("err", self._streams.error_file),
        ) if stream is not None}
        stdin = spec.stdin if spec.stdin is not None and not isinstance(spec.stdin, (str, os.PathLike)) else None
        if not use_stdin:
            stdin = None
        return await _LocalDirectOperation(self.backend, request, files=files, stdin=stdin).started()

    async def stop(self, process, *, discard_output=False):
        if discard_output:
            process.close_output()
        process.cancel()
        await process.wait()
        return True

    def detached_output_paths(self, spec):
        return {
            kind: path for kind, value in (("out", spec.output_file), ("err", spec.error_file))
            if (path := file_path(value, spec.directory)) is not None
        } if self.detached else {}

    def output(self, data, kind):
        # Target files were already written by DirectBackend.stream().
        return None

    def exit_status(self, code):
        return None

    def close(self, process):
        if process is not None:
            process.close()
        streams, self._streams = self._streams, None
        if streams is not None and self.files is None:
            streams.close()


class SshDirectChannel:
    """SSH control and local output delivery; no target process implementation."""

    expose_pid = False
    uncertain_after_launch = True

    def __init__(self, context):
        self.context = context
        self._request = None
        self._stream_writer = None
        self._cancel_unconfirmed = False
        self.detached = False

    def configure_detached(self, detached):
        self.detached = detached

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
        request, _ = await prepare_direct_request(self.context.transport, self.context.client.backend, request)
        request["detached"] = self.detached
        return await self.context.transport.start_operation(
            BackendOperation.for_backend(type(self.context.client.backend), "stream"), request,
        )

    async def stop(self, process, *, discard_output=False):
        readers = []
        if discard_output:
            # DirectObservation has already stopped delivery after an error.
            # Keep draining transport pipes while the target handles cancel.
            async def discard(source):
                while await source.read(65536):
                    pass
            readers = [asyncio.create_task(discard(source))
                       for source in (process.stdout, process.stderr) if source is not None]
        try:
            return await self._stop(process)
        finally:
            for task in readers:
                task.cancel()
            await asyncio.gather(*readers, return_exceptions=True)

    async def _stop(self, process):
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
        return {}

    def output(self, data, kind):
        if self._stream_writer is not None:
            try:
                self._stream_writer(data, kind)
            except Exception as exc:
                return exc
        return None

    def exit_status(self, code):
        if code == 255:
            return RunState.UNKNOWN, None, "SSH disconnected or remote command returned status 255"
        if self._cancel_unconfirmed:
            return RunState.UNKNOWN, None, "Unable to confirm target cancellation over SSH"
        return None

    def close(self, process):
        if process is not None and process.stdin is not None and not process.stdin.is_closing():
            process.stdin.close()
