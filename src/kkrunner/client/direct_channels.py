"""Connect DirectSession to the same backend method locally or through SSH.

These channels own client I/O resources. Process execution and process-group
termination belong exclusively to DirectBackend.stream().
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import replace

from kkrunner.backends.direct import DirectBackend
from kkrunner.backends.support.process import resolve_mpi_options
from kkrunner.client.execution_request import execution_request
from kkrunner.client.operations import BackendOperation
from kkrunner.client.transport import LocalTransport
from kkrunner.files.availability import prepare_target_outputs
from kkrunner.files.paths import file_path
from kkrunner.files.streams import open_output_streams
from kkrunner.runs.base import MpiNotFoundWarning, RunState


async def prepare_direct_request(transport, request):
    """Resolve target MPI and prepare declared files before starting a session."""
    import warnings

    request = dict(request)
    use_stdin = True
    mpi = (request.get("launch") or {}).get("mpi") or {}
    if request.get("mpi_argv"):
        launcher = await transport.call(BackendOperation.standalone(resolve_mpi_options), mpi)
        if launcher:
            request["argv"] = [*launcher, *request["mpi_argv"]]
            if not request.get("mpi_uses_stdin", True):
                request["stdin"] = None
                use_stdin = False
        elif mpi.get("required"):
            raise FileNotFoundError("MPI was requested but no MPI launcher was found")
        elif mpi.get("autodetect") and mpi.get("warning", True):
            warnings.warn("MPI autodetection did not find an MPI launcher; the calculation will run without MPI.",
                          MpiNotFoundWarning, stacklevel=3)
    await transport.call(BackendOperation.standalone(prepare_target_outputs), request)
    return request, use_stdin


class _OutputChannel:
    """Bounded asynchronous sink/reader for a local backend invocation."""

    def __init__(self):
        self._chunks = asyncio.Queue(maxsize=8)
        self._buffer = b""
        self._eof = False
        self._closed = False

    async def write(self, data):
        if not self._closed:
            await self._chunks.put(bytes(data))

    def flush(self):
        pass

    async def finish(self):
        if not self._closed:
            await self._chunks.put(None)

    def close(self):
        # Release pending producers when the session has lost its reader.
        self._closed = True
        while not self._chunks.empty():
            self._chunks.get_nowait()

    async def read(self, n=-1):
        if n == 0:
            return b""
        chunks = []
        while True:
            if self._buffer:
                size = len(self._buffer) if n < 0 else min(n, len(self._buffer))
                chunks.append(self._buffer[:size])
                self._buffer = self._buffer[size:]
                if n >= 0:
                    return b"".join(chunks)
            if self._eof:
                return b"".join(chunks)
            chunk = await self._chunks.get()
            if chunk is None:
                self._eof = True
            else:
                self._buffer = chunk


class _LocalDirectOperation:
    """Own one backend invocation and its bounded client byte channels.

    No worker thread or Python subprocess: the backend task and its readers
    use the Job's existing event loop. Only the backend starts the calculation.
    """

    def __init__(self, backend, request, *, files, stdin):
        self.pid = None
        self.returncode = None
        self.stdout = _OutputChannel()
        self.stderr = _OutputChannel()
        self.control = asyncio.StreamReader()
        self._started = asyncio.get_running_loop().create_future()
        self._task = asyncio.create_task(self._execute(backend, request, files, stdin))

    def _on_started(self, pid):
        self.pid = pid
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

    def __init__(self, backend=DirectBackend):
        self.backend = backend
        self._streams = None
        self.detached = False

    def configure_detached(self, detached):
        self.detached = detached

    def configure_submission(self, request, **observation):
        raise RuntimeError("Local direct execution has no remote staging request")

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
        request, use_stdin = await prepare_direct_request(LocalTransport(), execution_request(declaration))
        request["detached"] = self.detached
        self._streams = open_output_streams(spec)
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
        if streams is not None:
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
        request, _ = await prepare_direct_request(self.context.transport, request)
        request["detached"] = self.detached
        return await self.context.transport.start_operation(
            BackendOperation.for_backend(self.context.endpoint.backend, "stream"), request,
        )

    async def stop(self, process, *, discard_output=False):
        readers = []
        if discard_output:
            # DirectSession has already stopped delivery after an error.
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
