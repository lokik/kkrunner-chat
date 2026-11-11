"""Direct process backend: local process and portable SSH target execution."""
from __future__ import annotations

from typing import Protocol

from kkrunner.runs.base import ExecutionSpec


class DirectProcessAdapter(Protocol):
    """Process mechanism, not a second Job lifecycle or state owner."""

    expose_pid: bool
    uncertain_after_launch: bool
    detached: bool

    def prepare(self, spec: ExecutionSpec) -> None: ...

    async def launch(self, spec: ExecutionSpec): ...

    async def stop(self, process) -> bool: ...

    def output(self, data: bytes, kind: str): ...

    def detached_output_paths(self, spec: ExecutionSpec) -> dict: ...

    def exit_status(self, code: int): ...

    def close(self, process) -> None: ...


from inspect import getsource

from kkrunner.backends.support import resolve_mpi_options
from kkrunner.files.availability import prepare_target_outputs

# One implementation of each target rule; the remaining worker code owns only
# SSH-direct child process streams and cancellation.
WORKER_SOURCE = (
    getsource(resolve_mpi_options) + "\n" + getsource(prepare_target_outputs) + "\n" + r'''
import glob
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
from contextlib import ExitStack

def choose_command(request):
    serial = request["argv"]
    parallel = request.get("mpi_argv")
    mpi = (request.get("launch") or {}).get("mpi") or {}
    if not parallel or not (mpi.get("required") or mpi.get("autodetect") or mpi.get("launcher")):
        return serial, request.get("stdin")
    launcher = resolve_mpi_options(mpi)
    if not launcher:
        if mpi.get("required"):
            raise FileNotFoundError("Required remote MPI launcher is missing")
        return serial, request.get("stdin")
    return list(launcher) + parallel, (request.get("stdin") if request.get("mpi_uses_stdin", True) else None)

def execute(request):
    os.chdir(request["directory"])
    prepare_target_outputs(request)
    argv, stdin_path = choose_command(request)
    with ExitStack() as resources:
        stdin = (resources.enter_context(open(stdin_path, "rb"))
                 if stdin_path else subprocess.DEVNULL)
        stream_files = {}
        for kind, target in (("out", request.get("output_file")),
                             ("err", request.get("error_file"))):
            if target is not None:
                os.makedirs(os.path.dirname(os.path.abspath(target)), exist_ok=True)
                if target not in stream_files:
                    stream_files[target] = resources.enter_context(open(target, "wb", buffering=0))
        detached = bool(request.get("detached", False))
        child = subprocess.Popen(
            argv, cwd=request["directory"], stdin=stdin,
            stdout=(
                stream_files.get(request.get("output_file"), subprocess.DEVNULL)
                if detached else subprocess.PIPE
            ),
            stderr=(
                stream_files.get(request.get("error_file"), subprocess.DEVNULL)
                if detached else subprocess.PIPE
            ),
            start_new_session=True,
        )
        # Keep the control channel alive while the client remains attached:
        # detached execution must still support explicit cancel and wait.
        # Independent child streams make a broken SSH connection harmless.
        stopping = threading.Event()
        def stop():
            if stopping.is_set():
                return
            stopping.set()
            try:
                os.killpg(child.pid, signal.SIGTERM)
            except ProcessLookupError:
                return
            def force_stop():
                # A child may ignore SIGTERM. Kill its entire group after a
                # short grace period, including surviving descendants.
                import time
                time.sleep(1.0)
                try:
                    os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            threading.Thread(target=force_stop, daemon=True).start()
        def control():
            try:
                for line in sys.stdin.buffer:
                    if line.strip() == b"cancel":
                        stop()
                        return
            finally:
                # An unexpected SSH disconnect closes stdin. Without this,
                # the remote calculation could outlive the direct Job.
                if not detached and child.poll() is None:
                    stop()
        threading.Thread(target=control, daemon=True).start()
        stream_lock = threading.Lock()
        def pump(source, destination, kind):
            target = request.get("output_file" if kind == "out" else "error_file")
            while True:
                data = source.read(65536)
                if not data:
                    return
                if target is not None:
                    with stream_lock:
                        stream_files[target].write(data)
                try:
                    destination.write(data)
                    destination.flush()
                except BrokenPipeError:
                    stop()
                    return
        if detached:
            # The child writes directly to its declared files (or DEVNULL).
            # No relay thread may depend on the SSH channel's pipes.
            returncode = child.wait()
        else:
            out = threading.Thread(target=pump, args=(child.stdout, sys.stdout.buffer, "out"))
            err = threading.Thread(target=pump, args=(child.stderr, sys.stderr.buffer, "err"))
            out.start()
            err.start()
            try:
                returncode = child.wait()
            finally:
                out.join()
                err.join()
        code = returncode
        return code if code >= 0 else 128 - code

try:
    sys.exit(execute(json.loads(sys.argv[1])))
except Exception as error:
    sys.stderr.write("SSH direct worker: " + str(error) + "\n")
    sys.exit(127)
'''
)


from kkrunner.backends.support import materialize_local_spec, start_local_process, stop_local_process
from kkrunner.files.availability import prepare_target_outputs
from kkrunner.files.paths import file_path
from kkrunner.files.streams import open_output_streams


class LocalProcessAdapter:
    """Direct subprocess mechanism, separate from common Job lifecycle."""

    expose_pid = True
    uncertain_after_launch = False

    def __init__(self):
        self._streams = None
        self._detached = False

    @property
    def detached(self):
        return self._detached

    def configure_detached(self, detached):
        self._detached = detached

    def configure_submission(self, request, *, stream_offsets, stream_writer, stream_paths):
        raise RuntimeError("Local direct execution has no remote staging request")

    def prepare(self, spec):
        def paths(values):
            return [path for value in values
                    if (path := file_path(value, spec.directory)) is not None]

        prepare_target_outputs({
            "input_file": file_path(spec.input_file, spec.directory),
            "stdin": file_path(spec.stdin, spec.directory),
            "input_files": paths(spec.input_files),
            "output_file": file_path(spec.output_file, spec.directory),
            "error_file": file_path(spec.error_file, spec.directory),
            "client_sync": {
                "output_files": paths(spec.output_files),
                "cache_files": [{"path": path} for path in paths(spec.cache_files)],
            },
        })
        self._streams = open_output_streams(spec)

    async def launch(self, spec):
        # Resolve MPI on the execution host, not while constructing the Job.
        prepared = materialize_local_spec(spec)
        if self._detached:
            import asyncio
            import os
            # No child descriptor may rely on a client pipe. Named streams
            # write directly into files; absent streams go to DEVNULL.
            for value in (spec.output_file, spec.error_file):
                if value is not None and not isinstance(value, (str, os.PathLike)):
                    raise ValueError("Detached direct output requires a named file or None")
            return await start_local_process(
                prepared,
                stdin=asyncio.subprocess.DEVNULL if prepared.stdin is None else prepared.stdin,
                stdout=self._streams.output_file or asyncio.subprocess.DEVNULL,
                stderr=self._streams.error_file or asyncio.subprocess.DEVNULL,
                start_new_session=True,
            )
        return await start_local_process(prepared)

    async def stop(self, process):
        await stop_local_process(process, process_group=self._detached)
        return True

    def detached_output_paths(self, spec):
        if not self._detached:
            return {}
        return {
            kind: path
            for kind, value in (("out", spec.output_file), ("err", spec.error_file))
            if (path := file_path(value, spec.directory)) is not None
        }

    def output(self, data, kind):
        target = (
            self._streams.error_file if kind == "err" else self._streams.output_file
        )
        if target is not None:
            target.write(data)
            target.flush()

    def exit_status(self, code):
        return None  # Common Job interprets ordinary exit codes.

    def close(self, process):
        streams, self._streams = self._streams, None
        if streams is not None:
            streams.close()


import asyncio
import json

from kkrunner.runs.base import RunState


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


from kkrunner.backends.contracts import Backend
from kkrunner.runs.base import ExecutionSpec

class DirectProcessBackend(Backend):
    """Create direct process Jobs, locally or using an internal transport adapter."""

    name = "direct-process"
    supports_result_file_retrieval = False
    supports_tracked_runs = True
    requires_backend_identity = False
    default_persistent = False
    default_detached = False

    def __init__(self, *, adapter_factory=None):
        # The adapter is a private implementation detail of the direct Job session.
        # Local and raw SSH direct both go through this backend factory.
        self._adapter_factory = adapter_factory

    def create_job(self, spec: ExecutionSpec, *, submission_key=None, callback=None):
        spec.require_complete()
        from kkrunner.client.job import Job
        adapter = self._adapter_factory() if self._adapter_factory is not None else None
        return Job.direct(spec, callback=callback, adapter=adapter)



import asyncio
from dataclasses import dataclass
from threading import Event
from typing import Callable

from kkrunner.backends.support import pump_stream
from kkrunner.support.growing_file_tail import follow_growing_files
from kkrunner.runs.base import FailurePhase, RunState


@dataclass(frozen=True)
class DirectOutcome:
    """One observed native process outcome, without client delivery state."""

    state: RunState
    return_code: int | None = None
    failure_phase: FailurePhase | None = None
    message: str | None = None


class DirectSession:
    """Own one direct process session, leaving lifecycle decisions to Job."""

    def __init__(self, adapter):
        self.adapter = adapter
        self.detached = False
        self.pid = None
        self._process = None
        self._loop = None
        self._stop_task = None
        self._cancel_requested = Event()

    def configure_detached(self, detached):
        self.adapter.configure_detached(detached)
        self.detached = detached

    def configure_submission(self, request, *, stream_offsets, stream_writer, stream_paths):
        self.adapter.configure_submission(
            request, stream_offsets=stream_offsets,
            stream_writer=stream_writer, stream_paths=stream_paths,
        )

    def output(self, data, kind):
        return self.adapter.output(data, kind)

    def request_cancel(self):
        """Request termination, even if launch has not entered the event loop."""
        self._cancel_requested.set()
        loop = self._loop
        if loop is not None:
            try:
                loop.call_soon_threadsafe(self._ensure_stop_task)
            except RuntimeError:
                pass

    def _ensure_stop_task(self):
        process = self._process
        if process is None or process.returncode is not None:
            return None
        if self._stop_task is None:
            self._stop_task = asyncio.create_task(self.adapter.stop(process))
        return self._stop_task

    async def execute(
        self, spec, *, on_started: Callable, on_output: Callable,
        on_detached_output: Callable, on_finish: Callable,
    ) -> DirectOutcome:
        """Execute without writing any Job state or emitting Job callbacks."""
        self._loop = asyncio.get_running_loop()
        if self._cancel_requested.is_set():
            try:
                return DirectOutcome(RunState.CANCELLED)
            finally:
                self._loop = None

        try:
            self.adapter.prepare(spec)
            self._process = await self.adapter.launch(spec)
            self.pid = self._process.pid
            on_started(self.pid)

            # Pump before stopping: a full pipe must not deadlock cancellation.
            wait_task = asyncio.create_task(self._process.wait())
            stdout_task = (
                asyncio.create_task(pump_stream(
                    self._process.stdout, lambda data: on_output(data, "out"),
                ))
                if self._process.stdout is not None else None
            )
            stderr_task = (
                asyncio.create_task(pump_stream(
                    self._process.stderr, lambda data: on_output(data, "err"),
                ))
                if self._process.stderr is not None else None
            )
            tasks = (wait_task,) + tuple(
                task for task in (stdout_task, stderr_task) if task is not None
            )
            paths = self.adapter.detached_output_paths(spec) if self.detached else {}
            if paths:
                async def terminal():
                    return wait_task.result() if wait_task.done() else None

                # Detached output is already written by the child; do not
                # send it through adapter.output a second time.
                tasks += (asyncio.create_task(follow_growing_files(
                    paths, on_detached_output, terminal, wakeup=wait_task,
                )),)
            if self._cancel_requested.is_set():
                self._ensure_stop_task()
            try:
                return_code = (await asyncio.gather(*tasks))[0]
            finally:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

            override = self.adapter.exit_status(return_code)
            if override is not None:
                state, phase, message = override
                return DirectOutcome(state, failure_phase=phase, message=message)

            if self._cancel_requested.is_set() and self._stop_task is not None:
                try:
                    confirmed = await self._stop_task
                except Exception as exc:
                    return DirectOutcome(
                        RunState.UNKNOWN, return_code=return_code,
                        message=str(exc),
                    )
                return DirectOutcome(
                    RunState.CANCELLED if confirmed else RunState.UNKNOWN,
                    return_code=return_code,
                    message=None if confirmed else "Unable to confirm direct process cancellation",
                )
            if return_code == 0:
                return DirectOutcome(RunState.FINISHED, return_code=0)
            return DirectOutcome(
                RunState.FAILED, return_code=return_code,
                failure_phase=FailurePhase.EXECUTION,
                message=f"Process exited with status {return_code}",
            )
        except BaseException:
            if (
                self._process is not None
                and self._process.returncode is None
                and (not self.detached or self._cancel_requested.is_set())
            ):
                # A detached child survives observation failure unless
                # cancellation was explicitly requested.
                await self.adapter.stop(self._process)
            raise
        finally:
            try:
                if self._stop_task is not None:
                    await asyncio.gather(self._stop_task, return_exceptions=True)
                on_finish()
            finally:
                try:
                    self.adapter.close(self._process)
                finally:
                    self._process = None
                    self._loop = None



def stream(request):
    """Execute one direct target request using the self-contained source.

    No remote kkrunner installation is needed when source is transported.
    This operation owns no persistent backend state.
    """
    import json
    import subprocess
    import sys
    from kkrunner.backends.direct import WORKER_SOURCE
    return subprocess.call(
        (sys.executable, "-u", "-c", WORKER_SOURCE, json.dumps(request)),
        stdin=sys.stdin.buffer,
        stdout=sys.stdout.buffer,
        stderr=sys.stderr.buffer,
    )
