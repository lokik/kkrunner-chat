"""Remote backend job: submission, observation and cancellation only.

Local streams, file mappings, staging and downloads belong to RemoteFileSession
and are orchestrated by Run. No filesystem transfer lifecycle is stored here.
"""

from __future__ import annotations

import asyncio
import subprocess
from threading import Event, Lock

from kkrunner.runs.base import (
    ExecutionOutcome, FailurePhase, OutputObservers, RunState,
    finish_monitor, request_cancellation, start_monitor, wait_for_monitor,
)
from kkrunner.support.async_utils import run_coro_sync as _run_sync


class RemoteJob:
    """One execution on a remote process or scheduler backend."""

    def __init__(
        self, context, spec, *, callback=None,
        backend_id=None, remote_directory=None, status=None,
        output_listener=None,
    ):
        self.context = context
        self.spec = spec
        self.callback = callback
        self._backend_id = backend_id
        self._remote_directory = remote_directory
        self._state = RunState.NEW
        self._lock = Lock()
        self._thread = None
        self._done = Event()
        self._exception = None
        self._callback_exception = None
        self._failure_phase = None
        self._failure_message = None
        self._return_code = None
        self._cancel_future = None
        self._observation_stopped = Event()
        self._follow_process = None
        self._loop = None
        self._request = None
        self._stream_offsets = None
        self._stream_paths = (None, None)
        self._stream_writer = None
        self._output_observers = OutputObservers(output_listener)
        self._output_error = None
        self.result = ExecutionOutcome()
        if status is not None:
            self._apply_backend_facts(status)

    @property
    def backend_id(self):
        return self._backend_id

    @property
    def ref(self):
        from kkrunner.backends.contracts import JobRef
        return None if self._backend_id is None else JobRef(self.context.backend, self._backend_id)

    @property
    def observing(self):
        """Whether an observation worker has taken over stream ownership."""
        return self._thread is not None

    @property
    def output_error(self):
        """Local stream-delivery error; never evidence of backend failure."""
        return self._output_error

    @property
    def state(self):
        if self._thread is not None:
            return self._state
        if self._backend_id is None or self._state.is_terminal:
            return self._state
        facts = _run_sync(self.context._status(self._remote_directory, self._backend_id))
        self._apply_backend_facts(facts)
        return self._state

    def status(self):
        from kkrunner.backends.contracts import job_status_from_facts
        if self._backend_id is None:
            return job_status_from_facts(self._state)
        facts = _run_sync(self.context._status(self._remote_directory, self._backend_id))
        return job_status_from_facts(
            facts.get("state", "unknown"),
            return_code=facts.get("return_code"),
            failure_phase=facts.get("failure_phase"),
            message=facts.get("error"),
        )

    @property
    def return_code(self):
        return self._return_code

    @property
    def failure_phase(self):
        return self._failure_phase

    @property
    def failure_message(self):
        return self._failure_message

    @property
    def exception(self):
        return self._exception

    def add_output_listener(self, callback):
        self._output_observers.add(callback)

    def add_output_error_listener(self, callback):
        self._output_observers.add_error_listener(callback)

    def _apply_backend_facts(self, facts):
        self._state = RunState(facts.get("state", "unknown"))
        self._return_code = facts.get("return_code")
        self.result.return_code = self._return_code
        phase = facts.get("failure_phase")
        self._failure_phase = None if phase is None else FailurePhase(phase)
        self._failure_message = facts.get("error")

    def configure_submission(self, request, *, stream_offsets, stream_writer, stream_paths):
        if self._request is not None or self._backend_id is not None:
            raise RuntimeError("Job submission is already configured")
        self.configure_observation(
            stream_offsets=stream_offsets, stream_writer=stream_writer,
            stream_paths=stream_paths,
        )
        # Publish the request only after all local observation configuration
        # succeeded. A pre-submit exception must not leave a submit-ready Job.
        self._request = request

    def configure_observation(self, *, stream_offsets, stream_writer, stream_paths):
        if self._thread is not None:
            raise RuntimeError("Cannot reconfigure a running observer")
        self._stream_offsets = dict(stream_offsets)
        self._stream_writer = stream_writer
        self._stream_paths = stream_paths

    def submit(self):
        with self._lock:
            if self._state != RunState.NEW or self._request is None:
                raise RuntimeError("Remote job is not prepared for submission")
            request = self._request
            self._request = None
            self._state = RunState.SUBMITTING
            self._remote_directory = request["directory"]
            try:
                facts = _run_sync(self.context._submit(request))
            except BaseException:
                # An accepted submission may have lost its reply.
                self._state = RunState.UNKNOWN
                raise
            remote_id = facts.get("remote_id")
            if not remote_id:
                self._state = RunState.UNKNOWN
                raise RuntimeError("Backend submit returned without job identity")
            self._backend_id = str(remote_id)
            self._apply_backend_facts(facts)
        self.start()

    async def _pump(self, source, kind):
        while True:
            data = await source.read(65536)
            if not data:
                return
            if self._stream_writer is not None:
                self._stream_writer(data, kind)
            self._output_observers.emit_bytes(data, kind)

    async def _monitor(self):
        self._loop = asyncio.get_running_loop()
        stdout_task = stderr_task = None
        try:
            if self._observation_stopped.is_set():
                self._state = RunState.UNKNOWN
                return self.result
            try:
                self._follow_process = await self.context._follow(
                    self._remote_directory, self._backend_id,
                    offsets=self._stream_offsets,
                    stdout_path=self._stream_paths[0],
                    stderr_path=self._stream_paths[1],
                )
            except (OSError, ConnectionError, subprocess.SubprocessError, RuntimeError, ValueError) as exc:
                self._state = RunState.UNKNOWN
                self._failure_phase = None
                self._failure_message = str(exc)
                return self.result
            if self._observation_stopped.is_set():
                self._terminate_follow()
            stdout_task = asyncio.create_task(self._pump(self._follow_process.stdout, "out"))
            stderr_task = asyncio.create_task(self._pump(self._follow_process.stderr, "err"))
            stream_error = None
            try:
                await asyncio.gather(self._follow_process.wait(), stdout_task, stderr_task)
            except Exception as exc:
                stream_error = exc
                self._output_error = exc
                # A failed local writer must not leave a FOLLOW subprocess
                # alive while the client queries the authoritative job state.
                self._terminate_follow()
            if self._observation_stopped.is_set():
                self._state = RunState.UNKNOWN
                self._failure_phase = None
                self._failure_message = "Observation detached"
                return self.result
            facts = self._follow_process.terminal
            if facts is None:
                try:
                    facts = await self.context._status(self._remote_directory, self._backend_id)
                except (OSError, ConnectionError, subprocess.SubprocessError, RuntimeError, ValueError) as exc:
                    self._state = RunState.UNKNOWN
                    self._failure_phase = None
                    self._failure_message = str(stream_error or exc)
                    return self.result
            self._apply_backend_facts(facts)
            if not self._state.is_terminal:
                self._state = RunState.UNKNOWN
                self._failure_phase = None
                self._failure_message = str(stream_error or "Output follow ended without terminal backend status")
            return self.result
        except BaseException as exc:
            if self._observation_stopped.is_set():
                self._state = RunState.UNKNOWN
                self._failure_phase = None
                self._failure_message = "Observation detached"
                return self.result
            self._state = RunState.UNKNOWN
            self._failure_phase = None
            self._failure_message = self._failure_message or str(exc)
            self._exception = exc
            raise
        finally:
            for task in (stdout_task, stderr_task):
                if task is not None and not task.done():
                    task.cancel()
            tasks = tuple(task for task in (stdout_task, stderr_task) if task is not None)
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            self._loop = None

    def _terminate_follow(self):
        if self._follow_process is not None:
            try:
                self._follow_process.terminate()
            except (ProcessLookupError, RuntimeError):
                pass

    def stop_observation(self):
        self._observation_stopped.set()
        if self._loop is not None:
            try:
                self._loop.call_soon_threadsafe(self._terminate_follow)
            except RuntimeError:
                pass

    def _thread_main(self):
        try:
            asyncio.run(self._monitor())
        except BaseException as exc:
            self._exception = exc
        finally:
            try:
                self._output_observers.finish_bytes()
            except BaseException as exc:
                if self._output_error is None:
                    self._output_error = exc
            finally:
                finish_monitor(self)

    def start(self):
        return start_monitor(self, missing_backend="Remote Job has no backend identity")

    def wait(self):
        return wait_for_monitor(self)

    def run(self):
        if self._thread is None:
            if self._backend_id is None:
                self.submit()
            else:
                self.start()
        return self.wait()

    async def run_async(self):
        return await asyncio.to_thread(self.run)

    def cancel(self):
        if self._backend_id is None:
            raise RuntimeError("Job has no confirmed backend identity")
        return request_cancellation(
            self,
            lambda: _run_sync(self.context._cancel(self._remote_directory, self._backend_id)),
        )
