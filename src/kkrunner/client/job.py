"""Generic client handle for a backend execution.

Job owns client submission/observation state and listeners. Its backend binding
owns execution semantics; the transport owns invocation resources. No process
identity, return-code interpretation or backend-mode selection lives here.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import Future
from threading import Event, Lock, Thread

from kkrunner.backends.contracts import SubmissionNotStarted, job_status_from_facts
from kkrunner.client.contracts import BackendExecution
from kkrunner.runs.base import (
    ExecutionOutcome, FailurePhase, OutputObservers, RunState,
    dispatch_cancellation, finish_monitor, start_monitor, wait_for_monitor,
)
from kkrunner.support.async_utils import run_coro_sync as _run_sync


class Job:
    """One client handle for any selected execution backend."""

    def __init__(self, execution: BackendExecution, spec, *, callback=None, output_listener=None):
        self.execution = execution
        self.spec = spec
        self.callback = callback
        self._state = RunState.NEW
        self._lock = Lock()
        self._thread = None
        self._done = Event()
        self._submitted = Event()
        self._submission_error = None
        self._exception = None
        self._callback_exception = None
        self._failure_phase = None
        self._failure_message = None
        self._loop = None
        self._request = None
        self._cancel_requested = Event()
        self._cancel_future = None
        self._cancel_task = None
        self._stop_task = None
        self._observation_stopped = Event()
        self._stream_writer = None
        self._output_observers = OutputObservers(output_listener)
        self._output_error = None
        from kkrunner.files.paths import file_path
        self.result = ExecutionOutcome(
            input_file=file_path(spec.input_file, spec.directory),
            output_file=file_path(spec.output_file, spec.directory),
            error_file=file_path(spec.error_file, spec.directory),
        )
        self._apply_status(execution.snapshot)

    @property
    def ref(self):
        return self.execution.ref

    @property
    def backend_id(self):
        return None if self.ref is None else self.ref.token

    @property
    def observing(self):
        return self._thread is not None

    @property
    def output_error(self):
        """A client delivery error is independent of the backend's outcome."""
        return self._output_error

    @property
    def state(self):
        """Last observed state; inspecting a Job never performs I/O."""
        return self._state

    @property
    def return_code(self):
        return self.result.return_code

    @property
    def failure_phase(self):
        return self._failure_phase

    @property
    def failure_message(self):
        return self._failure_message

    @property
    def exception(self):
        return self._exception

    def status(self):
        return job_status_from_facts(
            self._state, return_code=self.return_code,
            failure_phase=self.failure_phase, message=self.failure_message,
        )

    def refresh_status(self):
        self._apply_status(_run_sync(self.execution.status()))
        return self.status()

    def _apply_status(self, status):
        self._state = status.state
        self.result.return_code = status.return_code
        phase = status.failure_phase
        self._failure_phase = None if phase is None else FailurePhase(phase.value)
        self._failure_message = status.message

    def _observation_lost(self, message):
        self._state = RunState.UNKNOWN
        self._failure_phase = None
        self._failure_message = message

    def add_output_listener(self, callback):
        self._output_observers.add(callback)

    def add_output_error_listener(self, callback):
        self._output_observers.add_error_listener(callback)

    @property
    def requires_stream_retrieval(self):
        return self.execution.requires_stream_retrieval

    def configure_detached(self, detached):
        if type(detached) is not bool:
            raise TypeError("detached must be bool")
        if self._state != RunState.NEW:
            raise RuntimeError("Cannot change execution policy after submission")
        self.execution.configure_detached(detached)

    def configure_observation(self, *, stream_offsets, stream_writer, stream_paths):
        if self._thread is not None:
            raise RuntimeError("Cannot reconfigure a running observer")
        self.execution.configure_observation(stream_offsets=stream_offsets, stream_paths=stream_paths)
        self._stream_writer = stream_writer

    def configure_submission(self, request, **observation):
        if self._state != RunState.NEW or self._request is not None or self.ref is not None:
            raise RuntimeError("Job submission is already configured")
        self.configure_observation(**observation)
        self._request = request

    def _observed_output(self, data, kind):
        if self._stream_writer is not None and self._output_error is None:
            try:
                self._stream_writer(data, kind)
            except Exception as exc:
                self._output_error = exc
        if not self._observation_stopped.is_set():
            self._output_observers.emit_bytes(data, kind)

    def submit(self):
        with self._lock:
            if self._state == RunState.CANCELLED and self._thread is None:
                raise SubmissionNotStarted("Job was cancelled before submission")
            if self._state != RunState.NEW or self._request is None:
                raise RuntimeError("Job is not prepared for submission")
            self._state = RunState.SUBMITTING
            self._thread = Thread(target=self._thread_main, daemon=True)
            try:
                self._thread.start()
            except Exception as exc:
                self._thread = None
                self._state = RunState.FAILED
                self._failure_phase = FailurePhase.SUBMISSION
                self._failure_message = str(exc)
                raise SubmissionNotStarted(str(exc)) from exc
        self._submitted.wait()
        if self._submission_error is not None:
            self._done.wait()
            raise self._submission_error

    async def _monitor(self):
        self._loop = asyncio.get_running_loop()
        try:
            if self._request is not None:
                request, self._request = self._request, None
                try:
                    status = await self.execution.submit(request, self._observed_output)
                except BaseException as exc:
                    self._apply_status(self.execution.snapshot)
                    self._submission_error = exc
                    raise
                self._apply_status(status)
                self._submitted.set()
            if self._cancel_requested.is_set():
                self._ensure_cancel_task()
            if self._observation_stopped.is_set():
                await self.execution.stop_observation()
            try:
                status = await self.execution.observe(self._observed_output)
            except asyncio.CancelledError:
                if not self._observation_stopped.is_set():
                    raise
                self._observation_lost("Observation detached")
                return
            except Exception as exc:
                self._output_error = self._output_error or exc
                try:
                    status = await self.execution.status()
                except Exception:
                    self._observation_lost(str(exc))
                    return
                self._apply_status(status)
                if not self._state.is_terminal:
                    self._observation_lost(str(exc))
                return
            self._apply_status(status)
            if not self._state.is_terminal:
                self._observation_lost("Observation ended without terminal backend status")
        finally:
            try:
                pending = [task for task in (self._cancel_task, self._stop_task) if task is not None]
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)
                try:
                    await self.execution.close()
                except BaseException as exc:
                    self._output_error = self._output_error or exc
                    if not self._state.is_terminal:
                        self._observation_lost(str(exc))
            finally:
                self._loop = None
                self._submitted.set()
                if self._cancel_future is not None and not self._cancel_future.done():
                    self._cancel_future.set_result(None)

    def _thread_main(self):
        try:
            asyncio.run(self._monitor())
        except BaseException as exc:
            self._exception = exc
            if not self._state.is_terminal:
                self._observation_lost(str(exc))
        finally:
            try:
                self._output_observers.finish_bytes()
            except BaseException as exc:
                self._output_error = self._output_error or exc
                if not self._state.is_terminal:
                    self._observation_lost(str(exc))
            finish_monitor(self)

    def _ensure_cancel_task(self):
        if self._cancel_task is not None or self.ref is None:
            return
        self._cancel_task = asyncio.create_task(self.execution.cancel())
        self._cancel_task.add_done_callback(self._cancel_completed)

    def _cancel_completed(self, task):
        if self._cancel_future.done():
            return
        try:
            self._cancel_future.set_result(task.result())
        except BaseException as exc:
            self._cancel_future.set_exception(exc)

    def cancel(self):
        with self._lock:
            if (
                self._cancel_future is None and self._loop is None
                and self.ref is not None and not self._state.is_terminal
            ):
                self._cancel_future = dispatch_cancellation(lambda: _run_sync(self.execution.cancel()))
                return self._cancel_future
            if self._cancel_future is None:
                self._cancel_future = Future()
                self._cancel_future.set_running_or_notify_cancel()
            if self._state.is_terminal:
                if not self._cancel_future.done():
                    self._cancel_future.set_result(None)
                return self._cancel_future
            self._cancel_requested.set()
            if self._state == RunState.NEW:
                self._state = RunState.CANCELLED
                self._cancel_future.set_result(None)
            elif self._loop is not None:
                self._schedule(self._ensure_cancel_task)
            return self._cancel_future

    def _schedule(self, action):
        try:
            self._loop.call_soon_threadsafe(action)
        except (AttributeError, RuntimeError):
            pass  # Observation has already closed its loop.

    def _stop_observation(self):
        if self._stop_task is None:
            self._stop_task = asyncio.create_task(self.execution.stop_observation())

    def stop_observation(self):
        self._observation_stopped.set()
        if self._loop is not None:
            self._schedule(self._stop_observation)

    def start(self):
        if self._thread is None and self.ref is None:
            return self.submit()
        return start_monitor(self)

    def wait(self):
        if self._state == RunState.CANCELLED and self._thread is None:
            return self.result
        return wait_for_monitor(self)

    def run(self):
        if self._thread is None:
            self.start()
        return self.wait()

    async def run_async(self):
        return await asyncio.to_thread(self.run)
