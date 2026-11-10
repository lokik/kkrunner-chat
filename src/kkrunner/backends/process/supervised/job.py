"""Reconnectable supervisor-backed local Job, using shared agent operations.

The supervisor owns the process; this Job owns only client-side observation.
"""

from __future__ import annotations

import asyncio
import os
from threading import Event, Lock

from kkrunner.files.paths import file_path
from kkrunner.backends.process.supervised.functions import cancel as cancel_remote, follow as follow_remote, submit as submit_remote
from kkrunner.runs.base import (
    ExecutionOutcome, ExecutionSpec, FailurePhase, OutputObservers, RunState,
    close_output_files, finish_monitor, request_cancellation, start_monitor,
    wait_for_monitor,
)
from .local import local_status


def _file_path(value, directory=None):
    # Local project submission flushes file objects before publishing paths.
    return file_path(value, directory, flush=True)


class _ObserverSink:
    """Adapt the supervisor's byte stream to the existing listener contract."""

    def __init__(self, observers, kind):
        self.observers = observers
        self.kind = kind

    def write(self, data):
        self.observers.emit_bytes(data, self.kind)

    def flush(self):
        pass


class SupervisedProcessJob:
    """Managed local process with supervisor-based follow and reattach."""

    def __init__(
        self,
        executor,
        spec: ExecutionSpec,
        *,
        run_id=None,
        callback=None,
        output_listener=None,
        start=False,
        backend_id=None,
        state=RunState.NEW,
        failure_phase=None,
        failure_message=None,
    ):
        self.executor = executor
        self.spec = spec
        self.callback = callback
        self._run_id = run_id
        self._backend_id = backend_id
        self.result = ExecutionOutcome(
            output_file=_file_path(spec.output_file, spec.directory),
            input_file=_file_path(spec.input_file, spec.directory),
            error_file=_file_path(spec.error_file, spec.directory),
        )
        self._state = RunState(state)
        self._failure_phase = failure_phase
        self._failure_message = failure_message
        self._return_code = None
        self._output_observers = OutputObservers(output_listener)
        self._lock = Lock()
        self._done = Event()
        self._thread = None
        self._exception = None
        self._callback_exception = None
        self._cancel_future = None
        self._observation_stopped = Event()
        self._observation_loop = None
        self._observation_task = None
        if start:
            self.start()

    @property
    def backend_id(self):
        return self._backend_id

    @property
    def ref(self):
        from kkrunner.backends.contracts import JobRef
        return None if self.backend_id is None else JobRef("process", self.backend_id)

    def status(self):
        from kkrunner.backends.contracts import job_status_from_facts
        state = self.state
        return job_status_from_facts(
            state, return_code=self.return_code,
            failure_phase=self.failure_phase, message=self.failure_message,
        )

    def _refresh(self):
        # A terminal result is immutable. After acknowledgment the receipt
        # may disappear while the supervisor process is still shutting down.
        if self._backend_id is None or self._state.is_terminal:
            return self._state
        status = local_status(self._backend_id)
        self._state = status.state
        self._failure_phase = status.failure_phase
        self._failure_message = status.message
        self._return_code = status.return_code
        self.result.return_code = self._return_code
        return self._state

    @property
    def state(self):
        if self._observation_stopped.is_set():
            return RunState.UNKNOWN
        return self._refresh() if self._backend_id is not None else self._state

    @property
    def exception(self):
        return self._exception

    @property
    def return_code(self):
        return self._return_code

    @property
    def failure_phase(self):
        return self._failure_phase

    @property
    def failure_message(self):
        return self._failure_message

    def add_output_listener(self, callback):
        self._output_observers.add(callback)

    def add_output_error_listener(self, callback):
        self._output_observers.add_error_listener(callback)

    def _monitor(self):
        try:
            follow_remote(
                self._backend_id,
                _ObserverSink(self._output_observers, "out"),
                _ObserverSink(self._output_observers, "err"),
                stdout_path=self.result.output_file,
                stderr_path=self.result.error_file,
                observation_started=self._observation_started,
            )
        except asyncio.CancelledError:
            if not self._observation_stopped.is_set():
                raise
            self._state = RunState.UNKNOWN
            self._failure_message = "Observation detached"
            return self.result
        except (OSError, ConnectionError, RuntimeError) as exc:
            # The follow channel is observational. A supervisor that has
            # already exited may have no socket, or no replayable spool.
            # Backend outcome is authoritative, never a guessed exit code.
            status = self._refresh()
            if not status.is_terminal:
                self._state = RunState.UNKNOWN
                self._failure_message = str(exc)
                return self.result
        self._refresh()
        return self.result

    def _observation_started(self, loop, task):
        self._observation_loop, self._observation_task = loop, task
        if self._observation_stopped.is_set():
            task.cancel()

    def stop_observation(self):
        """Cancel only this socket reader, retaining the supervisor process."""
        self._observation_stopped.set()
        loop, task = self._observation_loop, self._observation_task
        if loop is not None and task is not None:
            try:
                loop.call_soon_threadsafe(task.cancel)
            except RuntimeError:
                pass

    def _thread_main(self):
        # Monitoring and both cleanups must be attempted independently; a
        # secondary cleanup error must not hide an earlier monitor failure.
        try:
            for action, observation_only in (
                (self._monitor, True),
                (self._output_observers.finish_bytes, False),
                (lambda: close_output_files(self.spec.output_file, self.spec.error_file), False),
            ):
                try:
                    action()
                except BaseException as exc:
                    if self._exception is None:
                        self._exception = exc
                        self._failure_message = str(exc)
                    if observation_only:
                        # FOLLOW or status loss cannot prove a job failed.
                        if self._state not in (RunState.FAILED, RunState.CANCELLED):
                            self._state = RunState.UNKNOWN
                            self._failure_phase = None
                    else:
                        # Failing to finalize local output is an output failure
                        # only if execution was otherwise known to succeed.
                        if self._state == RunState.FINISHED:
                            self._state = RunState.FAILED
                            self._failure_phase = FailurePhase.ARTIFACTS
                        elif not self._state.is_terminal:
                            self._state = RunState.UNKNOWN
                            self._failure_phase = None
        finally:
            finish_monitor(self)

    def _request(self):
        from kkrunner.client.execution_request import execution_request
        return {
            **execution_request(self.spec),
            "executor": "process",
            "id": self._run_id,
            "detached": True,
        }

    def submit(self):
        if self._state != RunState.NEW:
            raise RuntimeError(f"Run is already {self._state.value}")
        if not self._run_id:
            raise RuntimeError("Managed run has no run id")
        from kkrunner.runs.base import SubmissionNotStarted

        # Constructing the request is preflight. Once submit_remote begins,
        # a thrown exception might mean an accepted but unacknowledged job.
        request = self._request()
        self._state = RunState.SUBMITTING
        try:
            reply = submit_remote(request)
        except SubmissionNotStarted as exc:
            self._state = RunState.FAILED
            self._failure_phase = FailurePhase.SUBMISSION
            self._failure_message = str(exc)
            raise
        self._backend_id = reply["remote_id"]
        self._state = RunState(reply["state"])
        start_monitor(self, missing_backend="Local run has no backend identity")

    def start(self):
        if self._backend_id is None:
            return self.submit()
        return start_monitor(self, missing_backend="Local run has no backend identity")

    def wait(self):
        return wait_for_monitor(self)

    def run(self):
        if self._thread is None:
            self.start()
        return self.wait()

    async def run_async(self):
        return await asyncio.to_thread(self.run)

    def cancel(self):
        if self._backend_id is None:
            raise RuntimeError("Run has not been submitted")
        return request_cancellation(self, lambda: cancel_remote(self._backend_id))

