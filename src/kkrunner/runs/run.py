"""One client-side Run lifecycle, with optional durable RunRecord storage.

Backend jobs own execution; Run owns observation completion and application
postprocessing.  RunManager is the sole authority for durable state changes.
"""

from __future__ import annotations

import asyncio
from threading import Event, Lock, Thread

from kkrunner.runs.base import (
    FailurePhase, RunState, SubmissionNotStarted, finish_monitor, wait_for_monitor,
)
from kkrunner.runs.processing import ProcessingPending


class Run:
    """Client-side execution with one completion and processing state machine.

    An ordinary Run is memory-only. A tracked Run uses RunManager to commit
    state; it never independently writes a RunRecord.  The supplied job handles
    raw execution and observation, not client postprocessing or persistence.
    """

    def __init__(
        self, job, *, backend=None, manager=None, record=None,
        process_result=None, callback=None,
    ):
        if (manager is None) != (record is None):
            raise ValueError("RunManager and RunRecord must be supplied together")
        if record is not None and process_result is not None and not record.processing_required:
            raise ValueError("Result processing was not declared in RunRecord")
        self._job = job
        self.backend = backend
        self.manager = manager
        self._record = record
        self._process_result = process_result
        self.callback = callback
        self.result = None
        self._state = RunState.NEW
        self._failure_phase = None
        self._failure_message = None
        self._exception = None
        self._callback_exception = None
        self._pending_exception = None
        self._processing = False
        self._settled = False
        self._started = False
        self._thread = None
        self._done = Event()
        self._lock = Lock()
        self._completion_lock = Lock()
        self._completion_persisted = False
        self._monitor_record_version = None if record is None else record.updated_at

    @classmethod
    def direct(cls, backend, spec, *, process_result=None, callback=None):
        """Create one untracked Run with the same client lifecycle."""
        return cls(backend.create_run(spec), backend=backend,
                   process_result=process_result, callback=callback)

    @property
    def id(self):
        return None if self._record is None else self._record.id

    @property
    def record(self):
        return self._record

    @property
    def spec(self):
        return self._job.spec

    @property
    def backend_id(self):
        return self._job.backend_id

    @property
    def execution(self):
        proposed = getattr(self._job, "execution", None)
        if proposed is not None:
            return proposed
        if self._record is None or self.backend_id is None:
            return None
        return self._record.execution.submitted(self.backend_id)

    @property
    def files(self):
        return getattr(self._job, "files", None)

    @property
    def exception(self):
        return self._exception or self._job.exception

    @property
    def failure_phase(self):
        from kkrunner.runs.manager import _failure_phase
        return _failure_phase(self._failure_phase or getattr(self._job, "failure_phase", None))

    @property
    def failure_message(self):
        return self._failure_message or getattr(self._job, "failure_message", None)

    @property
    def return_code(self):
        return getattr(self._job, "return_code", None)

    @property
    def state(self):
        if self._record is not None and self._record.state.is_terminal:
            return self._record.state
        if self._settled:
            current = self._state
        elif self._processing:
            current = RunState.POSTPROCESSING
        else:
            current = self._job.state
            if current == RunState.FINISHED and self._started:
                if self._process_result is not None or (
                    self._record is not None and self._record.processing_required
                ):
                    current = RunState.POSTPROCESSING
        if self._record is not None:
            if current == RunState.NEW and self._record.state != RunState.NEW:
                return self._record.state
            if current == RunState.UNKNOWN and self._record.state not in (RunState.NEW, RunState.UNKNOWN):
                return self._record.state
        return current

    def add_output_listener(self, callback):
        return self._job.add_output_listener(callback)

    def add_output_error_listener(self, callback):
        return self._job.add_output_error_listener(callback)

    def snapshot_file(self, file, *, destination=None):
        if self.manager is None:
            raise RuntimeError("Snapshot of an untracked Run requires a workspace")
        return self.manager.snapshot_file(self.id, file, destination=destination)

    def _finish(self):
        try:
            outcome = self._job.wait()
            raw_state = self._job.state
            self.result = outcome
            if raw_state == RunState.UNKNOWN:
                self._state = RunState.UNKNOWN
                self._failure_message = getattr(self._job, "failure_message", None)
                return
            if raw_state == RunState.CANCELLED:
                self._state = RunState.CANCELLED
                return
            if raw_state == RunState.FAILED:
                self._state = RunState.FAILED
                self._failure_phase = getattr(self._job, "failure_phase", None) or FailurePhase.EXECUTION
                self._failure_message = getattr(self._job, "failure_message", None)
                return
            if raw_state != RunState.FINISHED:
                raise RuntimeError(f"Backend observer completed in unexpected state {raw_state.value}")

            self._processing = True
            try:
                if self._record is not None and self._record.processing_required:
                    self.result = self.manager._process_outcome(
                        self._record, outcome, self._process_result,
                        files=self.files,
                        execution=self.execution or self._record.execution,
                    )
                elif self._process_result is not None:
                    self.result = self._process_result(outcome)
            except ProcessingPending as exc:
                self.result = None
                self._pending_exception = exc
                self._state = RunState.POSTPROCESSING
                return
            except BaseException as exc:
                self._exception = exc
                self._failure_phase = FailurePhase.POSTPROCESSING
                self._failure_message = str(exc)
                self._state = RunState.FAILED
                return
            finally:
                self._processing = False
            self._state = RunState.FINISHED
        except BaseException as exc:
            self._exception = exc
            try:
                raw_state = self._job.state
            except Exception:
                raw_state = RunState.UNKNOWN
            if raw_state == RunState.UNKNOWN:
                self._state = RunState.UNKNOWN
                self._failure_phase = None
            else:
                self._state = RunState.CANCELLED if raw_state == RunState.CANCELLED else RunState.FAILED
                self._failure_phase = getattr(self._job, "failure_phase", None) or FailurePhase.EXECUTION
            self._failure_message = getattr(self._job, "failure_message", None) or str(exc)
        finally:
            self._settled = True
            if self.manager is not None and self._record.state != RunState.NEW:
                try:
                    self._sync_completion()
                except BaseException as exc:
                    if self._exception is None:
                        self._exception = exc
            if self.manager is not None and self._record.state == RunState.FINISHED:
                if self._record.processing_required and self._state != RunState.FINISHED:
                    # An independent observer completed the processing first.
                    self.result = None
            finish_monitor(self)

    def _start_completion(self):
        self._started = True
        self._thread = Thread(target=self._finish, daemon=True)
        self._thread.start()

    def submit(self):
        if self._started:
            raise RuntimeError("Run already submitted")
        if self.manager is None:
            self._job.submit()
            self._start_completion()
            return

        if self._record.state != RunState.NEW:
            raise RuntimeError(f"Run is already {self._record.state.value}")
        from kkrunner.runs.manager import _UNCHANGED
        resolve = getattr(self.backend, "resolve_submission", None)
        if resolve is not None:
            self._record = resolve(self._record, self.spec)
        self._record = self.manager.reserve(self._record)
        self._monitor_record_version = self._record.updated_at
        try:
            prepare = getattr(self._job, "prepare_submission", None)
            if prepare is not None:
                prepare()
                self._record = self.manager.transition(
                    self._record,
                    execution=self.execution,
                    files=self.files if self.files is not None else _UNCHANGED,
                )
                self._monitor_record_version = self._record.updated_at
            self._job.submit()
            self._start_completion()
        except SubmissionNotStarted as exc:
            self._record = self.manager.complete(
                self._record,
                state=RunState.FAILED,
                message=self.failure_message or str(exc),
                failure_phase=self.failure_phase or FailurePhase.SUBMISSION,
                artifacts_available=False,
            )
            raise
        except BaseException:
            if self.backend_id is not None:
                self._record = self.manager.transition(
                    self._record,
                    state=self._job.state,
                    execution=self.execution,
                    failure_phase=self.failure_phase,
                )
            # Without a backend identity, SUBMITTING remains unresolved.
            raise

        if self.backend_id is None:
            raise RuntimeError("Backend submit returned without a job identity")
        self.manager.register_executor(self.backend, self._record.execution.endpoint)
        with self._completion_lock:
            if not self._completion_persisted:
                self._record = self.manager.transition(
                    self._record,
                    state=self._job.state,
                    execution=self.execution,
                    failure_phase=self.failure_phase,
                    files=self.files if self.files is not None else _UNCHANGED,
                )
                self._monitor_record_version = self._record.updated_at
        self.manager._acknowledge_submission(self.backend, self._record)

    def start(self):
        if self._started:
            return
        start = getattr(self._job, "start", None)
        if start is not None:
            start()
        elif self._job.state == RunState.NEW:
            self._job.submit()
        self._start_completion()
        if self.manager is None:
            return
        with self._completion_lock:
            state = self._job.state
            if not self._completion_persisted and self.backend_id is not None and state != RunState.NEW:
                self._record = self.manager.transition(
                    self._record, state=state, execution=self.execution,
                    failure_phase=self.failure_phase,
                )
                self._monitor_record_version = self._record.updated_at

    def _sync_completion(self):
        if self.manager is None:
            return
        from kkrunner.runs.manager import _NO_ARTIFACTS_FAILURES
        with self._completion_lock:
            if self._completion_persisted:
                return
            latest = self.manager.store.get(self.id)
            if latest.state.is_terminal:
                self._record = latest
                self._completion_persisted = True
                self.manager._acknowledge_result(latest)
                return
            if self._state == RunState.UNKNOWN:
                self._record = self.manager.transition(
                    self._record, state=self._state, execution=self.execution,
                    message=self.failure_message,
                    expected_updated_at=self._monitor_record_version,
                )
                self._completion_persisted = True
                return
            if not self._state.is_terminal:
                return
            phase = self.failure_phase
            if self._state == RunState.FAILED and phase is None:
                phase = FailurePhase.EXECUTION
            self._record = self.manager.complete(
                self._record, state=self._state, execution=self.execution,
                return_code=self.return_code, message=self.failure_message,
                failure_phase=phase,
                artifacts_available=phase not in _NO_ARTIFACTS_FAILURES,
                files=self.files,
            )
            if self._record.state.is_terminal:
                self._completion_persisted = True

    def wait(self):
        if not self._started:
            raise RuntimeError("Run has not started")
        try:
            result = wait_for_monitor(self)
        finally:
            if self.manager is not None and self._record.state != RunState.NEW:
                self._sync_completion()
        if self._pending_exception is not None:
            raise self._pending_exception
        if (
            self._record is not None
            and self._record.state == RunState.FINISHED
            and self._record.processing_required
            and self._state != RunState.FINISHED
        ):
            raise ProcessingPending("Another observer processed this Run")
        return result

    def run(self):
        if not self._started:
            if self._record is None or self._record.state == RunState.NEW:
                self.submit()
            else:
                self.start()
        return self.wait()

    async def run_async(self):
        return await asyncio.to_thread(self.run)

    def refresh(self):
        if self.manager is None:
            raise RuntimeError("Untracked Run has no durable status")
        with self._completion_lock:
            self._record = self.manager.refresh((self._record,))[0]
            return self._record

    def stop_observation(self):
        stop = getattr(self._job, "stop_observation", None)
        if stop is None:
            raise NotImplementedError("Selected backend cannot detach observation")
        return stop()

    def cancel(self):
        try:
            return self._job.cancel()
        finally:
            if self.manager is not None and self._record.state != RunState.NEW:
                with self._completion_lock:
                    if not self._completion_persisted:
                        self._record = self.manager.transition(
                            self._record, state=self._job.state,
                            execution=self.execution,
                            failure_phase=self.failure_phase,
                        )


__all__ = ["Run"]
