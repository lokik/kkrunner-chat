"""One client-side Run lifecycle, with optional durable RunRecord storage.

Backend jobs own execution; Run owns observation completion and application
postprocessing.  RunManager is the sole authority for durable state changes.
"""

from __future__ import annotations

import asyncio
from threading import Event, Lock, Thread

from kkrunner.backends.contracts import BackendCapability
from kkrunner.runs.base import (
    ExecutionResources,
    ExecutionSpec,
    FailurePhase,
    RunState,
    SubmissionNotStarted,
    finish_monitor,
    wait_for_monitor,
)
from kkrunner.runs.processing import ProcessingPending


class Run:
    """Client-side execution with one completion and processing state machine.

    An ordinary Run is memory-only. A tracked Run uses RunManager to commit
    state; it never independently writes a RunRecord.  The supplied job handles
    raw execution and observation, not client postprocessing or persistence.

    owned_resources transfers additional application resources to this Run.
    They outlive file finalization and postprocessing; uncertain execution or
    pending processing retains them until completion can be confirmed.
    """

    def __init__(
        self, job, *, client=None, manager=None, record=None,
        process_result=None, callback=None, file_session=None, parts=None, owned_resources=None,
    ):
        if (manager is None) != (record is None):
            raise ValueError("RunManager and RunRecord must be supplied together")
        if record is not None and process_result is not None and not record.processing_required:
            raise ValueError("Result processing was not declared in RunRecord")
        if owned_resources is not None and not isinstance(owned_resources, ExecutionResources):
            raise TypeError("owned_resources must be ExecutionResources or None")
        from kkrunner.client.run_parts import RunParts
        if parts is None:
            if file_session is None:
                from kkrunner.client.endpoint import EndpointClient
                client = client if client is not None else EndpointClient()
                file_session = client.file_session(job.spec)
            parts = RunParts(job, file_session)
        self._parts = parts
        self._owned_resources = owned_resources if owned_resources is not None else ExecutionResources()
        self._submitting = False
        self._closed = False
        self._job = parts.job
        self._file_session = parts.files
        self.client = client
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
    def create(
        cls, client, spec, *, persistent=None, detached=None,
        process_result=None, callback=None, owned_resources=None, **record_options,
    ):
        """Create one Run; persistence never selects its execution mechanism."""
        from kkrunner.runs.manager import RunManager

        if persistent is None:
            persistent = client.default_persistent
        if isinstance(persistent, RunManager):
            manager = persistent
        elif persistent is True:
            manager = RunManager.get_default()
        elif persistent is False:
            manager = None
        else:
            raise TypeError("persistent must be None, bool, or a RunManager")

        if detached is None:
            detached = client.default_detached
        if type(detached) is not bool:
            raise TypeError("detached must be None or bool")

        if manager is not None:
            return manager.create_run(
                client, spec, process_result=process_result,
                callback=callback, detached=detached, owned_resources=owned_resources, **record_options,
            )
        if record_options:
            raise ValueError("Record metadata requires a persistent RunManager")
        return cls.untracked(client, spec, process_result=process_result,
                          callback=callback, detached=detached, owned_resources=owned_resources)

    @classmethod
    def untracked(
        cls, client, spec, *, process_result=None, callback=None, detached=None, owned_resources=None,
    ):
        """Create one untracked Run with the same client lifecycle."""
        if isinstance(spec, ExecutionSpec):
            spec.require_complete()
        parts = client.create_run_parts(spec)
        parts.configure_detached(
            client.default_detached if detached is None else detached,
        )
        return cls(parts.job, client=client, parts=parts,
                   process_result=process_result, callback=callback, owned_resources=owned_resources)

    @property
    def id(self):
        return None if self._record is None else self._record.id

    @property
    def record(self):
        return self._record

    @property
    def spec(self):
        return self._file_session.spec

    @property
    def backend_id(self):
        return self._job.backend_id

    @property
    def execution(self):
        proposed = self._file_session.execution(self.backend_id)
        if proposed is None or self._record is None:
            return proposed
        from dataclasses import replace
        return replace(proposed, submission_key=self._record.execution.submission_key)

    @property
    def files(self):
        return self._file_session.files

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
            from kkrunner.runs.states import project_run_state
            current = project_run_state(
                self._job.state, files_pending=self._started,
                processing_pending=self._process_result is not None,
                failure_phase=self._job.failure_phase,
            )
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
            outcome = self._file_session.result
            outcome.return_code = self._job.return_code
            self.result = outcome
            if raw_state.is_terminal:
                self._state = RunState.DOWNLOADING
                try:
                    if raw_state == RunState.FINISHED and getattr(self._job, "output_error", None) is not None:
                        raise self._job.output_error
                    self._file_session.finalize(self._job.status())
                    # Flush and close the client-owned streams *before*
                    # postprocessing can durably publish a finished Run.
                    self._parts.close()
                except Exception as exc:
                    if raw_state == RunState.FINISHED:
                        self._file_session.mark_transfer_failed(str(exc))
                        self._exception = exc
                        self._state = RunState.FAILED
                        self._failure_phase = (FailurePhase.OUTPUT if self._job.output_error is not None else
                                               FailurePhase.ARTIFACTS if self._file_session.is_local else FailurePhase.DOWNLOAD)
                        self._failure_message = str(exc)
                        return
                    # Diagnostic transfer errors cannot replace a failed or
                    # cancelled backend outcome.
                outcome = self._file_session.result
                outcome.return_code = self._job.return_code
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
            if raw_state == RunState.CANCELLED:
                self._state = RunState.CANCELLED
                self._failure_phase = None
            elif raw_state == RunState.FAILED:
                self._state = RunState.FAILED
                self._failure_phase = getattr(self._job, "failure_phase", None) or FailurePhase.EXECUTION
            else:
                # wait() may have failed because observation, output delivery,
                # or a local observer failed. Neither RUNNING nor even a
                # separately confirmed FINISHED job proves this client Run
                # can safely complete after that exception.
                self._state = RunState.UNKNOWN
                self._failure_phase = None
            self._failure_message = getattr(self._job, "failure_message", None) or str(exc)
        finally:
            try:
                self._parts.close()
            except BaseException as exc:
                # File cleanup must never prevent finish_monitor from
                # waking waiters. For a successful Job, an unflushed
                # output is a client transfer failure, not Job failure.
                if self._exception is None:
                    self._exception = exc
                if self._state == RunState.FINISHED:
                    self._state = RunState.FAILED
                    self._failure_phase = (FailurePhase.OUTPUT if self._job.output_error is not None else
                                           FailurePhase.ARTIFACTS if self._file_session.is_local else FailurePhase.DOWNLOAD)
                    self._failure_message = str(exc)
            if self.manager is not None and self._record.state != RunState.NEW:
                try:
                    self._sync_completion()
                except BaseException as exc:
                    if self._exception is None:
                        self._exception = exc
            # The durable record must capture file versions before an owned
            # temporary directory or prepared calculation can be released.
            if self._state.is_terminal and (self.manager is None or self._completion_persisted):
                try:
                    self._owned_resources.close(suppress_errors=self.exception is not None)
                except BaseException as exc:
                    self._exception = exc
            if self.manager is not None and self._record.state == RunState.FINISHED:
                if self._record.processing_required and self._state != RunState.FINISHED:
                    # An independent observer completed the processing first.
                    self.result = None
            self._settled = True
            finish_monitor(self)

    def _start_completion(self):
        self._started = True
        self._thread = Thread(target=self._finish, daemon=True)
        try:
            self._thread.start()
            self._parts.claim()
        except BaseException:
            # A monitor that never started cannot own completion or cleanup.
            # The caller retains the accepted backend identity for recovery.
            self._thread = None
            self._started = False
            raise

    def _observed_submission_state(self):
        """Persist execution progress, never a premature client completion.

        A backend can finish between submit()/start() and the Run monitor
        starting. Backend terminal status is only a raw execution fact:
        output synchronization and optional postprocessing still belong to
        _finish(), which alone may publish the terminal Run outcome.
        """
        state = self._job.state
        if state.is_terminal:
            return RunState.RUNNING
        return state

    def _close_files_after_failed_submission(self):
        """Cleanup must not replace the submission error or skip its record."""
        try:
            self._parts.close()
        except BaseException as cleanup_error:
            import logging
            logging.getLogger(__name__).warning(
                "File cleanup after failed submission also failed: %s", cleanup_error,
            )

    def submit(self):
        """Submit exactly once, whether or not a RunRecord is persisted.

        The irreversible boundary is Job.submit(). File staging precedes it,
        and accepted backend identity is stored before the completion monitor
        starts. A lost reply after that boundary is never retried implicitly.
        """
        with self._lock:
            if self._closed:
                raise RuntimeError("Run is closed")
            if self._settled and isinstance(self._exception, asyncio.CancelledError):
                raise self._exception
            if self._submitting or self._started or self._settled:
                raise RuntimeError("Run already submitted")
            if self._record is not None and self._record.state != RunState.NEW:
                raise RuntimeError(f"Run is already {self._record.state.value}")
            if self._job.state != RunState.NEW:
                raise RuntimeError(f"Job is already {self._job.state.value}")
            self._submitting = True
        try:
            return self._submit()
        finally:
            with self._lock:
                self._submitting = False

    def _submit(self):
        manager = self.manager
        submitted = False
        reserved = False
        try:
            if manager is not None:
                from dataclasses import replace
                directory, files = self._file_session.submission_footprint(self._record.files)
                self._record = replace(
                    self._record, files=files,
                    execution=replace(self._record.execution, directory=directory),
                )
                # Reservation and duplicate-submit checks are atomic at the store.
                self._record = manager.reserve(self._record)
                reserved = True
                self._monitor_record_version = self._record.updated_at

            # Stage using the same session operations for tracked/untracked
            # execution. Only the optional durable snapshot differs.
            if self._job.state == RunState.CANCELLED:
                raise asyncio.CancelledError("Run was cancelled before submission")
            prepared = self._parts.prepare_submission()
            if manager is not None and prepared:
                from kkrunner.runs.manager import _UNCHANGED
                self._record = manager.transition(
                    self._record,
                    execution=self.execution,
                    files=self.files if self.files is not None else _UNCHANGED,
                )
                self._monitor_record_version = self._record.updated_at

            if self._job.state == RunState.CANCELLED:
                raise asyncio.CancelledError("Run was cancelled before submission")
            self._parts.prepare_job()

            submitted = True
            self._job.submit()
            if manager is not None:
                if self.backend_id is None and BackendCapability.REATTACH in self.client.backend.supports:
                    # An attached-only direct Job may have no durable ID.
                    # Every other tracked backend must confirm an identity
                    # before the completion monitor can claim its resources.
                    raise RuntimeError("Backend submit returned without a job identity")
                from kkrunner.runs.manager import _UNCHANGED
                # Commit accepted identity before giving a monitor ownership
                # of streams. Receipt ACK is only safe after that commit.
                self._record = manager.transition(
                    self._record,
                    execution=self.execution,
                    files=self.files if self.files is not None else _UNCHANGED,
                )
                self._monitor_record_version = self._record.updated_at
            self._start_completion()
        except BaseException as exc:
            # A preflight error is provably before Job.submit(). Once the
            # boundary has been crossed, only SubmissionNotStarted proves
            # that nothing launched. Every other failure is uncertain.
            definitely_absent = isinstance(exc, SubmissionNotStarted) or not submitted
            self._close_files_after_failed_submission()
            cancelled = definitely_absent and self._job.state == RunState.CANCELLED
            self._exception = asyncio.CancelledError("Run was cancelled before submission") if cancelled else exc
            self._failure_message = str(self._exception)
            try:
                if definitely_absent:
                    self._state = RunState.CANCELLED if cancelled else RunState.FAILED
                    self._failure_phase = None if cancelled else FailurePhase.SUBMISSION
                    if reserved:
                        self._record = manager.complete(
                            self._record,
                            state=self._state,
                            message=self._failure_message,
                            failure_phase=self._failure_phase,
                            artifacts_available=False,
                        )
                        self._completion_persisted = self._record.state.is_terminal
                else:
                    self._state = RunState.UNKNOWN
                    self._failure_phase = None
                    if manager is not None and self.backend_id is not None:
                        # Preserve unresolved submissions for explicit recovery.
                        self._record = manager.transition(
                            self._record, state=RunState.UNKNOWN,
                            execution=self.execution, failure_phase=None,
                        )
            finally:
                if definitely_absent:
                    self._owned_resources.close(suppress_errors=True)
                self._settled = True
            if cancelled:
                raise self._exception from exc
            raise

        if manager is not None:
            manager.register_client(self.client)
            with self._completion_lock:
                if not self._completion_persisted:
                    from kkrunner.runs.manager import _UNCHANGED
                    self._record = manager.transition(
                        self._record,
                        state=self._observed_submission_state(),
                        execution=self.execution,
                        failure_phase=self.failure_phase,
                        files=self.files if self.files is not None else _UNCHANGED,
                    )
                    self._monitor_record_version = self._record.updated_at
            manager._acknowledge_submission(self.client, self._record)

    def start(self):
        if self._closed:
            raise RuntimeError("Run is closed")
        if self._started:
            return
        if self._settled:
            raise RuntimeError("A failed or uncertain Run cannot be restarted")
        if self._record is not None:
            if self._record.state.is_terminal:
                raise RuntimeError("A completed Run cannot be started again")
            if self._record.execution.backend_id is None:
                # Only recoverable records may be restarted through attach.
                # A direct Job without an ID is observable only by its owner.
                raise RuntimeError(
                    "Run has no persisted backend identity; recover the submission before attaching"
                )
        self._parts.configure_observation()
        start = getattr(self._job, "start", None)
        if start is not None:
            start()
        elif self._job.state == RunState.NEW:
            self._job.submit()
        # Completion claims the file session only after its observer started.
        # A failed monitor start must leave abort() able to close the streams.
        self._start_completion()
        if self.manager is None:
            return
        with self._completion_lock:
            state = self._observed_submission_state()
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
            if self._settled and self._exception is not None:
                raise self._exception
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

    def close(self):
        """Release an unstarted or terminal Run's resources; active work must settle first."""
        with self._lock:
            if self._closed:
                return
            unstarted = not self._started and self._job.state in (RunState.NEW, RunState.CANCELLED)
            completed = (
                self._settled and self.state.is_terminal
                and (self.manager is None or self._record.state.is_terminal)
            )
            if not completed and (self._submitting or not unstarted):
                raise RuntimeError(f"Cannot close resources while Run is {self.state.value}")
            self._closed = True
        from kkrunner.files.streams import _close_distinct
        _close_distinct((self._parts, self._owned_resources))

    def cancel(self):
        try:
            with self._lock:
                result = self._job.cancel()
                cancelled_before_submit = (
                    not self._submitting and not self._started and not self._settled
                    and self._job.state == RunState.CANCELLED
                )
                if cancelled_before_submit:
                    self._state = RunState.CANCELLED
                    self._exception = asyncio.CancelledError("Run was cancelled before submission")
                    self._settled = True
            if cancelled_before_submit:
                self._close_files_after_failed_submission()
                self._owned_resources.close(suppress_errors=True)
            return result
        finally:
            # Cancellation during staging has no accepted execution yet.
            # submit() owns its reservation until it records cancellation or
            # starts the accepted job's completion monitor.
            if self.manager is not None and self._started and self._record.state != RunState.NEW:
                with self._completion_lock:
                    if not self._completion_persisted:
                        self._record = self.manager.transition(
                            self._record, state=self._observed_submission_state(),
                            execution=self.execution,
                            failure_phase=self.failure_phase,
                        )


__all__ = ["Run"]
