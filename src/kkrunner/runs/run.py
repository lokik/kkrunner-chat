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

    preprocess is an optional zero-argument client callback, invoked before
    staging or backend submission. Tracked runs reserve their file footprint
    before invoking it; untracked runs do not participate in conflict checking.
    Its writes must be declared as writable inputs or outputs in ExecutionSpec.

    owned_resources transfers additional application resources to this Run.
    They outlive file finalization and postprocessing. When the client stops
    observing without confirmed completion, it closes handles without deleting
    files potentially still needed by the backend or a future observer.
    """

    def __init__(
        self, parts, *, client, manager=None, record=None,
        process_result=None, callback=None, owned_resources=None,
        preprocess=None, detached=False, observation_lock=None,
    ):
        if (manager is None) != (record is None):
            raise ValueError("RunManager and RunRecord must be supplied together")
        if record is not None and process_result is not None and not record.processing_required:
            raise ValueError("Result processing was not declared in RunRecord")
        if owned_resources is not None and not isinstance(owned_resources, ExecutionResources):
            raise TypeError("owned_resources must be ExecutionResources or None")
        self._parts = parts
        self._owned_resources = owned_resources
        self._submitting = False
        self._closed = False
        self._job = parts.job
        self._file_session = parts.files
        self.client = client
        self.detached = detached if record is None else record.detached
        self.manager = manager
        self._record = record
        if preprocess is not None and not callable(preprocess):
            raise TypeError("preprocess must be callable")
        self._preprocess = preprocess
        self._process_result = process_result
        self.callback = callback
        self.result = None
        self._state = RunState.NEW
        self._failure_phase = None
        self._failure_message = None
        self._exception = None
        self._callback_exception = None
        self._pending_exception = None
        self._settled = False
        self._started = False
        self._thread = None
        self._done = Event()
        self._lock = Lock()
        self._completion_lock = Lock()
        self._completion_persisted = False
        self._observation_lock = observation_lock
        self._monitor_record_version = None if record is None else record.updated_at

    @classmethod
    def create(
        cls, spec, *, target=None, backend=None, tracked=False, detached=None, process_result=None,
        callback=None, output_listener=None, owned_resources=None, preprocess=None,
        metadata=None, processing_required=None, run_id=None,
    ):
        """Create an unstarted Run using a new execution client."""
        from kkrunner.client.execution_client import ExecutionClient

        return cls._create(
            ExecutionClient(backend, target=target), spec,
            tracked=tracked, detached=detached, process_result=process_result,
            callback=callback, output_listener=output_listener, owned_resources=owned_resources,
            preprocess=preprocess, metadata=metadata, processing_required=processing_required,
            run_id=run_id,
        )

    @classmethod
    def _create(
        cls, client, spec, *, tracked=False, detached=None, process_result=None,
        callback=None, output_listener=None, owned_resources=None, preprocess=None,
        metadata=None, processing_required=None, run_id=None,
    ):
        """Assemble a new Run through one path, with optional tracking."""
        from uuid import uuid4

        from kkrunner.runs.base import ExecutionSpec
        from kkrunner.runs.manager import RunManager
        from kkrunner.targets import require_persistable_target

        if not isinstance(spec, ExecutionSpec):
            spec = ExecutionSpec(spec)
        spec.require_complete()

        if isinstance(tracked, RunManager):
            manager = tracked
        elif tracked is True:
            manager = RunManager.get_default()
        elif tracked is False:
            manager = None
        else:
            raise TypeError("tracked must be bool or a RunManager")

        if detached is None:
            detached = client.default_detached
        if type(detached) is not bool:
            raise TypeError("detached must be None or bool")

        if manager is None:
            if metadata is not None or processing_required is not None or run_id is not None:
                raise ValueError("Record metadata requires a RunManager")
            parts = client.create_run_parts(spec)
            record = None
        else:
            if manager.store is None:
                raise ValueError("A tracked Run requires a RunManager with a store")
            if processing_required is False and process_result is not None:
                raise ValueError("A result callback requires processing_required=True")
            config = require_persistable_target(client.target)
            submission_key = uuid4().hex
            record = manager.new_record(
                metadata=metadata,
                processing_required=process_result is not None if processing_required is None else processing_required,
                directory=spec.directory,
                target=config,
                backend=client.backend_config,
                remote_directory=None if config.is_local else client.submission_directory(spec),
                run_id=run_id,
                submission_key=submission_key,
                detached=detached,
            )
            from dataclasses import replace

            parts = client.create_run_parts(spec, submission_key=submission_key)
            record = replace(record, files=parts.files.files)
            manager.register_client(client)

        parts.configure_detached(detached)
        if output_listener is not None:
            parts.job.add_output_listener(output_listener)
        return cls(
            parts, client=client, manager=manager, record=record,
            process_result=process_result, callback=callback, detached=detached,
            owned_resources=owned_resources, preprocess=preprocess,
        )

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
        from kkrunner.runs.model import ExecutionMetadata
        if self.backend_id is None and self._file_session.remote_directory is None:
            return None
        return ExecutionMetadata(
            target=self.client.target,
            directory=self._file_session.working_directory,
            backend=self.client.backend_config,
            backend_id=self.backend_id,
            submission_key=None if self._record is None else self._record.execution.submission_key,
        )

    @property
    def files(self):
        return self._file_session.files

    @property
    def exception(self):
        return self._exception or self._job.exception

    @property
    def failure_phase(self):
        from kkrunner.runs.manager import _failure_phase
        if self._record is not None and self._record.state.is_terminal:
            return self._record.failure_phase
        return _failure_phase(self._failure_phase or self._job.failure_phase)

    @property
    def failure_message(self):
        return self._failure_message or self._job.failure_message

    @property
    def return_code(self):
        return self._job.return_code

    @property
    def state(self):
        if self._record is not None and self._record.state.is_terminal:
            return self._record.state
        if self._settled:
            current = self._state
        elif self._state == RunState.PREPROCESSING:
            current = RunState.PREPROCESSING
        elif self._submitting and not self._started:
            current = self._record.state if self._record is not None else RunState.SUBMITTING
        elif self._state == RunState.POSTPROCESSING:
            current = RunState.POSTPROCESSING
        elif self._started and self._job.state.is_terminal:
            # The native job has ended, but client finalization is not published.
            current = RunState.DOWNLOADING
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
        return self.manager.snapshot_file(self._record, file, destination=destination)

    def _file_failure_phase(self):
        if self._job.output_error is not None:
            return FailurePhase.OUTPUT
        return FailurePhase.ARTIFACTS if self._file_session.is_local else FailurePhase.DOWNLOAD

    def _finish(self):
        try:
            self._job.wait()
            raw_state = self._job.state
            outcome = self._file_session.result
            outcome.return_code = self._job.return_code
            self.result = outcome
            if raw_state.is_terminal:
                self._state = RunState.DOWNLOADING
                try:
                    if (
                        raw_state == RunState.FINISHED and self._job.return_code == 0
                        and self._job.output_error is not None
                    ):
                        raise self._job.output_error
                    if raw_state == RunState.FINISHED and self._job.return_code is None:
                        # The original SSH observer cannot guarantee that it
                        # delivered every byte. Re-read target-owned streams.
                        if not self._file_session.is_local:
                            self._file_session.configure_stream_retrieval(True)
                    # FileSession finalizes and closes its streams before processing.
                    self._file_session.finalize(self._job.status())
                except Exception as exc:
                    if raw_state == RunState.FINISHED:
                        self._file_session.mark_transfer_failed(str(exc))
                        self._exception = exc
                        self._state = RunState.FAILED
                        self._failure_phase = self._file_failure_phase()
                        self._failure_message = str(exc)
                        return
                    # Diagnostic transfer errors cannot replace a failed or
                    # cancelled backend outcome.
                outcome = self._file_session.result
                outcome.return_code = self._job.return_code
            self.result = outcome
            if raw_state == RunState.UNKNOWN:
                self._state = RunState.UNKNOWN
                self._failure_message = self._job.failure_message
                return
            if raw_state == RunState.CANCELLED:
                self._state = RunState.CANCELLED
                return
            if raw_state == RunState.FAILED:
                self._state = RunState.FAILED
                self._failure_phase = self._job.failure_phase or FailurePhase.EXECUTION
                self._failure_message = self._job.failure_message
                return
            if raw_state != RunState.FINISHED:
                raise RuntimeError(f"Backend observer completed in unexpected state {raw_state.value}")

            # An unobserved exit is terminal, but not proof of successful
            # execution. Preserve results without running success-only parsers.
            if self._job.return_code is None:
                self._state = RunState.FINISHED
                self._failure_message = (
                    self._job.failure_message
                    or "Process ended; exit code unavailable"
                )
                return
            self._state = RunState.POSTPROCESSING
            try:
                if self._record is not None and self._record.processing_required:
                    self._record = self.manager.begin_processing(
                        self._record, return_code=outcome.return_code, files=self.files,
                        execution=self.execution or self._record.execution,
                    )
                    self.result = self.manager.process_outcome(self._record, outcome, self._process_result)
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
                self._failure_phase = self._job.failure_phase or FailurePhase.EXECUTION
            else:
                # wait() may have failed because observation, output delivery,
                # or a local observer failed. Neither RUNNING nor even a
                # separately confirmed FINISHED job proves this client Run
                # can safely complete after that exception.
                self._state = RunState.UNKNOWN
                self._failure_phase = None
            self._failure_message = self._job.failure_message or str(exc)
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
                    self._failure_phase = self._file_failure_phase()
                    self._failure_message = str(exc)
            try:
                if self.manager is not None:
                    if self._record.state != RunState.NEW:
                        self._sync_completion()
                else:
                    self._release_resources(
                        preserve=not self._state.is_terminal,
                        suppress_errors=self.exception is not None or not self._state.is_terminal,
                    )
            except BaseException as exc:
                if self._exception is None:
                    self._exception = exc
                if self.manager is None:
                    self._state = RunState.FAILED
                    self._failure_phase = FailurePhase.ARTIFACTS
                    self._failure_message = str(exc)
            finally:
                # A nonterminal observation or a failed persistence operation
                # may leave resources not yet consumed by completion.
                if self._owned_resources is not None:
                    self._release_resources(preserve=True, suppress_errors=True)
            if self.manager is not None and self._record.state == RunState.FINISHED:
                if self._record.processing_required and self._state != RunState.FINISHED:
                    # An independent observer completed the processing first.
                    self.result = None
            try:
                self._release_observation()
            finally:
                self._settled = True
                finish_monitor(self)

    def _start_completion(self):
        self._started = True
        self._thread = Thread(target=self._finish, daemon=True)
        try:
            self._thread.start()
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

    def submit(self, *, validate_inputs=None):
        """Submit exactly once, whether or not a RunRecord is persisted.

        The irreversible boundary is Job.submit(). File staging precedes it,
        and accepted backend identity is stored before the completion monitor
        starts. A lost reply after that boundary is never retried implicitly.
        An optional validate_inputs callback receives the staged FileManifest
        immediately before Job.submit().
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
            return self._submit(validate_inputs)
        finally:
            with self._lock:
                self._submitting = False

    def _submit(self, validate_inputs=None):
        manager = self.manager
        submitted = False
        reserved = False
        try:
            if manager is not None:
                from dataclasses import replace
                directory, files = self._file_session.submission_footprint()
                self._record = replace(
                    self._record, files=files,
                    execution=replace(self._record.execution, directory=directory),
                )
                # Keep staging ownership from the instant the reservation becomes visible.
                self._claim_observation()
                # Reservation and duplicate-submit checks are atomic at the store.
                self._record = manager.reserve(self._record, preprocessing=True, recover_conflicts=True)
                reserved = True
                self._monitor_record_version = self._record.updated_at
            self._state = RunState.PREPROCESSING
            if self._job.state == RunState.CANCELLED:
                raise asyncio.CancelledError("Run was cancelled before preprocessing")
            if self._preprocess is not None:
                self._preprocess()
            if self._job.state == RunState.CANCELLED:
                raise asyncio.CancelledError("Run was cancelled during preprocessing")
            # Stage using the same session operations for tracked/untracked
            # execution. Only the optional durable snapshot differs.
            self._file_session.prepare_submission()
            if manager is not None:

                self._record = manager.transition(
                    self._record,
                    execution=self.execution,
                    files=self.files,
                )
                self._monitor_record_version = self._record.updated_at

            if self._job.state == RunState.CANCELLED:
                raise asyncio.CancelledError("Run was cancelled before submission")
            self._parts.prepare_job()
            if validate_inputs is not None:
                validate_inputs(self.files)
                if self._job.state == RunState.CANCELLED:
                    raise asyncio.CancelledError("Run was cancelled before submission")

            if manager is not None:
                self._record = manager.begin_submission(self._record)
                self._monitor_record_version = self._record.updated_at
            self._state = RunState.SUBMITTING
            submitted = True
            self._job.submit()
            if manager is not None:
                if self.backend_id is None and BackendCapability.REATTACH in self.client.backend.supports:
                    # An attached-only direct Job may have no durable ID.
                    # Every other tracked backend must confirm an identity
                    # before the completion monitor can claim its resources.
                    raise RuntimeError("Backend submit returned without a job identity")

                # Commit accepted identity before giving a monitor ownership
                # of streams. Receipt ACK is only safe after that commit.
                self._record = manager.transition(
                    self._record,
                    execution=self.execution,
                    files=self.files,
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
                    phase = (FailurePhase.PREPROCESSING
                             if self._state == RunState.PREPROCESSING
                             else FailurePhase.SUBMISSION)
                    self._state = RunState.CANCELLED if cancelled else RunState.FAILED
                    self._failure_phase = None if cancelled else phase
                    if reserved:
                        self._complete_record(
                            self._record, state=self._state,
                            message=self._failure_message, failure_phase=self._failure_phase,
                        )
                        self._completion_persisted = self._record.state.is_terminal
                    else:
                        self._release_resources(suppress_errors=True)
                else:
                    self._state = RunState.UNKNOWN
                    self._failure_phase = None
                    self._release_resources(preserve=True, suppress_errors=True)
                    if manager is not None and self.backend_id is not None:
                        # Preserve unresolved submissions for explicit recovery.
                        self._record = manager.transition(
                            self._record, state=RunState.UNKNOWN,
                            execution=self.execution, failure_phase=None,
                        )
            finally:
                if not self._started:
                    self._release_observation()
                self._settled = True
            if cancelled:
                raise self._exception from exc
            raise

        if manager is not None:
            with self._completion_lock:
                if not self._completion_persisted:

                    self._record = manager.transition(
                        self._record,
                        state=self._observed_submission_state(),
                        execution=self.execution,
                        failure_phase=self.failure_phase,
                        files=self.files,
                    )
                    self._monitor_record_version = self._record.updated_at
            manager._acknowledge_submission(self._record, client=self.client)

    def _claim_observation(self):
        if self.manager is not None and self._observation_lock is None:
            self._observation_lock = self.manager._claim_client_files(self._record)

    def _release_observation(self):
        lock = self._observation_lock
        self._observation_lock = None
        if lock is not None:
            lock.release()

    def start(self):
        if self._closed:
            raise RuntimeError("Run is closed")
        if self._started:
            return
        if self._record is not None and self._record.state == RunState.NEW:
            return self.submit()
        try:
            if self._settled:
                raise RuntimeError("A failed or uncertain Run cannot be restarted")
            if self._record is not None:
                if self._record.state.is_terminal:
                    raise RuntimeError("A completed Run cannot be started again")
                if self._record.execution.backend_id is None:
                    raise RuntimeError(
                        "Run has no persisted backend identity; recover the submission before attaching"
                    )
            self._claim_observation()
            self._parts.configure_observation()
            self._job.start()
            self._start_completion()
        except BaseException:
            if not self._started:
                try:
                    self._parts.close()
                finally:
                    try:
                        self._release_resources(preserve=True, suppress_errors=True)
                    finally:
                        self._release_observation()
            raise
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

    def _release_resources(self, *, preserve=False, suppress_errors=False):
        """Consume this Run's application resource ownership."""
        resources, self._owned_resources = self._owned_resources, None
        if resources is not None:
            resources.close(suppress_errors=suppress_errors, detach=preserve)

    def _complete_record(self, record, **kwargs):
        """Capture evidence, close resources, then publish the terminal record."""
        manager = self.manager
        expected = record if record.state == RunState.POSTPROCESSING else None
        try:
            fields = manager.completion_fields(record, **kwargs)
        except BaseException:
            self._release_resources(preserve=True, suppress_errors=True)
            raise

        # A durable record must remain recoverable even if the client dies
        # between resource release and publication. Only ephemeral execution
        # may delete its working files at this boundary.
        preserve = (manager._store_for(record).durable
                    or fields["failure_phase"] in (FailurePhase.DOWNLOAD, FailurePhase.POSTPROCESSING))
        cleanup_error = None
        try:
            self._release_resources(preserve=preserve, suppress_errors=fields["state"] != RunState.FINISHED)
        except BaseException as exc:
            cleanup_error = exc
            fields.update(state=RunState.FAILED, failure_phase=FailurePhase.ARTIFACTS, message=str(exc))
        self._record = manager.publish_completion(record, expected_record=expected, **fields)
        if cleanup_error is not None:
            raise cleanup_error

    def _sync_completion(self):
        if self.manager is None:
            return
        from kkrunner.runs.manager import _NO_ARTIFACTS_FAILURES
        with self._completion_lock:
            if self._completion_persisted:
                return
            from kkrunner.runs.stores import resolve_record
            latest = resolve_record(self._record, self.manager.store)
            if latest.state.is_terminal and not (
                latest.state == RunState.FINISHED and latest.return_code is None
                and self.return_code is not None and self._state.is_terminal
            ):
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
            try:
                self._complete_record(
                    latest, state=self._state, execution=self.execution,
                    return_code=self.return_code, message=self.failure_message,
                    failure_phase=phase,
                    artifacts_available=phase not in _NO_ARTIFACTS_FAILURES,
                    files=self.files,
                )
            except BaseException:
                # A cleanup error is reported to the waiter even if its failed
                # completion has already been saved. Keep the public record current.
                self._record = resolve_record(latest, self.manager.store)
                raise
            finally:
                self._completion_persisted = self._record.state.is_terminal

    def wait(self):
        if not self._started:
            if self._settled and self._exception is not None:
                raise self._exception
            raise RuntimeError("Run has not started")
        result = wait_for_monitor(self)
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
        return self._job.stop_observation()

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
        resources, self._owned_resources = self._owned_resources, None
        _close_distinct((self._parts, resources))

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
                self._release_resources(suppress_errors=True)
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
