"""One client-side Run lifecycle, with optional durable RunRecord storage.

Backend jobs own execution; Run owns observation completion and application
postprocessing.  RunManager is the sole authority for durable state changes.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import Future
from threading import Lock

from kkrunner.execution import RunFileState
from kkrunner.runs.base import (
    ExecutionOutcome,
    ExecutionResources,
    FailurePhase,
    RunState,
    RunStatus,
    SubmissionNotStarted,
)
from kkrunner.runs.processing import ProcessingPending, captured_outcome
from kkrunner.runs.states import NO_EXECUTION_FAILURES
from kkrunner.runs.stores import resolve_record, store_for


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
        self._busy = False
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
        # Only launch facts outside Job's responsibility need local storage.
        # Ordinary execution facts live in Job or the authoritative record.
        self._launch_status = None
        self._file_state = RunFileState.NEW if record is None else record.file_state
        self._file_failure_phase = None
        self._file_message = None
        self._exception = None
        self._callback_exception = None
        self._pending_exception = None
        self._settled = False
        self._started = False
        self._lock = Lock()
        self._completion_lock = Lock()
        self._observation_lock = observation_lock
        self._monitor_record = record

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
        if self._record is not None and (not self._record.is_active or self._record.files.captured_at is not None):
            return self._record.files
        return self._file_session.files

    @property
    def exception(self):
        return self._exception or self._job.exception

    def _native_source(self):
        """Choose one source for all execution facts, without I/O.

        The live Job may observe an exit before the Run can publish it.
        A settled tracked handle instead follows the record, including
        explicit refresh by this client. Launch failures without a record
        or a native job are facts owned by Run itself.
        """
        record = self._record
        if record is not None and (
            record.state.is_terminal
            or (self._settled and record.file_state != RunFileState.NEW)
        ):
            return record
        if self._launch_status is not None and (self._settled or self._job.state == RunState.NEW):
            return self._launch_status
        if self._job.state.is_terminal:
            return self._job
        if record is not None and record.state != RunState.NEW:
            return record
        return self._job

    @property
    def failure_phase(self):
        phase = self._native_source().failure_phase
        return None if phase is None else FailurePhase(phase)

    @property
    def failure_message(self):
        source = self._native_source()
        return source.failure_message if source is self._job else source.message

    @property
    def return_code(self):
        return self._native_source().return_code

    @property
    def state(self):
        """Native execution facts; file preparation and parsing never change them."""
        return self._native_source().state

    @property
    def file_state(self):
        return self._file_state if self._record is None or self._record.file_state == RunFileState.NEW else self._record.file_state

    @property
    def file_failure_phase(self):
        return self._file_failure_phase if self._record is None or self._record.file_state == RunFileState.NEW else self._record.file_failure_phase

    @property
    def file_message(self):
        return self._file_message if self._record is None or self._record.file_state == RunFileState.NEW else self._record.file_message

    def _set_file_state(self, state, *, files=None):
        self._file_state = state
        if self.manager is not None:
            options = {} if files is None else {"files": files}
            with self._completion_lock:
                self._record = self.manager._commit(self._record, file_state=state, **options)
                self._file_state = self._record.file_state

    def _publish_execution(self):
        """Publish observed native facts, preferring confirmed stored cancellation."""
        status = self._job.status()
        if self.manager is not None:
            with self._completion_lock:
                self._record = self.manager._record_execution(
                    self._record, RunStatus.from_job_status(status, execution=self.execution),
                    observed_from=self._monitor_record,
                )
                from dataclasses import replace
                pending = status.output_pending or (
                    self._record.state.is_terminal and not status.state.is_terminal
                    and status.file_error is None
                    and self._record.file_state in (RunFileState.NEW, RunFileState.WAITING)
                )
                status = replace(self.manager._native_status(self._record),
                                 output_pending=pending, file_error=status.file_error)
        return status

    def _fail_files(self, phase, exc):
        self._file_state = RunFileState.FAILED
        self._file_failure_phase = phase
        self._file_message = str(exc)
        self._exception = exc

    def add_output_listener(self, callback):
        return self._job.add_output_listener(callback)

    def remove_output_listener(self, callback):
        return self._job.remove_output_listener(callback)

    def add_output_error_listener(self, callback):
        return self._job.add_output_error_listener(callback)

    def snapshot_file(self, file, *, destination=None):
        if self.backend_id is None:
            raise RuntimeError("Run has not been submitted")
        from kkrunner.client.file_access import FileAccess
        return FileAccess(self.client.target).snapshot_file(self, file, destination=destination)

    def _file_failure_phase_for_target(self):
        return FailurePhase.ARTIFACTS if self._file_session.files_are_local else FailurePhase.DOWNLOAD

    async def _finish_async(self, *, on_submitted=None):
        """Observe execution once, then independently finalize its files."""
        try:
            record = self._record
            if on_submitted is None and record is not None and not record.is_active:
                self._file_state = record.file_state
                if record.file_state == RunFileState.FAILED:
                    self._exception = RuntimeError(record.file_message or "Run file processing failed")
                elif record.file_state == RunFileState.POSTPROCESSED:
                    self._pending_exception = ProcessingPending("Another observer processed this Run")
                else:
                    self.result = self.manager._result_outcome(record)
                return
            # A previously confirmed terminal record needs no backend query.
            # Fresh execution still owns its live call and must drain it.
            if (on_submitted is None and record is not None and record.state.is_terminal
                    and record.file_state in (RunFileState.COLLECTING, RunFileState.READY, RunFileState.POSTPROCESSING)):
                status = self.manager._native_status(record)
            else:
                try:
                    if on_submitted is None:
                        await self._job.wait_async()
                    else:
                        await self._job.run_async(on_submitted=on_submitted)
                except BaseException as exc:
                    if on_submitted is not None and not self._started:
                        raise
                    self._exception = exc
                status = self._publish_execution()
            outcome = ExecutionOutcome(
                input_file=self._file_session.input_file,
                output_file=self._file_session.output_file,
                error_file=self._file_session.error_file,
                return_code=status.return_code, state=status.state,
            )
            self.result = outcome
            if not status.state.is_terminal:
                return
            if status.output_pending:
                self._pending_exception = ProcessingPending("Backend output is still being published")
                self.result = None
                return
            if status.file_error is not None:
                self._fail_files(FailurePhase.OUTPUT, OSError(status.file_error))
                return
            if status.failure_phase in NO_EXECUTION_FAILURES:
                await self._file_session.finalize_async(status)
                self.result = captured_outcome(self._file_session.files, outcome)
                self._file_state = RunFileState.DONE
                return

            if self.file_state == RunFileState.POSTPROCESSING:
                self._pending_exception = ProcessingPending("Interrupted processing requires explicit retry")
                return

            if self.file_state != RunFileState.READY:
                self._set_file_state(RunFileState.COLLECTING)
                try:
                    def persist_plan(files):
                        with self._completion_lock:
                            self._record = self.manager._commit(self._record, files=files)

                    await self._file_session.finalize_async(
                        status, on_resolved=persist_plan if self.manager is not None else None,
                    )
                    if self._file_session.files.failed_artifacts:
                        raise OSError(self._file_session.files.availability_message or "File collection failed")
                except Exception as exc:
                    self._fail_files(self._file_failure_phase_for_target(), exc)
                    return

            required = (self._process_result is not None if self._record is None
                        else self._record.processing_required)
            if not required:
                self.result = captured_outcome(self._file_session.files, outcome)
                self._file_state = RunFileState.DONE
                return
            self._set_file_state(RunFileState.READY, files=self._file_session.files)
            if self._process_result is None:
                self._pending_exception = ProcessingPending("Postprocessing requires a client callback")
                self.result = None
                return
            try:
                if self.manager is not None:
                    self._record = await self.manager._begin_processing_async(self._record)
                    self._file_state = self._record.file_state
                    self.result = self.manager._process_outcome(
                        self._record, self._process_result, outcome=outcome,
                    )
                else:
                    self._file_state = RunFileState.POSTPROCESSING
                    self.result = self._process_result(captured_outcome(self._file_session.files, outcome))
            except ProcessingPending as exc:
                self._pending_exception = exc
                self.result = None
                return
            except BaseException as exc:
                self._fail_files(FailurePhase.POSTPROCESSING, exc)
                return
            self._file_state = RunFileState.POSTPROCESSED
        except BaseException as exc:
            if on_submitted is not None and not self._started:
                raise
            self._exception = exc
            if self.state.is_terminal:
                self._fail_files(self._file_failure_phase_for_target(), exc)
        finally:
            if on_submitted is None or self._started:
                await self._settle_async()

    async def _settle_async(self):
        """Release resources, publish completion, and then release file ownership."""
        receipt = None
        try:
            self._parts.close()
        except BaseException as exc:
            if self._exception is None:
                self._exception = exc
            if self.state.is_terminal and self._file_state != RunFileState.FAILED:
                self._fail_files(self._file_failure_phase_for_target(), exc)
        try:
            if self.manager is not None:
                if self._record.file_state != RunFileState.NEW:
                    receipt = self._sync_completion()
            else:
                try:
                    self._release_resources(
                        preserve=not self._file_state.is_terminal,
                        suppress_errors=self.exception is not None or not self._file_state.is_terminal,
                    )
                except BaseException as exc:
                    self._fail_files(FailurePhase.ARTIFACTS, exc)
        except BaseException as exc:
            if self._exception is None:
                self._exception = exc
        finally:
            if self._owned_resources is not None:
                self._release_resources(preserve=True, suppress_errors=True)
        try:
            self._release_observation()
            # Receipt cleanup is independent of file ownership. A slow or
            # unavailable backend must not prevent another client claiming
            # already captured files for processing.
            if receipt is not None:
                try:
                    await self.manager._acknowledge_result_async(receipt)
                except BaseException as exc:
                    if self._exception is None:
                        self._exception = exc
        finally:
            try:
                if self.callback is not None:
                    self.callback(self.result)
            except BaseException as exc:
                self._callback_exception = exc
            finally:
                self._settled = True

    def _close_files_after_failed_submission(self):
        """Cleanup must not replace the submission error or skip its record."""
        try:
            self._parts.close()
        except BaseException as cleanup_error:
            import logging
            logging.getLogger(__name__).warning(
                "File cleanup after failed submission also failed: %s", cleanup_error,
            )

    def submit(self, *, validate_inputs=None, on_submitted=None):
        """Launch independently; do not start an observer."""
        from kkrunner.support.async_utils import run_coro_sync
        return run_coro_sync(self.submit_async(
            validate_inputs=validate_inputs, on_submitted=on_submitted,
        ))

    async def submit_async(self, *, validate_inputs=None, on_submitted=None):
        if not self.detached:
            from kkrunner.backends.contracts import UnsupportedCapability
            raise UnsupportedCapability("This execution requires run() or run_async()")
        self._begin_submission()
        try:
            await self._submit_impl(validate_inputs, observe=False, on_submitted=on_submitted)
        finally:
            with self._lock:
                self._busy = False

    def _begin_submission(self):
        with self._lock:
            if self._closed:
                raise RuntimeError("Run is closed")
            if self._settled and isinstance(self._exception, asyncio.CancelledError):
                raise self._exception
            if self._busy or self._started or self._settled:
                raise RuntimeError("Run already submitted")
            if self._record is not None and (self._record.state != RunState.NEW or self._record.file_state != RunFileState.NEW):
                raise RuntimeError(f"Run is already {self._record.state.value}")
            if self._job.state != RunState.NEW:
                raise RuntimeError(f"Job is already {self._job.state.value}")
            self._busy = True
 
    async def _submit_impl(self, validate_inputs=None, *, observe=True, on_submitted=None):
        manager = self.manager
        submitted = False
        reserved = False
        self._file_state = RunFileState.PREPROCESSING
        try:
            if manager is not None:
                from dataclasses import replace
                directory, files = await self._file_session.submission_footprint_async()
                self._record = replace(
                    self._record, files=files,
                    execution=replace(self._record.execution, directory=directory),
                )
                # Keep staging ownership from the instant the reservation becomes visible.
                self._claim_observation()
                # Reservation and duplicate-submit checks are atomic at the store.
                self._record = await manager.reserve_async(self._record, preprocessing=True, recover_conflicts=True)
                reserved = True
                self._monitor_record = self._record
            if self._job.state == RunState.CANCELLED:
                raise asyncio.CancelledError("Run was cancelled before preprocessing")
            if self._preprocess is not None:
                self._preprocess()
            if self._job.state == RunState.CANCELLED:
                raise asyncio.CancelledError("Run was cancelled during preprocessing")
            # Stage using the same session operations for tracked/untracked
            # execution. Only the optional durable snapshot differs.
            await self._file_session.prepare_submission_async()
            if manager is not None:

                self._record = manager._commit(
                    self._record,
                    execution=self.execution,
                    files=self.files,
                )
                self._monitor_record = self._record

            if self._job.state == RunState.CANCELLED:
                raise asyncio.CancelledError("Run was cancelled before submission")
            self._parts.prepare_job()
            if validate_inputs is not None:
                validate_inputs(self.files)
                if self._job.state == RunState.CANCELLED:
                    raise asyncio.CancelledError("Run was cancelled before submission")

            if manager is not None:
                self._record = manager._begin_submission_owned(self._record)
                self._monitor_record = self._record
            self._launch_status = RunStatus(RunState.SUBMITTING)
            self._file_state = RunFileState.WAITING
            submitted = True

            async def accepted():
                if manager is not None:
                    self._record = manager._commit(
                        self._record, state=self._job.state,
                        execution=self.execution, files=self.files,
                        return_code=self._job.return_code, message=self._job.failure_message,
                        failure_phase=self._job.failure_phase,
                    )
                    self._monitor_record = self._record
                self._launch_status = None
                self._started = True
                if manager is not None:
                    await manager._acknowledge_submission_async(self._record, client=self.client)
                if on_submitted is not None:
                    # Notification is not part of the launch transaction.
                    try:
                        on_submitted(self)
                    except Exception:
                        import logging
                        logging.getLogger(__name__).exception("Submission observer failed")

            if observe:
                await self._finish_async(on_submitted=accepted)
            else:
                await self._job.submit_async(on_submitted=accepted)
                self._publish_execution()
                self._parts.close()
                self._release_observation()
        except BaseException as exc:
            # A preflight error is provably before Job.submit(). Once the
            # boundary has been crossed, only SubmissionNotStarted proves
            # that nothing launched. Every other failure is uncertain.
            definitely_absent = isinstance(exc, SubmissionNotStarted) or not submitted
            self._close_files_after_failed_submission()
            cancelled = definitely_absent and self._job.state == RunState.CANCELLED
            self._exception = asyncio.CancelledError("Run was cancelled before submission") if cancelled else exc
            try:
                if definitely_absent:
                    preprocessing = self._file_state == RunFileState.PREPROCESSING
                    state = (RunState.CANCELLED if cancelled else
                             RunState.NEW if preprocessing else RunState.FAILED)
                    self._file_state = RunFileState.FAILED if preprocessing and not cancelled else RunFileState.DONE
                    self._file_failure_phase = FailurePhase.PREPROCESSING if preprocessing and not cancelled else None
                    self._file_message = str(exc) if self._file_failure_phase is not None else None
                    self._launch_status = RunStatus(
                        state, message=None if preprocessing else str(self._exception),
                        failure_phase=None if cancelled or preprocessing else FailurePhase.SUBMISSION,
                    )
                    if reserved:
                        cleanup_error = self._persist_completion(
                            self._record, state=state, file_state=self._file_state,
                            message=self._launch_status.message,
                            failure_phase=self._launch_status.failure_phase,
                            file_failure_phase=self._file_failure_phase, file_message=self._file_message,
                            files=self._file_session.files.skip_pending_artifacts(),
                        )
                        if cleanup_error is not None:
                            raise cleanup_error
                    else:
                        self._release_resources(suppress_errors=True)
                else:
                    # Missing acceptance is a recoverable submission, not a
                    # job with a known identity whose observation was lost.
                    state = (RunState.SUBMITTING if manager is not None and self.backend_id is None
                             else RunState.UNKNOWN)
                    # Cleanup errors after a confirmed native exit must not
                    # replace the known calculation outcome.
                    self._launch_status = (None if self._job.state.is_terminal
                                           else RunStatus(state, message=str(exc)))
                    self._release_resources(preserve=True, suppress_errors=True)
                    if manager is not None and reserved:
                        self._record = manager._commit(
                            self._record, state=state, message=str(exc),
                            execution=self.execution, failure_phase=None,
                            observed_from=self._monitor_record,
                        )

            finally:
                self._release_observation()
                self._settled = True
            if cancelled:
                raise self._exception from exc
            raise

    def _claim_observation(self):
        if self.manager is not None and self._observation_lock is None:
            self._observation_lock = self.manager._claim_client_files(self._record)

    def _release_observation(self):
        lock = self._observation_lock
        self._observation_lock = None
        if lock is not None:
            lock.release()

    def _release_resources(self, *, preserve=False, suppress_errors=False):
        """Consume this Run's application resource ownership."""
        resources, self._owned_resources = self._owned_resources, None
        if resources is not None:
            resources.close(suppress_errors=suppress_errors, detach=preserve)

    def _persist_completion(self, record, *, file_state, files=None,
                            file_failure_phase=None, file_message=None, **native):
        """Release resources and record file completion before ending reservation."""
        manager = self.manager
        try:
            fields = manager.completion_fields(
                record, file_state=file_state, files=files,
                file_failure_phase=file_failure_phase, file_message=file_message,
            )
        except BaseException:
            self._release_resources(preserve=True, suppress_errors=True)
            raise
        preserve = (store_for(record, manager.store).durable
                    or fields["file_failure_phase"] in (FailurePhase.DOWNLOAD, FailurePhase.POSTPROCESSING))
        cleanup_error = None
        try:
            self._release_resources(preserve=preserve,
                                    suppress_errors=fields["file_state"] == RunFileState.FAILED)
        except BaseException as exc:
            cleanup_error = exc
            fields.update(file_state=RunFileState.FAILED,
                          file_failure_phase=FailurePhase.ARTIFACTS, file_message=str(exc))
        self._record = manager._commit(record, **native, **fields)
        # Another observer may have invalidated captured artifacts during processing.
        if cleanup_error is None and self._record.file_state == RunFileState.FAILED and self._exception is None:
            cleanup_error = OSError(self._record.file_message or "Run file processing failed")
        return cleanup_error

    def _sync_completion(self):
        """Commit file completion and return the record eligible for receipt cleanup."""
        with self._completion_lock:
            latest = resolve_record(self._record, self.manager.store)
            self._record = latest
            if not latest.is_active:
                return None
            if not self._file_state.is_terminal:
                return latest if latest.file_state == RunFileState.READY else None
            try:
                cleanup_error = self._persist_completion(
                    latest, files=self._file_session.files,
                    file_state=self._file_state, file_failure_phase=self._file_failure_phase,
                    file_message=self._file_message,
                )
            except BaseException:
                self._record = resolve_record(latest, self.manager.store)
                raise
            if cleanup_error is not None and self._exception is None:
                self._exception = cleanup_error
            return self._record

    def _result_or_raise(self):
        if self._exception is not None:
            raise self._exception
        if self._callback_exception is not None:
            raise self._callback_exception
        if self._pending_exception is not None:
            raise self._pending_exception
        return self.result

    def wait(self):
        """Observe and finalize an already submitted execution in this thread."""
        from kkrunner.support.async_utils import run_coro_sync
        if self._settled:
            return self._result_or_raise()
        return run_coro_sync(self.wait_async())

    async def wait_async(self):
        with self._lock:
            if self._settled:
                return self._result_or_raise()
            if self._closed:
                raise RuntimeError("Run is closed")
            if self._busy:
                raise RuntimeError("Run already has an active caller")
            if self.backend_id is None:
                raise RuntimeError("Run has not been submitted")
            self._busy = True
        try:
            self._claim_observation()
            # A rejected claim has not started an observation and must not
            # consume this handle's resources. Only the owner may proceed.
            try:
                if self.manager is not None:
                    self._record = resolve_record(self._record, self.manager.store)
                    self._monitor_record = self._record
                    self._file_state = self._record.file_state
                    if self._record.is_active and (
                        self._record.files != self._file_session.files
                        or self._record.execution.directory != self._file_session.working_directory
                    ):
                        # Another owner may have frozen a collection plan or
                        # captured files since submit() released ownership.
                        # Resume that manifest, not the submission snapshot.
                        from kkrunner.client.file_session import FileSession

                        self._parts.close()
                        self._parts.files = self._file_session = FileSession.from_attachment(
                            self.client.target, self._record,
                        )
                if (self._record is None or not self._record.state.is_terminal
                        or self._record.file_state in (RunFileState.NEW, RunFileState.WAITING)):
                    self._parts.configure_observation()
                self._started = True
                await self._finish_async()
            except BaseException:
                if not self._settled:
                    try:
                        self._parts.close()
                    finally:
                        try:
                            self._release_resources(preserve=True, suppress_errors=True)
                        finally:
                            self._release_observation()
                raise
        finally:
            with self._lock:
                self._busy = False
        return self._result_or_raise()

    def run(self, *, validate_inputs=None, on_submitted=None):
        """Execute in the caller's thread, without starting client monitors."""
        from kkrunner.support.async_utils import run_coro_sync
        if self._settled:
            return self._result_or_raise()
        return run_coro_sync(self.run_async(
            validate_inputs=validate_inputs, on_submitted=on_submitted,
        ))

    async def run_async(self, *, validate_inputs=None, on_submitted=None):
        """Stage, observe and finalize in the caller's asyncio loop."""
        if self._settled:
            return self._result_or_raise()
        if self._started or self._job.state != RunState.NEW:
            if self.backend_id is not None:
                return await self.wait_async()
        self._begin_submission()
        try:
            await self._submit_impl(validate_inputs, on_submitted=on_submitted)
        finally:
            with self._lock:
                self._busy = False
        return self._result_or_raise()

    def refresh(self):
        """Explicitly refresh native execution and tracked file progress."""
        with self._completion_lock:
            if self.manager is not None:
                self._record = self.manager.refresh((self._record,))[0]
                return self.state
            if self.backend_id is not None and (
                not self.state.is_terminal or self._job.status().output_pending
            ):
                self._job.refresh_status()
                self._launch_status = None
            return self.state

    def stop_observation(self):
        return self._job.stop_observation()

    def close(self):
        """Close the client handle after its own work settles, without cancelling the job."""
        with self._lock:
            if self._closed:
                return
            if self._busy and not self._settled:
                raise RuntimeError(f"Cannot close resources while Run is {self.state.value}")
            self._closed = True
        try:
            self._parts.close()
        finally:
            try:
                self._release_resources(preserve=self.backend_id is not None)
            finally:
                self._release_observation()

    def cancel(self):
        with self._lock:
            if self._record is not None and self._record.state.is_terminal:
                result = Future()
                result.set_result(None)
                return result
        result = self._job.cancel()
        with self._lock:
            cancelled_before_submit = (
                not self._busy and not self._started and not self._settled
                and self.backend_id is None and self._job.state == RunState.CANCELLED
            )
            if cancelled_before_submit:
                self._file_state = RunFileState.DONE
                self._exception = asyncio.CancelledError("Run was cancelled before submission")
                self._settled = True
        if cancelled_before_submit:
            self._close_files_after_failed_submission()
            self._release_resources(suppress_errors=True)
        if self.manager is None or cancelled_before_submit:
            return result
        # A tracked cancellation confirmed without an observing caller must
        # still publish its native facts, before the cancellation future resolves.
        published = Future()

        def persist(source):
            try:
                status = source.result()
                if status is not None and (status.state.is_terminal or status.file_error is not None) and self._started:
                    with self._completion_lock:
                        self._record = self.manager.update(
                            self._record, RunStatus.from_job_status(status, execution=self.execution),
                        )
                published.set_result(status)
            except BaseException as exc:
                published.set_exception(exc)

        result.add_done_callback(persist)
        return published


__all__ = ["Run"]
