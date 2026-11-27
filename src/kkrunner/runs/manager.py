"""Persistent run history and lifecycle orchestration."""

from __future__ import annotations

import os
from subprocess import SubprocessError
from contextlib import contextmanager, ExitStack
from contextvars import ContextVar
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from uuid import uuid4

from kkrunner.backends.config import BackendConfig
from kkrunner.backends.contracts import BackendCapability, RecoveryDisposition, UnsupportedCapability
from kkrunner.client.contracts import JobAttachment
from kkrunner.client.pool import default_client_pool
from kkrunner.targets import (
    LocalTarget,
    require_persistable_target,
)
from kkrunner.files.manifest import FileEntry, FileManifest
from kkrunner.files.versions import FileState
from kkrunner.runs.base import FailurePhase, RunState, RunStatus
from kkrunner.runs.conflicts import conflicting_files, submission_conflicts
from kkrunner.files.availability import capture_result_files, file_is_current, sync_result_files
from kkrunner.runs.files import RunFiles
from kkrunner.runs.model import ExecutionMetadata, WorkingDirectory
from kkrunner.runs.processing import ProcessingPending
from kkrunner.runs.records import RunRecord
from kkrunner.runs.states import NO_ARTIFACTS_FAILURES, project_run_state
from kkrunner.runs.stores import resolve_record, store_for

_STATE_RANK = {
    RunState.NEW: 0,
    RunState.PREPROCESSING: 1,
    RunState.SUBMITTING: 2,
    RunState.QUEUED: 3,
    RunState.RUNNING: 4,
    RunState.DOWNLOADING: 5,
    RunState.POSTPROCESSING: 6,
    RunState.FINISHED: 7,
    RunState.FAILED: 7,
    RunState.CANCELLED: 7,
}
_UNCHANGED = object()


def _now():
    return datetime.now(timezone.utc).isoformat()


def _timestamp(value):
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _failure_phase(value):
    if value is None or isinstance(value, FailurePhase):
        return value
    return FailurePhase(value)


class DuplicateSubmissionError(RuntimeError):
    """A persistent run would duplicate an active calculation namespace."""

    def __init__(self, record, conflicts):
        self.record = record
        self.conflicts = tuple(conflicts)
        self.file_conflicts = {other.key: conflicting_files(record, other) for other in self.conflicts}
        super().__init__(
            "Run conflicts with {} active run{}".format(len(self.conflicts), "" if len(self.conflicts) == 1 else "s")
        )


class RunManager:
    """Persist public run lifecycle without task-specific knowledge."""

    _default_override = ContextVar("kkrunner_default_run_manager", default=None)
    _default_lock = Lock()
    _fallback = None

    @classmethod
    def get_default(cls):
        """Return a context-selected manager or a stable user-level manager."""
        selected = RunManager._default_override.get()
        if selected is not None:
            return selected
        with RunManager._default_lock:
            if RunManager._fallback is None:
                from kkrunner.runs.records import RunStore
                RunManager._fallback = RunManager(RunStore())
            return RunManager._fallback

    @classmethod
    @contextmanager
    def as_default(cls, manager):
        """Select a manager for the current context without affecting other contexts."""
        if not isinstance(manager, RunManager):
            raise TypeError("Default manager must be a RunManager")
        token = RunManager._default_override.set(manager)
        try:
            yield manager
        finally:
            RunManager._default_override.reset(token)

    def __init__(self, store=None, *, client_pool=None, submitting_grace=60.0, conflict_stores=()):
        if submitting_grace < 0:
            raise ValueError("submitting_grace must be non-negative")
        self.store = store
        self.conflict_stores = tuple(conflict_stores)
        self.client_pool = client_pool if client_pool is not None else default_client_pool()
        self._files = RunFiles(store=self.store, client_pool=self.client_pool)
        self._ack_lock = Lock()
        self._acknowledged_results = set()
        self.submitting_grace = float(submitting_grace)

    def _store_for(self, record):
        return store_for(record, self.store)

    def _record_key(self, record):
        return record.key if record.store is not None else (self.store.lock_key, record.id)

    def _reservation_stores(self):
        # Deduplicate disk stores opened through separate objects. The primary
        # store wins, and all managers take locks in the same order.
        stores = {self.store.lock_key: self.store}
        for store in self.conflict_stores:
            stores.setdefault(store.lock_key, store)
        return tuple(stores[key] for key in sorted(stores))

    def register_client(self, client):
        key = (require_persistable_target(client.target), client.backend_config)
        self.client_pool.get(key, lambda: client)
        return key

    def client_for(self, record):
        return self.client_pool.for_config(record.execution.target, record.execution.backend)

    def input_file_is_current(self, run):
        """Whether the local primary input matches its captured version."""
        return self._files.input_file_is_current(run)

    def output_file_is_current(self, run):
        """Whether the local primary output matches its captured version."""
        return self._files.output_file_is_current(run)

    def update_artifact_availability(self, record, path, state, message=None):
        """Persist artifact availability independently of the run outcome."""
        return self._files.update_artifact_availability(record, path, state, message)

    def snapshot_file(self, run, file, *, destination=None):
        """Copy a declared output or cache as currently visible, without updating history."""
        return self._files.snapshot_file(run, file, destination=destination)

    def retrieve_result_files(self, run):
        """Restore missing results, verifying captured digests before replacement."""
        @contextmanager
        def reserve(record, entries):
            from kkrunner.files.manifest import FileMapping, FileSyncPolicy

            execution = record.execution
            mapping = record.files.mapping
            if mapping is None and execution.directory.remote is not None:
                mapping = FileMapping(execution.directory.local, execution.directory.remote)
            if mapping is not None:
                mapping = replace(mapping, input_sync=FileSyncPolicy.NEVER,
                                  output_sync=FileSyncPolicy.NEVER)
            # This is a temporary reservation, not a new project calculation.
            # An unbound project manager can restore any historical record.
            scope = RunManager.new_record(
                self, project_id=record.project_id,
                directory=execution.directory.local,
                remote_directory=execution.directory.remote,
                target=execution.target,
                backend=execution.backend,
                files=FileManifest(
                    inputs=tuple(FileEntry(remote_path=entry.remote_path) for entry in entries
                                 if entry.remote_path is not None),
                    outputs=tuple(FileEntry(local_path=entry.local_path) for entry in entries),
                    mapping=mapping,
                ),
            )
            scope = replace(scope, store=self._store_for(record))
            with self._scoped_reservation(scope):
                yield

        with self._history_edit(run) as (_, record):
            return self._files.retrieve_result_files(record, reserve=reserve)

    def new_record(
        self,
        *,
        directory,
        target=None,
        backend=None,
        remote_directory=None,
        files=None,
        run_id=None,
        project_id=None,
        metadata=None,
        processing_required=False,
        submission_key=None,
        detached=False,
    ):
        now = _now()
        local = os.path.abspath(os.fspath(directory))
        return RunRecord(
            id=run_id or uuid4().hex,
            state=RunState.NEW,
            created_at=now,
            updated_at=now,
            metadata={} if metadata is None else metadata,
            processing_required=processing_required,
            execution=ExecutionMetadata(
                LocalTarget() if target is None else target,
                WorkingDirectory(local, remote_directory),
                submission_key=submission_key,
                backend=backend or BackendConfig(),
            ),
            files=files if files is not None else FileManifest(),
            project_id=project_id,
            detached=detached,
            store=self.store,
        )

    @staticmethod
    def _merged_state(latest, requested, *, return_code=None):
        if requested is None:
            return latest.state
        requested = RunState(requested)
        if latest.state.is_terminal:
            # Later authoritative exit codes refine a FINISHED/unknown-code
            # observation, including a nonzero execution failure.
            if (
                latest.state == RunState.FINISHED and latest.return_code is None
                and return_code is not None and requested.is_terminal
            ):
                return requested
            return latest.state
        if requested == RunState.UNKNOWN:
            # Uncertainty cannot retract confirmed execution completion.
            if latest.state in (RunState.POSTPROCESSING, RunState.DOWNLOADING):
                return latest.state
            return requested
        if latest.state == RunState.UNKNOWN:
            return requested
        if _STATE_RANK[requested] < _STATE_RANK[latest.state]:
            return latest.state
        return requested

    @staticmethod
    def _merged_result_files(latest, requested):
        if requested is _UNCHANGED or requested is None:
            return latest.files
        if isinstance(requested, dict):
            requested = FileManifest(**requested)
        current = latest.files
        if current is None:
            return requested
        if current.captured_at is not None:
            # Immutable historical evidence: an old runner must not replace
            # the captured digests with its uncaptured in-memory manifest.
            return current
        if current.input_versions:
            return replace(requested, input_versions=current.input_versions)
        return requested

    def transition(
        self,
        record,
        *,
        state=None,
        execution=_UNCHANGED,
        return_code=_UNCHANGED,
        message=_UNCHANGED,
        failure_phase=_UNCHANGED,
        files=_UNCHANGED,
        expected_updated_at=_UNCHANGED,
        expected_record=None,
    ):
        def merge(latest):
            if latest.state == RunState.POSTPROCESSING:
                # Only the client holding observation_lock may advance
                # processing, using the existing compare-and-update contract.
                if expected_record is None:
                    return latest
                if state == RunState.FINISHED and latest.files.failed_artifacts:
                    raise OSError(latest.files.availability_message or "Artifacts became unavailable during processing")
            if expected_record is not None and latest != expected_record:
                raise RuntimeError("Run changed during postprocessing retry")
            if expected_updated_at is not _UNCHANGED and latest.updated_at != expected_updated_at:
                # An independent refresh has superseded this observation.
                return latest
            final_locked = latest.state.is_terminal
            if final_locked and state is not None and not RunState(state).is_terminal:
                # A stale progress update cannot refine a published outcome.
                return latest
            now = _now()
            merged_state = self._merged_state(
                latest, state, return_code=return_code if type(return_code) is int else None,
            )
            confirmed_late = (
                latest.state == RunState.FINISHED and latest.return_code is None
                and type(return_code) is int and merged_state.is_terminal
            )
            if state is not None and RunState(state) == RunState.UNKNOWN and merged_state != RunState.UNKNOWN:
                # Ignore the entire stale observation, including its message
                # and artifact metadata, not merely its proposed state.
                return latest
            changes = {"state": merged_state, "updated_at": now}
            if not latest.state.is_terminal and merged_state.is_terminal:
                changes["finished_at"] = now
            merged_result_files = self._merged_result_files(latest, files)
            if merged_result_files is not latest.files:
                changes["files"] = merged_result_files
            if execution is not _UNCHANGED and execution is not None:
                if latest.execution.backend_id is not None and execution.backend_id != latest.execution.backend_id:
                    raise ValueError("Backend identity cannot change")
                if execution.submission_key != latest.execution.submission_key:
                    raise ValueError("Submission key cannot change")
                changes["execution"] = execution
            if return_code is not _UNCHANGED and return_code is not None:
                if not final_locked or latest.return_code is None:
                    changes["return_code"] = return_code
            if message is not _UNCHANGED:
                if confirmed_late:
                    changes["message"] = message
                elif message is None and latest.state == RunState.UNKNOWN and merged_state != RunState.UNKNOWN:
                    # A later scheduler observation supersedes the stale
                    # connection-error diagnostic.
                    changes["message"] = None
                elif message is not None and (not final_locked or latest.message is None):
                    changes["message"] = message
            if failure_phase is not _UNCHANGED:
                normalized = _failure_phase(failure_phase)
                if confirmed_late or not final_locked or latest.failure_phase is None:
                    changes["failure_phase"] = normalized
            if all(getattr(latest, key) == value for key, value in changes.items() if key != "updated_at"):
                return latest
            return replace(latest, **changes)

        return self._store_for(record).update(record, merge)

    def _is_stale_submitting(self, record, *, now=None):
        if record.state != RunState.SUBMITTING or record.execution.backend_id is not None:
            return False
        now = now or datetime.now(timezone.utc)
        return (now - _timestamp(record.updated_at)).total_seconds() >= self.submitting_grace

    def recover_submitting(self, record):
        store = self._store_for(record)
        lock = store.observation_lock(record)
        if not lock.acquire(blocking=False):
            return store.get(record.id)
        try:
            return self._recover_submitting(record)
        finally:
            lock.release()

    def _recover_submitting(self, record):
        record = resolve_record(record, self.store)
        if not record.execution.available or not self._is_stale_submitting(record):
            return record
        client = self.client_for(record)
        from kkrunner.runs.model import WorkingDirectory
        from kkrunner.client.contracts import SubmissionContext
        recovery = client.recover_submission(
            record.execution.submission_key or record.id,
            SubmissionContext(record.execution),
            submitted_at=record.updated_at,
        )
        if recovery.disposition is RecoveryDisposition.FOUND:
            config = record.execution.target
            if recovery.ref.backend != record.execution.backend.name:
                raise ValueError("Recovered job belongs to a different backend")
            execution = record.execution.submitted(recovery.ref.token)
            if recovery.directory is not None and recovery.directory != execution.directory.target:
                execution = replace(
                    execution,
                    directory=WorkingDirectory(execution.directory.local, recovery.directory),
                )
            files = record.files
            if not config.is_local:
                files = files.with_remote_stream_paths(
                    stdout=recovery.output_file, stderr=recovery.error_file,
                )
            # Persist identity and stream paths before asking the backend for
            # status. A failed observation must not lose the recovered identity.
            attached = self.transition(record, execution=execution, files=files)
            self._acknowledge_submission(attached, client=client)
            return self._record_observation(
                attached,
                client.status(JobAttachment(attached.execution, attached.files))[0],
            )
        if recovery.disposition is RecoveryDisposition.UNSUPPORTED:
            return self.transition(
                record, state=RunState.UNKNOWN,
                message=recovery.message or "Submission recovery is not supported by this client",
            )
        if recovery.disposition is not RecoveryDisposition.ABSENT:
            return record

        def mark_absent(latest):
            if latest.state != RunState.SUBMITTING or latest.execution.backend_id is not None:
                return latest
            now = _now()
            return replace(
                latest,
                state=RunState.FAILED,
                updated_at=now,
                finished_at=now,
                message="Stale submission was not found by its execution backend",
                failure_phase=FailurePhase.SUBMISSION,
            )

        return self._store_for(record).update(record, mark_absent)

    def recover_preprocessing(self, record):
        """Recover abandoned preprocessing without consulting an execution backend."""
        store = self._store_for(record)
        lock = store.observation_lock(record)
        if not lock.acquire(blocking=False):
            return store.get(record.id)
        try:
            latest = store.get(record.id)
            if latest.state != RunState.PREPROCESSING:
                return latest
            return self.complete(
                latest, state=RunState.FAILED, failure_phase=FailurePhase.PREPROCESSING,
                message="Client preprocessing was interrupted before submission",
                artifacts_available=False,
            )
        finally:
            lock.release()

    def begin_submission(self, record):
        """Publish the irreversible submission phase immediately before backend submit."""
        if record.state != RunState.PREPROCESSING:
            raise ValueError("Run is not preprocessing")
        return self.transition(record, state=RunState.SUBMITTING, expected_record=record)

    @contextmanager
    def preprocessing_reservation(self, spec):
        """Protect save-only preprocessing using a caller-owned memory store.

        This deliberately never creates a record or tombstone on disk.
        Other stores participate only in conflict detection.
        """
        from kkrunner.runs.memory_store import MemoryRunStore
        if not isinstance(self.store, MemoryRunStore):
            raise TypeError("Save-only preprocessing requires MemoryRunStore")
        record = self.new_record(directory=spec.directory, files=FileManifest.from_spec(spec))
        with self._scoped_reservation(record) as reserved:
            yield
            sync_result_files((*reserved.files.output_files, *(
                entry.local_path for entry in reserved.files.input_entries
                if entry.write and entry.local_path is not None
            )))

    @contextmanager
    def _scoped_reservation(self, record):
        """Reserve a synchronous scope without recovering other runs."""
        store = self._store_for(record)
        with store.observation_lock(record):
            reserved = self._reserve(record, lambda: store.save(
                replace(record, state=RunState.PREPROCESSING, updated_at=_now()),
            ))
            try:
                yield reserved
            finally:
                store.discard(reserved)

    def _conflicts(self, record, stores):
        return tuple(
            other
            for store in stores
            for other in store.runs(active=True)
            if self._record_key(other) != self._record_key(record)
            and submission_conflicts(record, other)
        )

    def conflicts(self, record):
        """Inspect the same reservation domain as reserve(), without reserving."""
        stores = self._reservation_stores()
        with ExitStack() as locks:
            for store in stores:
                locks.enter_context(store.submission_lock())
            return self._conflicts(record, stores)

    def reserve(self, record, *, preprocessing=False, recover_conflicts=False):
        """Reserve atomically; optionally request a separate conflict recovery first."""
        if self._store_for(record).lock_key != self.store.lock_key:
            raise ValueError("Cannot reserve a record owned by another store")
        record = replace(record, store=self.store)

        def activate():
            try:
                existing = self.store.get(record.id)
            except FileNotFoundError:
                existing = None
            if existing is not None and (existing != record or existing.state != RunState.NEW):
                raise ValueError(f"Run id {record.id!r} already exists; use attach()")
            return self.transition(
                record, state=RunState.PREPROCESSING if preprocessing else RunState.SUBMITTING,
            )

        reserve = self._reserve_recovering if recover_conflicts else self._reserve
        return reserve(record, activate)

    def _reserve(self, record, activate):
        """Check the footprint and activate a record under the same store locks."""
        stores = self._reservation_stores()
        owner = self._store_for(record)
        if all(store.lock_key != owner.lock_key for store in stores):
            stores = tuple(sorted((*stores, owner), key=lambda store: store.lock_key))
        with ExitStack() as locks:
            for store in stores:
                locks.enter_context(store.submission_lock())
            conflicts = self._conflicts(record, stores)
            if conflicts:
                raise DuplicateSubmissionError(record, conflicts)
            return activate()

    def _reserve_recovering(self, record, activate):
        """Explicit higher-level policy: recover abandoned conflicts outside reservation locks."""
        attempted_refresh = set()
        while True:
            try:
                return self._reserve(record, activate)
            except DuplicateSubmissionError as exc:
                refreshable = tuple(
                    conflict for conflict in exc.conflicts
                    if self._record_key(conflict) not in attempted_refresh
                    and (
                        conflict.state == RunState.PREPROCESSING
                        or conflict.execution.available and (
                            self._is_stale_submitting(conflict)
                            or (conflict.execution.backend_id is not None
                                and BackendCapability.STATUS in conflict.execution.backend.supports
                                and BackendCapability.INDEPENDENT_OUTCOME not in conflict.execution.backend.supports)
                        )
                    )
                )
                if not refreshable:
                    raise
            attempted_refresh.update(self._record_key(conflict) for conflict in refreshable)
            self.refresh(refreshable)

    def _apply_observation(self, record, status, operation, **kwargs):
        # A native exit is not client completion while a Run is observing it.
        if not status.state.is_terminal:
            return operation(record, status, **kwargs)
        owner = self._store_for(record).observation_lock(record)
        if not owner.acquire(blocking=False):
            return resolve_record(record, self.store)
        try:
            return operation(record, status, **kwargs)
        finally:
            owner.release()

    @staticmethod
    def _normalize_observed_status(record, status):
        if status.return_code is None and record.return_code is not None:
            # Process liveness is weaker evidence than an already known exit
            # status (e.g. while result retrieval is pending after a failure).
            failed = status.state == RunState.FINISHED and record.return_code != 0
            status = replace(
                status, return_code=record.return_code,
                state=RunState.FAILED if failed else status.state,
                failure_phase=(record.failure_phase or FailurePhase.EXECUTION)
                if failed else status.failure_phase,
            )
        return status

    def _observation_changes(self, record, status, *, files_pending):
        phase = _failure_phase(status.failure_phase)
        return_code = status.return_code
        state = project_run_state(
            status.state, files_pending=files_pending,
            processing_pending=record.processing_required and return_code == 0,
            failure_phase=phase,
        )
        execution = status.execution
        if execution is None and status.backend_id is not None:
            execution = record.execution.submitted(status.backend_id)
        elif execution is not None and record.execution.submission_key is not None:
            # A backend reports execution facts, not the client submission
            # ticket. Preserve our original ticket, but reject disagreement.
            if execution.submission_key is None:
                execution = replace(
                    execution, submission_key=record.execution.submission_key,
                )
            elif execution.submission_key != record.execution.submission_key:
                raise ValueError("Backend status returned another submission key")
        return dict(
            state=state,
            execution=execution,
            files=status.files if status.files is not None else _UNCHANGED,
            return_code=return_code,
            message=status.message,
            failure_phase=phase,
        )

    def _current_observation(self, record, status):
        """Use current history; ignore backend facts once client processing owns the state."""
        record = resolve_record(record, self.store)
        if record.state.is_terminal or record.state in (RunState.PREPROCESSING, RunState.POSTPROCESSING):
            return record, None
        return record, self._normalize_observed_status(record, status)

    def update(self, record, status: RunStatus):
        """Apply supplied execution facts; do not query or retrieve artifacts."""
        return self._apply_observation(record, status, self._update)

    def _update(self, record, status: RunStatus):
        record, status = self._current_observation(record, status)
        if status is None:
            return record
        # Supplied execution facts alone cannot publish uncaptured diagnostic
        # or successful output. Keep the reservation until finalization.
        files = status.files if status.files is not None else record.files
        changes = self._observation_changes(
            record, status,
            files_pending=(
                (not record.execution.target.is_local and status.files is None)
                or files.captured_at is None
                or (status.state == RunState.FINISHED and bool(files.failed_artifacts))
            ),
        )
        return self.transition(record, **changes)

    def _record_observation(self, record, status, *, retrieve_ended=False):
        """Finalize local artifacts; retrieve unobserved remote output only on request."""
        observed_at = record.updated_at
        record, status = self._current_observation(record, status)
        if status is None:
            return record
        if (
            record.execution.target.is_local
            and status.state.is_terminal and status.files is None
        ):
            # Backend status contains execution facts only. The client-side
            # manifest projection owns streams and application artifacts.
            from kkrunner.files.availability import local_manifest
            status = replace(status, files=local_manifest(record.files, status))
        if (
            retrieve_ended
            and BackendCapability.INDEPENDENT_OUTCOME not in record.execution.backend.supports
            and not record.execution.target.is_local
            and (status.state == RunState.CANCELLED or (
                status.state == RunState.FINISHED and status.return_code is None
            ))
            and status.files is None
        ):
            # No live client owns finalization. A liveness-only status/follow cannot
            # recover cancellation on a later observation, so collect any
            # diagnostics now, under the same ownership as completion.
            try:
                files = self.client_for(record).collect_ended_files(record, status)
            except (OSError, RuntimeError, ValueError, SubprocessError) as exc:
                if status.state != RunState.CANCELLED:
                    return self.transition(
                        record, state=RunState.DOWNLOADING,
                        message=f"Execution ended; output retrieval pending: {exc}",
                    )
                # As in Run._finish(), diagnostic transfer failure cannot
                # change a confirmed cancelled execution into another outcome.
                message = f"Execution cancelled; output retrieval failed: {exc}"
                files = record.files.fail_pending_artifacts(message)
                status = replace(status, message=message)
            status = replace(status, files=files)
        changes = self._observation_changes(
            record, status,
            files_pending=not record.execution.target.is_local and status.files is None,
        )
        if changes["state"] == RunState.UNKNOWN:
            # Uncertainty must not replace a more recent observation or cause
            # further receipt requests over a possibly unavailable transport.
            return self.transition(record, expected_updated_at=observed_at, **changes)
        if changes["state"] in (RunState.FINISHED, RunState.POSTPROCESSING) or (
            status.state.is_terminal and changes["state"].is_terminal
            and status.files is not None
            and changes["failure_phase"] not in NO_ARTIFACTS_FAILURES
        ):
            changes["files"] = status.files if status.files is not None else record.files
            updated = self.complete(record, **changes, artifacts_available=True)
        else:
            updated = self.transition(record, **changes)
            self._acknowledge_result(updated)
        return updated

    def completion_fields(
        self,
        record,
        *,
        state,
        execution=None,
        return_code=None,
        message=None,
        failure_phase=None,
        artifacts_available=False,
        files=None,
    ):
        """Resolve the completion data while the artifacts still exist."""
        state = RunState(state)
        phase = _failure_phase(failure_phase)
        result_files = self._merged_result_files(record, files)

        if result_files is not None and result_files.failed_artifacts:
            if state == RunState.FINISHED or phase == FailurePhase.POSTPROCESSING:
                artifacts_available = False
                # The successful backend does not excuse a required download
                # failure; the failed file retains the specific diagnostics.
                state = RunState.FAILED
                phase = (
                    FailurePhase.DOWNLOAD if record.execution.directory.remote is not None else FailurePhase.ARTIFACTS
                )
                message = result_files.availability_message or message

        if artifacts_available and result_files is not None:
            try:
                result_files = capture_result_files(
                    result_files, require_outputs=state == RunState.FINISHED and return_code == 0,
                )
            except OSError as exc:
                capture_error = str(exc)
                if state in (RunState.FINISHED, RunState.POSTPROCESSING):
                    state = RunState.FAILED
                    phase = FailurePhase.ARTIFACTS
                    message = capture_error
                result_files = result_files.fail_pending_artifacts(capture_error)

        if (
            result_files is not None
            and result_files.failed_artifacts
            and state in (RunState.FINISHED, RunState.POSTPROCESSING)
        ):
            state, phase = RunState.FAILED, FailurePhase.ARTIFACTS
            message = result_files.availability_message or message

        return dict(state=state, execution=execution, return_code=return_code,
                    message=message, failure_phase=phase, files=result_files)

    def publish_completion(self, record, *, expected_record=None, **fields):
        """Commit a prepared completion and acknowledge its backend receipt."""
        updated = self.transition(record, expected_record=expected_record, **fields)
        self._acknowledge_result(updated)
        return updated

    def complete(self, record, *, expected_record=None, **kwargs):
        """Complete an operation which owns no application resources."""
        return self.publish_completion(
            record, expected_record=expected_record,
            **self.completion_fields(record, **kwargs),
        )

    def resume_download(self, run):
        """Explicitly reopen a failed post-execution transfer for retry.

        This is not an execution resubmit. The original backend handle remains
        authoritative and the attached raw runner replays/synchronizes files
        before task postprocessing can finish.
        """
        record = resolve_record(run, self.store)

        def reopen(latest):
            if (
                latest.state != RunState.FAILED
                or latest.failure_phase != FailurePhase.DOWNLOAD
                or latest.return_code != 0
                or latest.execution.backend_id is None
                or latest.files is None
                or latest.files.captured_at is not None
            ):
                raise ValueError("Only failed downloads of successful backend jobs can be retried")
            # Keep already transferred artifacts and all bytes on disk.
            # Only failed transfers become eligible for a fresh attempt.
            files = latest.files._map_artifacts(
                lambda item: (
                    replace(
                        item,
                        availability_state="pending",
                        availability_message=None,
                    )
                    if item.availability_state == "failed"
                    else item
                )
            )
            return replace(
                latest,
                state=RunState.DOWNLOADING,
                finished_at=None,
                failure_phase=None,
                message=None,
                files=files,
                updated_at=_now(),
            )

        with self._history_edit(record) as (store, current):
            return self._reserve_recovering(current, lambda: store.update(current, reopen))

    def begin_processing(self, record, *, return_code, files=None, execution=None, retry=False):
        """Claim processing under the caller's existing client ownership.

        A historical retry must reactivate the same reservation before reading
        or validating artifacts. An interrupted active run already reserves it.
        """
        store = self._store_for(record)
        latest = store.get(record.id)
        if latest.state == RunState.FINISHED and latest.return_code is not None:
            raise ProcessingPending("Result already processed by another observer; load its historical result")
        if latest.state.is_terminal and not (
            (retry and latest.state == RunState.FAILED and latest.failure_phase == FailurePhase.POSTPROCESSING)
            or (latest.state == RunState.FINISHED and latest.return_code is None)
        ):
            raise RuntimeError("Run is already terminal")

        def activate(current):
            if current.state.is_terminal and current != latest:
                raise ProcessingPending("Run changed before processing claim")
            return replace(
                current, state=RunState.POSTPROCESSING,
                updated_at=_now(), finished_at=None,
                failure_phase=None, message=None, return_code=return_code,
                execution=current.execution if execution is None else execution,
            )

        # A historical attempt must first become active. The actual file
        # validation can then run without submission or store locks.
        claimed = (self._reserve_recovering(latest, lambda: store.update(latest, activate))
                   if latest.state.is_terminal else latest)
        try:
            manifest = capture_result_files(self._merged_result_files(claimed, files))
            if manifest.failed_artifacts:
                raise OSError(manifest.availability_message or "Output artifacts unavailable")
            if any(v.state() is not FileState.CURRENT for v in manifest.output_versions):
                if retry:
                    raise ValueError("Postprocessing retry requires unchanged historical artifacts")
                raise OSError("Historical artifacts changed before postprocessing")
        except BaseException:
            if latest.state.is_terminal:
                # Validation did not begin a new processing attempt. Restore
                # the prior outcome and release the reactivated reservation.
                store.update(claimed, lambda current: replace(
                    current, state=latest.state, finished_at=latest.finished_at,
                    failure_phase=latest.failure_phase, message=latest.message,
                    return_code=latest.return_code, execution=latest.execution,
                    updated_at=_now(),
                ))
            raise

        return store.update(claimed, lambda current: replace(activate(current), files=manifest))

    @staticmethod
    def process_outcome(record, outcome, process_result):
        """Invoke a parser, without taking ownership or publishing completion."""
        if process_result is None:
            raise ProcessingPending("Postprocessing requires a client callback")
        result = process_result(outcome)
        if any(v.state() is not FileState.CURRENT for v in record.files.output_versions):
            raise RuntimeError("Historical artifacts changed during postprocessing retry")
        return result

    def retry_postprocessing(self, run, process_result):
        """Retry a failed or interrupted processing attempt without rerunning a job."""
        from kkrunner.runs.base import ExecutionOutcome

        record = resolve_record(run, self.store)
        lock = self._store_for(record).observation_lock(record)
        if not lock.acquire(blocking=False):
            raise ProcessingPending("Another client owns postprocessing")
        try:
            record = resolve_record(record, self.store)
            if (
                not record.processing_required
                or record.return_code != 0
                or not (
                    record.state == RunState.POSTPROCESSING
                    or (record.state == RunState.FAILED and record.failure_phase == FailurePhase.POSTPROCESSING)
                )
            ):
                raise ValueError("Only postprocessing failures or interrupted processing can be retried")
            files = record.files
            if files.captured_at is None:
                raise ValueError("Postprocessing retry requires captured artifacts")
            claimed = self.begin_processing(record, return_code=record.return_code, retry=True)
            files = claimed.files
            outcome = ExecutionOutcome(
                input_file=(
                    files.input_file if file_is_current(files, files.input_file, files.input_versions) else None
                ),
                output_file=files.output_file,
                error_file=None if files.stderr is None else files.stderr.local_path,
                return_code=claimed.return_code,
            )
            try:
                result = self.process_outcome(claimed, outcome, process_result)
            except ProcessingPending:
                raise
            except BaseException as exc:
                self.complete(claimed, state=RunState.FAILED, return_code=outcome.return_code,
                              message=str(exc), failure_phase=FailurePhase.POSTPROCESSING,
                              expected_record=claimed)
                raise
            self.complete(claimed, state=RunState.FINISHED, return_code=outcome.return_code,
                          expected_record=claimed)
            return result
        finally:
            lock.release()

    def _acknowledge_result(self, record):
        """Best-effort ACK after durable completion; failed ACKs remain retryable."""
        if (not record.state.is_terminal or record.execution.backend_id is None
                or BackendCapability.ACKNOWLEDGE not in record.execution.backend.supports):
            return False
        if record.failure_phase == FailurePhase.DOWNLOAD and record.return_code == 0:
            # Retry will reattach to download preserved output. Retain the
            # terminal backend receipt until that end-to-end attempt settles.
            return False
        key = (self._record_key(record), record.execution.backend_id)
        with self._ack_lock:
            if key in self._acknowledged_results:
                return True
            try:
                client = self.client_for(record)
                client.acknowledge(record)
            except (OSError, SubprocessError, RuntimeError, ValueError):
                return False
            self._acknowledged_results.add(key)
            return True

    def reconcile_acknowledgements(self, *, on_error=None):
        """Explicitly retry receipt ACKs; return IDs with acknowledged outcomes."""
        options = {} if on_error is None else {"on_error": on_error}
        acknowledged = []
        for record in self.store.runs(**options):
            if not record.execution.available:
                continue
            self._acknowledge_submission(record)
            if record.state.is_terminal and self._acknowledge_result(record):
                acknowledged.append(record.id)
        return tuple(acknowledged)

    def _acknowledge_submission(self, record, *, client=None):
        """Discard the submission receipt only after RunStore is durable."""
        if (record.execution.backend_id is None
                or BackendCapability.ACKNOWLEDGE not in record.execution.backend.supports):
            return
        try:
            if client is None:
                client = self.client_for(record)
            client.acknowledge_submission(record)
        except (OSError, SubprocessError, RuntimeError):
            # Explicit receipt reconciliation can retry this cleanup.
            pass

    def refresh(self, records=None, *, on_error=None, retrieve_ended=False):
        """Refresh statuses; opt in to retrieving ended, unobserved remote output."""

        options = {} if on_error is None else {"on_error": on_error}
        records = tuple(self.store.runs(active=True, **options) if records is None else records)
        current = {self._record_key(record): record for record in records}
        groups = {}
        for record in records:
            if record.state == RunState.PREPROCESSING:
                try:
                    current[self._record_key(record)] = self.recover_preprocessing(record)
                except FileNotFoundError:
                    current.pop(self._record_key(record), None)
                continue
            if not record.execution.available:
                continue  # Browsable history; no supported target implementation.
            if record.execution.backend_id is None and self._is_stale_submitting(record):
                current[self._record_key(record)] = self.recover_submitting(record)
                continue  # Recovery already observed a newly discovered backend job.
            if record.execution.backend_id is None or record.state.is_terminal:
                continue
            if record.state == RunState.POSTPROCESSING:
                # Execution has already completed and the client owns the
                # processing attempt. Re-querying backend status could
                # overwrite local processing state with a stale failure.
                continue
            client = self.client_for(record)
            groups.setdefault(client, []).append(record)

        for client, group in groups.items():
            if BackendCapability.STATUS not in client.backend.supports:
                for record in group:
                    current[self._record_key(record)] = self.transition(
                        record, state=RunState.UNKNOWN,
                        message="This execution backend has no independent status query",
                    )
                continue
            needs_exclusive_status = BackendCapability.INDEPENDENT_OUTCOME not in client.backend.supports
            locks = []
            guarded = []
            try:
                for record in group:
                    if needs_exclusive_status:
                        ownership = self._store_for(record).observation_lock(record)
                        if not ownership.acquire(blocking=False):
                            # The original observer is alive, possibly already
                            # downloading or processing after the child exited.
                            # It owns completion; do not release its reservation.
                            current[self._record_key(record)] = resolve_record(record, self.store)
                            continue
                        locks.append(ownership)
                    guarded.append(record)
                if not guarded:
                    continue
                statuses = client.status(
                    *(JobAttachment(record.execution, record.files) for record in guarded)
                )
                if len(statuses) != len(guarded):
                    raise RuntimeError("Target returned wrong number of run statuses")
                for record, status in zip(guarded, statuses):
                    if needs_exclusive_status:
                        updated = self._record_observation(record, status, retrieve_ended=retrieve_ended)
                    else:
                        updated = self._apply_observation(record, status, self._record_observation)
                    current[self._record_key(record)] = updated
            finally:
                for ownership in reversed(locks):
                    ownership.release()
        return tuple(current[self._record_key(record)] for record in records
                     if self._record_key(record) in current)

    def _claim_client_files(self, record):
        owner = self._store_for(record).observation_lock(record)
        if not owner.acquire(blocking=False):
            raise RuntimeError("A live client owns this run's files; cancel it or wait for completion")
        return owner

    @contextmanager
    def _history_edit(self, run):
        """Manual history edits may not revoke live client file ownership."""
        store = store_for(run, self.store)
        if store is None:
            raise RuntimeError("No run store is configured")
        record = resolve_record(run, self.store)
        owner = self._claim_client_files(record)
        try:
            yield store, store.get(record.id)
        finally:
            owner.release()

    def mark_failed(self, run, *, message=None):
        """Explicitly override an unowned record, without asserting process termination."""
        with self._history_edit(run) as (_, record):
            return self.transition(record, state=RunState.FAILED, expected_record=record, message=message)

    def forget(self, run, *, force=False):
        """Remove stored history without touching native processes or result files."""
        with self._history_edit(run) as (store, record):
            return store.forget(record, force=force)

    def cancel(self, run):
        """Ask a persisted backend to cancel a job, without assuming completion."""
        record = resolve_record(run, self.store)
        if record.state.is_terminal:
            return record
        if record.state in (RunState.DOWNLOADING, RunState.POSTPROCESSING):
            raise RuntimeError("Execution has finished; postprocessing cannot be cancelled as a native job")
        if record.execution.backend_id is None:
            raise UnsupportedCapability("No durable execution reference is available for cancellation")
        client = self.client_for(record)
        if BackendCapability.CANCEL not in client.backend.supports:
            raise UnsupportedCapability("This backend cannot cancel a job after restart")
        try:
            status = client.cancel(JobAttachment(record.execution, record.files))
        except NotImplementedError as exc:
            # Remote direct execution cannot prove the identity of legacy jobs.
            # Translate the backend-level limitation at the public manager boundary.
            raise UnsupportedCapability(f"This backend cannot cancel this run: {exc}") from exc
        return self._apply_observation(
            record, status, self._record_observation, retrieve_ended=True,
        )

    def attach(
        self, run, *, process_result=None, client=None,
        callback=None, output_listener=None, owned_resources=None,
    ):
        """Open a tracked Run without resubmitting its backend job."""
        from kkrunner.runs.run import Run
        record = resolve_record(run, self.store)
        if record.state.is_terminal:
            raise RuntimeError("Run is already terminal; use explicit retry")
        if process_result is not None and not record.processing_required:
            raise ValueError("This Run did not request postprocessing")
        if record.execution.backend_id is None:
            raise RuntimeError(f"Run {record.id} has no backend reference")
        client = self.client_for(record) if client is None else client
        self.register_client(client)
        if BackendCapability.REATTACH not in client.backend.supports:
            from kkrunner.backends.contracts import UnsupportedCapability
            raise UnsupportedCapability("This backend does not support reattachment")
        owner = self._claim_client_files(record)
        parts = None
        try:
            # Check again under ownership, before opening any output streams.
            record = resolve_record(record, self.store)
            if record.state.is_terminal:
                raise RuntimeError("Run is already terminal; use explicit retry")
            parts = client.open_run_parts(
                JobAttachment(record.execution, record.files),
                output_listener=output_listener,
            )
            attached = Run(
                parts, client=client, manager=self, record=record,
                process_result=process_result, callback=callback, owned_resources=owned_resources,
                observation_lock=owner,
            )
        except BaseException:
            try:
                if parts is not None:
                    parts.close()
            finally:
                owner.release()
            raise
        # Ownership was transferred to Run; it also handles startup failure.
        attached.start()
        return attached
