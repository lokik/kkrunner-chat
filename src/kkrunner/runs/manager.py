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
from kkrunner.execution import RunFileState, merge_terminal_outcome
from kkrunner.runs.base import FailurePhase, RunState, RunStatus
from kkrunner.runs.conflicts import conflicting_files, submission_conflicts
from kkrunner.files.availability import capture_result_files, sync_result_files
from kkrunner.runs.files import RunFiles
from kkrunner.runs.memory_store import MemoryRunStore
from kkrunner.runs.model import ExecutionMetadata, WorkingDirectory
from kkrunner.runs.processing import ProcessingPending, captured_outcome
from kkrunner.runs.records import RunRecord
from kkrunner.runs.states import NO_EXECUTION_FAILURES
from kkrunner.runs.stores import resolve_record, store_for
from kkrunner.support.async_utils import run_coro_sync

_UNCHANGED = object()

# Internal, process-wide reservations are not application run history.
_TEMPORARY_RESERVATIONS = MemoryRunStore()


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
        self._files = RunFiles(store=self.store)
        self.submitting_grace = float(submitting_grace)

    def _store_for(self, record):
        return store_for(record, self.store)

    def _record_key(self, record):
        if record.store is not None or self.store is None:
            return record.key
        return (self.store.lock_key, record.id)

    def _reservation_stores(self, *additional):
        # Deduplicate disk stores opened through separate objects. The primary
        # store wins, and all managers take locks in the same order.
        stores = {} if self.store is None else {self.store.lock_key: self.store}
        for store in (_TEMPORARY_RESERVATIONS, *self.conflict_stores, *additional):
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
                                  output_sync=FileSyncPolicy.MISSING)
            # This is a temporary reservation, not a new project calculation.
            # An unbound project manager can restore any historical record.
            scope = RunManager.new_record(
                self,
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
            with self._scoped_reservation(scope, conflict_stores=(self._store_for(record),)):
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
            file_state=RunFileState.NEW,
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
            return merge_terminal_outcome(
                latest.state, latest.return_code, requested,
                return_code if type(return_code) is int else None,
            )[0]
        # A scheduler may legitimately requeue a running calculation.
        return requested

    @staticmethod
    def _merged_result_files(latest, requested):
        if requested is _UNCHANGED or requested is None:
            return latest.files
        if isinstance(requested, dict):
            requested = FileManifest(**requested)
        current = latest.files
        if current.captured_at is not None:
            # Immutable historical evidence: an old runner must not replace
            # the captured digests with its uncaptured in-memory manifest.
            return current
        if current.remote_patterns_resolved is not None and requested.remote_patterns_resolved is None:
            # Backend observations may carry the pre-discovery manifest.
            # They cannot discard an already decided result file set.
            return current
        if current.input_versions:
            return replace(requested, input_versions=current.input_versions)
        return requested

    def transition(
        self, record, *, file_state=None, files=_UNCHANGED,
        file_failure_phase=_UNCHANGED, file_message=_UNCHANGED, **native,
    ):
        """Apply explicit record changes; file mutations require exclusive ownership.

        Native observations are independent of file ownership. Internal lifecycle
        operations already holding the file lock use _commit() directly.
        """
        if (file_state is None and files is _UNCHANGED
                and file_failure_phase is _UNCHANGED and file_message is _UNCHANGED):
            return self._commit(record, **native)
        owner = self._claim_client_files(record)
        try:
            return self._commit(
                record, file_state=file_state, files=files,
                file_failure_phase=file_failure_phase, file_message=file_message, **native,
            )
        finally:
            owner.release()

    def _commit(
        self, record, *, state=None, file_state=None, execution=_UNCHANGED,
        return_code=_UNCHANGED, message=_UNCHANGED, failure_phase=_UNCHANGED,
        files=_UNCHANGED, file_failure_phase=_UNCHANGED, file_message=_UNCHANGED,
        observed_from=None, expected_record=None,
    ):
        """Merge native facts; the caller must own any file mutation."""
        def merge(latest):
            if expected_record is not None and latest != replace(expected_record, project_id=latest.project_id):
                raise RuntimeError("Run changed during client processing")
            requested = None if state is None else RunState(state)
            if requested is not None and not requested.is_terminal:
                # A nonterminal observation is conditional on the native
                # facts it started from, not on file or project changes.
                # Terminal evidence can still refine the record at any time.
                baseline = record if observed_from is None else observed_from
                if (self._native_status(latest) != self._native_status(baseline)
                        or (latest.state not in (RunState.NEW, RunState.SUBMITTING)
                            and requested in (RunState.NEW, RunState.SUBMITTING))):
                    requested = None
            native = self._merged_state(latest, requested, return_code=return_code)
            # File progress belongs to the owner of the current phase, not to
            # whichever observer reports the numerically most advanced state.
            # Native facts may still arrive while that owner is working.
            same_file_phase = latest.is_active and latest.file_state == record.file_state
            client = (RunFileState(file_state)
                      if same_file_phase and file_state is not None else latest.file_state)
            manifest = self._merged_result_files(latest, files) if same_file_phase else latest.files
            file_failure = file_failure_phase
            file_error = file_message
            if client != latest.file_state:
                if client in (RunFileState.COLLECTING, RunFileState.READY,
                              RunFileState.POSTPROCESSING, RunFileState.POSTPROCESSED,
                              RunFileState.DONE) and not native.is_terminal:
                    raise ValueError("File finalization requires confirmed native termination")
                if client == RunFileState.POSTPROCESSED and latest.file_state != RunFileState.POSTPROCESSING:
                    raise ValueError("Only active postprocessing can become postprocessed")
                if client in (RunFileState.READY, RunFileState.POSTPROCESSING) and (
                    not latest.processing_required or manifest.captured_at is None or manifest.failed_artifacts
                ):
                    raise ValueError("Postprocessing requires a declared processor and a complete capture")
                if client in (RunFileState.NEW, RunFileState.PREPROCESSING, RunFileState.WAITING) and (
                    latest.file_state not in (RunFileState.NEW, RunFileState.PREPROCESSING)
                    or client == RunFileState.NEW
                ):
                    raise ValueError("Cannot reopen file preparation through transition")
                if client == RunFileState.COLLECTING and latest.file_state in (
                    RunFileState.READY, RunFileState.POSTPROCESSING,
                ):
                    raise ValueError("Cannot collect already captured files")
                if latest.file_state == RunFileState.POSTPROCESSING and client in (
                    RunFileState.READY, RunFileState.DONE,
                ):
                    raise ValueError("Active postprocessing must finish or fail")
                if latest.file_state == RunFileState.READY and client == RunFileState.DONE:
                    raise ValueError("Ready files require postprocessing")
                if client == RunFileState.POSTPROCESSED and manifest.failed_artifacts:
                    client = RunFileState.FAILED
                    file_failure = FailurePhase.POSTPROCESSING
                    file_error = manifest.availability_message or "Output artifacts unavailable"
            changes = dict(state=native, file_state=client)
            if latest.is_active and client.is_terminal:
                changes["finished_at"] = _now()
            if execution is not _UNCHANGED and execution is not None:
                # A status response derived from an older attachment must
                # not revert a directory resolved by submission recovery.
                if (latest.execution != record.execution
                        and latest.execution.backend_id is not None
                        and execution.backend_id in (None, latest.execution.backend_id)):
                    execution_update = latest.execution
                else:
                    execution_update = execution
                changes["execution"] = execution_update
            if requested is not None and latest.state.is_terminal:
                _, code = merge_terminal_outcome(
                    latest.state, latest.return_code, requested,
                    return_code if type(return_code) is int else None,
                )
                changes["return_code"] = code
            # Stale/unknown liveness cannot erase known terminal facts.
            if requested is not None and native == requested:
                if not latest.state.is_terminal and return_code is not _UNCHANGED and return_code is not None:
                    changes["return_code"] = return_code
                if message is not _UNCHANGED and (
                    not latest.state.is_terminal or native != latest.state or latest.message is None
                    or (latest.return_code is None and type(return_code) is int)
                ):
                    changes["message"] = message
                if failure_phase is not _UNCHANGED and (
                    not latest.state.is_terminal or native != latest.state
                    or (latest.return_code is None and type(return_code) is int)
                ):
                    changes["failure_phase"] = _failure_phase(failure_phase)
                if native == RunState.CANCELLED:
                    changes["failure_phase"] = None
            if same_file_phase:
                if manifest is not latest.files:
                    changes["files"] = manifest
                if file_state is not None:
                    if file_failure is not _UNCHANGED:
                        changes["file_failure_phase"] = _failure_phase(file_failure)
                    if file_error is not _UNCHANGED:
                        changes["file_message"] = file_error
            if all(getattr(latest, key) == value for key, value in changes.items()):
                return latest
            return replace(latest, updated_at=_now(), **changes)

        return self._store_for(record).update(record, merge)

    def _is_stale_submitting(self, record, *, now=None):
        if record.state != RunState.SUBMITTING or record.execution.backend_id is not None:
            return False
        now = now or datetime.now(timezone.utc)
        return (now - _timestamp(record.updated_at)).total_seconds() >= self.submitting_grace

    def recover_submitting(self, record):
        return run_coro_sync(self.recover_submitting_async(record))

    async def recover_submitting_async(self, record):
        store = self._store_for(record)
        lock = store.observation_lock(record)
        if not lock.acquire(blocking=False):
            return store.get(record.id)
        try:
            recovered, acknowledge = await self._recover_submitting(record)
        finally:
            lock.release()
        if acknowledge:
            await self._acknowledge_result_async(recovered)
        return recovered

    async def _recover_submitting(self, record):
        record = resolve_record(record, self.store)
        if not record.execution.available or not self._is_stale_submitting(record):
            return record, False
        if BackendCapability.RECOVER not in record.execution.backend.supports:
            return self._commit(
                record, state=RunState.UNKNOWN,
                message="The selected backend has no submission recovery",
            ), False
        client = self.client_for(record)
        from kkrunner.runs.model import WorkingDirectory
        from kkrunner.client.contracts import SubmissionContext
        recovery = await client.recover_submission_async(
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
            attached = self._commit(record, execution=execution, files=files)
            await self._acknowledge_submission_async(attached, client=client)
            status, = await client.status_async(JobAttachment(attached.execution, attached.files))
            return await self._record_observation_owned(attached, status)
        if recovery.disposition is RecoveryDisposition.UNSUPPORTED:
            return self._commit(
                record, state=RunState.UNKNOWN,
                message=recovery.message or "Submission recovery is not supported by this client",
            ), False
        if recovery.disposition is not RecoveryDisposition.ABSENT:
            return record, False

        def mark_absent(latest):
            if latest.state != RunState.SUBMITTING or latest.execution.backend_id is not None:
                return latest
            now = _now()
            return replace(
                latest,
                state=RunState.FAILED,
                file_state=RunFileState.DONE,
                updated_at=now,
                finished_at=now,
                message="Stale submission was not found by its execution backend",
                failure_phase=FailurePhase.SUBMISSION,
                files=latest.files.skip_pending_artifacts(),
            )

        return self._store_for(record).update(record, mark_absent), False

    def recover_preprocessing(self, record):
        """Recover abandoned preprocessing without consulting an execution backend."""
        store = self._store_for(record)
        lock = store.observation_lock(record)
        if not lock.acquire(blocking=False):
            return store.get(record.id)
        try:
            latest = store.get(record.id)
            if latest.file_state != RunFileState.PREPROCESSING:
                return latest
            return self._commit(
                latest, **self.completion_fields(
                    latest, files=latest.files.skip_pending_artifacts(),
                    file_state=RunFileState.FAILED,
                    file_failure_phase=FailurePhase.PREPROCESSING,
                    file_message="Client preprocessing was interrupted before submission",
                ),
            )
        finally:
            lock.release()

    def begin_submission(self, record):
        """Publish the submission boundary with exclusive file ownership."""
        owner = self._claim_client_files(record)
        try:
            return self._begin_submission_owned(record)
        finally:
            owner.release()

    def _begin_submission_owned(self, record):
        """Publish the irreversible submission phase immediately before backend submit."""
        if record.file_state != RunFileState.PREPROCESSING:
            raise ValueError("Run is not preprocessing")
        return self._commit(record, state=RunState.SUBMITTING,
                               file_state=RunFileState.WAITING, expected_record=record)

    @contextmanager
    def preprocessing_reservation(self, spec):
        """Protect save-only preprocessing without creating a history entry."""
        record = RunManager.new_record(
            self, directory=spec.directory, files=FileManifest.from_spec(spec),
        )
        with self._scoped_reservation(record) as reserved:
            yield
            sync_result_files((*reserved.files.output_files, *(
                entry.local_path for entry in reserved.files.input_entries
                if entry.write and entry.local_path is not None
            )))

    @contextmanager
    def _scoped_reservation(self, record, *, conflict_stores=()):
        """Reserve a synchronous scope in the internal store, never in run history."""
        store = _TEMPORARY_RESERVATIONS
        record = replace(record, store=store)
        with store.observation_lock(record):
            reserved = self._reserve(record, lambda: store.save(
                replace(record, file_state=RunFileState.PREPROCESSING, updated_at=_now()),
            ), conflict_stores=conflict_stores)
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
        store = self._store_for(record)
        stores = self._reservation_stores(*(() if store is None else (store,)))
        with ExitStack() as locks:
            for store in stores:
                locks.enter_context(store.submission_lock())
            return self._conflicts(record, stores)

    def reserve(self, record, *, preprocessing=False, recover_conflicts=False):
        """Reserve atomically; optionally request a separate conflict recovery first."""
        if recover_conflicts:
            return run_coro_sync(self.reserve_async(
                record, preprocessing=preprocessing, recover_conflicts=True,
            ))
        if self._store_for(record).lock_key != self.store.lock_key:
            raise ValueError("Cannot reserve a record owned by another store")
        record = replace(record, store=self.store)

        state = RunState.NEW if preprocessing else RunState.SUBMITTING
        file_state = RunFileState.PREPROCESSING if preprocessing else RunFileState.WAITING

        def activate(latest):
            if latest != record or latest.state != RunState.NEW or latest.file_state != RunFileState.NEW:
                raise ValueError(f"Run id {record.id!r} already exists; use attach()")
            return replace(latest, state=state, file_state=file_state, updated_at=_now())

        return self._reserve(record, lambda: self.store.update(record, activate))

    async def reserve_async(self, record, *, preprocessing=False, recover_conflicts=False):
        def attempt():
            return self.reserve(record, preprocessing=preprocessing)

        if recover_conflicts:
            return await self._reserve_recovering(attempt)
        return attempt()

    def _reserve(self, record, activate, *, conflict_stores=()):
        """Check the footprint and activate a record under the same store locks."""
        stores = self._reservation_stores(self._store_for(record), *conflict_stores)
        with ExitStack() as locks:
            for store in stores:
                locks.enter_context(store.submission_lock())
            conflicts = self._conflicts(record, stores)
            if conflicts:
                raise DuplicateSubmissionError(record, conflicts)
            return activate()

    @contextmanager
    def file_mutation(self, entries, *, remote_of=None):
        """Reserve files for a destructive operation within this client process.

        Entries specify exact local paths (or remote paths in remote_of's
        target namespace) and file/directory kinds. Check existing tracked
        reservations, then keep a process-local reservation until the operation
        returns. Unrelated submissions remain possible. Other processes cannot
        see the temporary reservation; no run is added to history.
        """
        entries = tuple(entries)
        if remote_of is None:
            if any(entry.local_path is None or entry.remote_path is not None
                   or not os.path.isabs(entry.local_path) for entry in entries):
                raise ValueError("File mutation requires absolute local paths")
            scope = RunManager.new_record(
                self, directory=os.getcwd(), files=FileManifest(outputs=entries),
            )
            conflict_stores = ()
        else:
            execution = remote_of.execution
            if not execution.target.transfers_files(shared_filesystem=remote_of.files.shared_filesystem):
                raise ValueError("File mutation requires a separate target filesystem")
            if execution.directory.remote is None:
                raise ValueError("Remote file mutation requires a target working directory")
            if any(entry.remote_path is None or entry.local_path is not None
                   for entry in entries):
                raise ValueError("Remote file mutation requires remote-only paths")
            scope = RunManager.new_record(
                self, directory=execution.directory.local, remote_directory=execution.directory.remote,
                target=execution.target,
                files=FileManifest(outputs=entries, mapping=remote_of.files.mapping),
            )
            store = self._store_for(remote_of)
            conflict_stores = () if store is None else (store,)
        with self._scoped_reservation(scope, conflict_stores=conflict_stores):
            yield

    async def _reserve_recovering(self, attempt):
        """Explicit higher-level policy: recover abandoned conflicts outside reservation locks."""
        attempted_refresh = set()
        while True:
            try:
                return attempt()
            except DuplicateSubmissionError as exc:
                refreshable = tuple(
                    conflict for conflict in exc.conflicts
                    if conflict.store is not _TEMPORARY_RESERVATIONS
                    and self._record_key(conflict) not in attempted_refresh
                    and (
                        conflict.file_state == RunFileState.PREPROCESSING
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
            await self.refresh_async(refreshable)

    @contextmanager
    def _observation_scope(self, record, status):
        """Publish native facts even when another client owns file finalization."""
        if not status.state.is_terminal and status.file_error is None:
            yield None
            return
        owner = self._store_for(record).observation_lock(record)
        if not owner.acquire(blocking=False):
            yield self._record_execution(record, status)
            return
        try:
            yield None
        finally:
            owner.release()

    def _record_execution(self, record, status, *, observed_from=None):
        execution = status.execution
        if record.execution.backend_id is not None:
            # Identity and working directory already belong to the record.
            # An observation cannot change the execution configuration.
            if (status.backend_id not in (None, record.execution.backend_id)
                    or (execution is not None
                        and execution.backend_id not in (None, record.execution.backend_id))):
                raise ValueError("Backend status returned another identity")
            execution = None
        elif execution is None and status.backend_id is not None:
            execution = record.execution.submitted(status.backend_id)
        elif execution is not None and record.execution.submission_key is not None:
            if execution.submission_key is None:
                execution = replace(execution, submission_key=record.execution.submission_key)
            elif execution.submission_key != record.execution.submission_key:
                raise ValueError("Backend status returned another submission key")
        return self._commit(
            record, state=status.state, execution=execution,
            return_code=status.return_code, message=status.message,
            failure_phase=status.failure_phase,
            observed_from=observed_from,
        )

    @staticmethod
    def _native_status(record):
        return RunStatus(
            record.state, backend_id=record.execution.backend_id,
            return_code=record.return_code, message=record.message,
            failure_phase=record.failure_phase,
        )

    def _apply_observation(self, record, status):
        """Persist supplied facts only; caller owns file progress, not the backend."""
        current = self._record_execution(record, status)
        return self._apply_file_observation(record, current, status)

    def _apply_file_observation(self, record, current, status):
        if ((not status.state.is_terminal and status.file_error is None) or status.output_pending
                or not current.is_active or not current.state.is_terminal
                or current.file_state in (RunFileState.READY, RunFileState.POSTPROCESSING)):
            return current
        # COLLECTING proves that publication has already been confirmed. A late
        # reply about the publisher (e.g. after receipt cleanup) cannot undo it.
        if current.file_state == RunFileState.COLLECTING and status.file_error is not None:
            return current
        # The supplied manifest is a snapshot from the original observation.
        # A newer file owner may already have frozen a plan or retried it.
        files = (self._merged_result_files(current, status.files)
                 if current.file_state == record.file_state and current.files == record.files
                 else current.files)
        if status.file_error is not None:
            return self._commit(
                current, file_state=RunFileState.FAILED,
                file_failure_phase=FailurePhase.OUTPUT, file_message=status.file_error,
                files=files.fail_pending_artifacts(status.file_error),
            )
        if current.failure_phase in NO_EXECUTION_FAILURES:
            return self._commit(current, file_state=RunFileState.DONE,
                                   files=files.skip_pending_artifacts())
        if files.failed_artifacts:
            return self._commit(
                current, file_state=RunFileState.FAILED,
                file_failure_phase=self._collection_failure_phase(current, files),
                file_message=files.availability_message, files=files,
            )
        phase = RunFileState.COLLECTING
        if files.captured_at is not None:
            phase = RunFileState.READY if current.processing_required else RunFileState.DONE
        return self._commit(current, file_state=phase, files=files)

    def update(self, record, status: RunStatus):
        """Apply supplied facts; never query the backend or collect files."""
        with self._observation_scope(record, status) as existing:
            return self._apply_observation(record, status) if existing is None else existing

    @staticmethod
    def _collection_failure_phase(record, files):
        return (FailurePhase.DOWNLOAD
                if record.execution.target.transfers_files(shared_filesystem=files.shared_filesystem)
                else FailurePhase.ARTIFACTS)

    async def _observe_async(self, record, status, *, retrieve_ended=False):
        """Own file collection only until its changes have been committed."""
        with self._observation_scope(record, status) as existing:
            if existing is not None:
                return existing
            current, acknowledge = await self._record_observation_owned(
                record, status, retrieve_ended=retrieve_ended,
            )
        if acknowledge:
            await self._acknowledge_result_async(current)
        return current

    async def _record_observation_owned(self, record, status, *, retrieve_ended=False):
        """Apply supplied facts and collect files, returning whether receipt cleanup is due."""
        previous = self._record_execution(record, status)
        current = self._apply_file_observation(record, previous, status)
        if current.file_state != RunFileState.COLLECTING:
            return current, current.file_state != previous.file_state
        if not status.state.is_terminal or status.output_pending or status.file_error is not None:
            return current, False
        transfers = current.execution.target.transfers_files(shared_filesystem=current.files.shared_filesystem)
        # Only a captured manifest can finish collection without I/O.
        # A supplied but uncaptured status manifest is not a result snapshot.
        if transfers and not retrieve_ended:
            return current, False
        from kkrunner.client.file_access import FileAccess
        files, error = await FileAccess(current.execution.target).collect_ended_files_async(
            current, self._native_status(current),
            on_resolved=lambda manifest: self._commit(current, files=manifest),
        )
        if error is not None:
            fields = self.completion_fields(
                current, files=files, file_state=RunFileState.FAILED,
                file_failure_phase=self._collection_failure_phase(current, files),
                file_message=str(error),
            )
        else:
            if files.captured_at is None:
                raise RuntimeError("Successful file collection did not capture its artifacts")
            fields = self.completion_fields(
                current, files=files,
                file_state=RunFileState.READY if current.processing_required else RunFileState.DONE,
            )
        return self._commit(current, **fields), True

    def completion_fields(
        self, record, *, file_state, files=None, file_failure_phase=None, file_message=None,
    ):
        """Prepare file-only record changes, without I/O or native state interpretation."""
        result_files = self._merged_result_files(record, files)
        if file_failure_phase == FailurePhase.OUTPUT:
            result_files = result_files.fail_pending_artifacts(file_message or "Output publication failed")
        if result_files.failed_artifacts and file_failure_phase is None:
            file_state = RunFileState.FAILED
            file_failure_phase = self._collection_failure_phase(record, result_files)
            file_message = result_files.availability_message or file_message
        return dict(files=result_files, file_state=file_state,
                    file_failure_phase=file_failure_phase, file_message=file_message)

    def _import_completion_fields(
        self, record, *, state, return_code=None, failure_phase=None,
        artifacts_available=False, files=None, file_state=None,
        file_failure_phase=None, file_message=None, **native,
    ):
        """Explicitly capture existing local results supplied to complete()."""
        requested = RunState(state)
        effective = self._merged_state(record, requested, return_code=return_code)
        phase = _failure_phase(failure_phase)
        code = return_code
        if record.state.is_terminal:
            _, code = merge_terminal_outcome(record.state, record.return_code, requested, return_code)
            if effective == record.state and (requested != effective or record.failure_phase is not None):
                phase = record.failure_phase
        files = self._merged_result_files(record, files)
        if file_failure_phase == FailurePhase.OUTPUT or phase in NO_EXECUTION_FAILURES or effective == RunState.NEW:
            artifacts_available = False
        if file_state is None:
            file_state = (RunFileState.READY if record.processing_required and artifacts_available
                          else RunFileState.DONE)
        if artifacts_available and not files.failed_artifacts and files.captured_at is None:
            try:
                files = capture_result_files(
                    files, require_outputs=effective == RunState.FINISHED and code == 0,
                )
            except OSError as exc:
                files = files.fail_pending_artifacts(str(exc))
                file_state, file_failure_phase, file_message = (
                    RunFileState.FAILED, FailurePhase.ARTIFACTS, str(exc),
                )
        if phase in NO_EXECUTION_FAILURES or effective == RunState.NEW:
            files = files.skip_pending_artifacts()
        fields = self.completion_fields(
            record, files=files, file_state=file_state,
            file_failure_phase=file_failure_phase, file_message=file_message,
        )
        return dict(state=requested, return_code=return_code, failure_phase=failure_phase,
                    **native, **fields)

    def complete(self, record, *, expected_record=None, **kwargs):
        return run_coro_sync(self.complete_async(record, expected_record=expected_record, **kwargs))

    async def complete_async(self, record, *, expected_record=None, **kwargs):
        """Record explicit completion, optionally importing existing local artifacts."""
        owner = self._claim_client_files(record)
        try:
            try:
                current = resolve_record(record, self.store)
            except FileNotFoundError:
                # An explicit completion may import a new record.
                if record.state != RunState.NEW or record.file_state != RunFileState.NEW:
                    raise
                current = record
            if current.file_state.is_terminal:
                # The file transaction is over, but a delayed native exit
                # may still refine the independent calculation outcome.
                return self._commit(
                    current, expected_record=expected_record,
                    state=kwargs["state"], execution=kwargs.get("execution", _UNCHANGED),
                    return_code=kwargs.get("return_code", _UNCHANGED),
                    message=kwargs.get("message", _UNCHANGED),
                    failure_phase=kwargs.get("failure_phase", _UNCHANGED),
                )
            if current.file_state != record.file_state or current.files != record.files:
                raise ProcessingPending("File progress changed before completion")
            updated = self._commit(
                current, expected_record=expected_record,
                **self._import_completion_fields(current, **kwargs),
            )
        finally:
            owner.release()
        await self._acknowledge_result_async(updated)
        return updated

    async def _reopen_download_owned_async(self, current):
        """Restore the reservation under the existing file ownership lock."""
        store = self._store_for(current)

        def reopen(latest):
            if (latest.file_state != RunFileState.FAILED
                    or latest.file_failure_phase != FailurePhase.DOWNLOAD
                    or not latest.state.is_terminal
                    or latest.files.captured_at is not None):
                raise ValueError("Only failed downloads of ended jobs can be retried")
            if latest.files.remote_patterns_resolved is False:
                raise ValueError("Remote result pattern discovery failed; there is no download plan to retry")
            return replace(latest, file_state=RunFileState.COLLECTING, finished_at=None,
                           file_failure_phase=None, file_message=None,
                           files=latest.files.retry_failed_artifacts(), updated_at=_now())

        collecting = replace(current, file_state=RunFileState.COLLECTING)
        return await self._reserve_recovering(
            lambda: self._reserve(collecting, lambda: store.update(current, reopen)),
        )

    def collect_files(self, run):
        """Finish confirmed output collection, including an explicit download retry.

        Only the filesystem target is required for collection. Native execution
        is never restarted or observed; postprocessing is a separate operation.
        """
        return run_coro_sync(self.collect_files_async(run))

    async def collect_files_async(self, run):
        with self._history_edit(run) as (_, current):
            if current.file_state in (RunFileState.READY, RunFileState.DONE, RunFileState.POSTPROCESSED):
                return current
            if not current.state.is_terminal or not current.execution.target.available:
                raise ValueError("File collection requires a confirmed exit and an available target")
            if current.file_state == RunFileState.FAILED:
                current = await self._reopen_download_owned_async(current)
            if current.file_state != RunFileState.COLLECTING:
                raise ValueError("Backend output has not been confirmed ready for collection")
            current, acknowledge = await self._record_observation_owned(
                current, self._native_status(current), retrieve_ended=True,
            )
        if acknowledge:
            await self._acknowledge_result_async(current)
        return current

    async def _begin_processing_async(self, record, *, retry=False):
        """Enter processing under the caller's existing observation lock."""
        store = self._store_for(record)
        latest = store.get(record.id)
        if not latest.processing_required or not latest.state.is_terminal:
            raise ValueError("Processing requires an ended calculation and a declared processor")
        if retry:
            if not (latest.file_state in (RunFileState.READY, RunFileState.POSTPROCESSING)
                    or (latest.file_state == RunFileState.FAILED
                        and latest.file_failure_phase == FailurePhase.POSTPROCESSING)):
                raise ValueError("Only ready, failed or interrupted postprocessing can be retried")
        elif latest.file_state != RunFileState.READY:
            raise ProcessingPending("Run files are not awaiting processing")
        if latest.files.captured_at is None:
            raise ValueError("Postprocessing requires captured artifacts")

        def activate(current):
            if current.file_state != latest.file_state:
                raise ProcessingPending("File phase changed before processing claim")
            return replace(current, file_state=RunFileState.POSTPROCESSING,
                           updated_at=_now(), finished_at=None,
                           file_failure_phase=None, file_message=None)

        claimed = latest
        if not latest.is_active:
            # Compare the prospective processing footprint, not the ended
            # calculation's original producer/transfer footprint.
            processing = replace(latest, file_state=RunFileState.POSTPROCESSING)
            claimed = await self._reserve_recovering(
                lambda: self._reserve(processing, lambda: store.update(latest, activate)),
            )
        try:
            # READY means a complete snapshot was committed by collection.
            # Postprocessing must never create a new provenance baseline.
            manifest = claimed.files
            if manifest.failed_artifacts:
                raise OSError(manifest.availability_message or "Output artifacts unavailable")
            if any(v.state() is not FileState.CURRENT for v in manifest.output_versions):
                raise ValueError("Postprocessing requires unchanged historical artifacts")
        except BaseException:
            if not latest.is_active:
                store.update(claimed, lambda current: replace(
                    current, file_state=latest.file_state, finished_at=latest.finished_at,
                    file_failure_phase=latest.file_failure_phase, file_message=latest.file_message,
                    updated_at=_now(),
                ))
            raise

        if not latest.is_active:
            return claimed
        return store.update(claimed, activate)

    @staticmethod
    def _result_outcome(record, outcome=None):
        """Return only named artifacts belonging to the captured calculation."""
        from kkrunner.runs.base import ExecutionOutcome

        files = record.files
        if outcome is None:
            outcome = ExecutionOutcome(
                input_file=files.input_file, output_file=files.output_file,
                error_file=None if files.stderr is None else files.stderr.local_path,
            )
        outcome.state = record.state
        outcome.return_code = record.return_code
        return captured_outcome(files, outcome)

    def _process_outcome(self, record, process_result, *, outcome=None):
        """Interpret the captured files, under the caller's observation lock."""
        if process_result is None:
            raise ProcessingPending("Postprocessing requires a client callback")
        if record.files.failed_artifacts:
            raise OSError(record.files.availability_message or "Output artifacts unavailable")
        result = process_result(self._result_outcome(record, outcome))
        latest = self._store_for(record).get(record.id)
        if latest.files.failed_artifacts:
            raise OSError(latest.files.availability_message or "Output artifacts unavailable")
        if any(v.state() is not FileState.CURRENT for v in latest.files.output_versions):
            raise RuntimeError("Historical artifacts changed during postprocessing")
        return result

    def retry_postprocessing(self, run, process_result):
        """Retry a failed or interrupted processing attempt without rerunning a job."""
        if not callable(process_result):
            raise TypeError("Postprocessing retry requires a callable result processor")
        record = resolve_record(run, self.store) if isinstance(run, str) else run
        lock = self._store_for(record).observation_lock(record)
        if not lock.acquire(blocking=False):
            raise ProcessingPending("Another client owns postprocessing")
        try:
            claimed = run_coro_sync(self._begin_processing_async(record, retry=True))
            try:
                result = self._process_outcome(claimed, process_result)
            except ProcessingPending:
                raise
            except BaseException as exc:
                self._commit(
                    claimed, file_state=RunFileState.FAILED, file_message=str(exc),
                    file_failure_phase=FailurePhase.POSTPROCESSING,
                )
                raise
            # Purely local processing; receipt cleanup, if still needed,
            # belongs to explicit acknowledgement reconciliation.
            completed = self._commit(claimed, file_state=RunFileState.POSTPROCESSED)
            if completed.file_state != RunFileState.POSTPROCESSED:
                raise OSError(completed.file_message or "Postprocessing could not be committed")
            return result
        finally:
            lock.release()

    def _acknowledge_result(self, record):
        return run_coro_sync(self._acknowledge_result_async(record))

    async def _acknowledge_result_async(self, record):
        """Best-effort ACK after durable completion; failed ACKs remain retryable."""
        # Once the complete manifest is captured, the native receipt is no
        # longer needed for local processing, even if its reservation is active.
        prepared = (record.file_state in (RunFileState.READY, RunFileState.POSTPROCESSING)
                    and record.files.captured_at is not None)
        if ((record.is_active and not prepared) or not record.state.is_terminal
                or record.execution.backend_id is None
                or BackendCapability.ACKNOWLEDGE not in record.execution.backend.supports):
            return False
        if (record.file_failure_phase == FailurePhase.DOWNLOAD
                and record.files.remote_patterns_resolved is not False):
            # Retain the terminal backend receipt until collection settles.
            return False
        # The backend ACK is idempotent. Do not serialize unrelated remote
        # operations or retain a second, process-local history of completions.
        try:
            client = self.client_for(record)
            return await client.acknowledge_async(record)
        except (OSError, SubprocessError, RuntimeError, ValueError):
            return False

    def reconcile_acknowledgements(self, *, on_error=None):
        """Explicitly retry receipt ACKs; return IDs whose outcome receipts were removed."""
        options = {} if on_error is None else {"on_error": on_error}
        acknowledged = []
        for record in self.store.runs(**options):
            if not record.execution.available:
                continue
            self._acknowledge_submission(record)
            if self._acknowledge_result(record):
                acknowledged.append(record.id)
        return tuple(acknowledged)

    def _acknowledge_submission(self, record, *, client=None):
        return run_coro_sync(self._acknowledge_submission_async(record, client=client))

    async def _acknowledge_submission_async(self, record, *, client=None):
        """Discard the submission receipt only after RunStore is durable."""
        if (record.execution.backend_id is None
                or BackendCapability.ACKNOWLEDGE not in record.execution.backend.supports):
            return
        try:
            if client is None:
                client = self.client_for(record)
            await client.acknowledge_submission_async(record)
        except (OSError, SubprocessError, RuntimeError):
            # Explicit receipt reconciliation can retry this cleanup.
            pass

    def refresh(self, records=None, *, on_error=None, retrieve_ended=False):
        return run_coro_sync(self.refresh_async(
            records, on_error=on_error, retrieve_ended=retrieve_ended,
        ))

    async def refresh_async(self, records=None, *, on_error=None, retrieve_ended=False):
        """Refresh native facts and optionally collect ended jobs' files."""
        options = {} if on_error is None else {"on_error": on_error}
        if records is None:
            records = tuple(self.store.runs(active=True, **options))
        else:
            records = tuple(
                resolve_record(record, self.store)
                if record.execution.backend_id is None and record.state not in (RunState.NEW,)
                and record.is_active
                else record
                for record in records
            )
        current = {self._record_key(record): record for record in records}
        groups = {}
        for record in tuple(current.values()):
            key = self._record_key(record)
            if record.file_state == RunFileState.PREPROCESSING:
                try:
                    record = self.recover_preprocessing(record)
                except FileNotFoundError:
                    current.pop(key, None)
                    continue
                current[key] = record
                if record.file_state == RunFileState.PREPROCESSING:
                    continue
            if record.file_state in (RunFileState.READY, RunFileState.POSTPROCESSING):
                current[key] = resolve_record(record, self.store)
                continue
            if record.state.is_terminal and record.file_state == RunFileState.COLLECTING:
                # The producer's publication is already confirmed. Only the
                # filesystem target is needed, not a backend or job identity.
                if record.execution.target.available:
                    status = self._native_status(record)
                    current[key] = await self._observe_async(
                        record, status, retrieve_ended=retrieve_ended,
                    )
                continue
            if not record.execution.available:
                continue
            if record.execution.backend_id is None:
                if record.is_active and self._is_stale_submitting(record):
                    current[key] = await self.recover_submitting_async(record)
                continue
            if record.state.is_terminal and not record.is_active:
                continue
            client = self.client_for(record)
            groups.setdefault(client, []).append(record)

        for client, group in groups.items():
            if BackendCapability.STATUS not in client.backend.supports:
                for record in group:
                    current[self._record_key(record)] = self._commit(
                        record, state=RunState.UNKNOWN,
                        message="This execution backend has no independent status query",
                    )
                continue
            statuses = await client.status_async(
                *(JobAttachment(record.execution, record.files) for record in group)
            )
            if len(statuses) != len(group):
                raise RuntimeError("Target returned wrong number of run statuses")
            for record, status in zip(group, statuses):
                current[self._record_key(record)] = await self._observe_async(
                    record, status, retrieve_ended=retrieve_ended,
                )
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
        # A supplied record already identifies its observation lock. Read
        # the current contents only after claiming that lock.
        record = store.get(run) if isinstance(run, str) else run
        owner = self._claim_client_files(record)
        try:
            yield store, store.get(record.id)
        finally:
            owner.release()

    def mark_failed(self, run, *, message=None):
        """Explicitly override an unowned record, without asserting process termination."""
        with self._history_edit(run) as (_, record):
            return self._commit(record, file_state=RunFileState.FAILED,
                                   expected_record=record, file_message=message)

    def forget(self, run, *, force=False):
        """Remove stored history without touching native processes or result files."""
        with self._history_edit(run) as (store, record):
            return store.forget(record, force=force)

    def cancel(self, run):
        return run_coro_sync(self.cancel_async(run))

    async def cancel_async(self, run):
        """Request native cancellation and record its facts, without collecting files.

        The live observer retains its file ownership. Without one, confirmed
        publication makes collection available as a separate operation.
        """
        record = resolve_record(run, self.store)
        if record.state.is_terminal:
            return record
        if record.execution.backend_id is None:
            raise UnsupportedCapability("No durable execution reference is available for cancellation")
        client = self.client_for(record)
        status = await client.cancel_async(JobAttachment(record.execution, record.files))
        return self.update(record, status)

    def attach(
        self, run, *, process_result=None, client=None,
        callback=None, output_listener=None, owned_resources=None,
    ):
        return run_coro_sync(self.attach_async(
            run, process_result=process_result, client=client, callback=callback,
            output_listener=output_listener, owned_resources=owned_resources,
        ))

    async def attach_async(
        self, run, *, process_result=None, client=None,
        callback=None, output_listener=None, owned_resources=None,
    ):
        """Open a tracked Run without resubmitting or observing its backend job."""
        from kkrunner.runs.run import Run
        record = resolve_record(run, self.store) if isinstance(run, str) else run
        owner = self._claim_client_files(record)
        parts = None
        try:
            record = resolve_record(record, self.store)
            if not record.is_active:
                raise RuntimeError("Run files are already finalized; use explicit retry")
            if record.file_state in (RunFileState.READY, RunFileState.POSTPROCESSING):
                raise ProcessingPending("Use retry_postprocessing() for already collected files")
            if record.file_state == RunFileState.COLLECTING:
                raise ProcessingPending("Use collect_files() for already published outputs")
            if process_result is not None and not record.processing_required:
                raise ValueError("This Run did not request postprocessing")
            if record.execution.backend_id is None:
                raise RuntimeError(f"Run {record.id} has no backend reference")
            client = self.client_for(record) if client is None else client
            parts = await client.open_run_parts_async(
                JobAttachment(record.execution, record.files),
                output_listener=output_listener,
            )
            self.register_client(client)
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
        # Ownership was transferred to Run. The caller explicitly observes
        # and finalizes it with wait()/wait_async(); no background work starts.
        return attached
