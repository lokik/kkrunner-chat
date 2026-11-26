"""Persistent run history and lifecycle orchestration."""

from __future__ import annotations

import os
from contextlib import contextmanager
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
from kkrunner.transports import (
    LocalTransport,
    SshTransport,
    require_persistable_transport,
)
from kkrunner.files.manifest import FileManifest
from kkrunner.files.versions import FileState
from kkrunner.runs.base import ExecutionSpec, FailurePhase, RunState, RunStatus
from kkrunner.runs.conflicts import conflicting_files, submission_conflicts
from kkrunner.runs.files import RunFiles, capture_result_files, file_is_current, sync_result_files
from kkrunner.runs.model import ExecutionMetadata, WorkingDirectory
from kkrunner.runs.processing import ProcessingPending
from kkrunner.runs.records import RunRecord

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
_NO_ARTIFACTS_FAILURES = {
    FailurePhase.PREPROCESSING,
    FailurePhase.SUBMISSION,
    FailurePhase.LAUNCH,
    FailurePhase.OUTPUT,
    FailurePhase.ARTIFACTS,
    FailurePhase.DOWNLOAD,
}


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
        self.file_conflicts = {other.id: conflicting_files(record, other) for other in self.conflicts}
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
                from platformdirs import user_state_dir
                RunManager._fallback = RunManager(RunStore(Path(user_state_dir("kkrunner")) / "projects_runs"))
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

    def __init__(self, store=None, *, client_pool=None, submitting_grace=60.0):
        if submitting_grace < 0:
            raise ValueError("submitting_grace must be non-negative")
        self.store = store
        self.client_pool = client_pool if client_pool is not None else default_client_pool()
        self._files = RunFiles(store=self.store, client_pool=self.client_pool)
        self._ack_lock = Lock()
        self._acknowledged_results = set()
        self.submitting_grace = float(submitting_grace)

    def register_client(self, client):
        key = (require_persistable_transport(client.transport), client.backend_config)
        self.client_pool.get(key, lambda: client)
        return key

    def client_for(self, record):
        return self.client_pool.for_config(record.execution.transport, record.execution.backend)

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
        return self._files.retrieve_result_files(run)

    def new_record(
        self,
        *,
        directory,
        transport=None,
        backend=None,
        remote_directory=None,
        files=None,
        run_id=None,
        project_id=None,
        metadata=None,
        processing_required=False,
        submission_key=None,
        retain_history=True,
        submission_started=None,
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
                LocalTransport() if transport is None else transport,
                WorkingDirectory(local, remote_directory),
                submission_key=submission_key,
                backend=backend or BackendConfig(),
            ),
            files=files if files is not None else FileManifest(),
            project_id=project_id,
            retain_history=retain_history,
            submission_started=submission_started,
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
        expected_processing_attempt=None,
        submission_started=_UNCHANGED,
    ):
        def merge(latest):
            if latest.state == RunState.POSTPROCESSING and expected_processing_attempt is None:
                # An external observation cannot complete or fail a run after
                # its processor has claimed ownership. This check must live
                # inside the RunStore update lock; checking before transition
                # leaves a race between refresh and the processing claim.
                return latest
            if expected_processing_attempt is not None and (
                latest.processing_attempt != expected_processing_attempt or latest.state != RunState.POSTPROCESSING
            ):
                raise ProcessingPending("Processing attempt no longer owns completion")
            if expected_processing_attempt is not None and state == RunState.FINISHED and latest.files.failed_artifacts:
                raise OSError(latest.files.availability_message or "Artifacts became unavailable during processing")
            if expected_record is not None and latest != expected_record:
                raise RuntimeError("Run changed during postprocessing retry")
            if expected_updated_at is not _UNCHANGED and latest.updated_at != expected_updated_at:
                # An independent refresh has superseded this observation.
                return latest
            final_locked = latest.state.is_terminal
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
            if submission_started is not _UNCHANGED:
                if final_locked or latest.submission_started is True and submission_started is not True:
                    raise ValueError("Native submission boundary cannot be retracted")
                changes["submission_started"] = submission_started
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

        return self.store.update(record, merge)

    def _is_stale_submitting(self, record, *, now=None):
        if record.state != RunState.SUBMITTING or record.execution.backend_id is not None:
            return False
        if record.submission_started is False:
            # No backend operation is possible until the owner commits the
            # irreversible boundary. Its lock, not the clock, protects staging.
            return True
        now = now or datetime.now(timezone.utc)
        return (now - _timestamp(record.updated_at)).total_seconds() >= self.submitting_grace

    def recover_submitting(self, record):
        owner = self.store.observation_lock(record)
        if not owner.acquire(blocking=False):
            return self.store.get(record.id)
        try:
            return self._recover_submitting(record)
        finally:
            owner.release()

    def _recover_submitting(self, record):
        record = self.store.get(record.id)
        if not self._is_stale_submitting(record):
            return record
        if record.submission_started is False:
            return self.complete(
                record, state=RunState.FAILED, failure_phase=FailurePhase.SUBMISSION,
                message="Client staging was interrupted before native submission",
            )
        if not record.execution.available:
            return record
        client = self.client_for(record)
        if not hasattr(client, "recover_submission"):
            # A tracked direct process has no durable backend identity. Keep
            # uncertainty rather than claiming that no process was launched.
            return self.transition(
                record, state=RunState.UNKNOWN,
                message="Submission recovery is not supported by this client",
            )
        from kkrunner.runs.model import WorkingDirectory
        try:
            from kkrunner.client.contracts import SubmissionContext
            recovery = client.recover_submission(
                record.execution.submission_key or record.id,
                SubmissionContext(record.execution),
                submitted_at=record.updated_at,
            )
        except NotImplementedError:
            return record
        if recovery.disposition is RecoveryDisposition.FOUND:
            config = record.execution.transport
            if recovery.ref.backend != record.execution.backend.name:
                raise ValueError("Recovered job belongs to a different backend")
            execution = record.execution.submitted(recovery.ref.token)
            if recovery.directory is not None and recovery.directory != execution.directory.target:
                execution = replace(
                    execution,
                    directory=WorkingDirectory(execution.directory.local, recovery.directory),
                )
            files = record.files
            if isinstance(config, SshTransport):
                files = files.with_remote_stream_paths(
                    stdout=recovery.output_file, stderr=recovery.error_file,
                )
            # Persist identity and stream paths before asking the backend for
            # status. A failed observation must not lose the recovered identity.
            attached = self.transition(record, execution=execution, files=files)
            return self.update(
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

        return self.store.update(record, mark_absent)

    def recover_preprocessing(self, record):
        """Abandon preprocessing only after its owning client has released its OS lock."""
        lock = self.store.preprocessing_lock(record)
        if not lock.acquire(blocking=False):
            return self.store.get(record.id)
        try:
            latest = self.store.get(record.id)
            if latest.state != RunState.PREPROCESSING:
                return latest
            return self.complete(
                latest, state=RunState.FAILED, failure_phase=FailurePhase.PREPROCESSING,
                message="Client preprocessing was interrupted before submission",
            )
        finally:
            lock.release()

    @contextmanager
    def preprocessing_reservation(self, spec):
        """Reserve local application reads/writes without submitting a backend job.

        Use this for operations such as saving generated input files. The spec
        declares the local footprint only; no native execution is constructed.
        The reservation is hidden from history and collected after cleanup.
        """
        record = self.new_record(
            directory=spec.directory, files=FileManifest.from_spec(spec), retain_history=False,
        )
        reserved = None
        try:
            with self.store.observation_lock(record), self.store.preprocessing_lock(record):
                reserved = self.reserve(record, preprocessing=True)
                try:
                    yield
                    sync_result_files((*reserved.files.input_files, *(
                        entry.local_path for entry in reserved.files.preprocessing_files
                        if entry.local_path is not None
                    )))
                except BaseException as exc:
                    reserved = self.complete(
                        reserved, state=RunState.FAILED, failure_phase=FailurePhase.PREPROCESSING,
                        message=str(exc),
                    )
                    raise
                else:
                    reserved = self.complete(reserved, state=RunState.FINISHED)
        finally:
            if reserved is not None and reserved.state.is_terminal:
                self.store.discard_terminal_reservation(reserved)

    def begin_submission(self, record):
        """Publish the irreversible submission phase only after preprocessing and input sync."""
        if record.state != RunState.PREPROCESSING:
            raise ValueError("Run is not preprocessing")
        sync_result_files(record.files.input_files)
        return self.transition(record, state=RunState.SUBMITTING, expected_record=record)

    def reserve(self, record, *, preprocessing=False):
        """Reserve the footprint; preprocessing callers must hold their ownership lock."""
        # Ordinary CLI clients may never call refresh(). Collect completed
        # hidden runs opportunistically, outside the global submission lock.
        for terminal in self.store.runs(active=False, include_reservations=True):
            if not terminal.retain_history:
                self.discard_reservation(terminal)
        attempted_recovery = set()
        attempted_preprocessing = set()
        attempted_refresh = set()
        while True:
            with self.store.submission_lock():
                try:
                    existing = self.store.get(record.id)
                except FileNotFoundError:
                    existing = None
                if existing is not None and (existing != record or existing.state != RunState.NEW):
                    raise ValueError(f"Run id {record.id!r} already exists; use attach()")
                conflicts = []
                for active in self.store.runs(include_reservations=True):
                    if active.id == record.id or not submission_conflicts(record, active):
                        continue
                    if active.state.is_terminal:
                        # Both tracked and hidden runs can publish a terminal state
                        # before their owner finishes closing application resources.
                        owner = self.store.observation_lock(active)
                        if owner.acquire(blocking=False):
                            owner.release()
                            if (not active.retain_history and (
                                active.execution.backend_id is None or active.execution.backend.name == "direct"
                            )):
                                self.store.discard_terminal_reservation(active)
                            continue
                    conflicts.append(active)
                conflicts = tuple(conflicts)
                if not conflicts:
                    if not preprocessing and record.files is not None:
                        sync_result_files(record.files.input_files)
                    return self.transition(
                        record,
                        state=RunState.PREPROCESSING if preprocessing else RunState.SUBMITTING,
                    )
                preparing = tuple(
                    conflict for conflict in conflicts
                    if conflict.id not in attempted_preprocessing and conflict.state == RunState.PREPROCESSING
                )
                stale = tuple(
                    conflict
                    for conflict in conflicts
                    if conflict.id not in attempted_recovery
                    and (conflict.execution.available or conflict.submission_started is False)
                    and self._is_stale_submitting(conflict)
                )
                refreshable = tuple(
                    conflict for conflict in conflicts
                    if conflict.id not in attempted_refresh
                    and conflict.execution.available
                    and (not conflict.retain_history or conflict.execution.backend.name == "direct")
                    and (conflict.execution.backend_id is not None or conflict.state == RunState.POSTPROCESSING)
                    and not conflict.state.is_terminal
                )

            if not preparing and not stale and not refreshable:
                raise DuplicateSubmissionError(record, conflicts)
            progressed = False
            for conflict in preparing:
                attempted_preprocessing.add(conflict.id)
                try:
                    recovered = self.recover_preprocessing(conflict)
                except FileNotFoundError:
                    if conflict.retain_history:
                        raise
                    progressed = True
                    continue
                if recovered.state != RunState.PREPROCESSING:
                    progressed = True
            for conflict in stale:
                attempted_recovery.add(conflict.id)
                try:
                    recovered = self.recover_submitting(conflict)
                except FileNotFoundError:
                    if conflict.retain_history:
                        raise
                    progressed = True
                    continue
                if recovered.state != RunState.SUBMITTING or recovered.execution.backend_id is not None:
                    progressed = True
            for conflict in refreshable:
                attempted_refresh.add(conflict.id)
                if not conflict.retain_history:
                    refreshed = self._refresh_reservation(conflict)
                else:
                    client = self.client_for(conflict)
                    if BackendCapability.STATUS not in client.backend.supports:
                        continue
                    refreshed = self.refresh((conflict,))[0]
                if refreshed is None or refreshed.state.is_terminal:
                    progressed = True
            if not progressed:
                raise DuplicateSubmissionError(record, conflicts)

    @staticmethod
    def _observed_run_state(record, status):
        from kkrunner.runs.states import project_run_state
        return project_run_state(status.state,
                                 files_pending=isinstance(record.execution.transport, SshTransport) and status.files is None,
                                 processing_pending=record.processing_required and status.return_code == 0,
                                 failure_phase=status.failure_phase)

    def update(self, record, status: RunStatus):
        # Backend refresh is observational. It cannot retract local processing
        # or a terminal result, including when a scheduler returns stale data.
        latest = self.store.get(record.id)
        if latest.state.is_terminal or latest.state in (RunState.PREPROCESSING, RunState.POSTPROCESSING):
            return latest
        record = latest
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
        if (
            isinstance(record.execution.transport, LocalTransport)
            and status.state.is_terminal and status.files is None
        ):
            # Backend status contains execution facts only. The client-side
            # manifest projection owns streams and application artifacts.
            from kkrunner.files.availability import local_manifest
            status = replace(status, files=local_manifest(record.files, status))
        if (
            record.execution.backend.name == "direct"
            and isinstance(record.execution.transport, SshTransport)
            and status.state == RunState.FINISHED
            and status.return_code is None
            and status.files is None
        ):
            # The live SSH observer is gone. Download whatever was produced
            # without asserting that an unknown exit code means success.
            try:
                files = self.client_for(record).collect_ended_direct_files(record, status)
            except (OSError, RuntimeError, ValueError) as exc:
                return self.transition(
                    record, state=RunState.DOWNLOADING,
                    message=f"Execution ended; output retrieval pending: {exc}",
                )
            status = replace(status, files=files)
        phase = _failure_phase(status.failure_phase)
        return_code = status.return_code
        state = self._observed_run_state(record, status)
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
        if state in (RunState.FINISHED, RunState.POSTPROCESSING):
            result_files = status.files if status.files is not None else record.files
            if record.execution.directory.remote is None and result_files is not None:
                from kkrunner.files.availability import discover_local_files
                result_files = discover_local_files(
                    result_files, require_outputs=return_code == 0,
                )
            updated = self.complete(
                record,
                state=state,
                execution=execution,
                files=result_files,
                return_code=return_code,
                message=status.message,
                failure_phase=phase,
                artifacts_available=True,
            )
        else:
            updated = self.transition(
                record,
                state=state,
                execution=execution,
                files=status.files if status.files is not None else _UNCHANGED,
                return_code=return_code,
                message=status.message,
                failure_phase=phase,
            )
        self._acknowledge_submission(self.client_for(updated), updated)
        self._acknowledge_result(updated)
        return updated

    def complete(
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
        expected_record=None,
        expected_processing_attempt=None,
    ):
        state = RunState(state)
        phase = _failure_phase(failure_phase)
        result_files = record.files if files is None else files

        if result_files is not None and result_files.failed_artifacts:
            artifacts_available = False
            if state == RunState.FINISHED or phase == FailurePhase.POSTPROCESSING:
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
                state = RunState.FAILED
                phase = FailurePhase.ARTIFACTS
                message = str(exc)
                result_files = result_files._map_artifacts(
                    lambda item: (
                        replace(
                            item,
                            availability_state="failed",
                            availability_message=message,
                        )
                        if item.availability_state in (None, "pending")
                        else item
                    )
                )

        if (
            result_files is not None
            and result_files.failed_artifacts
            and state in (RunState.FINISHED, RunState.POSTPROCESSING)
        ):
            state, phase = RunState.FAILED, FailurePhase.ARTIFACTS
            message = result_files.availability_message or message

        updated = self.transition(
            record,
            state=state,
            execution=execution,
            return_code=return_code,
            message=message,
            failure_phase=phase,
            files=result_files,
            expected_record=expected_record,
            expected_processing_attempt=expected_processing_attempt,
        )
        self._acknowledge_result(updated)
        return updated

    def resume_download(self, run):
        """Explicitly reopen a failed post-execution transfer for retry.

        This is not an execution resubmit. The original backend handle remains
        authoritative and the attached raw runner replays/synchronizes files
        before task postprocessing can finish.
        """
        record = self.store.get(run) if isinstance(run, str) else self.store.get(run.id)

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

        return self.store.update(record, reopen)

    def _process_outcome(self, record, outcome, process_result, *, files=None, execution=None, retry=False):
        """Claim processing until its durable completion, including across clients.

        All clients of a store must share its filesystem lock namespace. The OS
        releases this lock when a client dies; POSTPROCESSING then remains
        attachable/retryable. A live owner is never displaced by a timeout.
        """
        lock = self.store.processing_lock(record)
        if not lock.acquire(blocking=False):
            raise ProcessingPending("Another client owns postprocessing")
        try:
            latest = self.store.get(record.id)
            if latest.state == RunState.FINISHED and latest.return_code is not None:
                # A different observer already completed it; no second callback.
                raise ProcessingPending("Result already processed by another observer; load its historical result")
            if latest.state.is_terminal and not (
                (retry and latest.state == RunState.FAILED and latest.failure_phase == FailurePhase.POSTPROCESSING)
                or (latest.state == RunState.FINISHED and latest.return_code is None)
            ):
                raise RuntimeError("Run is already terminal")
            manifest = self._merged_result_files(latest, files)
            manifest = capture_result_files(manifest)
            if manifest.failed_artifacts:
                raise OSError(manifest.availability_message or "Output artifacts unavailable")
            if any(v.state() is not FileState.CURRENT for v in manifest.output_versions):
                if retry:
                    raise ValueError("Postprocessing retry requires unchanged historical artifacts")
                raise OSError("Historical artifacts changed before postprocessing")

            def claim(current):
                if current.state.is_terminal and not (
                    (retry and current.state == RunState.FAILED and current.failure_phase == FailurePhase.POSTPROCESSING)
                    or (current.state == RunState.FINISHED and current.return_code is None)
                ):
                    raise ProcessingPending("Run completed before processing claim")
                return replace(
                    current,
                    state=RunState.POSTPROCESSING,
                    files=manifest,
                    processing_attempt=uuid4().hex,
                    updated_at=_now(),
                    finished_at=None,
                    failure_phase=None,
                    message=None,
                    return_code=outcome.return_code,
                    execution=current.execution if execution is None else execution,
                )

            claimed = self.store.update(latest, claim)
            if process_result is None:
                raise ProcessingPending("Postprocessing requires a client callback")
            try:
                result = process_result(outcome)
                if any(v.state() is not FileState.CURRENT for v in manifest.output_versions):
                    raise RuntimeError("Historical artifacts changed during postprocessing retry")
            except BaseException as exc:
                self.complete(
                    claimed,
                    state=RunState.FAILED,
                    return_code=outcome.return_code,
                    message=str(exc),
                    failure_phase=FailurePhase.POSTPROCESSING,
                    expected_processing_attempt=claimed.processing_attempt,
                )
                raise
            self.complete(
                claimed,
                state=RunState.FINISHED,
                return_code=outcome.return_code,
                files=manifest,
                expected_processing_attempt=claimed.processing_attempt,
            )
            return result
        finally:
            lock.release()

    def retry_postprocessing(self, run, process_result):
        """Retry a failed or interrupted processing attempt without rerunning a job."""
        from kkrunner.runs.base import ExecutionOutcome

        record = self.store.get(run) if isinstance(run, str) else self.store.get(run.id)
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
        outcome = ExecutionOutcome(
            input_file=(
                files.input_file if file_is_current(files, files.input_file, files.input_versions) else None
            ),
            output_file=files.output_file,
            error_file=None if files.stderr is None else files.stderr.local_path,
            return_code=record.return_code,
        )
        return self._process_outcome(record, outcome, process_result, retry=True)

    def _acknowledge_result(self, record, *, collect=False):
        """Best-effort ACK after durable completion; failed ACKs remain retryable."""
        if not record.retain_history and not collect:
            # A hidden record owns its receipt until reservation collection, not
            # until any particular observer finishes. Low-level reattachment
            # remains possible while this record still exists.
            return False
        if not record.state.is_terminal or record.execution.backend_id is None:
            return False
        if record.failure_phase == FailurePhase.DOWNLOAD and record.return_code == 0 and not collect:
            # Retry will reattach to download preserved output. Retain the
            # terminal backend receipt until that end-to-end attempt settles.
            return False
        key = (record.id, record.execution.backend_id)
        with self._ack_lock:
            if key in self._acknowledged_results:
                return True
            try:
                client = self.client_for(record)
                client.acknowledge(record)
            except (OSError, RuntimeError, ValueError, NotImplementedError):
                return False
            self._acknowledged_results.add(key)
            return True

    def discard_reservation(self, record):
        """Collect hidden history only after any recoverable backend receipt is acknowledged."""
        if record.retain_history or not record.state.is_terminal:
            return False
        owner = self.store.observation_lock(record)
        if not owner.acquire(blocking=False):
            return False
        owner.release()
        if (record.execution.available and record.execution.backend_id is not None
                and record.execution.backend.name != "direct"
                and not self._acknowledge_result(record, collect=True)):
            return False
        return self.store.discard_terminal_reservation(record)

    def reconcile_acknowledgements(self, *, on_error=None):
        """Retry idempotent result ACKs for durable terminal records."""
        options = {} if on_error is None else {"on_error": on_error}
        return tuple(
            record.id for record in self.store.runs(active=False, **options)
            if self._acknowledge_result(record)
        )

    @staticmethod
    def _acknowledge_submission(client, record):
        """Discard the submission receipt only after RunStore is durable."""
        if record.execution.backend_id is None:
            return
        try:
            client.acknowledge_submission(record)
        except (OSError, RuntimeError):
            # The receipt remains recoverable until a subsequent refresh.
            pass

    def create_run(
        self, client, spec, *, process_result=None, metadata=None,
        processing_required=None, callback=None, run_id=None, detached=None,
        owned_resources=None, preprocess=None, retain_history=True,
    ):
        """Create one reserved Run through the configured client."""
        from kkrunner.runs.run import Run
        if isinstance(spec, ExecutionSpec):
            spec.require_complete()
        if processing_required is False and process_result is not None:
            raise ValueError("A result callback requires processing_required=True")
        if retain_history:
            config = require_persistable_transport(client.transport)
        else:
            try:
                config = require_persistable_transport(client.transport)
            except TypeError:
                # Custom live transports cannot be reconstructed by another process.
                # Preserve a conservative namespace rather than bypassing reservations.
                from kkrunner.transports.implementations.unavailable import UnavailableTransport
                config = UnavailableTransport({"kind": "custom", "host": getattr(client.transport, "host", None)})
        record = self.new_record(
            metadata=metadata,
            processing_required=(process_result is not None if processing_required is None else processing_required),
            directory=spec.directory,
            transport=config,
            backend=client.backend_config,
            remote_directory=(None if client.transport.is_local else client.submission_directory(spec)),
            files=FileManifest.from_spec(spec),
            run_id=run_id,
            submission_key=uuid4().hex,
            retain_history=retain_history,
            submission_started=False,
        )
        if config.available:
            self.register_client(client)
        parts = client.create_run_parts(
            spec, submission_key=record.execution.submission_key,
        )
        parts.configure_detached(
            client.default_detached if detached is None else detached,
        )
        return Run(
            parts.job, client=client, manager=self, record=record,
            process_result=process_result, callback=callback, parts=parts, owned_resources=owned_resources,
            preprocess=preprocess,
        )

    def _refresh_reservation(self, record):
        """Observe an orphaned hidden reservation without starting client processing."""
        owner = self.store.observation_lock(record)
        if not owner.acquire(blocking=False):
            return record
        try:
            try:
                current = self.store.get(record.id)
            except FileNotFoundError:
                return None  # Another instance already collected this reservation.
            if current.state.is_terminal:
                return current
            if current.state == RunState.PREPROCESSING:
                return self.recover_preprocessing(current)
            if current.state == RunState.POSTPROCESSING:
                # The native execution ended; no live client remains to process it.
                return self.store.update(current, lambda latest: replace(
                    latest, state=RunState.FAILED, updated_at=_now(), finished_at=_now(),
                    failure_phase=FailurePhase.POSTPROCESSING, message="Client processing was abandoned",
                ) if latest.state == RunState.POSTPROCESSING else latest)
            if self._is_stale_submitting(current):
                current = self._recover_submitting(current)
            if not current.execution.available:
                return current  # Cannot prove that the calculation stopped.
            if current.state.is_terminal or current.execution.backend_id is None:
                return current
            client = self.client_for(current)
            if BackendCapability.STATUS not in client.backend.supports:
                return current
            try:
                status = client.status(JobAttachment(current.execution, current.files))[0]
            except (OSError, RuntimeError, ValueError, NotImplementedError):
                return current
            if status.state.is_terminal:
                # No live client can transfer or process files anymore. The native
                # job is confirmed ended, so the file reservation can be released.
                return self.complete(current, state=status.state, return_code=status.return_code,
                                     failure_phase=status.failure_phase, message=status.message)
            return self.update(current, status)
        finally:
            owner.release()

    def refresh(self, records=None, *, on_error=None):
        """Refresh records; optionally report and skip unreadable history entries."""
        self.reconcile_acknowledgements(on_error=on_error)
        options = {} if on_error is None else {"on_error": on_error}
        if records is None:
            # Maintenance is independent of the history visible to the caller.
            for reservation in self.store.runs(include_reservations=True, **options):
                if not reservation.retain_history:
                    refreshed = self._refresh_reservation(reservation)
                    if refreshed is not None:
                        self.discard_reservation(refreshed)
            records = self.store.runs(active=True, **options)
        records = tuple(records)
        current = {record.id: record for record in records}
        groups = {}
        for record in records:
            if not record.retain_history:
                refreshed = self._refresh_reservation(record)
                if refreshed is not None:
                    current[record.id] = refreshed
                continue
            if record.state == RunState.PREPROCESSING:
                current[record.id] = self.recover_preprocessing(record)
                continue
            if not record.execution.available:
                continue  # Browsable history; no supported target implementation.
            if record.execution.backend_id is None and self._is_stale_submitting(record):
                record = self.recover_submitting(record)
                current[record.id] = record
            if record.execution.backend_id is None or record.state.is_terminal:
                # Recovery may have completed this record and acknowledged
                # its backend receipt. Do not immediately observe it again.
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
                    current[record.id] = self.transition(
                        record, state=RunState.UNKNOWN,
                        message="This execution backend has no independent status query",
                    )
                continue
            locks = []
            guarded = []
            try:
                for record in group:
                    if client.backend.name == "direct":
                        ownership = self.store.observation_lock(record)
                        if not ownership.acquire(blocking=False):
                            # The original observer is alive, possibly already
                            # downloading or processing after the child exited.
                            # It owns completion; do not release its reservation.
                            current[record.id] = self.store.get(record.id)
                            continue
                        locks.append(ownership)
                    guarded.append(record)
                if not guarded:
                    continue
                try:
                    statuses = client.status(
                        *(JobAttachment(record.execution, record.files) for record in guarded)
                    )
                except Exception as exc:
                    if client.backend.name != "direct":
                        raise
                    # A lost connection is uncertainty, not proof of exit.
                    for record in guarded:
                        current[record.id] = self.transition(
                            record, state=RunState.UNKNOWN,
                            message=f"Cannot query direct process status: {exc}",
                        )
                    continue
                if len(statuses) != len(guarded):
                    raise RuntimeError("Transport returned wrong number of run statuses")
                for record, status in zip(guarded, statuses):
                    current[record.id] = self.update(record, status)
            finally:
                for ownership in reversed(locks):
                    ownership.release()
        return tuple(current[record.id] for record in records)

    def forget(self, run, *, force=False):
        """Remove stored history without touching native processes or result files."""
        if self.store is None:
            raise RuntimeError("No persistent run store is configured")
        return self.store.forget(run, force=force)

    def cancel(self, run):
        """Ask a persisted backend to cancel a job, without assuming completion."""
        record = self.store.get(run) if isinstance(run, str) else self.store.get(run.id)
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
        return self.update(record, status)

    def attach(
        self, run, *, process_result=None, client=None,
        callback=None, output_listener=None, owned_resources=None,
    ):
        """Open a tracked Run without resubmitting its backend job."""
        from kkrunner.runs.run import Run
        record = self.store.get(run) if isinstance(run, str) else self.store.get(run.id)
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
        owner = None
        parts = None
        try:
            if not record.retain_history or client.backend.name == "direct":
                owner = self.store.observation_lock(record)
                if not owner.acquire(blocking=False):
                    raise RuntimeError("Run already has a live client observer")
                # A concurrent refresh may have completed the run just before
                # we acquired ownership. Do not open streams for a terminal run.
                record = self.store.get(record.id)
                if record.state.is_terminal:
                    raise RuntimeError("Run is already terminal; use explicit retry")
            parts = client.open_run_parts(
                JobAttachment(record.execution, record.files),
                output_listener=output_listener,
            )
            attached = Run(
                parts.job, client=client, manager=self, record=record,
                process_result=process_result, callback=callback, parts=parts, owned_resources=owned_resources,
            )
            attached._observation_lock = owner
            attached.start()
        except BaseException:
            if parts is not None:
                parts.abort()
            if owner is not None:
                owner.release()
            raise
        return attached
