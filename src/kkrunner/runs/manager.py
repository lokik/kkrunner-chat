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
from kkrunner.client.transports import (
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
    RunState.SUBMITTING: 1,
    RunState.QUEUED: 2,
    RunState.RUNNING: 3,
    RunState.DOWNLOADING: 4,
    RunState.POSTPROCESSING: 5,
    RunState.FINISHED: 6,
    RunState.FAILED: 6,
    RunState.CANCELLED: 6,
}
_UNCHANGED = object()
_NO_ARTIFACTS_FAILURES = {
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
                root = os.environ.get("XDG_STATE_HOME") or (Path.home() / ".local" / "state")
                RunManager._fallback = RunManager(RunStore(Path(root) / "kkrunner" / "runs"))
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
        )

    @staticmethod
    def _merged_state(latest, requested):
        if requested is None:
            return latest.state
        requested = RunState(requested)
        if latest.state.is_terminal:
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
            merged_state = self._merged_state(latest, state)
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
                if message is None and latest.state == RunState.UNKNOWN and merged_state != RunState.UNKNOWN:
                    # A later scheduler observation supersedes the stale
                    # connection-error diagnostic.
                    changes["message"] = None
                elif message is not None and (not final_locked or latest.message is None):
                    changes["message"] = message
            if failure_phase is not _UNCHANGED:
                normalized = _failure_phase(failure_phase)
                if not final_locked or latest.failure_phase is None:
                    changes["failure_phase"] = normalized
            if all(getattr(latest, key) == value for key, value in changes.items() if key != "updated_at"):
                return latest
            return replace(latest, **changes)

        return self.store.update(record, merge)

    def _is_stale_submitting(self, record, *, now=None):
        if record.state != RunState.SUBMITTING or record.execution.backend_id is not None:
            return False
        now = now or datetime.now(timezone.utc)
        return (now - _timestamp(record.updated_at)).total_seconds() >= self.submitting_grace

    def recover_submitting(self, record):
        record = self.store.get(record.id)
        if not self._is_stale_submitting(record):
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

    def reserve(self, record):
        attempted_recovery = set()
        while True:
            with self.store.submission_lock():
                try:
                    existing = self.store.get(record.id)
                except FileNotFoundError:
                    existing = None
                if existing is not None and (existing != record or existing.state != RunState.NEW):
                    raise ValueError(f"Run id {record.id!r} already exists; use attach()")
                conflicts = tuple(
                    active
                    for active in self.store.runs(active=True)
                    if active.id != record.id and submission_conflicts(record, active)
                )
                if not conflicts:
                    if record.files is not None:
                        sync_result_files(record.files.input_files)
                    return self.transition(
                        record,
                        state=RunState.SUBMITTING,
                    )
                stale = tuple(
                    conflict
                    for conflict in conflicts
                    if conflict.id not in attempted_recovery and self._is_stale_submitting(conflict)
                )

            if not stale:
                raise DuplicateSubmissionError(record, conflicts)
            progressed = False
            for conflict in stale:
                attempted_recovery.add(conflict.id)
                recovered = self.recover_submitting(conflict)
                if recovered.state != RunState.SUBMITTING or recovered.execution.backend_id is not None:
                    progressed = True
            if not progressed:
                raise DuplicateSubmissionError(record, conflicts)

    @staticmethod
    def _observed_run_state(record, status):
        from kkrunner.runs.states import project_run_state
        return project_run_state(status.state,
                                 files_pending=isinstance(record.execution.transport, SshTransport),
                                 processing_pending=record.processing_required,
                                 failure_phase=status.failure_phase)

    def update(self, record, status: RunStatus):
        # Backend refresh is observational. It cannot retract local processing
        # or a terminal result, including when a scheduler returns stale data.
        latest = self.store.get(record.id)
        if latest.state.is_terminal or latest.state == RunState.POSTPROCESSING:
            return latest
        record = latest
        if (
            isinstance(record.execution.transport, LocalTransport)
            and status.state.is_terminal and status.files is None
        ):
            # Backend status contains execution facts only. The client-side
            # manifest projection owns streams and application artifacts.
            from kkrunner.files.availability import local_manifest
            status = replace(status, files=local_manifest(record.files, status))
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
                result_files = discover_local_files(result_files)
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
                result_files = capture_result_files(result_files)
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
            if latest.state == RunState.FINISHED:
                # A different observer already completed it; no second callback.
                raise ProcessingPending("Result already processed by another observer; load its historical result")
            if latest.state.is_terminal and not (
                retry and latest.state == RunState.FAILED and latest.failure_phase == FailurePhase.POSTPROCESSING
            ):
                raise RuntimeError("Run is already terminal")
            manifest = latest.files if files is None else files
            if latest.files.captured_at is not None:
                manifest = latest.files
            manifest = capture_result_files(manifest)
            if manifest.failed_artifacts:
                raise OSError(manifest.availability_message or "Output artifacts unavailable")
            if any(v.state() is not FileState.CURRENT for v in manifest.output_versions):
                if retry:
                    raise ValueError("Postprocessing retry requires unchanged historical artifacts")
                raise OSError("Historical artifacts changed before postprocessing")

            def claim(current):
                if current.state.is_terminal and not (
                    retry and current.state == RunState.FAILED and current.failure_phase == FailurePhase.POSTPROCESSING
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

    def _acknowledge_result(self, record):
        """Best-effort ACK after durable completion; failed ACKs remain retryable."""
        if not record.state.is_terminal or record.execution.backend_id is None:
            return False
        if record.failure_phase == FailurePhase.DOWNLOAD and record.return_code == 0:
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

    def reconcile_acknowledgements(self):
        """Retry idempotent result ACKs for durable terminal records."""
        return tuple(record.id for record in self.store.runs(active=False) if self._acknowledge_result(record))

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
        owned_resources=None,
    ):
        """Create one tracked Run through the configured client."""
        from kkrunner.runs.run import Run
        if isinstance(spec, ExecutionSpec):
            spec.require_complete()
        if processing_required is False and process_result is not None:
            raise ValueError("A result callback requires processing_required=True")
        config = require_persistable_transport(client.transport)
        record = self.new_record(
            metadata=metadata,
            processing_required=(process_result is not None if processing_required is None else processing_required),
            directory=spec.directory,
            transport=config,
            backend=client.backend_config,
            remote_directory=(None if config.is_local else client.submission_directory(spec)),
            files=FileManifest.from_spec(spec),
            run_id=run_id,
            submission_key=uuid4().hex,
        )
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
        )

    def refresh(self, records=None):
        self.reconcile_acknowledgements()
        records = tuple(self.store.runs(active=True) if records is None else records)
        current = {record.id: record for record in records}
        groups = {}
        for record in records:
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
                # Direct Jobs have no independently queryable durable status.
                # Their client-side completion thread remains authoritative;
                # never infer FINISHED from a PID or a closed SSH channel.
                for record in group:
                    current[record.id] = self.transition(
                        record, state=RunState.UNKNOWN,
                        message="Direct execution has no independent recovery status",
                    )
                continue
            statuses = client.status(
                *(JobAttachment(record.execution, record.files) for record in group)
            )
            if len(statuses) != len(group):
                raise RuntimeError("Transport returned wrong number of run statuses")
            for record, status in zip(group, statuses):
                current[record.id] = self.update(record, status)
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
        parts = client.open_run_parts(
            JobAttachment(record.execution, record.files),
            output_listener=output_listener,
        )
        try:
            attached = Run(
                parts.job, client=client, manager=self, record=record,
                process_result=process_result, callback=callback, parts=parts, owned_resources=owned_resources,
            )
            attached.start()
        except BaseException:
            # Opening an attachment transfers stream ownership to Run only
            # once its observer has started. Do not leak file descriptors when
            # constructing or starting the attached observer fails.
            parts.abort()
            raise
        return attached
