"""File versions, snapshots and verified retrieval for tracked runs.

RunFiles receives the authoritative RunStore and client pool explicitly.
It updates file availability, while RunManager owns run lifecycle transitions.
Manifest capture and local file checks are independent of either service.
"""

from __future__ import annotations

import os
from dataclasses import replace
from datetime import datetime, timezone

from kkrunner.files.availability import file_is_current
from kkrunner.files.manifest import ArtifactAvailabilityState
from kkrunner.files.paths import expand_local_candidates, has_file_magic
from kkrunner.files.versions import FileState, file_digest
from kkrunner.runs.stores import resolve_record, store_for


class HistoricalFileMismatchError(OSError):
    """Restored contents differ from a run's captured SHA-256 digest."""


def _unique(values):
    return tuple(dict.fromkeys(value for value in values if value is not None))


def _missing_result_files(result_files):
    if result_files is None:
        return ()
    if result_files.captured_at is not None:
        # Automatic artifact retrieval works on ordinary files and excludes caches.
        return _unique(
            os.fspath(item.path) for item in (*result_files.input_versions, *result_files.output_versions)
            if item.kind == "file" and item.state() is FileState.MISSING
        )

    entries = (
        *result_files.input_entries, result_files.stdout, result_files.stderr,
        *(entry for entry in result_files.outputs if entry.collect),
    )
    return _unique(
        entry.local_path for entry in entries
        if entry is not None and entry.kind == "file" and entry.local_path is not None
        and not any(os.path.isfile(path) for path in expand_local_candidates(entry.local_path))
    )


class RunFiles:
    """File operations on tracked runs, independent of lifecycle orchestration."""

    def __init__(self, *, store, client_pool):
        self.store = store
        self.client_pool = client_pool

    def _client_for(self, record):
        return self.client_pool.for_config(record.execution.target, record.execution.backend)

    def input_file_is_current(self, run):
        record = resolve_record(run, self.store)
        files = record.files
        return file_is_current(files, files.input_file, files.input_versions)

    def output_file_is_current(self, run):
        record = resolve_record(run, self.store)
        files = record.files
        return file_is_current(files, files.output_file, files.output_versions)

    def update_artifact_availability(self, record, path, state, message=None):
        """Persist availability of one artifact without changing the run outcome."""
        state = ArtifactAvailabilityState(state)

        def update(latest):
            return replace(
                latest,
                updated_at=datetime.now(timezone.utc).isoformat(),
                files=latest.files.with_file_availability(path, state.value, message),
            )

        return store_for(record, self.store).update(record, update)

    def snapshot_file(self, run, file, *, destination=None):
        """Read current bytes of a declared output/cache, regardless of run state.

        The calculation may be writing concurrently: this is an observational
        copy, not a consistent version. Destination copies never replace run
        files or affect persistent artifact metadata.
        """
        record = resolve_record(run, self.store)
        if record.execution.backend_id is None:
            raise RuntimeError(f"Run {record.id} has not been submitted")
        if record.execution.directory.remote is not None:
            return self._client_for(record).snapshot_file(record, file, destination=destination)

        from kkrunner.files.snapshots import local_snapshot, snapshot_request
        path, _, destination = snapshot_request(
            record.files, record.execution.directory.local, file, destination,
        )
        return local_snapshot(path, destination=destination)

    def retrieve_result_files(self, run, *, reserve):
        record = resolve_record(run, self.store)
        if not record.state.is_terminal:
            raise RuntimeError(f"Run {record.id} is not finished")

        missing = _missing_result_files(record.files)
        if not missing or record.execution.backend_id is None or not record.execution.available:
            return record

        client = self._client_for(record)
        if not client.supports_result_file_retrieval:
            return record

        versions = {os.path.abspath(os.fspath(version.path)): version
                    for version in record.files.all_versions}

        def verify(target, temporary):
            version = versions.get(target)
            if version is not None and version.digest is not None:
                if file_digest(temporary) != version.digest:
                    raise HistoricalFileMismatchError(f"Historical result files have been overwritten: {target}")

        # Resolve all concrete pairs before reserving anything. Expansion is
        # read-only; the exact same pairs are passed to the transfer below.
        planned = {}
        errors = {}
        by_destination = {}
        for path in missing:
            try:
                entries = client.resolve_result_files(record, (path,))
                if not entries:
                    raise FileNotFoundError(f"Historical artifact was not retrieved: {path}")
                for entry in entries:
                    previous = by_destination.setdefault(entry.local_path, entry)
                    if previous.remote_path != entry.remote_path:
                        raise ValueError(f"Different historical sources map to {entry.local_path}")
                planned[path] = entries
            except OSError as exc:
                errors[path] = str(exc)

        from contextlib import nullcontext
        scope = reserve(record, tuple(by_destination.values())) if by_destination else nullcontext()
        with scope:
            for path, message in errors.items():
                record = self.update_artifact_availability(
                    record, path, ArtifactAvailabilityState.FAILED, message,
                )
            for path, entries in planned.items():
                try:
                    # Exact historical entries retain the original captured
                    # digest even if their local pathname was an alias.
                    def verify_entry(target, temporary):
                        verify(target if has_file_magic(path) else path, temporary)

                    client.download_result_files(entries, verify=verify_entry)
                    if not all(os.path.isfile(entry.local_path) for entry in entries):
                        raise FileNotFoundError(f"Historical artifact was not retrieved: {path}")
                    record = self.update_artifact_availability(
                        record, path, ArtifactAvailabilityState.AVAILABLE,
                    )
                except (OSError, RuntimeError) as exc:
                    record = self.update_artifact_availability(
                        record, path, ArtifactAvailabilityState.FAILED, str(exc),
                    )
        return record
