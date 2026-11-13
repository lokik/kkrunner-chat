"""File versions, snapshots and verified retrieval for tracked runs.

RunFiles receives the authoritative RunStore and client pool explicitly.
It updates file availability, while RunManager owns run lifecycle transitions.
Manifest capture and local file checks are independent of either service.
"""

from __future__ import annotations

import os
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from kkrunner.files.manifest import ArtifactAvailabilityState
from kkrunner.files.paths import expand_local_candidates, has_file_magic
from kkrunner.files.versions import FileState, FileVersion, file_digest
from kkrunner.support.durability import sync_file


class HistoricalFileMismatchError(OSError):
    """Restored contents differ from a run's captured SHA-256 digest."""


def _unique(values):
    return tuple(dict.fromkeys(value for value in values if value is not None))


def _concrete_result_files(manifests):
    paths = []
    seen = set()
    for manifest in manifests:
        for path in expand_local_candidates(manifest):
            path = os.path.abspath(path)
            if path in seen or not os.path.isfile(path):
                continue
            seen.add(path)
            paths.append(path)
    return tuple(paths)


def _result_files_snapshot(manifests, *, sync=False):
    versions = []
    for path in _concrete_result_files(manifests):
        if sync:
            sync_file(path)
        versions.append(FileVersion.capture(path, require_digest=True))
    return tuple(sorted(versions, key=lambda value: os.fspath(value.path)))


def sync_result_files(manifests):
    """Flush existing files selected by manifest paths before submission."""
    for path in _concrete_result_files(manifests):
        sync_file(path)


def file_is_current(result_files, path, versions):
    """Check a local file against captured versions, or existence before capture."""
    if result_files is None or path is None:
        return False
    path = os.path.abspath(path)
    if result_files.captured_at is None:
        return os.path.isfile(path)
    for version in versions:
        if os.path.abspath(os.fspath(version.path)) == path:
            return version.is_current()
    return False


def _missing_result_files(result_files):
    if result_files is None:
        return ()
    if result_files.captured_at is not None:
        return tuple(
            os.fspath(item.path) for item in result_files.all_versions if item.state() is FileState.MISSING
        )

    missing = []
    for manifest in result_files.manifests:
        if has_file_magic(manifest):
            if not expand_local_candidates(manifest):
                missing.append(manifest)
        elif not os.path.isfile(manifest):
            missing.append(manifest)
    return _unique(missing)


def capture_result_files(result_files, *, require_outputs=True):
    """Capture local versions without assuming an unknown exit means success."""
    if result_files is None or result_files.captured_at is not None:
        return result_files
    from kkrunner.files.availability import discover_local_files

    captured = replace(
        result_files,
        input_versions=_result_files_snapshot(result_files.input_files),
        output_versions=_result_files_snapshot(result_files.output_files, sync=True),
        captured_at=datetime.now(timezone.utc).isoformat(),
    )
    return discover_local_files(captured, require_outputs=require_outputs)


class RunFiles:
    """File operations on tracked runs, independent of lifecycle orchestration."""

    def __init__(self, *, store, client_pool):
        self.store = store
        self.client_pool = client_pool

    def _client_for(self, record):
        return self.client_pool.for_config(record.execution.transport, record.execution.backend)

    def input_file_is_current(self, run):
        record = self.store.get(run) if isinstance(run, str) else self.store.get(run.id)
        result_files = record.files
        return file_is_current(
            result_files,
            None if result_files is None else result_files.input_file,
            () if result_files is None else result_files.input_versions,
        )

    def output_file_is_current(self, run):
        record = self.store.get(run) if isinstance(run, str) else self.store.get(run.id)
        result_files = record.files
        return file_is_current(
            result_files,
            None if result_files is None else result_files.output_file,
            () if result_files is None else result_files.output_versions,
        )

    def update_artifact_availability(self, record, path, state, message=None):
        """Persist availability of one artifact without changing the run outcome."""
        state = ArtifactAvailabilityState(state)

        def update(latest):
            return replace(
                latest,
                updated_at=datetime.now(timezone.utc).isoformat(),
                files=latest.files.with_file_availability(path, state.value, message),
            )

        return self.store.update(record, update)

    def snapshot_file(self, run, file, *, destination=None):
        """Read current bytes of a declared output/cache, regardless of run state.

        The calculation may be writing concurrently: this is an observational
        copy, not a consistent version. Destination copies never replace run
        files or affect persistent artifact metadata.
        """
        from kkrunner.files.paths import file_path, has_file_magic
        from kkrunner.files.snapshots import matching_file, check_destination

        record = self.store.get(run) if isinstance(run, str) else self.store.get(run.id)
        if record.execution.backend_id is None:
            raise RuntimeError(f"Run {record.id} has not been submitted")
        selected = file.local_path if hasattr(file, "local_path") else file
        path = file_path(selected, record.execution.directory.local, required=True)
        if has_file_magic(path):
            raise ValueError("Snapshot requires one concrete file, not a wildcard")
        entry = matching_file(record.files, path)
        if entry is None:
            raise ValueError(f"File is not a declared output or cache: {path}")
        if destination is not None:
            destination = check_destination(record.files, path, destination)
        if record.execution.directory.remote is None:
            from kkrunner.files.snapshots import local_snapshot
            return local_snapshot(path, destination=destination)
        return self._client_for(record).snapshot_file(record, path, destination=destination)

    def retrieve_result_files(self, run):
        record = self.store.get(run) if isinstance(run, str) else self.store.get(run.id)
        if not record.state.is_terminal:
            raise RuntimeError(f"Run {record.id} is not finished")

        missing = _missing_result_files(record.files)
        if not missing or record.execution.backend_id is None:
            return record

        client = self._client_for(record)
        if not client.supports_result_file_retrieval:
            return record

        versions = {
            os.path.abspath(os.fspath(version.path)): version
            for version in (() if record.files is None else record.files.all_versions)
        }
        for path in missing:
            path = os.path.abspath(os.fspath(path))
            version = versions.get(path)
            staged = (
                Path(path).with_name(f".{Path(path).name}.{uuid4().hex}.tmp")
                if version is not None and version.digest is not None
                else None
            )
            try:
                if staged is None:
                    client.fetch_result_files(record, (path,))
                else:
                    client.fetch_result_files(
                        record,
                        (path,),
                        destinations={path: os.fspath(staged)},
                    )
                    if not staged.is_file():
                        raise FileNotFoundError(f"Historical artifact was not retrieved: {path}")
                    if file_digest(staged) != version.digest:
                        raise HistoricalFileMismatchError(f"Historical result files have been overwritten: {path}")
                    os.replace(staged, path)
            except OSError as exc:
                # An unavailable historical artifact does not prevent recovery
                # of unrelated files. Preserve its individual failure status;
                # consumers decide which files are necessary for their result.
                record = self.update_artifact_availability(
                    record,
                    path,
                    ArtifactAvailabilityState.FAILED,
                    str(exc),
                )
                continue
            finally:
                if staged is not None:
                    staged.unlink(missing_ok=True)
            refreshed = self.store.get(record.id)
            if path in _missing_result_files(refreshed.files):
                record = self.update_artifact_availability(
                    refreshed,
                    path,
                    ArtifactAvailabilityState.FAILED,
                    "Historical artifact is still missing",
                )
            else:
                record = self.update_artifact_availability(
                    refreshed,
                    path,
                    ArtifactAvailabilityState.AVAILABLE,
                )
        return record
