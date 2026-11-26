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

from kkrunner.files.manifest import ArtifactAvailabilityState, FileEntry
from kkrunner.files.paths import expand_local_candidates, has_file_magic
from kkrunner.files.versions import FileVersion, file_digest
from kkrunner.support.durability import sync_file
from kkrunner.runs.stores import resolve_record, store_for


class HistoricalFileMismatchError(OSError):
    """Restored contents differ from a run's captured SHA-256 digest."""


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


def _capturable_entries(entries):
    """Only versions actually delivered for this run enter its provenance.

    A skipped auxiliary output can still exist locally from an earlier run.
    Expanded glob declarations likewise must not capture unrelated old local
    matches: concrete discovered outputs have their own manifest entries.
    """
    return tuple(
        entry
        for entry in entries
        if entry is not None and entry.local_path is not None
        and entry.collect
        and entry.availability_state not in ("skipped", "failed")
        and (
            not has_file_magic(entry.local_path)
            or entry.availability_state in (None, "pending")
        )
    )


def snapshot_file_entries(entries, *, sync=False):
    """Capture concrete local files and declared directory contents."""
    paths = {}
    for entry in entries:
        if entry.local_path is None:
            continue
        if entry.kind == "directory":
            if os.path.isdir(entry.local_path):
                paths[os.path.abspath(entry.local_path)] = entry.kind
        else:
            paths.update(dict.fromkeys(_concrete_result_files((entry.local_path,)), "file"))
    versions = []
    for path, kind in sorted(paths.items()):
        if sync and kind == "file":
            sync_file(path)
        versions.append(FileVersion.capture(path, require_digest=True, kind=kind))
    return tuple(versions)


def sync_result_files(manifests):
    """Flush existing files selected by manifest paths before submission."""
    for path in _concrete_result_files(manifests):
        sync_file(path)


def file_is_current(result_files, path, versions):
    """Check a local file against captured versions, or existence before capture."""
    if result_files is None or path is None:
        return False
    path = os.path.abspath(path)
    if result_files.captured_at is None and not versions:
        return os.path.isfile(path)
    for version in versions:
        if os.path.abspath(os.fspath(version.path)) == path:
            return version.is_current()
    return False


def _missing_result_versions(manifest):
    """Only captured ordinary inputs and outputs are automatically restored."""
    versions = {version.path: version for version in (*manifest.input_versions, *manifest.output_versions)}
    return tuple(version for version in versions.values()
                 if version.kind == "file" and version.available and not os.path.lexists(version.path))


def capture_result_files(result_files, *, require_outputs=True, cache_versions=None):
    """Capture published outputs and caches, retaining submission-time inputs."""
    if result_files is None or result_files.captured_at is not None:
        return result_files
    from kkrunner.files.availability import discover_local_files

    output_entries = (result_files.stdout, result_files.stderr, *result_files.outputs)
    cache_entries = tuple(item.file for item in result_files.caches)
    captured = replace(
        result_files,
        output_versions=snapshot_file_entries(_capturable_entries(output_entries), sync=True),
        cache_versions=(snapshot_file_entries(_capturable_entries(cache_entries))
                        if cache_versions is None else cache_versions),
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
        record = resolve_record(run, self.store)
        result_files = record.files
        return file_is_current(
            result_files,
            None if result_files is None else result_files.input_file,
            () if result_files is None else result_files.input_versions,
        )

    def output_file_is_current(self, run):
        record = resolve_record(run, self.store)
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

        return store_for(record, self.store).update(record, update)

    def snapshot_file(self, run, file, *, destination=None):
        """Read current bytes of a declared output/cache, regardless of run state.

        The calculation may be writing concurrently: this is an observational
        copy, not a consistent version. Destination copies never replace run
        files or affect persistent artifact metadata.
        """
        from kkrunner.files.paths import file_path, has_file_magic
        from kkrunner.files.snapshots import matching_file, check_destination

        record = resolve_record(run, self.store)
        if record.execution.backend_id is None:
            raise RuntimeError(f"Run {record.id} has not been submitted")
        selected = file.local_path if isinstance(file, FileEntry) else file
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

    def retrieve_result_files(self, run, *, guard):
        record = resolve_record(run, self.store)
        if not record.state.is_terminal:
            raise RuntimeError(f"Run {record.id} is not finished")
        if record.files.captured_at is None:
            # There is no authoritative identity to restore. The explicit
            # endpoint fetch operation remains available for unverified files.
            return record

        missing = _missing_result_versions(record.files)
        if not missing or record.execution.backend_id is None or not record.execution.available:
            return record

        client = self._client_for(record)
        if not client.supports_result_file_retrieval:
            return record

        for version in missing:
            path = os.fspath(version.path)
            staged = version.path.with_name(f".{version.path.name}.{uuid4().hex}.tmp")
            try:
                client.fetch_result_files(
                    record, (path,), destinations={path: os.fspath(staged)},
                )
                if not staged.is_file():
                    raise FileNotFoundError(f"Historical artifact was not retrieved: {path}")
                if file_digest(staged) != version.digest:
                    raise HistoricalFileMismatchError(f"Historical result files have been overwritten: {path}")
                with guard({path: "file"}):
                    latest = resolve_record(record, self.store)
                    if not latest.state.is_terminal:
                        raise OSError(f"Run {record.id} is no longer finished")
                    # An existing path is never replaced, even if an external
                    # writer creates it between the check and publication.
                    try:
                        os.link(staged, path)
                    except FileExistsError:
                        pass
                if not version.is_current():
                    raise HistoricalFileMismatchError(f"Historical result files have been overwritten: {path}")
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
                staged.unlink(missing_ok=True)
            record = self.update_artifact_availability(
                record, path, ArtifactAvailabilityState.AVAILABLE,
            )
        return record
