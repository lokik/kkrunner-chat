"""File versions, snapshots and verified retrieval for tracked runs.

RunFiles receives the authoritative RunStore and uses each record's target.
It updates file availability, while RunManager owns run lifecycle transitions.
"""

from __future__ import annotations

import os
from dataclasses import replace
from datetime import datetime, timezone

from kkrunner.client.file_access import FileAccess
from kkrunner.files.availability import file_is_current
from kkrunner.files.manifest import ArtifactAvailabilityState
from kkrunner.files.paths import expand_local_candidates, has_file_magic
from kkrunner.files.versions import file_digest
from kkrunner.runs.stores import resolve_record, store_for
from kkrunner.runs.states import NO_EXECUTION_FAILURES
from kkrunner.support.async_utils import run_coro_sync


class HistoricalFileMismatchError(OSError):
    """Restored contents differ from a run's captured SHA-256 digest."""


def _unique(values):
    return tuple(dict.fromkeys(value for value in values if value is not None))


def _missing_result_files(result_files, *, include_outputs=True):
    # Submission captures inputs independently of terminal output capture.
    # Never expand a historical input glob again once its versions are known.
    captured = result_files.captured_at is not None
    versions = result_files.input_versions
    entries = () if captured or versions else result_files.input_entries
    if include_outputs and result_files.remote_patterns_resolved is not False:
        if captured:
            versions += result_files.output_versions
        else:
            entries += (
                result_files.stdout, result_files.stderr,
                *(entry for entry in result_files.outputs if entry.collect
                  and (result_files.remote_patterns_resolved is not True
                       or entry.source_pattern is not None or not has_file_magic(entry.local_path))),
            )

    # Retrieval needs existence, not content identity. Only verify the
    # contents of files actually being restored.
    missing = []
    for version in versions:
        if version.kind != "file":
            continue
        try:
            version.path.stat()
        except FileNotFoundError:
            missing.append(os.fspath(version.path))
        except OSError:
            continue
    missing.extend(
        entry.local_path for entry in entries
        if entry is not None and entry.kind == "file" and entry.local_path is not None
        and not any(
            os.path.isfile(path) and not (
                entry in result_files.outputs and has_file_magic(entry.local_path)
                and result_files.output_excluded(path)
            )
            for path in expand_local_candidates(entry.local_path)
        )
    )
    return _unique(missing)


class RunFiles:
    """File operations on tracked runs, independent of lifecycle orchestration."""

    def __init__(self, *, store):
        self.store = store

    def input_file_is_current(self, run):
        record = resolve_record(run, self.store)
        files = record.files
        return file_is_current(files, files.input_file, files.input_versions)

    def output_file_is_current(self, run):
        record = resolve_record(run, self.store)
        if record.failure_phase in NO_EXECUTION_FAILURES:
            return False
        files = record.files
        return file_is_current(files, files.output_file, files.output_versions)

    def update_artifact_availability(self, record, path, state, message=None):
        """Persist availability of one artifact without changing the run outcome."""
        state = ArtifactAvailabilityState(state)

        def update(latest):
            files = latest.files.with_file_availability(path, state.value, message)
            if files == latest.files:
                return latest
            return replace(latest, updated_at=datetime.now(timezone.utc).isoformat(), files=files)

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
        return FileAccess(record.execution.target).snapshot_file(record, file, destination=destination)

    def retrieve_result_files(self, run, *, reserve):
        record = resolve_record(run, self.store)
        if not record.state.is_terminal:
            raise RuntimeError(f"Run {record.id} is not finished")

        if not record.execution.target.transfers_files(shared_filesystem=record.files.shared_filesystem):
            return record  # There is no separate target copy to restore.
        if record.execution.backend_id is None or not record.execution.target.available:
            return record
        missing = _missing_result_files(
            record.files, include_outputs=record.failure_phase not in NO_EXECUTION_FAILURES,
        )
        if not missing:
            return record

        access = FileAccess(record.execution.target)

        versions = {os.path.abspath(os.fspath(version.path)): version
                    for version in record.files.all_versions}
        # Input identity is tracked by input_versions, not output availability.
        artifact_paths = {entry.local_path for entry in record.files.artifacts}
        artifact_paths.update(os.fspath(version.path) for version in record.files.output_versions)

        expected = {}

        def verify(target, temporary):
            digest = expected.get(target)
            if digest is not None and file_digest(temporary) != digest:
                raise HistoricalFileMismatchError(f"Historical result files have been overwritten: {target}")

        # Resolve all concrete pairs before reserving anything. Expansion is
        # read-only; the exact same pairs are passed to the transfer below.
        planned = {}
        errors = {}
        by_destination = {}
        for path in missing:
            try:
                entries = access.resolve_result_files(record, (path,))
                if not entries:
                    raise FileNotFoundError(f"Historical artifact was not retrieved: {path}")
                for entry in entries:
                    previous = by_destination.setdefault(entry.local_path, entry)
                    if previous.remote_path != entry.remote_path:
                        raise ValueError(f"Different historical sources map to {entry.local_path}")
                    # An exact declaration may retain an alias, whereas a
                    # wildcard resolves to a canonical transfer destination.
                    # Every route to that destination must enforce its digest.
                    for version in (versions.get(path), versions.get(entry.local_path)):
                        if version is None or version.digest is None:
                            continue
                        prior = expected.setdefault(entry.local_path, version.digest)
                        if prior != version.digest:
                            raise ValueError(f"Conflicting historical versions map to {entry.local_path}")
                planned[path] = entries
            except OSError as exc:
                errors[path] = str(exc)

        async def restore():
            # Transfer each reserved destination once, regardless of how
            # many declarations refer to it. One failed member must not
            # prevent the remaining members of a wildcard from being restored.
            failures = {}
            for entry in by_destination.values():
                path = entry.local_path
                try:
                    if not await access.download_result_file(entry, verify=verify):
                        raise FileExistsError(f"Historical artifact was not retrieved: {path} already exists")
                except (OSError, RuntimeError) as exc:
                    failures[path] = str(exc)
            return failures

        from contextlib import nullcontext
        scope = reserve(record, tuple(by_destination.values())) if by_destination else nullcontext()
        with scope:
            failures = run_coro_sync(restore()) if by_destination else {}
            for path, entries in planned.items():
                message = next((failures[entry.local_path] for entry in entries
                                if entry.local_path in failures), None)
                if message is not None:
                    errors[path] = message
            for path in (*errors, *(path for path in planned if path not in errors)):
                if path in artifact_paths:
                    message = errors.get(path)
                    state = (ArtifactAvailabilityState.AVAILABLE if message is None
                             else ArtifactAvailabilityState.FAILED)
                    record = self.update_artifact_availability(record, path, state, message)
        return record
