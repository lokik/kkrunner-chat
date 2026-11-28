"""Snapshots, retrieval and cleanup of declared files on a target."""

import asyncio
import glob
import os
import posixpath
from dataclasses import replace
from functools import partial

from kkrunner.client.file_context import FileContext
from kkrunner.files.paths import relative_local_path
from kkrunner.client.contracts import Operation
from kkrunner.files.manifest import FileEntry
from kkrunner.files.queries import remote_file_cleanup
from kkrunner.files.snapshots import local_snapshot, snapshot_request
from kkrunner.client.staging import StagingSpec
from kkrunner.support.async_utils import run_coro_sync as _run_sync


def _destination_path(path):
    """Canonicalize the parent without following the final directory entry."""
    path = os.fspath(path)
    return os.path.join(os.path.realpath(os.path.dirname(path)), os.path.basename(path))


def _literal_source(path, root):
    """Escape a concrete source name, leaving the query's root literal."""
    if not posixpath.isabs(path):
        return glob.escape(path)
    return posixpath.join(root, glob.escape(posixpath.relpath(path, root)))


class FileAccess:
    def __init__(self, target):
        self.target = target

    def _validate_target(self, run):
        if run.execution.target != self.target:
            raise ValueError("Run belongs to a different transport")

    def collect_ended_files(self, run, status, *, on_resolved=None):
        """Return (manifest, error) after finalizing an ended run's files.

        The caller owns the file reservation and any history update. A transfer
        failure returns the partially finalized manifest and the original error;
        successful collection returns the finalized manifest and None.
        """
        from kkrunner.client.file_session import FileSession

        self._validate_target(run)
        if not status.state.is_terminal:
            raise ValueError("File collection requires terminal execution status")
        if run.files.captured_at is not None:
            return run.files, None

        async def collect():
            session = None
            try:
                session = FileSession.from_attachment(
                    self.target, replace(run, files=run.files.retry_failed_artifacts()),
                )
                return await session.finalize_async(status, on_resolved=on_resolved), None
            except (OSError, RuntimeError, ValueError) as exc:
                # A session that failed to initialize has no partial state yet.
                files = run.files.fail_pending_artifacts(str(exc)) if session is None else session.files
                return files, exc
            finally:
                if session is not None:
                    session.close()

        return _run_sync(collect())

    async def _snapshot_file(self, run, file, *, destination=None):
        """Observe target-side content without affecting normal result downloads."""
        file, entry, destination = snapshot_request(
            run.files, run.execution.directory.local, file, destination,
        )
        context = FileContext(self.target, StagingSpec.from_mapping(run.files.mapping))
        if run.files.mapping is None:
            relative_local_path(file, context.local_root)
        root = await context._resolved_remote_root()
        remote = entry.remote_path if os.path.abspath(entry.local_path) == file else None
        if remote is None:
            remote = context._map_path(file, root)
        normalized = posixpath.normpath(remote)
        if not posixpath.isabs(normalized) or posixpath.commonpath((root, normalized)) != root:
            raise ValueError(f"Remote snapshot path is outside mapped root: {remote}")
        # Also reject remote symlinks that escape the mapped filesystem root.
        # query_files checks real path containment without requiring a stable file.
        observed = await self.target.query_files([_literal_source(normalized, root)], root=root)
        if not observed or observed[0]["status"] == "missing":
            raise FileNotFoundError(normalized)
        if observed[0]["type"] != "file":
            raise ValueError(f"Snapshot source is not a regular file: {normalized}")
        return await self.target.snapshot_file(normalized, destination=destination)

    def snapshot_file(self, run, file, *, destination=None):
        self._validate_target(run)
        if self.target.is_local or run.files.shared_filesystem:
            path, _, destination = snapshot_request(
                run.files, run.execution.directory.local, file, destination,
            )
            return local_snapshot(path, destination=destination)
        if not self.target.available:
            raise ValueError("Unsupported transport; only local history is available")
        return _run_sync(self._snapshot_file(run, file, destination=destination))

    async def _resolve_result_groups(self, run, files):
        """Read-only discovery: yield exact pairs or an I/O failure for each requested declaration."""
        context = FileContext(self.target, StagingSpec.from_mapping(run.files.mapping))
        if not context.transfers_files:
            return  # No independent target copy exists.
        remote_root = await context._resolved_remote_root()
        declared = {
            entry.local_path: entry for entry in (
                *run.files.input_entries, *run.files.outputs, *(cache.file for cache in run.files.caches),
                run.files.stdout, run.files.stderr,
            ) if entry is not None
        }
        captured_paths = {os.path.abspath(os.fspath(version.path)) for version in run.files.all_versions}
        for value in files:
            pairs = {}
            try:
                local = os.path.abspath(os.fspath(value))
                relative_local_path(local, context.local_root)
                entry = declared.get(local)
                remote = entry.remote_path if entry is not None else None
                if remote is None:
                    remote = context._map_path(local, remote_root)
                concrete = local in captured_paths or (entry is not None and not run.files.is_pattern(entry))
                wildcard = not concrete and glob.has_magic(remote)
                source = _literal_source(remote, remote_root) if concrete else remote
                for matched in await self.target.expand_files(source, remote_root):
                    destination = os.fspath(context._unmap_path(matched, remote_root)) if wildcard else local
                    if (entry is not None and entry in run.files.outputs and wildcard
                            and run.files.output_excluded(matched, remote=True)):
                        continue
                    # Retain exact historical file identities. A wildcard is
                    # expanded in the persisted mapped namespace, not today's alias.
                    # Resolve the parent, but never follow or replace a final
                    # symlink at the destination (including a dangling one).
                    target = _destination_path(destination)
                    pair = FileEntry(local_path=target, remote_path=matched)
                    previous = pairs.setdefault(target, pair)
                    if previous.remote_path != matched:
                        raise ValueError(f"Different historical sources map to {target}")
            except OSError as exc:
                yield value, exc
            else:
                yield value, tuple(pairs.values())

    def resolve_result_groups(self, run, files):
        """Resolve a history selection in one context, retaining per-path errors."""
        self._validate_target(run)

        async def resolve():
            return {path: result async for path, result in self._resolve_result_groups(run, files)}

        return _run_sync(resolve())

    async def _resolve_result_files(self, run, files):
        pairs = {}
        async for _, result in self._resolve_result_groups(run, files):
            if isinstance(result, OSError):
                raise result
            for entry in result:
                previous = pairs.setdefault(entry.local_path, entry)
                if previous.remote_path != entry.remote_path:
                    raise ValueError(f"Different historical sources map to {entry.local_path}")
        return tuple(pairs.values())

    def resolve_result_files(self, run, files):
        self._validate_target(run)
        return _run_sync(self._resolve_result_files(run, files))

    async def download_result_file(self, entry, *, verify=None):
        """Publish one exact pair without replacing an existing destination."""
        target = entry.local_path
        if os.path.lexists(target):
            return False
        try:
            await self.target.get_file(
                entry.remote_path, target,
                verify=None if verify is None else partial(verify, target),
                overwrite=False,
            )
        except FileExistsError:
            return False
        return True

    async def _download_result_files(self, entries, *, verify=None):
        """Download exact pairs without re-discovering or re-mapping paths."""
        downloaded = []
        for entry in entries:
            if await self.download_result_file(entry, verify=verify):
                downloaded.append(entry.local_path)
        return tuple(downloaded)

    def download_result_files(self, entries, *, verify=None):
        return _run_sync(self._download_result_files(entries, verify=verify))

    def fetch_result_files(self, run, files, *, destinations=None, verify=None):
        """Convenience for caller-coordinated retrieval outside a RunManager."""
        entries = self.resolve_result_files(run, files)
        if destinations is not None:
            targets = {_destination_path(path): _destination_path(target)
                       for path, target in destinations.items()}
            originals = {}
            staged = []
            for entry in entries:
                target = targets.get(entry.local_path, entry.local_path)
                if target in originals:
                    raise ValueError(f"Different historical sources map to {target}")
                originals[target] = entry.local_path
                staged.append(replace(entry, local_path=target))
            entries = tuple(staged)
            if verify is not None:
                original_verify = verify
                verify = lambda target, temporary: original_verify(originals[target], temporary)
            return tuple(originals[path] for path in self.download_result_files(entries, verify=verify))
        return self.download_result_files(entries, verify=verify)

    async def _remote_cleanup(self, run, paths, *, expected=None):
        """Operate on declared remote files, including TAUDIR outside the run directory."""
        self._validate_target(run)
        if not self.target.transfers_files(shared_filesystem=run.files.shared_filesystem):
            raise ValueError("Remote-file cleanup requires a separate target filesystem")
        entries = (
            *run.files.inputs, *run.files.outputs,
            *(cache.file for cache in run.files.caches),
            run.files.stdin, run.files.stdout, run.files.stderr,
        )
        declared = {}
        for entry in entries:
            if entry is None or entry.remote_path is None:
                continue
            previous = declared.setdefault(entry.remote_path, entry.kind)
            if previous != entry.kind:
                raise ValueError(f"Contradictory declared kinds for {entry.remote_path}")
        paths = tuple(paths)
        if not set(paths).issubset(declared) or len(paths) != len(set(paths)):
            raise ValueError("Only distinct, declared remote manifest paths may be inspected or deleted")
        # The manifest is the authority: execution.directory.target is not
        # necessarily the parent of all output files (e.g. TAUDIR under a
        # separate TAUPATH). The target still rejects symlinks, directories,
        # non-absolute paths and changed file identities.
        resolved = tuple(
            [await self.target.resolve_path(path) for path in paths]
        )
        expected_resolved = None if expected is None else {
            resolved_path: (
                dict(expected[original_path])
                if isinstance(expected[original_path], dict)
                else list(expected[original_path])
            )
            for original_path, resolved_path in zip(paths, resolved)
        }
        kinds_resolved = {
            resolved_path: declared[original_path]
            for original_path, resolved_path in zip(paths, resolved)
        }
        replies = await asyncio.wait_for(
            self.target.call(
                Operation.standalone(remote_file_cleanup),
                resolved, expected=expected_resolved, kinds=kinds_resolved,
            ),
            timeout=45 if expected is not None else 15,
        )
        return tuple(
            {**reply, "path": original_path}
            for original_path, reply in zip(paths, replies)
        )

    def inspect_remote_files(self, run, paths):
        """Retrieve exact remote file identities for a deletion confirmation."""
        return _run_sync(self._remote_cleanup(run, paths))

    def delete_remote_files(self, run, expected):
        """Delete explicitly approved files, reporting per-file failures."""
        return _run_sync(self._remote_cleanup(run, tuple(expected), expected=expected))
