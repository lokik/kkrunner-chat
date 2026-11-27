"""Snapshots and retrieval of declared files through a client's file context."""

import asyncio
import glob
import os
import posixpath
from functools import partial

from kkrunner.client.file_context import FileContext, _relative_local_path
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


class FileAccess:
    def __init__(self, client):
        self.client = client

    async def _snapshot_file(self, run, file, *, destination=None):
        """Observe target-side content without affecting normal result downloads."""
        file, entry, destination = snapshot_request(
            run.files, run.execution.directory.local, file, destination,
        )
        context = FileContext(self.client, StagingSpec.from_mapping(run.files.mapping))
        _relative_local_path(file, context.local_root)
        if self.client.target.is_local or context.shared_filesystem:
            return await asyncio.to_thread(local_snapshot, file, destination=destination)
        remote = entry.remote_path if os.path.abspath(entry.local_path) == file else None
        if remote is None:
            remote = await context.map_path(file)
        root = await context._resolved_remote_root()
        normalized = posixpath.normpath(remote)
        if not posixpath.isabs(normalized) or posixpath.commonpath((root, normalized)) != root:
            raise ValueError(f"Remote snapshot path is outside mapped root: {remote}")
        # Also reject remote symlinks that escape the mapped filesystem root.
        # query_files checks real path containment without requiring a stable file.
        observed = await self.client.target.query_files([normalized], root=root)
        if not observed or observed[0]["status"] == "missing":
            raise FileNotFoundError(normalized)
        if observed[0]["type"] != "file":
            raise ValueError(f"Snapshot source is not a regular file: {normalized}")
        return await self.client.target.snapshot_file(normalized, destination=destination)

    def snapshot_file(self, run, file, *, destination=None):
        return _run_sync(self._snapshot_file(run, file, destination=destination))

    async def _resolve_result_files(self, run, files):
        """Read-only discovery: freeze the exact source and destination pairs."""
        context = FileContext(self.client, StagingSpec.from_mapping(run.files.mapping))
        if not context.transfers_files:
            return ()  # No independent target copy exists.
        remote_root = await context._resolved_remote_root()
        declared = {
            entry.local_path: entry for entry in (
                *run.files.input_entries, *run.files.outputs, *(cache.file for cache in run.files.caches),
                run.files.stdout, run.files.stderr,
            ) if entry is not None
        }
        pairs = {}
        for value in files:
            local = os.path.abspath(os.fspath(value))
            _relative_local_path(local, context.local_root)
            entry = declared.get(local)
            remote = entry.remote_path if entry is not None else None
            if remote is None:
                remote = await context.map_path(local)
            for matched in await self.client.target.expand_files(remote, remote_root):
                destination = os.fspath(context._unmap_path(matched, remote_root)) if glob.has_magic(remote) else local
                _relative_local_path(destination, context.local_root)
                if (entry is not None and entry in run.files.outputs and glob.has_magic(remote)
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
        return tuple(pairs.values())

    def resolve_result_files(self, run, files):
        return _run_sync(self._resolve_result_files(run, files))

    async def _download_result_files(self, entries, *, verify=None):
        """Download exact pairs without re-discovering or re-mapping paths."""
        downloaded = []
        for entry in entries:
            target = entry.local_path
            if os.path.lexists(target):
                continue
            try:
                await self.client.target.get_file(
                    entry.remote_path, target,
                    verify=None if verify is None else partial(verify, target),
                    overwrite=False,
                )
            except FileExistsError:
                continue
            downloaded.append(target)
        return tuple(downloaded)

    def download_result_files(self, entries, *, verify=None):
        return _run_sync(self._download_result_files(entries, verify=verify))

    def fetch_result_files(self, run, files, *, destinations=None, verify=None):
        """Convenience for caller-coordinated retrieval outside a RunManager."""
        from dataclasses import replace

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
        if not self.client.target.transfers_files(shared_filesystem=run.files.shared_filesystem):
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
        context = FileContext(self.client, StagingSpec.from_mapping(run.files.mapping))
        # The manifest is the authority: execution.directory.target is not
        # necessarily the parent of all output files (e.g. TAUDIR under a
        # separate TAUPATH). The target still rejects symlinks, directories,
        # non-absolute paths and changed file identities.
        resolved = tuple(
            [await context.resolve_target_path(path) for path in paths]
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
            self.client.target.call(
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
