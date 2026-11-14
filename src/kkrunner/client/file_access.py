"""Snapshots and retrieval of declared files through a client's file context."""

import asyncio
import glob
import os
import posixpath

from kkrunner.client.file_context import FileContext, _relative_local_path
from kkrunner.client.contracts import Operation
from kkrunner.files.queries import remote_file_cleanup
from kkrunner.client.staging import StagingSpec
from kkrunner.support.async_utils import run_coro_sync as _run_sync


class FileAccess:
    def __init__(self, client):
        self.client = client

    async def _snapshot_file(self, run, file, *, destination=None):
        """Observe target-side content without affecting normal result downloads."""
        context = FileContext(self.client, StagingSpec.from_mapping(run.files.mapping))
        _relative_local_path(file, context.local_root)
        entry = next(
            (item for item in (
                run.files.stdout, run.files.stderr, *run.files.outputs,
                *(cache.file for cache in run.files.caches),
            ) if item is not None and item.local_path == file),
            None,
        )
        remote = entry.remote_path if entry is not None else None
        if remote is None:
            remote = await context.map_path(file)
        root = await context._resolved_remote_root()
        normalized = posixpath.normpath(remote)
        if not posixpath.isabs(normalized) or posixpath.commonpath((root, normalized)) != root:
            raise ValueError(f"Remote snapshot path is outside mapped root: {remote}")
        # Also reject remote symlinks that escape the mapped filesystem root.
        # query_files checks real path containment without requiring a stable file.
        observed = await self.client.transport.query_files([normalized], root=root)
        if not observed or observed[0]["status"] == "missing":
            raise FileNotFoundError(normalized)
        if observed[0]["type"] != "file":
            raise ValueError(f"Snapshot source is not a regular file: {normalized}")
        return await self.client.transport.snapshot_file(normalized, destination=destination)

    def snapshot_file(self, run, file, *, destination=None):
        return _run_sync(self._snapshot_file(run, file, destination=destination))

    async def _fetch_result_files(self, run, files, *, destinations=None):
        context = FileContext(self.client, StagingSpec.from_mapping(run.files.mapping))
        remote_root = await context._resolved_remote_root()
        destinations = (
            {}
            if destinations is None
            else {os.path.abspath(os.fspath(path)): os.fspath(target) for path, target in destinations.items()}
        )
        for value in files:
            local = os.path.abspath(os.fspath(value))
            entry = next(
                (
                    item
                    for item in (
                        *run.files.inputs,
                        *run.files.outputs,
                        *(cache.file for cache in run.files.caches),
                        run.files.stdout,
                        run.files.stderr,
                    )
                    if item is not None and item.local_path == local
                ),
                None,
            )
            remote = entry.remote_path if entry is not None else None
            if remote is None:
                remote = await context.map_path(local)
            # Exact paths and wildcards have the same source-side selection
            # and use the transport's verified single-file download.
            for matched in await self.client.transport.expand_files(remote, remote_root):
                destination = os.fspath(await context.unmap_path(matched)) if glob.has_magic(remote) else local
                _relative_local_path(destination, context.local_root)
                target = destinations.get(destination, destination)
                if not os.path.isfile(target):
                    await self.client.transport.get_file(matched, target)

    def fetch_result_files(self, run, files, *, destinations=None):
        return _run_sync(
            self._fetch_result_files(
                run,
                files,
                destinations=destinations,
            )
        )


    async def _remote_cleanup(self, run, paths, *, expected=None):
        """Operate on declared remote files, including TAUDIR outside the run directory."""
        if self.client.transport.is_local:
            raise ValueError("Remote-file cleanup requires a remote transport")
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
            self.client.transport.call(
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
