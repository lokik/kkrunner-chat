"""Local and remote filesystem mapping; no job execution or agent protocol."""

from __future__ import annotations

import os
import posixpath
from dataclasses import replace
from pathlib import Path, PurePosixPath

from kkrunner.files.manifest import FileMapping, FileSyncPolicy
from kkrunner.files.queries import checked_files, query_files
from kkrunner.client.staging import StagingSpec


async def query_remote_home(transport):
    """Resolve the target's home using the same rules for SSH and agent."""
    result = await transport.run(("sh", "-c", 'printf "%s" "$HOME"'))
    home = result.stdout.decode("utf8").strip()
    if not home or not posixpath.isabs(home):
        raise RuntimeError("Could not determine absolute remote home directory")
    return posixpath.normpath(home)


def _relative_local_path(path, local_root):
    """Resolve a local path and enforce the run's mapped root boundary."""
    path = Path(path).expanduser().resolve()
    root = Path(local_root).expanduser().resolve()
    try:
        return path.relative_to(root)
    except ValueError as exc:
        raise ValueError(
            f"Local path {path} is outside mapped root {root}; specify local_root/remote_root explicitly"
        ) from exc


class FileContext:
    """Per-run filesystem and synchronization policy."""

    def __init__(self, client, staging):
        if not isinstance(staging, StagingSpec):
            staging = StagingSpec(**dict(staging))
        self.client = client
        self.target = client.target
        self.filesystem_id = staging.filesystem_id
        self.shared_filesystem = staging.shared_filesystem
        self.transfers_files = self.target.transfers_files(shared_filesystem=self.shared_filesystem)
        self.local_root = Path(staging.local_root or (Path("/") if self.target.is_local else Path.home())).expanduser().resolve()
        self.remote_root = None if staging.remote_root is None else os.fspath(staging.remote_root)
        self.input_files = FileSyncPolicy(staging.input_sync) if self.transfers_files else FileSyncPolicy.NEVER
        self.output_files = FileSyncPolicy(staging.output_sync) if self.transfers_files else FileSyncPolicy.NEVER

    async def _remote_home(self):
        return await self.client._remote_home()

    def _target_root(self, home):
        """Use the same root rules for a symbolic preview and a resolved path."""
        if self.target.is_local:
            return os.fspath(self.local_root)
        root = self.remote_root
        if root is not None and posixpath.isabs(root):
            return posixpath.normpath(root)
        if root is None or root == "~":
            return home
        suffix = root[2:] if root.startswith("~/") else root
        if posixpath.isabs(home):
            return posixpath.normpath(posixpath.join(home, suffix))
        # Keep '~' symbolic: normalizing '~/..' before resolving the home
        # would turn an absolute target location into an unrelated relative path.
        suffix = posixpath.normpath(suffix)
        return home if suffix == "." else posixpath.join(home, suffix)

    async def _resolved_remote_root(self):
        if self.target.is_local or (self.remote_root is not None and posixpath.isabs(self.remote_root)):
            return self._target_root("~")
        return self._target_root(await self._remote_home())

    def _map_path(self, path, remote_root):
        if self.target.is_local:
            return os.path.abspath(os.fspath(path))
        relative = _relative_local_path(path, self.local_root)
        if not relative.parts:
            return remote_root
        return posixpath.join(remote_root, *relative.parts)

    def preview_path(self, path):
        """Map without target I/O, leaving the target home symbolic as '~'."""
        return self._map_path(path, self._target_root("~"))

    async def map_path(self, path):
        return self._map_path(path, await self._resolved_remote_root())

    async def resolve_target_path(self, path):
        """Resolve an already-target-side path, without mapping it again."""
        if self.target.is_local:
            return os.path.abspath(path)
        path = os.fspath(path)
        if path == "~" or path.startswith("~/"):
            home = await self._remote_home()
            return home if path == "~" else posixpath.normpath(posixpath.join(home, path[2:]))
        return posixpath.normpath(path)

    async def map_manifest(self, manifest):
        """Project declared file identities; do not stage files or capture versions."""
        if self.target.is_local:
            return manifest

        remote_root = await self._resolved_remote_root()

        def mapped(entry):
            if entry is None or entry.local_path is None:
                return entry
            return replace(entry, remote_path=self._map_path(entry.local_path, remote_root))

        mapping = FileMapping(
            local_root=os.fspath(self.local_root), remote_root=remote_root,
            input_sync=self.input_files.value, output_sync=self.output_files.value,
            filesystem_id=self.filesystem_id, shared_filesystem=self.shared_filesystem,
        )
        return replace(
            manifest,
            inputs=tuple(mapped(entry) for entry in manifest.inputs),
            outputs=tuple(mapped(entry) for entry in manifest.outputs),
            caches=tuple(replace(cache, file=mapped(cache.file)) for cache in manifest.caches),
            stdin=mapped(manifest.stdin), stdout=mapped(manifest.stdout),
            stderr=mapped(manifest.stderr), mapping=mapping,
        )

    def _unmap_path(self, path, remote_root):
        if self.target.is_local:
            return Path(os.path.abspath(path))
        path = posixpath.normpath(os.fspath(path))
        if not posixpath.isabs(path):
            raise ValueError(f"Remote path must be absolute: {path}")
        try:
            common = posixpath.commonpath((remote_root, path))
        except ValueError as exc:
            raise ValueError(f"Remote path {path} is outside mapped root {remote_root}") from exc
        if common != remote_root:
            raise ValueError(f"Remote path {path} is outside mapped root {remote_root}")
        relative = posixpath.relpath(path, remote_root)
        if relative == ".":
            return self.local_root
        local = self.local_root.joinpath(*PurePosixPath(relative).parts)
        _relative_local_path(local, self.local_root)
        return local

    async def unmap_path(self, path):
        return self._unmap_path(path, await self._resolved_remote_root())

    def expand_input(self, entry, remote_root):
        """Expand a reserved target pattern into exact local/target pairs.

        Never follow a wildcard match into a different reserved namespace.
        """
        source = self._unmap_path(entry.remote_path, remote_root)
        matches = []
        for item in checked_files(query_files((source,), root=self.local_root)):
            if item["status"] != "ok" or item["type"] != "file":
                continue
            path = item["path"]
            if os.path.realpath(path) != path:
                raise ValueError(f"Wildcard input contains a symbolic-link alias: {path}")
            matches.append(replace(entry, local_path=path,
                                   remote_path=self._map_path(path, remote_root)))
        return tuple(matches)

    async def map_directory(self, directory):
        return await self.map_path(directory)
