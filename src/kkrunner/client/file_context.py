"""Local and remote filesystem mapping; no job execution or agent protocol."""

from __future__ import annotations

import os
import posixpath
from pathlib import Path, PurePosixPath

from kkrunner.files.manifest import FileSyncPolicy


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
        self.client = client
        self.transport = client.transport
        self.filesystem_id = staging.filesystem_id
        self.local_root = Path(staging.local_root or (Path("/") if self.transport.is_local else Path.home())).expanduser().resolve()
        self.remote_root = None if staging.remote_root is None else os.fspath(staging.remote_root)
        self.input_files = FileSyncPolicy(staging.input_sync)
        self.output_files = FileSyncPolicy(staging.output_sync)

    async def _remote_home(self):
        return await self.client._remote_home()

    async def _resolved_remote_root(self):
        if self.transport.is_local:
            return os.fspath(self.local_root)
        root = self.remote_root
        if root is not None and posixpath.isabs(root):
            return posixpath.normpath(root)
        home = await self._remote_home()
        if root is None or root == "~":
            return home
        if root.startswith("~/"):
            return posixpath.join(home, root[2:])
        return posixpath.join(home, root)

    async def map_path(self, path):
        if self.transport.is_local:
            return os.path.abspath(os.fspath(path))
        relative = _relative_local_path(path, self.local_root)
        remote_root = await self._resolved_remote_root()
        if not relative.parts:
            return remote_root
        return posixpath.join(remote_root, *relative.parts)

    async def unmap_path(self, path):
        if self.transport.is_local:
            return Path(os.path.abspath(path))
        remote_root = await self._resolved_remote_root()
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

    async def map_directory(self, directory):
        return await self.map_path(directory)

