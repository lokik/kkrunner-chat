"""Low-level asynchronous command and file transports."""

from __future__ import annotations

import subprocess
from dataclasses import asdict
from pathlib import Path
from uuid import uuid4

from kkrunner.client.contracts import RunningCommand
from kkrunner.client.operations import BackendOperation
from kkrunner.files.snapshots import snapshot_destination


def _local_temporary_path(destination):
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    return destination.with_name(f".{destination.name}.{uuid4().hex}.tmp")


class Transport:
    """Run commands and move files on one target machine.

    Transport operations are asynchronous because remote execution is mostly
    I/O. ``put_file()`` and ``get_file()`` are exact one-file primitives.
    ``put_files()`` and ``get_files()`` accept one path/pattern or an iterable
    and preserve each matched file's path relative to the supplied source root.
    Wildcards are always expanded on the source side of the transfer.
    """

    is_local = False

    def to_dict(self):
        """Return the immutable connection settings used for persistence."""
        from . import require_persistable_transport
        return asdict(require_persistable_transport(self))

    def direct_channel(self, context, files):
        raise NotImplementedError

    async def follow(self, operation, identity, on_output, **options):
        raise NotImplementedError

    async def run(self, argv, *, cwd=None, input=None, check=True):
        raise NotImplementedError

    async def call(self, operation: BackendOperation, *args, **kwargs):
        """Run one context-independent operation and return its JSON result."""
        raise NotImplementedError

    async def start_operation(self, operation: BackendOperation, *args, **kwargs):
        """Start a streaming operation; stdout/stderr remain live channels."""
        raise NotImplementedError

    async def start(self, argv, *, cwd=None, stdin=None) -> RunningCommand:
        raise NotImplementedError

    async def put_file(self, source, destination):
        raise NotImplementedError

    async def get_file(self, source, destination):
        raise NotImplementedError

    async def put_files(self, sources, source_root, destination_root):
        raise NotImplementedError

    async def get_files(self, sources, source_root, destination_root):
        raise NotImplementedError

    async def expand_files(self, sources, root):
        """Expand paths/patterns on the target filesystem into concrete files."""
        raise NotImplementedError

    async def query_files(self, sources, *, root=None, digest=False):
        """Query literal paths, or expand root-confined patterns, in one batch."""
        raise NotImplementedError

    async def read_file(self, path):
        raise NotImplementedError

    async def snapshot_file(self, source, *, destination=None):
        """Read a changing file, bypassing the stable-result download contract."""
        if destination is None:
            return await self.read_file(source)
        with snapshot_destination(destination) as out:
            out.write(await self.read_file(source))
        return Path(destination)

    async def file_digest(self, path):
        """Return the SHA-256 digest of *path*, or ``None`` if it does not exist."""
        raise NotImplementedError

    async def file_signature(self, path):
        """Return ``(size, mtime_ns)`` for *path*, or ``None`` if absent."""
        raise NotImplementedError


async def _completed_process(process, argv, input=None, check=True):
    try:
        stdout, stderr = await process.communicate(input)
    except BaseException:
        # Cancelling a command must not abandon its subprocess (or leave an
        # unreaped zombie). This also handles failures during pipe I/O.
        if process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass
        await process.wait()
        raise
    result = subprocess.CompletedProcess(tuple(argv), process.returncode, stdout, stderr)
    if check and result.returncode:
        raise subprocess.CalledProcessError(
            result.returncode,
            result.args,
            output=result.stdout,
            stderr=result.stderr,
        )
    return result


