"""Low-level asynchronous command and file targets."""

from __future__ import annotations

import subprocess
from dataclasses import asdict
from pathlib import Path

from kkrunner.targets.contracts import Operation, RunningCommand
from kkrunner.files.snapshots import snapshot_destination


class Target:
    """Run commands and move files on one target machine.

    Target operations are asynchronous because remote execution is mostly
    I/O. Only targets supporting file transfer implement the transfer methods.
    ``put_file()`` and ``get_file()`` are exact one-file primitives.
    ``put_files()`` and ``get_files()`` accept exact relative paths or explicit
    source/destination pairs, with separate roots for the two filesystems.
    Pattern expansion belongs to the caller.
    """

    is_local = False
    supports_file_transfer = False
    available = True
    host: str | None

    def transfers_files(self, *, shared_filesystem=False):
        """Whether the workspace requires a separate target file copy."""
        return self.supports_file_transfer and not shared_filesystem

    def close(self):
        """Release transport resources, if any."""

    def to_dict(self):
        """Return the immutable connection settings used for persistence."""
        from . import require_persistable_target
        return asdict(require_persistable_target(self))

    async def open_call(self, operation, request, on_output, *, local_io=None, use_stdin=True,
                        tail_paths=None, observe_only=False):
        """Open an invocation; local I/O and file tails are explicitly requested.

        An observation-only invocation has no in-band cancellation commands;
        closing its connection releases the observation.
        """
        raise NotImplementedError

    async def follow(self, operation, identity, on_output, **options):
        raise NotImplementedError

    async def run(self, argv, *, cwd=None, input=None, check=True):
        raise NotImplementedError

    async def call(self, operation: Operation, *args, **kwargs):
        """Run one context-independent operation and return its JSON result."""
        raise NotImplementedError

    async def start_operation(self, operation: Operation, *args, **kwargs):
        """Start a streaming operation; stdout/stderr remain live channels."""
        raise NotImplementedError

    async def start(self, argv, *, cwd=None, stdin=None) -> RunningCommand:
        raise NotImplementedError

    async def put_file(self, source, destination):
        raise NotImplementedError

    async def put_directory(self, source, destination):
        """Transfer a complete directory tree."""
        raise NotImplementedError

    async def get_directory(self, source, destination):
        """Retrieve and replace a complete directory tree."""
        raise NotImplementedError

    async def get_file(self, source, destination, *, verify=None, overwrite=True):
        raise NotImplementedError

    async def put_files(self, files, source_directory, destination_directory):
        raise NotImplementedError

    async def get_files(self, files, source_directory, destination_directory):
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
        """Return the SHA-256 digest of a regular file, or None."""
        item = (await self.query_files([path], digest=True))[0]
        return item.get("digest")

    async def file_signature(self, path):
        """Return (size, mtime_ns) for a regular file, or None."""
        item = (await self.query_files([path]))[0]
        if item["status"] == "missing" or item["type"] != "file":
            return None
        return item["size"], item["mtime_ns"]


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
