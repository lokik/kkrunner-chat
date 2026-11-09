"""Low-level asynchronous command and file transports."""

from __future__ import annotations

import asyncio
import os
import posixpath
import shlex
import shutil
import subprocess
from pathlib import Path, PurePosixPath
from typing import Optional, Protocol
from uuid import uuid4

from kkrunner.files.queries import _sources, checked_files, expand_local_files, local_file_signature, query_files
from kkrunner.files.snapshots import snapshot_destination
from kkrunner.files.versions import file_digest
from kkrunner.remote.protocol import json_request


class RunningCommand(Protocol):
    """A running command with independently readable output streams."""

    stdout: asyncio.StreamReader
    stderr: asyncio.StreamReader
    returncode: Optional[int]

    async def wait(self) -> int: ...

    def terminate(self) -> None: ...


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

    async def run(self, argv, *, cwd=None, input=None, check=True):
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


class LocalTransport(Transport):
    """Transport backed by asyncio subprocesses and the local filesystem."""

    async def start(self, argv, *, cwd=None, stdin=None):
        return await asyncio.create_subprocess_exec(
            *(os.fspath(arg) for arg in argv),
            cwd=cwd,
            stdin=stdin,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

    async def run(self, argv, *, cwd=None, input=None, check=True):
        argv = tuple(os.fspath(arg) for arg in argv)
        process = await self.start(
            argv,
            cwd=cwd,
            stdin=asyncio.subprocess.PIPE if input is not None else None,
        )
        return await _completed_process(process, argv, input=input, check=check)

    async def put_file(self, source, destination):
        source = os.fspath(source)
        signature = local_file_signature(source)
        if signature is None:
            raise FileNotFoundError(source)
        destination = Path(destination)
        temporary = _local_temporary_path(destination)
        try:
            await asyncio.to_thread(shutil.copyfile, source, temporary)
            if local_file_signature(source) != signature:
                raise RuntimeError(f"Local file changed while copying: {source}")
            stat = temporary.stat()
            os.utime(temporary, ns=(stat.st_atime_ns, signature[1]))
            os.replace(temporary, destination)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass

    async def get_file(self, source, destination):
        await self.put_file(source, destination)

    async def query_files(self, sources, *, root=None, digest=False):
        return checked_files(await asyncio.to_thread(query_files, sources, root=root, digest=digest))

    async def expand_files(self, sources, root):
        return expand_local_files(sources, root)

    async def _transfer_files(self, paths, source_root, destination_root, transfer):
        """Preserve relative layout when copying selected local files."""
        source_root = Path(source_root).expanduser().resolve()
        destination_root = Path(destination_root).expanduser().resolve()
        transfers = []
        for source in paths:
            destination = destination_root / Path(source).relative_to(source_root)
            await transfer(source, destination)
            transfers.append((source, os.fspath(destination)))
        return tuple(transfers)

    async def put_files(self, sources, source_root, destination_root):
        paths = expand_local_files(sources, source_root)
        return await self._transfer_files(paths, source_root, destination_root, self.put_file)

    async def get_files(self, sources, source_root, destination_root):
        paths = await self.expand_files(sources, source_root)
        return await self._transfer_files(paths, source_root, destination_root, self.get_file)

    async def read_file(self, path):
        return await asyncio.to_thread(Path(path).read_bytes)

    async def snapshot_file(self, source, *, destination=None):
        if destination is None:
            return await self.read_file(source)
        with snapshot_destination(destination) as out:
            with open(source, "rb") as current:
                await asyncio.to_thread(shutil.copyfileobj, current, out)
        return Path(destination)

    async def file_digest(self, path):
        return await asyncio.to_thread(file_digest, path, missing_ok=True)

    async def file_signature(self, path):
        return await asyncio.to_thread(local_file_signature, path)


class SshTransport(Transport):
    """Transport using the system ``ssh`` client."""

    def __init__(self, host, *, ssh="ssh", options=(), zstd=False, command=("kkrunner",)):
        self.command = tuple(command)
        self.host = host
        self.ssh = ssh
        self.options = tuple(options)
        self.zstd = zstd
        self._tar_command = ("tar", "--zstd") if zstd else ("tar",)

    def _command(self, argv, cwd=None):
        command = shlex.join([os.fspath(arg) for arg in argv])
        if cwd is not None:
            command = "cd {} && exec {}".format(shlex.quote(os.fspath(cwd)), command)
        return [self.ssh, *self.options, self.host, command]

    async def start(self, argv, *, cwd=None, stdin=None):
        return await asyncio.create_subprocess_exec(
            *self._command(argv, cwd),
            stdin=stdin,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

    async def run(self, argv, *, cwd=None, input=None, check=True):
        command = self._command(argv, cwd)
        process = await self.start(
            argv,
            cwd=cwd,
            stdin=asyncio.subprocess.PIPE if input is not None else None,
        )
        return await _completed_process(process, command, input=input, check=check)

    async def put_file(self, source, destination):
        source = os.fspath(source)
        signature = local_file_signature(source)
        if signature is None:
            raise FileNotFoundError(source)

        destination = os.fspath(destination)
        parent = posixpath.dirname(destination)
        if parent:
            await self.run(("mkdir", "-p", "--", parent))
        temporary = posixpath.join(
            parent or ".",
            f".{posixpath.basename(destination)}.{uuid4().hex}.tmp",
        )

        command = self._command(("sh", "-c", 'cat > "$1"', "sh", temporary))
        try:
            with open(source, "rb") as source_file:
                process = await asyncio.create_subprocess_exec(
                    *command,
                    stdin=source_file,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                await _completed_process(process, command)
            if local_file_signature(source) != signature:
                raise RuntimeError(f"Local file changed while uploading: {source}")
            await json_request(
                self,
                self.command,
                "finalize-upload",
                payload={
                    "temporary": temporary,
                    "destination": destination,
                    "mtime_ns": signature[1],
                },
            )
        except BaseException:
            # Remote cleanup is best-effort and must not mask the upload error.
            try:
                await self.run(("rm", "-f", "--", temporary), check=False)
            except Exception:
                pass
            raise

    async def get_file(self, source, destination):
        source = os.fspath(source)
        signature = await self.file_signature(source)
        if signature is None:
            raise FileNotFoundError(source)

        destination = Path(destination)
        temporary = _local_temporary_path(destination)
        command = self._command(("cat", "--", source))
        try:
            with temporary.open("wb") as destination_file:
                process = await asyncio.create_subprocess_exec(
                    *command,
                    stdout=destination_file,
                    stderr=asyncio.subprocess.PIPE,
                )
                await _completed_process(process, command)
            if await self.file_signature(source) != signature:
                raise RuntimeError(f"Remote file changed while downloading: {source}")
            stat = temporary.stat()
            os.utime(temporary, ns=(stat.st_atime_ns, signature[1]))
            os.replace(temporary, destination)
        except BaseException:
            # Failure to remove the temporary download must not mask the
            # primary transfer error, including cancellation.
            try:
                temporary.unlink()
            except OSError:
                pass
            raise

    async def snapshot_file(self, source, *, destination=None):
        if destination is None:
            return await self.read_file(source)
        command = self._command(("cat", "--", os.fspath(source)))
        with snapshot_destination(destination) as out:
            process = await asyncio.create_subprocess_exec(
                *command, stdout=out, stderr=asyncio.subprocess.PIPE
            )
            await _completed_process(process, command)
        return Path(destination)

    async def query_files(self, sources, *, root=None, digest=False):
        results = await json_request(
            self,
            self.command,
            "query-files",
            payload={
                "paths": [os.fsdecode(path) for path in _sources(sources)],
                "root": None if root is None else os.fsdecode(root),
                "digest": digest,
            },
        )
        return checked_files(results)

    async def expand_files(self, sources, root):
        return tuple(
            sorted(
                item["path"]
                for item in await self.query_files(sources, root=root)
                if item["status"] == "ok" and item["type"] == "file"
            )
        )

    @staticmethod
    async def _pipe(source, destination):
        try:
            while True:
                chunk = await source.read(1024 * 1024)
                if not chunk:
                    break
                destination.write(chunk)
                await destination.drain()
        finally:
            destination.close()

    async def _transfer_tar(self, producer_command, consumer_command):
        """Stream an archive between two subprocesses and check both results."""
        consumer = await asyncio.create_subprocess_exec(
            *consumer_command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        producer = None
        try:
            producer = await asyncio.create_subprocess_exec(
                *producer_command,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            async def pipe():
                try:
                    await self._pipe(producer.stdout, consumer.stdin)
                except (BrokenPipeError, ConnectionResetError):
                    # If the extractor fails early, the archive producer can
                    # otherwise remain blocked on a full stdout pipe.
                    if producer.returncode is None:
                        producer.kill()

            _, producer_error, consumer_error, _, _ = await asyncio.gather(
                pipe(),
                producer.stderr.read(),
                consumer.stderr.read(),
                producer.wait(),
                consumer.wait(),
            )
            for process, command, stderr in (
                (producer, producer_command, producer_error),
                (consumer, consumer_command, consumer_error),
            ):
                if process.returncode:
                    raise subprocess.CalledProcessError(
                        process.returncode,
                        command,
                        stderr=stderr,
                    )
        finally:
            for process in (producer, consumer):
                if process is not None and process.returncode is None:
                    process.kill()
            await asyncio.gather(
                *(process.wait() for process in (producer, consumer) if process is not None),
            )

    async def put_files(self, sources, source_root, destination_root):
        source_root = Path(source_root).expanduser().resolve()
        destination_root = posixpath.normpath(os.fspath(destination_root))
        files = expand_local_files(sources, source_root)
        if not files:
            return ()

        relatives = [os.fspath(Path(source).relative_to(source_root)) for source in files]
        await self.run(("mkdir", "-p", "--", destination_root))
        producer = (*self._tar_command, "-C", os.fspath(source_root), "-cf", "-", "--", *relatives)
        consumer = self._command((*self._tar_command, "-C", destination_root, "-xf", "-"))
        await self._transfer_tar(producer, consumer)
        return tuple(
            (source, posixpath.join(destination_root, *Path(relative).parts))
            for source, relative in zip(files, relatives)
        )

    async def get_files(self, sources, source_root, destination_root):
        source_root = posixpath.normpath(os.fspath(source_root))
        destination_root = Path(destination_root).expanduser().resolve()
        files = await self.expand_files(sources, source_root)
        if not files:
            return ()

        relatives = [posixpath.relpath(source, source_root) for source in files]
        destination_root.mkdir(parents=True, exist_ok=True)
        producer = self._command((*self._tar_command, "-C", source_root, "-cf", "-", "--", *relatives))
        consumer = (*self._tar_command, "-C", os.fspath(destination_root), "-xf", "-")
        await self._transfer_tar(producer, consumer)
        return tuple(
            (source, os.fspath(destination_root.joinpath(*PurePosixPath(relative).parts)))
            for source, relative in zip(files, relatives)
        )

    async def read_file(self, path):
        return (await self.run(("cat", "--", os.fspath(path)))).stdout

    async def file_digest(self, path):
        item = (await self.query_files([path], digest=True))[0]
        return item.get("digest")

    async def file_signature(self, path):
        item = (await self.query_files([path]))[0]
        if item["status"] == "missing" or item["type"] != "file":
            return None
        return item["size"], item["mtime_ns"]
