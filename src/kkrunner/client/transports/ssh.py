"""SSH invocation, streaming and remote file transport."""

from __future__ import annotations

import asyncio
import os
import posixpath
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Literal, Tuple
from uuid import uuid4

from kkrunner.client.contracts import Operation
from kkrunner.files.queries import (
    _sources,
    checked_files,
    expand_local_files,
    finalize_upload,
    local_file_signature,
    query_files,
)
from kkrunner.files.snapshots import snapshot_destination

from .base import Transport, _completed_process, _local_temporary_path
from .rpc import operation_command, operation_payload, operation_result


@dataclass(frozen=True)
class SshTransport(Transport):
    """Transport using the system ``ssh`` client."""

    async def open_call(self, operation, request, on_output, *, local_io=None, use_stdin=True):
        from .calls import SshCall
        command = operation_command(operation, python=self.python_command, framed=True)
        process = await self.start(
            (*command, operation_payload((request,), {})), stdin=asyncio.subprocess.PIPE,
        )
        return SshCall(process, on_output)

    async def follow(self, operation, identity, on_output, **options):
        from .follow import follow_framed_command
        operation = Operation.for_backend(operation.backend, "follow_framed")
        process = await self.start_operation(operation, identity, **options)
        return await follow_framed_command(process, on_output)

    async def call(self, operation: Operation, *args, **kwargs):
        payload = operation_payload(args, kwargs).encode("utf-8")
        command = operation_command(operation, python=self.python_command)
        result = await self.run(command, input=payload)
        return operation_result(result.stdout)

    async def start_operation(self, operation: Operation, *args, **kwargs):
        payload = operation_payload(args, kwargs)
        command = operation_command(operation, python=self.python_command, streaming=True)
        return await self.start((*command, payload), stdin=asyncio.subprocess.PIPE)

    host: str
    ssh: str = "ssh"
    options: Tuple[str, ...] = ()
    zstd: bool = False
    python_command: Tuple[str, ...] = ("python3",)
    kind: Literal["ssh"] = "ssh"

    def __post_init__(self):
        if self.kind != "ssh":
            raise ValueError("SshTransport.kind must be 'ssh'")
        for name in ("host", "ssh"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise ValueError(f"SSH {name} must be a nonempty string")
        for name in ("options", "python_command"):
            value = getattr(self, name)
            value = (value,) if isinstance(value, str) else tuple(value)
            if any(not isinstance(item, str) for item in value):
                raise TypeError(f"SSH {name} must contain strings")
            object.__setattr__(self, name, value)
        if not self.python_command:
            raise ValueError("SSH python_command must not be empty")
        if type(self.zstd) is not bool:
            raise TypeError("SSH zstd must be bool")

    @property
    def _tar_command(self):
        return ("tar", "--zstd") if self.zstd else ("tar",)

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
            await self._finalize_upload(temporary, destination, signature[1])
        except BaseException:
            # Remote cleanup is best-effort and must not mask the upload error.
            try:
                await self.run(("rm", "-f", "--", temporary), check=False)
            except Exception:
                pass
            raise

    async def call_function(self, function, *args, **kwargs):
        """Invoke a portable shared function through the common operation path."""
        return await self.call(Operation.standalone(function), *args, **kwargs)

    async def _finalize_upload(self, temporary, destination, mtime_ns):
        await self.call_function(finalize_upload, temporary, destination, mtime_ns)

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
        # Identical portable implementation for raw SSH and agent-backed SSH.
        results = await self.call_function(
            query_files,
            [os.fsdecode(path) for path in _sources(sources)],
            root=None if root is None else os.fsdecode(root),
            digest=digest,
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
