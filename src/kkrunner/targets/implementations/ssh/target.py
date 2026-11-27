"""SSH invocation, streaming and remote file transport."""

from __future__ import annotations

import asyncio
import json
import sys
import tempfile
import os
import posixpath
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Literal, Tuple

from kkrunner.targets.contracts import Operation
from kkrunner.files.queries import (
    _sources,
    checked_files,
    query_files,
)
from kkrunner.files.snapshots import snapshot_destination

from kkrunner.files.staging import staged_local_file
from kkrunner.files.transfer import archive_compressions, relative_file_pairs, send_archive, receive_archive
from ...base import Target, _completed_process
from ...rpc import operation_payload, operation_result
from .rpc import operation_command
from .source import function_definition


@dataclass(frozen=True)
class SshTarget(Target):
    """Target using the system ``ssh`` client."""

    async def open_call(self, operation, request, on_output, *, local_io=None, use_stdin=True,
                        tail_paths=None, observe_only=False):
        from .call import SshCall
        command = operation_command(operation, python=self.python_command, framed="submit" if observe_only else "execute")
        process = await self.start(
            (*command, operation_payload((request,), {})), stdin=asyncio.subprocess.PIPE,
        )
        return SshCall(process, on_output, observe_only=observe_only)

    async def follow(self, operation, identity, on_output, **options):
        from .follow import follow_framed_command
        command = operation_command(operation, python=self.python_command, framed="follow")
        process = await self.start(
            (*command, operation_payload((identity,), options)),
            stdin=asyncio.subprocess.PIPE,
        )
        return await follow_framed_command(process, on_output, close_input=True)

    async def call(self, operation: Operation, *args, **kwargs):
        if self.pool_size and type(self) is SshTarget:
            from .sessions import pool_for
            return await pool_for(self).call(operation, *args, **kwargs)
        # Custom transport subclasses and pool_size=0 retain one-shot SSH.
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
    compression: Literal['auto', 'gz', 'bz2', 'xz', 'zstd'] | None = 'auto'
    python_command: Tuple[str, ...] = ("python3",)
    pool_size: int = 4
    pool_idle_seconds: float = 120.0
    kind: Literal["ssh"] = "ssh"

    def __post_init__(self):
        if self.kind != "ssh":
            raise ValueError("SshTarget.kind must be 'ssh'")
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
        if self.compression not in (None, "auto", "gz", "bz2", "xz", "zstd"):
            raise ValueError("Invalid SSH compression")
        # Runtime cache, deliberately excluded from dataclass persistence/identity.
        object.__setattr__(self, "_compression_mode", None)
        if type(self.pool_size) is not int or self.pool_size < 0 or self.pool_size > 64:
            raise ValueError("SSH pool_size must be between 0 and 64")
        if (type(self.pool_idle_seconds) not in (float, int)
                or not 1 <= self.pool_idle_seconds <= 86400):
            raise ValueError("SSH pool_idle_seconds must be between 1 and 86400")

    def close(self):
        """Close the shared persistent Python sessions for this SSH profile."""
        from .sessions import close_pool
        close_pool(self)
        object.__setattr__(self, "_compression_mode", None)

    async def _archive_compression(self):
        """Negotiate with the configured target Python, once per transport."""
        if self.compression is None:
            return ""
        if self._compression_mode is None:
            common = set(archive_compressions()).intersection(
                await self.call_function(archive_compressions),
            )
            requested = ("zstd", "gz") if self.compression == "auto" else (self.compression,)
            for name in requested:
                if name in common:
                    object.__setattr__(self, "_compression_mode", "zst" if name == "zstd" else name)
                    break
            else:
                raise ValueError(
                    f"Archive compression {self.compression!r} is not supported on both hosts"
                )
        return self._compression_mode

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
        """Use the same archive receiver and staging as a multi-file upload."""
        source = Path(source).resolve()
        if not source.is_file():
            raise FileNotFoundError(source)
        destination = PurePosixPath(destination)
        await self.put_files(
            [(source.name, destination.name)],
            source_directory=source.parent, destination_directory=destination.parent,
        )

    async def call_function(self, function, *args, **kwargs):
        """Invoke a portable shared function through the common operation path."""
        return await self.call(Operation.standalone(function), *args, **kwargs)

    async def get_file(self, source, destination, *, verify=None, overwrite=True):
        source = os.fspath(source)
        signature = await self.file_signature(source)
        if signature is None:
            raise FileNotFoundError(source)

        destination = Path(destination)
        command = self._command(("cat", "--", source))
        with staged_local_file(destination, verify=verify, overwrite=overwrite) as temporary:
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

    async def put_directory(self, source, destination):
        source = Path(source)
        if source.is_symlink() or not source.is_dir():
            raise ValueError(f"Directory source is invalid: {source}")
        await self._transfer_directory(source, destination, upload=True)

    async def get_directory(self, source, destination):
        await self._transfer_directory(source, destination, upload=False)

    async def _transfer_directory(self, source, destination, *, upload):
        compression = await self._archive_compression()
        producer = self._archive_command(send_archive, source, compression, remote=not upload, tree=True)
        consumer = self._archive_command(receive_archive, destination, compression, remote=upload, tree=True)
        await self._transfer_tar(producer, consumer)

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

    async def _transfer_tar(self, producer_command, consumer_command, *, producer_input=None):
        """Stream an archive and reap both subprocesses, even on pipe failure."""
        consumer = await asyncio.create_subprocess_exec(
            *consumer_command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        producer = None
        tasks = ()

        def kill(process):
            if process.returncode is None:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass

        try:
            producer = await asyncio.create_subprocess_exec(
                *producer_command,
                stdin=asyncio.subprocess.DEVNULL if producer_input is None else producer_input,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            async def pipe():
                try:
                    await self._pipe(producer.stdout, consumer.stdin)
                    return False
                except (BrokenPipeError, ConnectionResetError):
                    # An extractor that fails early can leave the producer
                    # blocked on stdout. Kill it and drain the pipe: wait()
                    # alone need not complete while unread bytes remain.
                    kill(producer)
                    while await producer.stdout.read(1024 * 1024):
                        pass
                    return True

            tasks = tuple(asyncio.create_task(operation) for operation in (
                pipe(), producer.stderr.read(), consumer.stderr.read(),
            ))
            broken_pipe, producer_error, consumer_error = await asyncio.gather(*tasks)
            await asyncio.gather(producer.wait(), consumer.wait())
        except BaseException:
            # Stop the active readers before communicate() takes over their
            # pipes. On EOF the receiver can discard its staged files itself.
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if producer is not None:
                kill(producer)
                await producer.communicate()
            consumer.stdin.close()
            try:
                await asyncio.wait_for(consumer.communicate(), timeout=5)
            except asyncio.TimeoutError:
                kill(consumer)
                await consumer.communicate()
            raise

        failures = (
            (producer, producer_command, producer_error),
            (consumer, consumer_command, consumer_error),
        )
        # Prefer the receiver's actual failure to the consequence of killing
        # its producer. Otherwise preserve the producer's original failure.
        if broken_pipe:
            failures = failures[::-1]
        for process, command, stderr in failures:
            if process.returncode:
                raise subprocess.CalledProcessError(process.returncode, command, stderr=stderr)

    def _archive_command(self, function, directory, compression, *, remote, tree=False):
        script = function_definition(function) + (
            f"\nimport sys\n{function.__name__}(sys.argv[1], sys.argv[2], sys.argv[3] == '1')\n"
        )
        python = self.python_command if remote else (sys.executable,)
        argv = (*python, "-c", script, os.fspath(directory), compression, str(int(tree)))
        return self._command(argv) if remote else argv

    async def _transfer_files(self, files, source_directory, destination_directory, *, upload):
        local = Path(source_directory if upload else destination_directory).expanduser().resolve()
        remote = posixpath.normpath(os.fspath(destination_directory if upload else source_directory))
        source_root, destination_root = (local, remote) if upload else (remote, local)
        source_path, destination_path = (Path, PurePosixPath) if upload else (PurePosixPath, Path)
        pairs = relative_file_pairs(
            files, source_root, destination_root,
            source_path=source_path, destination_path=destination_path,
        )
        if not pairs:
            return ()
        compression = await self._archive_compression()
        producer = self._archive_command(send_archive, source_root, compression, remote=not upload)
        consumer = self._archive_command(receive_archive, destination_root, compression, remote=upload)
        # A file descriptor supplies even very large manifests without ARG_MAX
        # or buffering the actual archive. Identical relative paths occur once.
        with tempfile.TemporaryFile() as manifest:
            manifest.write(json.dumps([source if source == target else (source, target)
                                       for source, target in pairs]).encode("utf-8"))
            manifest.seek(0)
            await self._transfer_tar(producer, consumer, producer_input=manifest)
        return tuple((os.fspath(source_path(source_root).joinpath(*PurePosixPath(source).parts)),
                      os.fspath(destination_path(destination_root).joinpath(*PurePosixPath(target).parts)))
                     for source, target in pairs)

    async def put_files(self, files, source_directory, destination_directory):
        return await self._transfer_files(files, source_directory, destination_directory, upload=True)

    async def get_files(self, files, source_directory, destination_directory):
        return await self._transfer_files(files, source_directory, destination_directory, upload=False)

    async def read_file(self, path):
        return (await self.run(("cat", "--", os.fspath(path)))).stdout

