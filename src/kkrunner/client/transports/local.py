"""Direct Python invocation and local file transport."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from kkrunner.client.contracts import Operation
from kkrunner.files.queries import (
    checked_files,
    expand_local_files,
    local_file_signature,
    query_files,
)
from kkrunner.files.snapshots import snapshot_destination
from kkrunner.files.versions import file_digest

from .base import Transport, _completed_process, _local_temporary_path
from .rpc import operation_payload


@dataclass(frozen=True)
class LocalTransport(Transport):
    """Transport backed by asyncio subprocesses and the local filesystem."""

    kind: Literal["local"] = "local"
    is_local = True

    def __post_init__(self):
        if self.kind != "local":
            raise ValueError("LocalTransport.kind must be 'local'")

    async def open_call(self, operation, request, on_output, *, local_io=None, use_stdin=True):
        from .calls import LocalCall
        files, stdin = ({}, None) if local_io is None else local_io(request.get("detached", False))
        paths = {
            kind: path for kind, path in (("out", request.get("output_file")), ("err", request.get("error_file")))
            if path is not None
        } if request.get("detached") else {}
        return LocalCall(
            getattr(operation.backend, operation.method), request, on_output,
            files=files, stdin=stdin if use_stdin else None, tail_paths=paths,
        )

    async def follow(self, operation, identity, on_output, **options):
        from .calls import OutputSink
        return await getattr(operation.backend, operation.method)(
            identity, OutputSink(on_output, "out"), OutputSink(on_output, "err"), **options,
        )

    async def call(self, operation: Operation, *args, **kwargs):
        # Local calls execute directly; argument payloads must still
        # be serializable to uphold the same contract as remote execution.
        import inspect

        encoded = operation_payload(args, kwargs)
        request = json.loads(encoded)
        function = (
            getattr(operation.backend, operation.method) if operation.backend is not None
            else operation.function
        )
        if inspect.iscoroutinefunction(function):
            result = await function(*request["args"], **request["kwargs"])
        else:
            result = await asyncio.to_thread(function, *request["args"], **request["kwargs"])
        # Verify portability of the response, including local-only execution.
        return json.loads(json.dumps(result, allow_nan=False))

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
        try:
            if os.path.samefile(source, destination):
                return
        except FileNotFoundError:
            pass  # A new destination will be published by atomic replacement.
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


