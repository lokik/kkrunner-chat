"""Direct Python invocation and local file transport."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from kkrunner.targets.contracts import Operation
from kkrunner.files.queries import (
    checked_files,
    expand_local_files,
    local_file_signature,
    query_files,
)
from kkrunner.files.snapshots import snapshot_destination
from kkrunner.files.versions import file_digest

from kkrunner.files.staging import staged_directory, staged_local_file
from kkrunner.files.transfer import relative_file_pairs, transfer_path
from ...base import Target, _completed_process
from ...rpc import operation_payload


@dataclass(frozen=True)
class LocalTarget(Target):
    """Target backed by asyncio subprocesses and the local filesystem."""

    kind: Literal["local"] = "local"
    is_local = True
    host = None

    def __post_init__(self):
        if self.kind != "local":
            raise ValueError("LocalTarget.kind must be 'local'")

    async def open_call(self, operation, request, on_output, *, local_io=None, use_stdin=True,
                        tail_paths=None, observe_only=False):
        from .call import LocalCall
        files, stdin = (None, None) if local_io is None else local_io()
        return LocalCall(
            getattr(operation.backend, operation.method), request, on_output,
            files=files, stdin=stdin if use_stdin else None, tail_paths=tail_paths,
            observe_only=observe_only,
        )

    async def follow(self, operation, identity, on_output, **options):
        from ...calls import OutputSink
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

    async def put_file(self, source, destination, *, verify=None, overwrite=True):
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
        with staged_local_file(destination, verify=verify, overwrite=overwrite) as temporary:
            await asyncio.to_thread(shutil.copyfile, source, temporary)
            if local_file_signature(source) != signature:
                raise RuntimeError(f"Local file changed while copying: {source}")
            stat = temporary.stat()
            os.utime(temporary, ns=(stat.st_atime_ns, signature[1]))

    # On a local filesystem both directions are the same stable copy.
    get_file = put_file

    async def put_directory(self, source, destination):
        """Copy a whole tree before replacing the destination."""
        source, destination = Path(source), Path(destination)
        if source.is_symlink() or not source.is_dir():
            raise ValueError(f"Directory source is invalid: {source}")
        if destination.is_symlink():
            raise ValueError(f"Cannot replace symbolic link with directory: {destination}")
        source_path, destination_path = source.resolve(), destination.resolve()
        if source_path == destination_path:
            return
        if source_path in destination_path.parents or destination_path in source_path.parents:
            raise ValueError("Directory source and destination overlap")
        with staged_directory(destination) as received:
            await asyncio.to_thread(shutil.copytree, source, received, dirs_exist_ok=True)

    get_directory = put_directory

    async def query_files(self, sources, *, root=None, digest=False):
        return checked_files(await asyncio.to_thread(query_files, sources, root=root, digest=digest))

    async def expand_files(self, sources, root):
        return expand_local_files(sources, root)

    async def _transfer_files(self, files, source_directory, destination_directory, transfer):
        source_root = Path(source_directory).expanduser().resolve()
        destination_root = Path(destination_directory).expanduser().resolve()
        pairs = relative_file_pairs(files, source_root, destination_root)
        result = []
        for source, destination in pairs:
            source = transfer_path(source_root, source, source=True)
            destination = transfer_path(destination_root, destination, source=False)
            await transfer(source, destination)
            result.append((os.fspath(source), os.fspath(destination)))
        return tuple(result)

    async def put_files(self, files, source_directory, destination_directory):
        return await self._transfer_files(files, source_directory, destination_directory, self.put_file)

    async def get_files(self, files, source_directory, destination_directory):
        return await self._transfer_files(files, source_directory, destination_directory, self.get_file)

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
