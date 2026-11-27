"""Direct Python invocation and local file transport."""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from kkrunner.targets.contracts import Operation
from kkrunner.files.queries import (
    checked_files,
    expand_local_files,
    query_files,
)
from kkrunner.files.snapshots import local_snapshot

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

    async def query_files(self, sources, *, root=None, digest=False):
        return checked_files(await asyncio.to_thread(query_files, sources, root=root, digest=digest))

    async def expand_files(self, sources, root):
        return expand_local_files(sources, root)

    async def read_file(self, path):
        return await asyncio.to_thread(Path(path).read_bytes)

    async def snapshot_file(self, source, *, destination=None):
        return await asyncio.to_thread(local_snapshot, source, destination=destination)

