"""Agentless SSH filesystem service: the same portable code runs locally."""

from __future__ import annotations

import asyncio
import json

from kkrunner.client.transport import SshTransport
from kkrunner.client.ssh_direct.functions import function_source
from kkrunner.files.queries import _sources, checked_files, finalize_upload, query_files


# Stable test/debug entrypoints, generated from the actual local functions.
_QUERY_SOURCE = function_source(query_files, invocation="query")
_FINALIZE_SOURCE = function_source(finalize_upload, invocation="finalize")


class SshDirectTransport(SshTransport):
    """Use stdlib Python on target without an installed agent or package."""

    def __init__(self, host, *, ssh="ssh", options=(), zstd=False, python_command=("python3",)):
        super().__init__(host, ssh=ssh, options=options, zstd=zstd, command=())
        self.python_command = tuple(python_command)

    async def call_function(self, fn, *args, **kwargs):
        """Return one JSON-serializable result; stdout is reserved for JSON."""
        script = function_source(fn, invocation="call")
        payload = json.dumps({"args": args, "kwargs": kwargs}).encode("utf-8")
        response = await self.run((*self.python_command, "-c", script), input=payload)
        return json.loads(response.stdout.decode("utf-8"))

    async def start_function(self, fn, *args, **kwargs):
        """Start a long-running function, retaining stdin for runtime control."""
        script = function_source(fn, invocation="start")
        payload = json.dumps({"args": args, "kwargs": kwargs})
        return await self.start(
            (*self.python_command, "-u", "-c", script, payload),
            stdin=asyncio.subprocess.PIPE,
        )

    async def _finalize_upload(self, temporary, destination, mtime_ns):
        await self.call_function(finalize_upload, temporary, destination, mtime_ns)

    async def query_files(self, sources, *, root=None, digest=False):
        results = await self.call_function(
            query_files, [str(path) for path in _sources(sources)],
            root=None if root is None else str(root), digest=digest,
        )
        return checked_files(results)
