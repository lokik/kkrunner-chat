"""Agentless SSH transport using the common raw SSH file operations."""

from __future__ import annotations

import asyncio
import json

from kkrunner.client.transport import SshTransport
from kkrunner.client.portable_functions import function_source
from kkrunner.files.queries import finalize_upload, query_files


# Stable portable-script entrypoints used by standalone regression tests.
_QUERY_SOURCE = function_source(query_files, invocation="query")
_FINALIZE_SOURCE = function_source(finalize_upload, invocation="finalize")


class SshDirectTransport(SshTransport):
    """No installed KKRunner is required on the SSH target."""

    def __init__(self, host, *, ssh="ssh", options=(), zstd=False, python_command=("python3",)):
        super().__init__(
            host, ssh=ssh, options=options, zstd=zstd,
            command=(), python_command=python_command,
        )

    async def start_function(self, fn, *args, **kwargs):
        """Start a long-running standalone function with its control stdin."""
        script = function_source(fn, invocation="start")
        payload = json.dumps({"args": args, "kwargs": kwargs})
        return await self.start(
            (*self.python_command, "-u", "-c", script, payload),
            stdin=asyncio.subprocess.PIPE,
        )
