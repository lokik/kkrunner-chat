"""Call installed backend operations locally without a long-lived backend object.

The same context-independent module functions are used by the remote agent.
This client context owns only transport and invocation configuration, not the
target process. The one-shot FOLLOW command uses the existing stream format.
"""

from __future__ import annotations

import asyncio
import sys
from uuid import uuid4

from kkrunner.backends.registry import operation_ref
from kkrunner.client.operations import BackendOperation
from kkrunner.client.transport import LocalTransport
from kkrunner.client.execution_request import execution_request
from kkrunner.remote.protocol import CONTROL_PROTOCOL_VERSION
from kkrunner.remote.stream import FramedRunningCommand
from kkrunner.runs.base import RunState


class LocalManagedContext:
    def __init__(self, backend, *, options=None, transport=None):
        self.backend = backend
        self.options = dict(options or {})
        self.transport = LocalTransport() if transport is None else transport

    def operation(self, name):
        return BackendOperation.installed(*operation_ref(self.backend, name))

    async def _submit(self, request):
        return await self.transport.call(self.operation("submit"), request)

    async def _status(self, directory, backend_id, files=None):
        return await self.transport.call(self.operation("status"), backend_id)

    async def _cancel(self, directory, backend_id, files=None):
        return await self.transport.call(self.operation("cancel"), backend_id)

    async def _follow(self, directory, backend_id, *, offsets=None,
                      stdout_path=None, stderr_path=None):
        options = []
        if stdout_path is not None:
            options.extend(("--stdout-path", stdout_path))
        if stderr_path is not None:
            options.extend(("--stderr-path", stderr_path))
        for kind, flag in (("out", "--stdout-offset"), ("err", "--stderr-offset")):
            value = None if offsets is None else offsets.get(kind)
            if value is None and offsets is not None:
                options.append("--live-" + ("stdout" if kind == "out" else "stderr"))
            elif value is not None:
                options.extend((flag, str(value)))
        command = (
            sys.executable, "-m", "kkrunner_cli",
            "--protocol-version", str(CONTROL_PROTOCOL_VERSION),
            "follow", str(backend_id), *options, "--framed",
        )
        return FramedRunningCommand(await self.transport.start(command))


def local_managed_job(spec, *, backend="process", options=None,
                      submission_key=None, callback=None):
    """Create a client Job with one fresh-context target request."""
    from kkrunner.client.job import Job

    context = LocalManagedContext(backend, options=options)
    def request():
        data = execution_request(spec)
        data.update({
            "executor": backend,
            "executor_options": dict(options or {}),
            "id": submission_key or uuid4().hex,
            "detached": True,
        })
        return data

    from kkrunner.files.streams import close_output_files
    return Job(
        context, spec, callback=callback, request_factory=request,
        completion_cleanup=lambda: close_output_files(spec.output_file, spec.error_file),
    )


def attach_local_managed(spec, backend_id, *, backend="process", options=None,
                         callback=None, output_listener=None, status=None, start=False):
    """Recreate one client observer without resubmitting its calculation."""
    from kkrunner.client.job import Job
    from kkrunner.files.paths import file_path

    context = LocalManagedContext(backend, options=options)
    output = file_path(spec.output_file, spec.directory)
    error = file_path(spec.error_file, spec.directory)
    job = Job(context, spec, callback=callback, backend_id=backend_id,
              remote_directory=spec.directory, status=status,
              output_listener=output_listener)
    job.configure_observation(
        stream_offsets={"out": 0, "err": 0},
        stream_writer=None,
        stream_paths=(output, error),
    )
    if start:
        job.start()
    return job
