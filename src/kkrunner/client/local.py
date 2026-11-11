"""Local endpoint: compose static target operations with client Job resources."""

from __future__ import annotations

from uuid import uuid4

from kkrunner.backends import configured_backend
from kkrunner.backends.contracts import BackendCapability, JobRef, RecoveryResult, UnsupportedCapability
from kkrunner.client.contracts import RunEndpoint
from kkrunner.client.execution_request import execution_request
from kkrunner.client.operations import BackendOperation
from kkrunner.client.transport import LocalTransport
from kkrunner.files.paths import file_path
from kkrunner.files.streams import close_output_files
from kkrunner.client.run_parts import RunParts
from kkrunner.runs.base import ExecutionSpec, RunState, RunStatus
from kkrunner.support.async_utils import run_coro_sync


class LocalRunEndpoint(RunEndpoint):
    """Invoke the selected configured backend on this host.

    Backend configuration belongs to ``self.backend``. Client attachment
    and transport resources belong here. The target operations
    see only an execution request or an opaque job reference.
    """

    supports_result_file_retrieval = False
    supports_tracked_runs = True

    def __init__(self, backend=None, *, backend_options=None):
        self.backend = configured_backend(backend, options=backend_options)
        self.transport = LocalTransport()

    @property
    def default_persistent(self):
        return self.backend.default_persistent

    @property
    def default_detached(self):
        return self.backend.default_detached

    def create_job(self, spec, *, submission_key=None, callback=None):
        from kkrunner.client.job import Job

        spec.require_complete()
        if self.backend.session_bound:
            from kkrunner.client.direct_channels import LocalDirectChannel
            return Job.direct(spec, callback=callback, adapter=LocalDirectChannel(self.backend))

        def request():
            return {
                **execution_request(spec),
                "backend": self.backend.name,
                "backend_options": dict(self.backend.options),
                "id": submission_key or uuid4().hex,
                "detached": True,
            }

        return Job(
            self, spec, callback=callback, request_factory=request,
            completion_cleanup=lambda: close_output_files(spec.output_file, spec.error_file),
        )

    def create_run_parts(self, spec, *, submission_key=None, callback=None):
        return RunParts(self.create_job(spec, submission_key=submission_key, callback=callback))

    def _operation(self, name):
        return BackendOperation.for_backend(type(self.backend), name)

    async def _submit(self, request):
        return await self.transport.call(self._operation("submit"), request)

    async def _status(self, directory, backend_id, files=None):
        return await self.transport.call(self._operation("status"), backend_id)

    async def _cancel(self, directory, backend_id, files=None):
        return await self.transport.call(self._operation("cancel"), backend_id)

    async def _follow(self, directory, backend_id, *, on_output, offsets=None,
                      stdout_path=None, stderr_path=None):
        # Follow in this event loop. Cancelling this coroutine closes only
        # observation resources; backend.cancel is a separate operation.
        from kkrunner.client.observation import OutputSink

        offsets = {"out": 0, "err": 0} if offsets is None else offsets
        return await self.backend.follow(
            backend_id, OutputSink(on_output, "out"), OutputSink(on_output, "err"),
            stdout_offset=offsets.get("out"),
            stderr_offset=offsets.get("err"),
            stdout_path=stdout_path, stderr_path=stderr_path,
        )

    def _require_managed(self):
        if BackendCapability.REATTACH not in self.backend.supports:
            raise UnsupportedCapability("Direct execution has no durable identity or reattachment")

    def _validate_run(self, run):
        from kkrunner.client.endpoint import describe_endpoint
        if run.execution.endpoint != describe_endpoint(self):
            raise ValueError("Run does not belong to this local endpoint")
        if not run.execution.backend_id:
            raise ValueError("Run lacks a backend identity")

    def status(self, *runs):
        self._require_managed()
        statuses = []
        for run in runs:
            self._validate_run(run)
            facts = run_coro_sync(self._status(None, run.execution.backend_id))
            statuses.append(RunStatus(
                RunState(facts.get("state", "unknown")), backend_id=run.execution.backend_id,
                return_code=facts.get("return_code"), message=facts.get("error"),
                failure_phase=facts.get("failure_phase"),
            ))
        return tuple(statuses)

    def open_run_parts(self, attachment, *, callback=None, output_listener=None):
        self._require_managed()
        self._validate_run(attachment)
        files = attachment.files
        spec = ExecutionSpec(
            argv=None, directory=attachment.execution.directory.local,
            input_file=files.input_file, output_file=files.output_file,
            error_file=None if files.stderr is None else files.stderr.local_path,
        )
        identity = attachment.execution.backend_id
        facts = run_coro_sync(self._status(None, identity))
        from kkrunner.client.job import Job
        job = Job(
            self, spec, callback=callback, backend_id=identity,
            remote_directory=spec.directory, status=facts, output_listener=output_listener,
        )
        job.configure_observation(
            stream_offsets={"out": 0, "err": 0}, stream_writer=None,
            stream_paths=(file_path(spec.output_file, spec.directory), file_path(spec.error_file, spec.directory)),
        )
        return RunParts(job)

    def recover_submission(self, submission_key, submission, *, submitted_at=None):
        if BackendCapability.RECOVER not in self.backend.supports:
            return RecoveryResult.unsupported("Direct execution has no submission recovery")
        reply = run_coro_sync(self.transport.call(
            self._operation("recover"), submission_key,
            {"backend": self.backend.name, "backend_options": dict(self.backend.options),
             "directory": submission.execution.directory.target},
            submitted_at=submitted_at,
        ))
        if reply is None:
            return RecoveryResult.indeterminate("No authoritative submission receipt was found")
        if reply.get("unsupported"):
            return RecoveryResult.unsupported(reply.get("error"))
        identity = reply["remote_id"]
        return RecoveryResult.found(
            JobRef(self.backend.name, identity), directory=reply.get("directory"),
            output_file=reply.get("output_file"), error_file=reply.get("error_file"),
        )

    def acknowledge_submission(self, run):
        if BackendCapability.REATTACH not in self.backend.supports:
            return None
        return run_coro_sync(self.transport.call(
            self._operation("acknowledge_submission"), run.execution.submission_key or run.id,
        ))

    def acknowledge(self, run):
        if BackendCapability.REATTACH not in self.backend.supports:
            return None
        self._validate_run(run)
        return run_coro_sync(self.transport.call(
            self._operation("acknowledge"), run.execution.backend_id,
        ))
