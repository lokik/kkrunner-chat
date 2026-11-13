"""Configured client access to a selected backend through a transport."""

from __future__ import annotations

from kkrunner.backends import configured_backend
from kkrunner.backends.config import BackendConfig
from kkrunner.backends.contracts import BackendCapability, JobRef, decode_job_status
from kkrunner.client.backend_execution import bind_execution
from kkrunner.client.contracts import Operation
from kkrunner.client.file_context import FileContext, query_remote_home
from kkrunner.client.file_session import FileSession
from kkrunner.client.job import Job
from kkrunner.client.run_parts import RunParts
from kkrunner.client.staging import StagingSpec
from kkrunner.runs.base import RunStatus
from kkrunner.support.async_utils import run_coro_sync as _run_sync


class EndpointClient:
    """Compose backend access, transport and per-run client resources.

    Instances can serve many runs. Execution lifetime belongs to backend
    bindings, file mapping/staging to file services, persistence to RunManager.
    """

    supports_result_file_retrieval = True

    def __init__(self, backend=None, *, transport=None, backend_options=None):
        from kkrunner.client.transports import LocalTransport
        self.backend = configured_backend(backend, options=backend_options)
        self.transport = LocalTransport() if transport is None else transport
        self.computer = getattr(self.transport, "host", None)
        self._remote_home_cache = None

    @classmethod
    def from_config(cls, transport, backend):
        """Reconstruct a client from supported stored connection/backend data."""
        from kkrunner.client.transports import require_persistable_transport, transport_from_dict
        if isinstance(transport, dict):
            transport = transport_from_dict(transport)
        if isinstance(backend, dict):
            backend = BackendConfig(**backend)
        require_persistable_transport(transport)
        return cls(backend.create(), transport=transport)

    @property
    def backend_config(self):
        return BackendConfig(self.backend.name, self.backend.options)

    @property
    def default_persistent(self):
        return self.backend.execution_mode == "managed"

    @property
    def default_detached(self):
        return self.backend.default_detached

    async def _remote_home(self):
        if self._remote_home_cache is None:
            self._remote_home_cache = await query_remote_home(self.transport)
        return self._remote_home_cache

    def submission_request(self, prepared_files, *, submission_key=None):
        """Add backend configuration to a request prepared by the file service."""
        return {
            **prepared_files, "submission_key": submission_key,
            "backend": self.backend.name, "backend_options": dict(self.backend.options),
            "detached": self.backend.default_detached,
        }

    async def _invoke(self, name, *args, **kwargs):
        operation = Operation.for_backend(type(self.backend), name)
        return await self.transport.call(operation, *args, **kwargs)

    def _validate_run(self, run):
        if run.execution.transport != self.transport or run.execution.backend != self.backend_config:
            raise ValueError("Run belongs to a different transport or backend")

    def submission_directory(self, spec):
        """Preview the file service's target mapping without contacting the target."""
        return FileContext(self, spec.staging).preview_path(spec.directory)

    def acknowledge_submission(self, run):
        """Release the submission receipt after the manager persisted acceptance."""
        if BackendCapability.REATTACH not in self.backend.supports:
            return None
        key = run.execution.submission_key or run.id
        return _run_sync(self._invoke("acknowledge_submission", key))

    def acknowledge(self, run):
        """Release a delivered receipt after the manager persisted completion."""
        if BackendCapability.REATTACH not in self.backend.supports:
            return None
        self._validate_run(run)
        return _run_sync(self._invoke("acknowledge", run.execution.backend_id))

    def status(self, *runs):
        if BackendCapability.STATUS not in self.backend.supports:
            from kkrunner.backends.contracts import UnsupportedCapability
            raise UnsupportedCapability("This backend has no independent status query")
        for run in runs:
            self._validate_run(run)
            if not run.execution.backend_id:
                raise ValueError("Run lacks a backend identity")

        async def query():
            result = []
            for run in runs:
                token = run.execution.backend_id
                status = decode_job_status(await self._invoke("status", token))
                result.append(RunStatus.from_job_status(status, backend_id=token))
            return tuple(result)

        return _run_sync(query()) if runs else ()

    def cancel(self, attachment):
        """Cancel an accepted job using its persisted native reference.

        Return backend facts, not a synthetic terminal cancellation status.
        """
        from kkrunner.backends.contracts import UnsupportedCapability
        if BackendCapability.CANCEL not in self.backend.supports:
            raise UnsupportedCapability("Backend does not support cancellation after restart")
        self._validate_run(attachment)
        native_id = attachment.execution.backend_id
        if native_id is None:
            raise ValueError("Run has no native execution identity")

        async def request():
            reply = await self._invoke("cancel", native_id)
            return RunStatus.from_job_status(
                decode_job_status(reply), backend_id=native_id,
            )

        return _run_sync(request())

    async def _recover_submission(self, submission_key, submission, *, submitted_at=None):
        context = FileContext(self, StagingSpec())
        directory = await context.resolve_target_path(submission.execution.directory.target)
        state = await self._invoke(
            "recover", submission_key,
            {"backend": self.backend.name, "backend_options": dict(self.backend.options), "directory": directory},
            submitted_at=submitted_at,
        )
        from kkrunner.backends.contracts import RecoveryResult
        if state is None:
            return RecoveryResult.indeterminate("Remote submission receipt was not found")
        if state.get("unsupported"):
            return RecoveryResult.unsupported(state.get("error"))
        remote_id = state.get("remote_id")
        if not remote_id:
            raise RuntimeError("Recovered submission has no remote_id")
        return RecoveryResult.found(
            JobRef(self.backend.name, str(remote_id)), directory=state.get("directory") or directory,
            output_file=state.get("output_file"), error_file=state.get("error_file"),
        )

    def recover_submission(self, submission_key, submission, *, submitted_at=None):
        if BackendCapability.RECOVER not in self.backend.supports:
            from kkrunner.backends.contracts import RecoveryResult
            return RecoveryResult.unsupported("The selected backend has no submission recovery")
        return _run_sync(self._recover_submission(submission_key, submission, submitted_at=submitted_at))

    def file_session(self, spec):
        return FileSession(FileContext(self, spec.staging), spec)

    def create_run_parts(self, spec, *, submission_key=None, callback=None):
        from uuid import uuid4
        spec.require_complete()
        key = submission_key or uuid4().hex
        files = self.file_session(spec)
        execution = bind_execution(self.backend, self.transport, local_io=files.local_call_io)
        job = Job(execution, spec, callback=callback)
        return RunParts(
            job, files,
            submission_builder=lambda prepared: self.submission_request(prepared, submission_key=key),
        )

    def snapshot_file(self, run, file, *, destination=None):
        from kkrunner.client.file_access import FileAccess
        self._validate_run(run)
        return FileAccess(self).snapshot_file(run, file, destination=destination)

    def fetch_result_files(self, run, files, *, destinations=None):
        from kkrunner.client.file_access import FileAccess
        self._validate_run(run)
        return FileAccess(self).fetch_result_files(run, files, destinations=destinations)

    async def _open_run_parts(self, attachment, *, callback=None, output_listener=None):
        self._validate_run(attachment)
        execution = attachment.execution
        if execution.backend_id is None:
            raise ValueError("Run lacks a backend identity")
        status = await self._invoke("status", execution.backend_id)
        context = FileContext(self, StagingSpec.from_mapping(attachment.files.mapping))
        files = await FileSession.from_attachment(context, attachment)
        try:
            binding = bind_execution(
                self.backend, self.transport, ref=JobRef(self.backend.name, execution.backend_id), status=status,
            )
            job = Job(binding, files.spec, callback=callback, output_listener=output_listener)
            return RunParts(job, files)
        except BaseException:
            files.close()
            raise

    def collect_ended_direct_files(self, record, status):
        """Recover outputs without reattaching or resubmitting direct execution.

        The original observer may have died, but the persisted manifest still
        defines which target files are eligible for retrieval.
        """
        from kkrunner.client.contracts import JobAttachment
        if self.backend.name != "direct":
            raise ValueError("Only direct execution uses unobserved file collection")
        self._validate_run(record)

        async def collect():
            context = FileContext(self, StagingSpec.from_mapping(record.files.mapping))
            session = await FileSession.from_attachment(
                context, JobAttachment(record.execution, record.files),
            )
            try:
                session.configure_stream_retrieval(True)
                return await session.finalize_async(status)
            finally:
                session.close()

        return _run_sync(collect())

    def open_run_parts(self, attachment, *, callback=None, output_listener=None):
        if BackendCapability.REATTACH not in self.backend.supports:
            from kkrunner.backends.contracts import UnsupportedCapability
            raise UnsupportedCapability("The selected backend does not support reattachment")
        return _run_sync(self._open_run_parts(attachment, callback=callback, output_listener=output_listener))
