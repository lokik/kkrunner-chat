"""Configured client access to a selected backend through a transport."""

from __future__ import annotations

from subprocess import SubprocessError

from kkrunner.backends import configured_backend
from kkrunner.backends.config import BackendConfig
from kkrunner.backends.contracts import BackendCapability, JobRef, decode_job_status
from kkrunner.client.backend_execution import bind_execution
from kkrunner.client.contracts import Operation
from kkrunner.client.file_context import FileContext
from kkrunner.client.file_session import FileSession
from kkrunner.client.job import Job
from kkrunner.client.run_parts import RunParts
from kkrunner.client.staging import StagingSpec
from kkrunner.runs.base import RunState, RunStatus
from kkrunner.support.async_utils import run_coro_sync as _run_sync


def normalize_config(target, backend):
    """Normalize a stored client configuration before reconstruction or pooling."""
    from kkrunner.targets import require_persistable_target, target_from_dict
    if isinstance(target, dict):
        target = target_from_dict(target)
    if isinstance(backend, dict):
        backend = BackendConfig(**backend)
    require_persistable_target(target)
    if not target.available or not backend.available:
        raise ValueError("Unsupported backend or transport; only local history is available")
    return target, backend


class ExecutionClient:
    """Compose backend access, transport and per-run client resources.

    Instances can serve many runs. Execution lifetime belongs to backend
    bindings, file mapping/staging to file services, persistence to RunManager.
    """

    def __init__(self, backend=None, *, target=None, backend_options=None):
        from kkrunner.targets import LocalTarget, SshTarget
        self.backend = configured_backend(backend, options=backend_options)
        if target is None:
            target = LocalTarget()
        elif isinstance(target, str):
            target = SshTarget(target)
        self.target = target
        if not self.target.available:
            raise ValueError(f"Unsupported transport {self.target.kind!r}; only local history is available")

    def create_run(
        self, spec, *, tracked=False, detached=None, process_result=None,
        callback=None, output_listener=None, owned_resources=None, preprocess=None,
        metadata=None, processing_required=None, run_id=None,
    ):
        """Create an unstarted Run using this configured client."""
        from kkrunner.runs.run import Run

        return Run._create(
            self, spec,
            tracked=tracked, detached=detached, process_result=process_result,
            callback=callback, output_listener=output_listener, owned_resources=owned_resources,
            preprocess=preprocess, metadata=metadata, processing_required=processing_required,
            run_id=run_id,
        )

    def run(
        self, argv, *, directory=".", input_file=None, output_file=None,
        stdin=None, error_file=None, input_files=(), output_files=(), cache_files=(),
        mpi_argv=None, mpi=None, staging=None, mpi_uses_stdin=True,
        tracked=False, detached=None, process_result=None, output_listener=None,
    ):
        """Run a command and wait, displaying available unredirected output."""
        return self.submit(
            argv, directory=directory, input_file=input_file, output_file=output_file,
            stdin=stdin, error_file=error_file, input_files=input_files,
            output_files=output_files, cache_files=cache_files, mpi_argv=mpi_argv,
            mpi=mpi, staging=staging, mpi_uses_stdin=mpi_uses_stdin,
            tracked=tracked, detached=detached, process_result=process_result,
            output_listener=output_listener,
        ).wait()

    def submit(
        self, argv, *, directory=".", input_file=None, output_file=None,
        stdin=None, error_file=None, input_files=(), output_files=(), cache_files=(),
        mpi_argv=None, mpi=None, staging=None, mpi_uses_stdin=True,
        tracked=False, detached=None, process_result=None, output_listener=None,
    ):
        """Submit a command and return its Run, displaying available unredirected output."""
        import sys

        from kkrunner.runs.base import ExecutionSpec

        spec = ExecutionSpec(
            argv, directory=directory, input_file=input_file, output_file=output_file,
            stdin=stdin, error_file=error_file, input_files=input_files,
            output_files=output_files, cache_files=cache_files, mpi_argv=mpi_argv,
            mpi=mpi, staging=StagingSpec() if staging is None else staging, mpi_uses_stdin=mpi_uses_stdin,
        )
        if output_listener is None:
            def output_listener(text, kind):
                if kind == "out" and spec.output_file is None:
                    sys.stdout.write(text)
                    sys.stdout.flush()
                elif kind == "err" and spec.error_file is None:
                    sys.stderr.write(text)
                    sys.stderr.flush()

        runner = self.create_run(
            spec, tracked=tracked, detached=detached,
            process_result=process_result, output_listener=output_listener,
        )
        runner.submit()
        return runner

    @classmethod
    def from_config(cls, target, backend):
        """Reconstruct a client from supported stored connection/backend data."""
        target, backend = normalize_config(target, backend)
        return cls(backend.create(), target=target)

    @property
    def backend_config(self):
        return BackendConfig(self.backend.name, self.backend.options)

    @property
    def default_detached(self):
        return self.backend.default_detached

    def submission_request(self, prepared_files, *, submission_key=None):
        """Add backend configuration to a request prepared by the file service."""
        return {
            **prepared_files, "submission_key": submission_key,
            "backend": self.backend.name, "backend_options": dict(self.backend.options),
            "detached": self.backend.default_detached,
        }

    async def _invoke(self, name, *args, **kwargs):
        operation = Operation.for_backend(type(self.backend), name)
        return await self.target.call(operation, *args, **kwargs)

    def _validate_run(self, run):
        if run.execution.target != self.target or run.execution.backend != self.backend_config:
            raise ValueError("Run belongs to a different transport or backend")

    def submission_directory(self, spec):
        """Preview the file service's target mapping without contacting the target."""
        return FileContext(self.target, spec.staging).preview_path(spec.directory)

    def acknowledge_submission(self, run):
        """Release the submission receipt after the manager persisted acceptance."""
        if BackendCapability.ACKNOWLEDGE not in self.backend.supports:
            return None
        self._validate_run(run)
        key = run.execution.submission_key or run.id
        return _run_sync(self._invoke("acknowledge_submission", key))

    def acknowledge(self, run):
        """Release a delivered receipt after the manager persisted completion."""
        if BackendCapability.ACKNOWLEDGE not in self.backend.supports:
            return None
        self._validate_run(run)
        return _run_sync(self._invoke("acknowledge", run.execution.backend_id))

    def status(self, *runs):
        """Query each native job; a failed connection leaves only that observation unknown."""
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
                try:
                    facts = await self._invoke("status", token)
                except (OSError, SubprocessError) as exc:
                    result.append(RunStatus(
                        RunState.UNKNOWN, backend_id=token,
                        message=f"Cannot query execution status: {exc}",
                    ))
                else:
                    result.append(RunStatus.from_job_status(decode_job_status(facts), backend_id=token))
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
        context = FileContext(self.target, StagingSpec())
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
        self._validate_run(submission)
        return _run_sync(self._recover_submission(submission_key, submission, submitted_at=submitted_at))

    def file_session(self, spec):
        return FileSession(FileContext(self.target, spec.staging), spec,
                           allow_live_streams=self.backend.execution_mode == "direct")

    def create_run_parts(self, spec, *, submission_key=None, callback=None):
        spec.require_complete()
        files = self.file_session(spec)
        execution = bind_execution(self.backend, self.target, local_io=files.local_call_io)
        job = Job(execution, spec, callback=callback)
        return RunParts(
            job, files,
            submission_builder=lambda prepared: self.submission_request(prepared, submission_key=submission_key),
        )

    def snapshot_file(self, run, file, *, destination=None):
        from kkrunner.client.file_access import FileAccess
        return FileAccess(self.target).snapshot_file(run, file, destination=destination)

    def resolve_result_files(self, run, files):
        """Resolve concrete transfer pairs without writing any files."""
        from kkrunner.client.file_access import FileAccess
        return FileAccess(self.target).resolve_result_files(run, files)

    def download_result_files(self, entries, *, verify=None):
        """Download exact, already coordinated source/destination pairs."""
        from kkrunner.client.file_access import FileAccess
        return FileAccess(self.target).download_result_files(entries, verify=verify)

    def fetch_result_files(self, run, files, *, destinations=None, verify=None):
        """Fetch declared files, optionally mapping each concrete local path.

        destinations may be a path-to-path mapping.
        verify(destination, temporary) checks each staged file before publishing
        it without replacement. Returns the concrete destinations retrieved.
        """
        from kkrunner.client.file_access import FileAccess
        return FileAccess(self.target).fetch_result_files(run, files, destinations=destinations, verify=verify)

    def inspect_remote_files(self, run, paths):
        """Inspect declared target files for an optional cleanup preview."""
        from kkrunner.client.file_access import FileAccess
        return FileAccess(self.target).inspect_remote_files(run, paths)

    def delete_remote_files(self, run, expected):
        """Best-effort per-file remote deletion using approved file identities."""
        from kkrunner.client.file_access import FileAccess
        return FileAccess(self.target).delete_remote_files(run, expected)

    async def _open_run_parts(self, attachment, *, callback=None, output_listener=None):
        self._validate_run(attachment)
        execution = attachment.execution
        if execution.backend_id is None:
            raise ValueError("Run lacks a backend identity")
        status = await self._invoke("status", execution.backend_id)
        context = FileContext(self.target, StagingSpec.from_mapping(attachment.files.mapping))
        files = await FileSession.from_attachment(context, attachment)
        try:
            binding = bind_execution(
                self.backend, self.target, ref=JobRef(self.backend.name, execution.backend_id), status=status,
            )
            job = Job(binding, files.spec, callback=callback, output_listener=output_listener)
            return RunParts(job, files)
        except BaseException:
            files.close()
            raise

    def collect_ended_files(self, record, status):
        """Collect named outputs of an ended execution without opening an observer.

        The original observer may have died, but the persisted manifest still
        defines which target files are eligible for retrieval.
        """
        from kkrunner.client.contracts import JobAttachment
        self._validate_run(record)

        async def collect():
            context = FileContext(self.target, StagingSpec.from_mapping(record.files.mapping))
            session = await FileSession.from_attachment(
                context, JobAttachment(record.execution, record.files),
            )
            try:
                return await session.finalize_async(status)
            finally:
                session.close()

        return _run_sync(collect())

    def open_run_parts(self, attachment, *, callback=None, output_listener=None):
        if BackendCapability.REATTACH not in self.backend.supports:
            from kkrunner.backends.contracts import UnsupportedCapability
            raise UnsupportedCapability("The selected backend does not support reattachment")
        return _run_sync(self._open_run_parts(attachment, callback=callback, output_listener=output_listener))
