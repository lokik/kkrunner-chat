"""Direct and supervisor-backed local process backends.

Execution mode is determined by the backend type, never inferred from the
presence of a client persistence/submission identifier.
"""

from __future__ import annotations

from kkrunner.runs.base import ExecutionSpec
from kkrunner.backends.contracts import Backend, RecoverableBackend


class DirectProcessBackend(Backend):
    """Create direct process Jobs, locally or using an internal transport adapter."""

    name = "direct-process"
    supports_result_file_retrieval = False
    supports_tracked_runs = True
    requires_backend_identity = False
    default_persistent = False
    default_detached = False

    def __init__(self, *, adapter_factory=None):
        # The adapter is a private implementation detail of DirectProcessJob.
        # Local and raw SSH direct both go through this backend factory.
        self._adapter_factory = adapter_factory

    def create_job(self, spec: ExecutionSpec, *, submission_key=None, callback=None):
        spec.require_complete()
        from kkrunner.backends.process.direct.job import DirectProcessJob
        adapter = self._adapter_factory() if self._adapter_factory is not None else None
        return DirectProcessJob(spec, callback=callback, adapter=adapter)


class SupervisedProcessBackend(RecoverableBackend):
    """Create recoverable supervisor-owned Jobs; not a direct-process subclass."""

    name = "process"
    supports_result_file_retrieval = False
    supports_tracked_runs = True
    requires_backend_identity = True
    default_persistent = True
    default_detached = True

    def create_job(self, spec: ExecutionSpec, *, submission_key=None, callback=None):
        spec.require_complete()
        from uuid import uuid4
        from kkrunner.backends.process.supervised.job import SupervisedProcessJob
        return SupervisedProcessJob(
            self, spec, run_id=submission_key or uuid4().hex, callback=callback,
        )

    def recover_submission(self, run_id, submission, *, submitted_at=None):
        from kkrunner.backends.process.supervised.operations import recover_remote
        reply = recover_remote(
            run_id,
            {"executor": "process", "directory": submission.execution.directory.local},
        )
        from kkrunner.backends.contracts import JobRef, RecoveryResult
        if reply is None:
            return RecoveryResult.indeterminate("No submission receipt was found")
        return RecoveryResult.found(
            JobRef("process", reply["remote_id"]),
            directory=reply.get("directory"),
            output_file=reply.get("output_file"),
            error_file=reply.get("error_file"),
        )

    def attach_job(self, context, *, callback=None, output_listener=None, start=False):
        from kkrunner.backends.process.supervised.local import attach_local
        return attach_local(
            self, context, callback=callback, output_listener=output_listener, start=start,
        )

    def acknowledge_submission(self, run):
        from kkrunner.backends.process.supervised.operations import acknowledge_submission
        acknowledge_submission(run.execution.submission_key or run.id)

    def acknowledge(self, run):
        from kkrunner.backends.process.supervised.operations import acknowledge_remote
        acknowledge_remote(run.execution.backend_id)

    def status(self, *runs):
        from kkrunner.backends.process.supervised.local import local_status
        statuses = []
        for run in runs:
            if run.execution.endpoint.executor != "local-supervised":
                raise ValueError("Run does not belong to the supervised local backend")
            statuses.append(local_status(run.execution.backend_id))
        return tuple(statuses)
