"""Direct and supervisor-backed local process backends.

Execution mode is determined by the backend type, never inferred from the
presence of a client persistence/submission identifier.
"""

from __future__ import annotations

from kkrunner.runs.base import ExecutionSpec


class ProcessBackend:
    """Direct local process backend; not durable or reopenable."""

    name = "process"
    supports_result_file_retrieval = False
    supports_tracked_runs = True
    default_persistent = False
    default_detached = False

    def create_job(self, spec: ExecutionSpec, *, submission_key=None, callback=None):
        spec.require_complete()
        from kkrunner.backends.process.direct.job import ProcessJob
        return ProcessJob(spec, callback=callback)


class SupervisedProcessBackend(ProcessBackend):
    """Reconnectable local execution using the durable process supervisor."""

    supports_tracked_runs = True
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

    def open_job(self, run, callback=None, output_listener=None, start=True):
        from kkrunner.backends.process.supervised.local import attach_local
        return attach_local(
            self, run, callback=callback, output_listener=output_listener, start=start,
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
            statuses.append(local_status(run.execution.backend_id, run.files))
        return tuple(statuses)
