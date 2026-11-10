"""Local ProcessBackend facade over direct and supervised process Jobs.

The direct ProcessJob is transport-neutral: direct local and direct SSH
share its lifecycle, with distinct launch/output adapters. Managed process
jobs use a separate durable supervisor and reconnectable lifecycle.
"""

from __future__ import annotations

from kkrunner.runs.base import ExecutionSpec


class ProcessBackend:
    """Select direct-local or reconnectable supervisor-backed process Jobs.

    The presence of a client submission key requests a durable supervised
    execution. Transport-specific direct Jobs (such as a future SSH-direct
    Job) should implement the shared Job/RunParts contract independently,
    rather than forwarding through the supervisor operations.
    """

    name = "process"
    supports_result_file_retrieval = False

    def create_run_parts(self, spec: ExecutionSpec, *, submission_key=None, callback=None):
        from kkrunner.client.run_parts import RunParts
        return RunParts(self.create_job(spec, submission_key=submission_key, callback=callback))

    def open_run_parts(self, attachment, *, callback=None, output_listener=None):
        from kkrunner.client.run_parts import RunParts
        return RunParts(self.open_job(
            attachment, callback=callback, output_listener=output_listener, start=False,
        ))

    def create_job(self, spec: ExecutionSpec, *, submission_key=None, callback=None):
        if submission_key is None:
            from kkrunner.backends.process.direct.job import ProcessJob
            from kkrunner.backends.process.primitives import materialize_local_spec
            return ProcessJob(materialize_local_spec(spec), callback=callback)
        from kkrunner.backends.process.supervised.job import SupervisedProcessJob
        return SupervisedProcessJob(
            self, spec, run_id=submission_key, callback=callback,
        )

    def recover_submission(self, run_id, submission, *, submitted_at=None):
        from kkrunner.backends.process.supervised.operations import recover_remote

        reply = recover_remote(
            run_id,
            {
                "executor": "process",
                "directory": submission.execution.directory.local,
            },
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
            self,
            run,
            callback=callback,
            output_listener=output_listener,
            start=start,
        )

    def acknowledge_submission(self, run):
        from kkrunner.backends.process.supervised.operations import acknowledge_submission

        acknowledge_submission(run.id)

    def acknowledge(self, run):
        from kkrunner.backends.process.supervised.operations import acknowledge_remote

        acknowledge_remote(run.execution.backend_id)

    def status(self, *runs):
        from kkrunner.backends.process.supervised.local import local_status

        statuses = []
        for run in runs:
            if run.execution.endpoint.executor != "local":
                raise ValueError("Run does not belong to the local backend")
            statuses.append(local_status(run.execution.backend_id, run.files))
        return tuple(statuses)
