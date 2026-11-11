"""Supervised backend interface; native mechanism is kept separate."""

from __future__ import annotations
from kkrunner.backends.contracts import RecoverableBackend
from kkrunner.runs.base import ExecutionSpec

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
        from kkrunner.client.local_managed import local_managed_job
        return local_managed_job(
            spec, backend="process", submission_key=submission_key or uuid4().hex,
            callback=callback,
        )

    def recover_submission(self, run_id, submission, *, submitted_at=None):
        recover_remote = recover
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
        from kkrunner.client.local_supervised import attach_local
        return attach_local(
            self, context, callback=callback, output_listener=output_listener, start=start,
        )

    def acknowledge_submission(self, run):
        from kkrunner.backends.supervised import acknowledge_submission
        acknowledge_submission(run.execution.submission_key or run.id)

    def acknowledge(self, run):
        acknowledge_remote = acknowledge
        acknowledge_remote(run.execution.backend_id)

    def status(self, *runs):
        from kkrunner.client.local_supervised import local_status
        statuses = []
        for run in runs:
            if run.execution.endpoint.executor != "local-supervised":
                raise ValueError("Run does not belong to the supervised local backend")
            statuses.append(local_status(run.execution.backend_id))
        return tuple(statuses)


def stream(job_ref, **options):
    import sys
    return follow(job_ref, sys.stdout.buffer, sys.stderr.buffer, **options)

def submit(request):
    from kkrunner.native.supervised.operations import _submit_impl
    return _submit_impl(request)

def status(job_ref):
    from kkrunner.native.supervised.operations import _status_impl
    return _status_impl(job_ref)

def cancel(job_ref):
    from kkrunner.native.supervised.operations import _cancel_impl
    return _cancel_impl(job_ref)

def recover(submission_key, request, *, submitted_at=None):
    from kkrunner.native.supervised.operations import _recover_impl
    return _recover_impl(submission_key, request, submitted_at=submitted_at)

def acknowledge(job_ref):
    from kkrunner.native.supervised.operations import _acknowledge_impl
    return _acknowledge_impl(job_ref)

def acknowledge_submission(submission_key):
    from kkrunner.native.supervised.operations import _acknowledge_submission_impl
    return _acknowledge_submission_impl(submission_key)

def follow(job_ref, stdout, stderr, **options):
    from kkrunner.native.supervised.operations import _follow_impl
    return _follow_impl(job_ref, stdout, stderr, **options)

class SupervisedBackend:
    """Installed target-side supervisor operations; no retained runtime state."""

    from kkrunner.backends.class_helper import class_helper as SshHelper

    submit = staticmethod(submit)
    status = staticmethod(status)
    cancel = staticmethod(cancel)
    recover = staticmethod(recover)
    acknowledge = staticmethod(acknowledge)
    acknowledge_submission = staticmethod(acknowledge_submission)
    follow = staticmethod(follow)
    stream = staticmethod(stream)
