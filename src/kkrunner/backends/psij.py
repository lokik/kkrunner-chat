"""PSI/J backend: compact static operations and public native executor.

Target execution requires installed kkrunner and psij-python. The SSH helper
sends only imports and static delegation, never the scheduler implementation.
"""

from kkrunner.native.psij.executor import PsijBackend
from kkrunner.native.psij.job import PsijJob, psij_state
from kkrunner.native.psij.slurm import status_native, recover_native, submission_marker


class PsijOperations:
    """Installed scheduler target operations; no executor instance crosses SSH."""

    from kkrunner.backends.class_helper import class_helper as SshHelper

    @staticmethod
    def submit(request):
        from kkrunner.native.psij.operations import _submit_impl
        return _submit_impl(request)

    @staticmethod
    def status(job_ref):
        from kkrunner.native.psij.operations import _status_impl
        return _status_impl(job_ref)

    @staticmethod
    def cancel(job_ref):
        from kkrunner.native.psij.operations import _cancel_impl
        return _cancel_impl(job_ref)

    @staticmethod
    def recover(submission_key, request, *, submitted_at=None):
        from kkrunner.native.psij.operations import _recover_impl
        return _recover_impl(submission_key, request, submitted_at=submitted_at)

    @staticmethod
    def acknowledge(job_ref):
        from kkrunner.native.psij.operations import _acknowledge_impl
        return _acknowledge_impl(job_ref)

    @staticmethod
    def acknowledge_submission(submission_key):
        from kkrunner.native.psij.operations import _acknowledge_submission_impl
        return _acknowledge_submission_impl(submission_key)

    @staticmethod
    def follow(job_ref, stdout, stderr, **options):
        from kkrunner.native.psij.operations import _follow_impl
        return _follow_impl(job_ref, stdout, stderr, **options)

    @staticmethod
    def stream(job_ref, **options):
        import sys
        return PsijOperations.follow(job_ref, sys.stdout.buffer, sys.stderr.buffer, **options)

    @staticmethod
    def follow_framed(job_ref, **options):
        from kkrunner.remote.stream import follow_framed
        return follow_framed(PsijOperations.follow, job_ref, **options)
