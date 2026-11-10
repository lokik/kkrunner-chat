"""Independent public operations for the process/supervised execution backend.

Every entry point can be imported into a fresh target Python interpreter;
no backend instance or module-level mutable state is needed between calls.
"""


def submit(request):
    from kkrunner.backends.process.supervised.operations import submit_remote
    return submit_remote(request)


def status(job_ref):
    from kkrunner.backends.process.supervised.operations import status_remote
    return status_remote(job_ref)


def cancel(job_ref):
    from kkrunner.backends.process.supervised.operations import cancel_remote
    return cancel_remote(job_ref)


def recover(submission_key, request, *, submitted_at=None):
    from kkrunner.backends.process.supervised.operations import recover_remote
    return recover_remote(submission_key, request, submitted_at=submitted_at)


def acknowledge(job_ref):
    from kkrunner.backends.process.supervised.operations import acknowledge_remote
    return acknowledge_remote(job_ref)


def acknowledge_submission(submission_key):
    from kkrunner.backends.process.supervised.operations import acknowledge_submission
    return acknowledge_submission(submission_key)


def follow(job_ref, stdout, stderr, **options):
    from kkrunner.backends.process.supervised.operations import follow_remote
    return follow_remote(job_ref, stdout, stderr, **options)


def stream(job_ref, **options):
    import sys
    from kkrunner.backends.process.supervised.functions import follow
    return follow(job_ref, sys.stdout.buffer, sys.stderr.buffer, **options)
