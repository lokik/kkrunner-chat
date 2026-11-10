"""Stateless agent adapter: transport protocol to backend operations.

All execution, status, cancellation, recovery, follow and acknowledgements
are delegated to the single backend registry.  No backend branching lives
in this module.
"""

from kkrunner.backends.factory import backend_for_kind, backend_for_ref, backend_for_request


def submit_remote(request):
    return backend_for_request(request).submit_remote(request)


def status_remote(remote_id):
    return backend_for_ref(remote_id).status_remote(remote_id)


def cancel_remote(remote_id):
    return backend_for_ref(remote_id).cancel_remote(remote_id)


def follow_remote(remote_id, stdout, stderr, **options):
    return backend_for_ref(remote_id).follow_remote(remote_id, stdout, stderr, **options)


def recover_remote(run_id, request, *, submitted_at=None):
    return backend_for_request(request).recover_remote(run_id, request, submitted_at=submitted_at)


def acknowledge_remote(remote_id):
    return backend_for_ref(remote_id).acknowledge_remote(remote_id)


def acknowledge_submission(backend, run_id):
    return backend_for_kind(backend).acknowledge_submission(run_id)
