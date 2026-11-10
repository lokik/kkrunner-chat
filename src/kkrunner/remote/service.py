"""Stateless agent adapter: transport protocol to backend operations.

All execution, status, cancellation, recovery, follow and acknowledgements
are delegated through the registry below. Backend-specific execution logic
remains in the respective backend modules.
"""

from importlib import import_module

from kkrunner.remote.handles import decode_handle


def _backend_for_kind(kind):
    from kkrunner.backends.registry import operation_module
    return import_module(operation_module(kind))


def _backend_for_request(request):
    return _backend_for_kind(request.get("executor", "process"))


def _backend_for_ref(ref):
    return _backend_for_kind(decode_handle(ref)["kind"])


def submit_remote(request):
    return _backend_for_request(request).submit_remote(request)


def status_remote(remote_id):
    return _backend_for_ref(remote_id).status_remote(remote_id)


def cancel_remote(remote_id):
    return _backend_for_ref(remote_id).cancel_remote(remote_id)


def follow_remote(remote_id, stdout, stderr, **options):
    return _backend_for_ref(remote_id).follow_remote(remote_id, stdout, stderr, **options)


def recover_remote(run_id, request, *, submitted_at=None):
    return _backend_for_request(request).recover_remote(run_id, request, submitted_at=submitted_at)


def acknowledge_remote(remote_id):
    return _backend_for_ref(remote_id).acknowledge_remote(remote_id)


def acknowledge_submission(backend, run_id):
    return _backend_for_kind(backend).acknowledge_submission(run_id)
