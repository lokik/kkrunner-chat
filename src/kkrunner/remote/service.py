"""Stateless agent adapter: transport protocol to backend operations.

All execution, status, cancellation, recovery, follow and acknowledgements
are delegated through the registry below. Backend-specific execution logic
remains in the respective backend modules.
"""

from kkrunner.remote.handles import decode_handle


def _backend_for_kind(kind):
    from kkrunner.backends import backend_module, installed_backend_names
    if kind not in installed_backend_names():
        raise ValueError(f"Backend {kind!r} is not available through the installed agent")
    return backend_module(kind)


def _backend_for_request(request):
    return _backend_for_kind(request.get("executor", "process"))


def _backend_for_ref(ref):
    return _backend_for_kind(decode_handle(ref)["kind"])


def submit_remote(request):
    return _backend_for_request(request).submit(request)


def status_remote(remote_id):
    return _backend_for_ref(remote_id).status(remote_id)


def cancel_remote(remote_id):
    return _backend_for_ref(remote_id).cancel(remote_id)


def follow_remote(remote_id, stdout, stderr, **options):
    return _backend_for_ref(remote_id).follow(remote_id, stdout, stderr, **options)


def recover_remote(run_id, request, *, submitted_at=None):
    return _backend_for_request(request).recover(run_id, request, submitted_at=submitted_at)


def acknowledge_remote(remote_id):
    return _backend_for_ref(remote_id).acknowledge(remote_id)


def acknowledge_submission(backend, run_id):
    return _backend_for_kind(backend).acknowledge_submission(run_id)
