"""Client transport/backend identity and pooled-client resource ownership."""

import pytest

from kkrunner.client.pool import ClientPool


def test_client_profile_identity_normalizes_backend_options_without_host_resolution():
    from kkrunner.backends.config import BackendConfig
    from kkrunner.client.transports import LocalTransport, SshTransport

    assert BackendConfig("psij") == BackendConfig("psij", {"executor_name": "slurm"})
    assert SshTransport("cluster") == SshTransport("cluster", python_command=("python3",))
    assert SshTransport("cluster") != SshTransport("cluster.example.org")
    assert LocalTransport() != SshTransport("cluster")



def test_client_pool_close_attempts_all_after_failure():
    """One failing backend must not prevent cleanup of the remaining backends."""
    pool = ClientPool()
    closed = []
    backends = {}

    class Executor:
        def __init__(self, key, error=None):
            self.key = key
            self.error = error

        def close(self):
            closed.append(self.key)
            if self.error is not None:
                raise self.error

    first_error = OSError("first close failed")
    for key, error in (("first", first_error), ("second", RuntimeError("second close failed")), ("third", None)):
        backend = Executor(key, error)
        backends[key] = backend
        assert pool.get(key, lambda backend=backend: backend) is backend
    with pytest.raises(OSError, match="first close failed") as caught:
        pool.close()
    assert caught.value is first_error
    assert closed == ["first", "second", "third"]
    replacement = object()
    assert pool.get("first", lambda: replacement) is replacement
    assert replacement is not backends["first"]
    pool.close()
    assert closed == ["first", "second", "third"]


def test_client_pool_close_accepts_backends_without_close():
    pool = ClientPool()
    executor = object()
    pool.get("no-close", lambda: executor)
    pool.close()
    assert pool.get("no-close", object) is not executor


def test_client_pool_closes_shared_backend_only_once():
    """A backend may be cached under several keys but is one owned object."""
    pool = ClientPool()
    calls = []

    class Shared:
        def close(self):
            calls.append("closed")

    shared = Shared()
    assert pool.get("a", lambda: shared) is shared
    assert pool.get("b", lambda: shared) is shared
    pool.close()
    assert calls == ["closed"]
