"""Generic endpoint identity and backend-pool resource ownership."""

import pytest

from kkrunner.client.pool import EndpointPool


def test_endpoint_normalizes_known_defaults_without_resolving_hostnames():
    from kkrunner.client.endpoint import ExecutionEndpointConfig

    assert ExecutionEndpointConfig(backend="psij") == ExecutionEndpointConfig(
        backend="psij", backend_options={"executor_name": "slurm"}
    )
    assert ExecutionEndpointConfig(location="ssh", host="cluster") == ExecutionEndpointConfig(
        location="ssh", host="cluster", python_command=("python3",)
    )
    assert ExecutionEndpointConfig(location="ssh", host="cluster") != ExecutionEndpointConfig(
        location="ssh", host="cluster.example.org"
    )


def test_executor_pool_close_attempts_all_after_failure():
    """One failing backend must not prevent cleanup of the remaining backends."""
    pool = EndpointPool()
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


def test_executor_pool_close_accepts_backends_without_close():
    pool = EndpointPool()
    executor = object()
    pool.get("no-close", lambda: executor)
    pool.close()
    assert pool.get("no-close", object) is not executor


def test_executor_pool_closes_shared_backend_only_once():
    """A backend may be cached under several keys but is one owned object."""
    pool = EndpointPool()
    calls = []

    class Shared:
        def close(self):
            calls.append("closed")

    shared = Shared()
    assert pool.get("a", lambda: shared) is shared
    assert pool.get("b", lambda: shared) is shared
    pool.close()
    assert calls == ["closed"]
