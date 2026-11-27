"""Client transport/backend identity and client-cache behavior."""

from kkrunner.client.pool import ClientPool


def test_client_profile_identity_normalizes_backend_options_without_host_resolution():
    from kkrunner.backends.config import BackendConfig
    from kkrunner.targets import LocalTarget, SshTarget

    assert BackendConfig("psij") == BackendConfig("psij", {"executor_name": "slurm"})
    assert SshTarget("cluster") == SshTarget("cluster", python_command=("python3",))
    assert SshTarget("cluster") != SshTarget("cluster.example.org")
    assert LocalTarget() != SshTarget("cluster")

def test_client_pool_discard_and_clear_only_forget_cached_clients():
    from kkrunner.client import ExecutionClient

    pool = ClientPool()
    first = pool.get("first", ExecutionClient)
    second = pool.get("second", ExecutionClient)
    assert pool.discard("first") is first
    assert pool.discard("first") is None
    assert pool.get("second", ExecutionClient) is second
    replacement = pool.get("first", ExecutionClient)
    assert replacement is not first

    pool.clear()
    assert pool.get("first", ExecutionClient) is not replacement
    assert pool.get("second", ExecutionClient) is not second
