"""Direct/supervised process strategy is an explicit backend decision."""

import pytest

from kkrunner import ExecutionSpec, RunManager, RunStore
from kkrunner.client.local import LocalRunEndpoint
from kkrunner.backends import SupervisedBackend
from kkrunner.client.job import Job
from kkrunner.client.endpoint import ExecutionEndpointConfig, endpoint_from_config, describe_endpoint


def test_direct_mode_never_switches_implicitly(tmp_path):
    backend = LocalRunEndpoint()
    spec = ExecutionSpec(("echo", "example"), directory=str(tmp_path))
    assert isinstance(backend.create_job(spec), Job)
    assert isinstance(backend.create_job(spec, submission_key="run-id"), Job)
    manager = RunManager(RunStore(tmp_path / "runs"))
    assert manager.create_run(backend, spec).record is not None


def test_supervised_mode_is_explicit_and_persistable(tmp_path):
    backend = LocalRunEndpoint(SupervisedBackend)
    spec = ExecutionSpec(("echo", "example"), directory=str(tmp_path))
    assert isinstance(backend.create_job(spec), Job)
    assert isinstance(backend.create_job(spec, submission_key="run-id"), Job)
    endpoint = describe_endpoint(backend)
    assert endpoint == ExecutionEndpointConfig(location="local", backend="process")
    assert endpoint_from_config(endpoint).backend is SupervisedBackend
    assert isinstance(endpoint_from_config(ExecutionEndpointConfig()), LocalRunEndpoint)
    assert endpoint != describe_endpoint(LocalRunEndpoint())


def test_native_client_capabilities_match_backend(tmp_path):
    direct = LocalRunEndpoint()
    supervised = LocalRunEndpoint(SupervisedBackend)
    assert direct.backend.session_bound
    assert callable(supervised.open_run_parts)


def test_backend_status_uses_shared_state_enum_without_client_phases():
    from kkrunner.backends.contracts import JobStatus
    from kkrunner.runs.base import RunState
    from kkrunner.runs.states import RunState as SharedState
    assert RunState is SharedState
    assert JobStatus("running").state is RunState.RUNNING
    with pytest.raises(ValueError, match="client lifecycle phase"):
        JobStatus(RunState.DOWNLOADING)
