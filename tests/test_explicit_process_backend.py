"""Direct/supervised process strategy is an explicit backend decision."""

import pytest

from kkrunner import ExecutionSpec, RunManager, RunStore
from kkrunner.backends import DirectProcessBackend, SupervisedProcessBackend
from kkrunner.backends.process.direct.job import DirectProcessJob
from kkrunner.client.job import Job
from kkrunner.client.endpoint import ExecutionEndpointConfig, executor_from_endpoint, endpoint_config_for_executor


def test_direct_mode_never_switches_implicitly(tmp_path):
    backend = DirectProcessBackend()
    spec = ExecutionSpec(("echo", "example"), directory=str(tmp_path))
    assert isinstance(backend.create_job(spec), DirectProcessJob)
    assert isinstance(backend.create_job(spec, submission_key="run-id"), DirectProcessJob)
    manager = RunManager(RunStore(tmp_path / "runs"))
    assert manager.create_run(backend, spec).record is not None


def test_supervised_mode_is_explicit_and_persistable(tmp_path):
    backend = SupervisedProcessBackend()
    spec = ExecutionSpec(("echo", "example"), directory=str(tmp_path))
    assert isinstance(backend.create_job(spec), Job)
    assert isinstance(backend.create_job(spec, submission_key="run-id"), Job)
    endpoint = endpoint_config_for_executor(backend)
    assert endpoint == ExecutionEndpointConfig(executor="local-supervised")
    assert isinstance(executor_from_endpoint(endpoint), SupervisedProcessBackend)
    assert isinstance(executor_from_endpoint(ExecutionEndpointConfig()), DirectProcessBackend)
    assert endpoint != endpoint_config_for_executor(DirectProcessBackend())


def test_native_client_capabilities_match_backend(tmp_path):
    from kkrunner.client.run_parts import run_endpoint_for_executor
    direct = run_endpoint_for_executor(DirectProcessBackend())
    supervised = run_endpoint_for_executor(SupervisedProcessBackend())
    assert not hasattr(direct, "open_run_parts")
    assert callable(supervised.open_run_parts)


def test_backend_status_uses_shared_state_enum_without_client_phases():
    from kkrunner.backends.contracts import JobStatus
    from kkrunner.runs.base import RunState
    from kkrunner.runs.states import RunState as SharedState
    assert RunState is SharedState
    assert JobStatus("running").state is RunState.RUNNING
    with pytest.raises(ValueError, match="client lifecycle phase"):
        JobStatus(RunState.DOWNLOADING)
