"""Direct/supervised process strategy is an explicit backend decision."""

import pytest

from kkrunner import ExecutionSpec, RunManager, RunStore
from kkrunner.client import RunClient, LocalTransport
from kkrunner.backends import SupervisedBackend
from kkrunner.client.job import Job
from kkrunner.backends.config import BackendConfig


def test_direct_mode_never_switches_implicitly(tmp_path):
    backend = RunClient()
    spec = ExecutionSpec(("echo", "example"), directory=str(tmp_path))
    assert isinstance(backend.create_run_parts(spec).job, Job)
    assert isinstance(backend.create_run_parts(spec, submission_key="run-id").job, Job)
    manager = RunManager(RunStore(tmp_path / "runs"))
    assert manager.create_run(backend, spec).record is not None


def test_supervised_mode_is_explicit_and_persistable(tmp_path):
    backend = RunClient(SupervisedBackend)
    spec = ExecutionSpec(("echo", "example"), directory=str(tmp_path))
    assert isinstance(backend.create_run_parts(spec).job, Job)
    assert isinstance(backend.create_run_parts(spec, submission_key="run-id").job, Job)
    assert backend.transport == LocalTransport()
    assert backend.backend_config == BackendConfig("process")
    restored = RunClient.from_config(backend.transport.to_dict(), backend.backend_config)
    assert isinstance(restored.backend, SupervisedBackend)
    assert isinstance(RunClient.from_config(LocalTransport().to_dict(), BackendConfig()).backend,
                      type(RunClient().backend))
    assert backend.backend_config != RunClient().backend_config


def test_native_client_capabilities_match_backend(tmp_path):
    direct = RunClient()
    supervised = RunClient(SupervisedBackend)
    assert direct.backend.execution_mode == "direct"
    assert callable(supervised.open_run_parts)


def test_backend_status_uses_shared_state_enum_without_client_phases():
    from kkrunner.backends.contracts import JobStatus
    from kkrunner.runs.base import RunState
    from kkrunner.runs.states import RunState as SharedState
    assert RunState is SharedState
    assert JobStatus("running").state is RunState.RUNNING
    with pytest.raises(ValueError, match="client lifecycle phase"):
        JobStatus(RunState.DOWNLOADING)
