"""Direct/supervised process strategy is an explicit backend decision."""

import pytest

from kkrunner import ExecutionSpec, RunManager, RunStore
from kkrunner.backends import ProcessBackend, SupervisedProcessBackend
from kkrunner.backends.process.direct.job import ProcessJob
from kkrunner.backends.process.supervised.job import SupervisedProcessJob
from kkrunner.client.endpoint import ExecutionEndpoint, create_backend_from_endpoint, endpoint_for_backend


def test_direct_mode_never_switches_implicitly(tmp_path):
    backend = ProcessBackend()
    spec = ExecutionSpec(("echo", "example"), directory=str(tmp_path))
    assert isinstance(backend.create_job(spec), ProcessJob)
    with pytest.raises(NotImplementedError, match="SupervisedProcessBackend"):
        backend.create_job(spec, submission_key="run-id")
    manager = RunManager(RunStore(tmp_path / "runs"))
    with pytest.raises(ValueError, match="supervised backend"):
        manager.create_run(backend, spec)


def test_supervised_mode_is_explicit_and_persistable(tmp_path):
    backend = SupervisedProcessBackend()
    spec = ExecutionSpec(("echo", "example"), directory=str(tmp_path))
    with pytest.raises(ValueError, match="submission key"):
        backend.create_job(spec)
    assert isinstance(backend.create_job(spec, submission_key="run-id"), SupervisedProcessJob)
    endpoint = endpoint_for_backend(backend)
    assert endpoint == ExecutionEndpoint(executor="local-supervised")
    assert isinstance(create_backend_from_endpoint(endpoint), SupervisedProcessBackend)
    assert isinstance(create_backend_from_endpoint(ExecutionEndpoint()), ProcessBackend)
    assert endpoint != endpoint_for_backend(ProcessBackend())
