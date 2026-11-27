"""PSI/J submission identity is injected into the native specification."""

from types import SimpleNamespace

import pytest

from kkrunner.backends.implementations.psij.slurm import submission_marker
from kkrunner.backends.implementations.psij.submission import prepare_job
from kkrunner import ExecutionSpec
from kkrunner.client.execution_request import execution_request


class _FakeJobSpec:
    def __init__(self, **values):
        self.__dict__.update(values)


class _FakeJobAttributes:
    def __init__(self, **values):
        self.__dict__.update(values)


class _FakeJob:
    def __init__(self, spec=None):
        self.spec = spec


_FAKE_PSIJ = SimpleNamespace(
    Job=_FakeJob, JobSpec=_FakeJobSpec, JobAttributes=_FakeJobAttributes,
)


def _request(tmp_path):
    return execution_request(ExecutionSpec(
        argv=("program",), directory=str(tmp_path), output_file="run.out",
    ))


@pytest.mark.parametrize("executor_name", ("slurm", "pbs"))
def test_project_job_carries_kkrunner_submission_identity(tmp_path, executor_name):
    job = prepare_job(
        _FAKE_PSIJ, _request(tmp_path), executor_name=executor_name,
        submission_key="run-123",
    )
    assert job.spec.environment == {"KKRunner_RUN_ID": "run-123"}
    if executor_name == "slurm":
        assert job.spec.attributes.custom_attributes == {
            "slurm.comment": submission_marker("run-123"),
        }
    else:
        assert job.spec.attributes is None

