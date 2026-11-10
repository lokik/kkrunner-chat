from types import SimpleNamespace

import pytest

from kkrunner import ExecutionSpec
import kkrunner.backends.psij.backend as psij_backend
from kkrunner.backends.psij import slurm as psij_slurm


class _JobSpec:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class _JobAttributes:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class _Job:
    def __init__(self, spec=None):
        self.spec = spec
        self.native_id = None
        self.status = SimpleNamespace(state=SimpleNamespace(name="NEW"))


class _JobExecutor:
    @staticmethod
    def get_instance(*args, **kwargs):
        raise AssertionError("test supplies the job executor")


_FAKE_PSIJ = SimpleNamespace(Job=_Job, JobSpec=_JobSpec, JobAttributes=_JobAttributes, JobExecutor=_JobExecutor)


def _executor(monkeypatch, backend="slurm"):
    monkeypatch.setattr(psij_backend, "_load_psij", lambda: _FAKE_PSIJ)
    return psij_backend.PsijBackend(backend, job_executor=object())


def _spec(tmp_path):
    output = tmp_path / "run.out"
    return ExecutionSpec(argv=("program",), input_file=None, output_file=str(output), directory=str(tmp_path))


@pytest.mark.parametrize("backend", ("slurm", "pbs"))
def test_project_job_carries_kkrunner_identity(monkeypatch, tmp_path, backend):
    executor = _executor(monkeypatch, backend)
    runner = executor.create_job(_spec(tmp_path), submission_key="run-123")
    job_spec = runner._job.spec
    assert job_spec.environment == {"KKRunner_RUN_ID": "run-123"}
    if backend == "slurm":
        assert job_spec.attributes.custom_attributes == {
            "slurm.comment": psij_slurm.submission_marker("run-123")
        }
    else:
        assert job_spec.attributes is None


def test_nonpersistent_psij_job_has_no_recovery_metadata(monkeypatch, tmp_path):
    executor = _executor(monkeypatch, "slurm")
    runner = executor.create_job(_spec(tmp_path))
    job_spec = runner._job.spec
    assert job_spec.environment is None
    assert job_spec.attributes is None
