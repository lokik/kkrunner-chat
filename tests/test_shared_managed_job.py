"""Managed process and PSI/J client handles share the same lifecycle class."""

from kkrunner import ExecutionSpec, RunState
from kkrunner.backends import PsijBackend
from kkrunner.client.job import Job
from kkrunner.client import ExecutionClient


def test_psij_local_client_does_not_construct_target_runtime(tmp_path):
    client = ExecutionClient(PsijBackend, backend_options={"executor_name": "slurm"})
    job = client.create_run_parts(
        ExecutionSpec(("true",), directory=str(tmp_path)),
        submission_key="request",
    ).job
    assert type(job) is Job
    assert job.execution.target == client.target
    assert isinstance(job.execution.backend, PsijBackend)
    assert dict(job.execution.backend.options)["executor_name"] == "slurm"
    assert job.ref is None  # No target identity until submission is accepted.
    assert job.backend_id is None
    assert job.state is RunState.NEW
