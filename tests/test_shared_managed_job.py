"""Managed process and PSI/J client handles share the same lifecycle class."""

from kkrunner import ExecutionSpec
from kkrunner.backends import PsijBackend, SupervisedBackend
from kkrunner.client.job import Job
from kkrunner.client import EndpointClient


def test_supervised_client_job_is_shared(tmp_path):
    spec = ExecutionSpec(("true",), directory=str(tmp_path))
    job = EndpointClient(SupervisedBackend).create_run_parts(spec).job
    assert type(job) is Job
    assert isinstance(job.execution.backend, SupervisedBackend)
    assert job.ref is None


def test_psij_local_client_does_not_construct_target_runtime(tmp_path):
    client = EndpointClient(PsijBackend, backend_options={"executor_name": "slurm"})
    job = client.create_run_parts(
        ExecutionSpec(("true",), directory=str(tmp_path)),
        submission_key="request",
    ).job
    assert type(job) is Job
    assert job.execution.transport == client.transport
    assert isinstance(job.execution.backend, PsijBackend)
    assert dict(job.execution.backend.options)["executor_name"] == "slurm"
    assert job.ref is None  # No target identity until submission is accepted.
