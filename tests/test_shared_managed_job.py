"""Managed process and PSI/J client handles share the same lifecycle class."""

from kkrunner import ExecutionSpec
from kkrunner.backends import PsijBackend, SupervisedBackend
from kkrunner.client.job import Job
from kkrunner.client.local import LocalRunEndpoint


def test_supervised_client_job_is_shared(tmp_path):
    spec = ExecutionSpec(("true",), directory=str(tmp_path))
    job = LocalRunEndpoint(SupervisedBackend).create_job(spec)
    assert type(job) is Job
    assert isinstance(job.context.backend, SupervisedBackend)
    assert job.ref is None


def test_psij_local_client_does_not_construct_target_runtime(tmp_path):
    endpoint = LocalRunEndpoint(PsijBackend, backend_options={"executor_name": "slurm"})
    job = endpoint.create_job(
        ExecutionSpec(("true",), directory=str(tmp_path)),
        submission_key="request",
    )
    assert type(job) is Job
    assert job.context is endpoint
    assert isinstance(job.context.backend, PsijBackend)
    assert job.context.backend.options["executor_name"] == "slurm"
    assert job.ref is None  # No target identity until submission is accepted.
