"""Managed process and PSI/J client handles share the same lifecycle class."""

from kkrunner.client.local import LocalRunEndpoint
from kkrunner import ExecutionSpec
from kkrunner.backends import SupervisedBackend
from kkrunner.client.job import Job
from kkrunner.client.local_managed import local_managed_job


def test_supervised_client_job_is_shared(tmp_path):
    spec = ExecutionSpec(("true",), directory=str(tmp_path))
    managed = LocalRunEndpoint(SupervisedBackend).create_job(spec)
    assert type(managed) is Job
    assert managed.context.backend == "process"


def test_local_psij_context_uses_scheduler_identity_without_an_object():
    from kkrunner.client.local_managed import LocalManagedContext
    from kkrunner.remote.handles import decode_handle
    context = LocalManagedContext("psij", options={"backend": "slurm", "url": None})
    handle = decode_handle(context._wire_ref("123"))
    assert handle["kind"] == "psij"
    assert handle["native_id"] == "123"
    assert handle["backend"] == "slurm"


def test_psij_local_client_does_not_construct_target_runtime(tmp_path):
    from kkrunner.backends import PsijBackend
    job = LocalRunEndpoint(PsijBackend, backend_options={"backend": "slurm"}).create_job(
        ExecutionSpec(("true",), directory=str(tmp_path)), submission_key="request",
    )
    assert type(job) is Job
    assert job.context.backend == "psij"
    assert job.context.options["backend"] == "slurm"
