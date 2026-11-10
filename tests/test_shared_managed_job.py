"""Managed process and PSI/J client handles share the same lifecycle class."""

from kkrunner import ExecutionSpec
from kkrunner.backends import SupervisedProcessBackend
from kkrunner.client.job import Job
from kkrunner.client.local_managed import local_managed_job


def test_supervised_client_job_is_shared(tmp_path):
    spec = ExecutionSpec(("true",), directory=str(tmp_path))
    managed = SupervisedProcessBackend().create_job(spec)
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


def test_psij_backend_constructs_shared_client_job_with_native_config(monkeypatch, tmp_path):
    import kkrunner.backends.psij.backend as native
    from types import SimpleNamespace

    class FakeExecutor:
        @classmethod
        def get_instance(cls, backend, url=None, config=None):
            return SimpleNamespace()

    monkeypatch.setattr(native, "_load_psij", lambda: SimpleNamespace(JobExecutor=FakeExecutor))
    backend = native.PsijBackend("slurm")
    spec = ExecutionSpec(("true",), directory=str(tmp_path))
    job = backend.create_job(spec, submission_key="request")
    assert type(job) is Job
    assert job.context.backend == "psij"
    assert job.context.options["backend"] == "slurm"


def test_psij_client_rejects_unserializable_executor_configuration(monkeypatch, tmp_path):
    import kkrunner.backends.psij.backend as native
    from types import SimpleNamespace
    monkeypatch.setattr(native, "_load_psij", lambda: SimpleNamespace())
    backend = native.PsijBackend("slurm", job_executor=object())
    with __import__("pytest").raises(ValueError, match="context-independent"):
        backend.create_job(ExecutionSpec(("true",), directory=str(tmp_path)))
