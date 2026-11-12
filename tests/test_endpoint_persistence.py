"""Transport/backend persistence and reconstruction without application adapters."""

from kkrunner import ExecutionSpec, RunState, RunStore
from kkrunner.backends import PsijBackend, SupervisedBackend
from kkrunner.backends.config import BackendConfig
from kkrunner.client import EndpointClient, LocalTransport, SshTransport
from kkrunner.client.contracts import JobAttachment
from kkrunner.client.job import Job
from kkrunner.client.pool import ClientPool
from kkrunner.client.run_parts import RunParts
from kkrunner.runs.manager import RunManager
from kkrunner.runs.run import Run


def test_run_store_roundtrips_transport_and_backend_independently(tmp_path):
    store = RunStore(tmp_path / "runs")
    manager = RunManager(store, client_pool=ClientPool())
    transport = SshTransport("cluster", options=("-v",), zstd=True)
    backend = BackendConfig("psij", {"executor_name": "slurm"})
    record = manager.new_record(
        directory=tmp_path, transport=transport, backend=backend,
        metadata={"example": {"task": "DOS", "dataset": "case"}},
        processing_required=True,
    )
    store.save(record)
    loaded = store.get(record.id)
    assert loaded.execution.transport == transport
    assert loaded.execution.backend == backend
    assert isinstance(loaded.execution.transport, SshTransport)


def test_manager_reconstructs_local_supervised_client_after_restart(tmp_path):
    store = RunStore(tmp_path / "runs")
    manager = RunManager(store, client_pool=ClientPool())
    record = manager.new_record(
        directory=tmp_path, transport=LocalTransport(), backend=BackendConfig("process"),
        metadata={"example": {"task": "DOS"}}, processing_required=True,
    )
    store.save(record)
    restarted = RunManager(store, client_pool=ClientPool())
    client = restarted.client_for(store.get(record.id))
    assert isinstance(client, EndpointClient)
    assert client.transport == LocalTransport()
    assert isinstance(client.backend, SupervisedBackend)


def test_attach_reconstructs_client_and_passes_persisted_attachment(tmp_path, monkeypatch):
    class RecordingPool(ClientPool):
        def __init__(self):
            super().__init__()
            self.lookups = []

        def for_config(self, transport, backend):
            self.lookups.append((transport, backend))
            return super().for_config(transport, backend)

    store = RunStore(tmp_path / "runs")
    transport = SshTransport("cluster")
    backend = BackendConfig("process")
    manager = RunManager(store, client_pool=ClientPool())
    record = manager.new_record(
        directory=tmp_path, transport=transport, backend=backend,
        metadata={"example": {"task": "DOS"}}, processing_required=True,
    )
    store.save(record)
    record = manager.transition(
        record, state=RunState.RUNNING,
        execution=record.execution.submitted("native-job-42"),
    )

    openings = []
    starts = []

    def fake_open(client, attachment, *, callback=None, output_listener=None):
        openings.append((client, attachment, callback, output_listener))
        spec = ExecutionSpec(argv=None, directory=str(tmp_path))
        files = client.file_session(spec)
        from kkrunner.backends.contracts import JobRef
        from kkrunner.client.backend_execution import bind_execution

        binding = bind_execution(
            client.backend, client.transport,
            ref=JobRef(client.backend.name, attachment.execution.backend_id),
            status={"state": "running"},
        )
        job = Job(binding, spec)
        return RunParts(job, files)

    monkeypatch.setattr(EndpointClient, "open_run_parts", fake_open)
    monkeypatch.setattr(Run, "start", lambda self: starts.append(self))
    pool = RecordingPool()
    restarted = RunManager(store, client_pool=pool)
    attached = restarted.attach(record.id, process_result=lambda outcome: outcome)
    try:
        assert pool.lookups == [(transport, backend)]
        assert attached.client is openings[0][0]
        assert openings[0][1] == JobAttachment(record.execution, record.files)
        assert openings[0][2:] == (None, None)
        assert attached._job.backend_id == "native-job-42"
        assert starts == [attached]
    finally:
        attached._parts.close()
        pool.close()


def test_pool_reconstructs_and_reuses_same_ssh_client():
    transport = SshTransport("cluster", options=("-p", "2222"), zstd=True)
    backend = BackendConfig("psij", {"executor_name": "slurm", "url": "local://slurm"})
    pool = ClientPool()
    first = pool.for_config(transport, backend)
    second = pool.for_config(transport.to_dict(), {
        "name": backend.name, "options": dict(backend.options),
    })
    assert first is second
    assert isinstance(first, EndpointClient)
    assert first.transport == transport
    assert first.transport.options == ("-p", "2222")
    assert first.transport.zstd is True
    assert isinstance(first.backend, PsijBackend)
    assert dict(first.backend.options) == {"executor_name": "slurm", "url": "local://slurm"}
    pool.close()


def test_default_psij_configuration_survives_client_reconstruction():
    live = EndpointClient(PsijBackend, transport=SshTransport("cluster"))
    restored = ClientPool().for_config(live.transport.to_dict(), live.backend_config)
    assert live.backend_config.name == "psij"
    assert isinstance(restored, EndpointClient)
    assert isinstance(restored.backend, PsijBackend)
    assert restored.transport == live.transport
    assert dict(restored.backend.options) == {"executor_name": "slurm", "url": None}
