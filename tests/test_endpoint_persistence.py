"""Endpoint persistence and reconstruction without application adapters."""

from threading import Event

from kkrunner import RunState, RunStore
from kkrunner.client.endpoint import ExecutionEndpointConfig, describe_endpoint
from kkrunner.client.pool import EndpointPool
from kkrunner.backends import SupervisedBackend, PsijBackend
from kkrunner.client.contracts import JobAttachment
from kkrunner.client.run_parts import RunParts
from kkrunner.client import SshRunEndpoint, SshTransport
from kkrunner.runs.manager import RunManager


def test_run_store_roundtrips_endpoint(tmp_path):
    store = RunStore(tmp_path / "runs")
    manager = RunManager(store, endpoint_pool=EndpointPool())
    endpoint = ExecutionEndpointConfig(location="ssh", host="cluster", ssh_options=("-v",), ssh_zstd=True)
    record = manager.new_record(
        directory=tmp_path,
        endpoint=endpoint,
        metadata={"example": {"task": "DOS".upper(), "dataset": "case"}},
        processing_required=True,
    )
    stored = store.save(record)
    loaded = store.get(stored.id)
    assert loaded.execution.endpoint == endpoint
    assert isinstance(loaded.execution.endpoint, ExecutionEndpointConfig)


def test_manager_reconstructs_local_executor_after_restart(tmp_path):
    store = RunStore(tmp_path / "runs")
    endpoint = ExecutionEndpointConfig(location="local", backend="process")
    first = RunManager(store, endpoint_pool=EndpointPool())
    record = first.new_record(
        directory=tmp_path,
        endpoint=endpoint,
        metadata={"example": {"task": "DOS".upper()}},
        processing_required=True,
    )
    store.save(record)
    restarted = RunManager(store, endpoint_pool=EndpointPool())
    executor = restarted.endpoint_for(store.get(record.id))
    assert isinstance(executor.backend, SupervisedBackend)


def test_attach_reconstructs_persisted_endpoint_without_task_reader(tmp_path):

    class AttachedRunner:
        def __init__(self, run):
            self.backend_id = run.execution.backend_id
            self.state = RunState.RUNNING
            self.result = None
            self.exception = None
            self.failure_phase = None
            self.failure_message = None
            self._done = Event()

        def start(self):
            pass

        def wait(self):
            self._done.wait()

        def cancel(self):
            self.state = RunState.CANCELLED
            self._done.set()

        def add_output_listener(self, callback):
            pass

        def add_output_error_listener(self, callback):
            pass

    class ReconstructedExecutor:
        def __init__(self):
            self.calls = []

        def open_run_parts(self, attachment, *, callback=None, output_listener=None):
            self.calls.append((attachment, callback, output_listener))
            return RunParts(AttachedRunner(attachment))

    class RecordingPool:
        def __init__(self, executor):
            self.executor = executor
            self.endpoints = []

        def for_config(self, endpoint):
            self.endpoints.append(endpoint)
            return self.executor

        def get(self, endpoint, factory):
            return self.executor

    store = RunStore(tmp_path / "runs")
    endpoint = ExecutionEndpointConfig(location="ssh", host="cluster")
    first = RunManager(store, endpoint_pool=EndpointPool())
    record = first.new_record(
        directory=tmp_path,
        endpoint=endpoint,
        metadata={"example": {"task": "DOS".upper()}},
        processing_required=True,
    )
    store.save(record)
    record = first.transition(record, state=RunState.RUNNING, execution=record.execution.submitted("42"))
    executor = ReconstructedExecutor()
    pool = RecordingPool(executor)
    restarted = RunManager(store, endpoint_pool=pool)
    attached = restarted.attach(record.id, process_result=lambda outcome: outcome)
    assert pool.endpoints == [endpoint]
    assert attached.endpoint is executor
    assert executor.calls == [(JobAttachment(record.execution, record.files), None, None)]
    attached.cancel()


def test_pool_reconstructs_and_reuses_ssh_endpoint():
    endpoint = ExecutionEndpointConfig(
        location="ssh", backend="psij",
        host="cluster",
        ssh_options=("-p", "2222"),
        ssh_zstd=True,
        backend_options={"executor_name": "slurm", "url": "local://slurm"},
    )
    pool = EndpointPool()
    first = pool.for_config(endpoint)
    second = pool.for_config(endpoint)
    assert first is second
    assert isinstance(first, SshRunEndpoint)
    assert isinstance(first.transport, SshTransport)
    assert first.transport.host == "cluster"
    assert first.transport.options == ("-p", "2222")
    assert first.transport.zstd is True
    assert isinstance(first.backend, PsijBackend)
    assert dict(first.backend.options) == {"executor_name": "slurm", "url": "local://slurm"}


def test_default_remote_psij_endpoint_survives_restart():
    live = SshRunEndpoint(SshTransport("cluster"), backend=PsijBackend)
    endpoint = describe_endpoint(live)
    restored = EndpointPool().for_config(endpoint)
    assert endpoint.backend == "psij"
    assert isinstance(restored, SshRunEndpoint)
    assert isinstance(restored.backend, PsijBackend)
    assert dict(restored.backend.options) == {"executor_name": "slurm", "url": None}
