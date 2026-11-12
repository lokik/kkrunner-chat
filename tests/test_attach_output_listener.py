"""Output listeners on reopened jobs must be installed before observation."""

import asyncio
from dataclasses import replace

import pytest

from kkrunner import RunState, RunStore
from kkrunner.backends import PsijBackend, SupervisedBackend
from kkrunner.backends.contracts import JobStatus
from kkrunner.backends.config import BackendConfig
from kkrunner.client import EndpointClient
from kkrunner.client.backend_execution import ManagedExecution
from kkrunner.client.contracts import JobAttachment
from kkrunner.files import FileEntry, FileManifest
from kkrunner.runs.manager import RunManager
from kkrunner.runs.model import ExecutionMetadata, WorkingDirectory


def _client_and_attachment(tmp_path, monkeypatch, backend_type=SupervisedBackend):
    client = EndpointClient(backend_type)
    backend_id = "test-backend-id"
    execution = ExecutionMetadata(
        client.transport, WorkingDirectory(str(tmp_path)),
        backend_id, backend=client.backend_config,
    )
    files = FileManifest(stdout=FileEntry(local_path=str(tmp_path / "run.out")))

    async def status(name, *args, **kwargs):
        if name == "status":
            assert args == (backend_id,)
            return {"state": "running", "return_code": None}
        if name in ("acknowledge", "acknowledge_submission"):
            return {"acknowledged": True}
        raise AssertionError(f"Unexpected backend operation: {name}")

    monkeypatch.setattr(client, "_invoke", status)
    return client, JobAttachment(execution, files)


@pytest.fixture
def replay_observer(monkeypatch):
    async def observe(self, on_output):
        on_output(b"replayed output\n", "out")
        await asyncio.sleep(0)
        return JobStatus(RunState.FINISHED, return_code=0)

    monkeypatch.setattr(ManagedExecution, "observe", observe)


def test_manager_attach_forwards_initial_output_listener(tmp_path, monkeypatch, replay_observer):
    client, attachment = _client_and_attachment(tmp_path, monkeypatch)
    (tmp_path / "run.out").write_bytes(b"replayed output\n")
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = manager.new_record(
        directory=tmp_path, backend=client.backend_config,
        transport=client.transport, files=attachment.files,
    )
    manager.store.save(replace(
        record, state=RunState.RUNNING,
        execution=record.execution.submitted(attachment.execution.backend_id),
    ))
    output = []
    runner = manager.attach(
        record, client=client,
        output_listener=lambda text, kind: output.append((text, kind)),
    )
    assert runner.wait().return_code == 0
    assert output == [("replayed output\n", "out")]


@pytest.mark.parametrize("backend_type", [SupervisedBackend, PsijBackend])
def test_attach_listener_is_installed_before_monitor(tmp_path, monkeypatch, replay_observer, backend_type):
    client, attachment = _client_and_attachment(tmp_path, monkeypatch, backend_type)
    events = []
    parts = client.open_run_parts(
        attachment, output_listener=lambda text, kind: events.append((text, kind)),
    )
    try:
        parts.configure_observation()
        parts.job.start()
        assert parts.job.wait().return_code == 0
        assert events == [("replayed output\n", "out")]
    finally:
        parts.close()


@pytest.mark.parametrize("backend_type", [SupervisedBackend, PsijBackend])
def test_attach_can_defer_monitor_start(tmp_path, monkeypatch, replay_observer, backend_type):
    client, attachment = _client_and_attachment(tmp_path, monkeypatch, backend_type)
    events = []
    parts = client.open_run_parts(
        attachment, output_listener=lambda text, kind: events.append((text, kind)),
    )
    try:
        assert parts.job._thread is None
        assert events == []
        parts.configure_observation()
        parts.job.start()
        assert parts.job.wait().return_code == 0
        assert events == [("replayed output\n", "out")]
    finally:
        parts.close()
