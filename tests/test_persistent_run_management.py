"""Durable persistent job management after client GUI restart."""
from dataclasses import replace
from types import SimpleNamespace

import pytest

from kkrunner import RunManager, RunState, RunStore
from kkrunner.backends import BackendConfig
from kkrunner.backends.contracts import BackendCapability, UnsupportedCapability
from kkrunner.client import EndpointClient, LocalTransport
from kkrunner.runs import ActiveRunDeletionError, ForgottenRunError, RunStatus


def stored(manager, directory, *, state=RunState.UNKNOWN, backend="process"):
    record = manager.new_record(
        directory=directory, backend=BackendConfig(backend),
        transport=LocalTransport(), metadata={"test": {"task": "SCF"}},
    )
    return manager.transition(
        record, state=state, execution=record.execution.submitted("opaque-42"),
    )


def test_cancel_unknown_preserves_running_reply(tmp_path, monkeypatch):
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = stored(manager, tmp_path)
    seen = []
    client = SimpleNamespace(
        backend=SimpleNamespace(supports={BackendCapability.CANCEL}),
        acknowledge_submission=lambda _record: None,
        acknowledge=lambda _record: None,
        cancel=lambda attachment: seen.append(attachment) or RunStatus(
            RunState.RUNNING, backend_id="opaque-42",
        ),
    )
    monkeypatch.setattr(manager, "client_for", lambda _record: client)
    latest = manager.cancel(record.id)
    assert seen[0].execution.backend_id == "opaque-42"
    assert latest.state == RunState.RUNNING
    assert manager.store.get(record.id).state == RunState.RUNNING


def test_cancel_unknown_accepts_confirmed_terminal_reply(tmp_path, monkeypatch):
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = stored(manager, tmp_path)
    monkeypatch.setattr(manager, "client_for", lambda _record: SimpleNamespace(
        backend=SimpleNamespace(supports={BackendCapability.CANCEL}),
        acknowledge_submission=lambda _record: None,
        acknowledge=lambda _record: None,
        cancel=lambda _attachment: RunStatus(
            RunState.CANCELLED, backend_id="opaque-42",
        ),
    ))
    assert manager.cancel(record.id).state == RunState.CANCELLED
    assert manager.store.get(record.id).state == RunState.CANCELLED


def test_cancellation_connection_error_preserves_unknown(tmp_path, monkeypatch):
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = stored(manager, tmp_path)
    def unavailable(_attachment):
        raise ConnectionError("ssh unavailable")
    monkeypatch.setattr(manager, "client_for", lambda _record: SimpleNamespace(
        backend=SimpleNamespace(supports={BackendCapability.CANCEL}),
        acknowledge_submission=lambda _record: None,
        acknowledge=lambda _record: None,
        cancel=unavailable,
    ))
    with pytest.raises(ConnectionError, match="ssh unavailable"):
        manager.cancel(record.id)
    assert manager.store.get(record.id) == record


def test_direct_job_cannot_cancel_after_restart(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = stored(manager, tmp_path, backend="direct")
    with pytest.raises(UnsupportedCapability, match="cannot cancel"):
        manager.cancel(record.id)
    assert manager.store.get(record.id) == record


def test_forget_unknown_requires_force_and_does_not_delete_output(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = stored(manager, tmp_path)
    result_file = tmp_path / "answer.out"
    result_file.write_text("keep")
    with pytest.raises(ActiveRunDeletionError, match="force=True"):
        manager.forget(record.id)
    assert manager.store.get(record.id) == record
    assert manager.forget(record.id, force=True) == record
    assert result_file.read_text() == "keep"
    with pytest.raises(FileNotFoundError):
        manager.store.get(record.id)
    assert manager.store.runs() == ()


def test_forget_terminal_without_force(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = stored(manager, tmp_path, state=RunState.FINISHED)
    assert manager.forget(record.id) == record


def test_stale_monitor_cannot_restore_forgotten_record(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = stored(manager, tmp_path)
    manager.forget(record.id, force=True)
    separate_store = RunStore(manager.store.directory)
    with pytest.raises(ForgottenRunError):
        separate_store.update(record, lambda latest: replace(
            latest, state=RunState.FINISHED,
        ))
    with pytest.raises(ForgottenRunError):
        separate_store.save(record)
    with pytest.raises(ForgottenRunError):
        manager.transition(record, state=RunState.FINISHED)
    assert separate_store.runs() == ()


def test_endpoint_cancel_uses_existing_backend_operation(tmp_path, monkeypatch):
    client = EndpointClient(backend="process", transport=LocalTransport())
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = manager.new_record(
        directory=tmp_path, backend=BackendConfig("process"),
        transport=LocalTransport(),
    )
    record = replace(record, execution=record.execution.submitted("opaque-42"))
    calls = []
    async def invoke(operation, *args, **kwargs):
        calls.append((operation, args))
        return {"state": "cancelled", "return_code": 130}
    monkeypatch.setattr(client, "_invoke", invoke)
    from kkrunner.client.contracts import JobAttachment
    status = client.cancel(JobAttachment(record.execution, record.files))
    assert calls == [("cancel", ("opaque-42",))]
    assert status.state == RunState.CANCELLED
    assert status.backend_id == "opaque-42"
