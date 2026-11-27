"""Refreshing live runs must not contact backends for historical receipts."""

from kkrunner import RunState, RunStore
import pytest

from kkrunner.backends import SupervisedBackend
from kkrunner.backends.contracts import JobRef, RecoveryResult
from kkrunner.runs import RunStatus
from kkrunner.client import ExecutionClient
from kkrunner.client.pool import ClientPool
from kkrunner.runs.manager import RunManager


def test_refresh_does_not_retry_historical_acknowledgements(tmp_path, monkeypatch):
    store = RunStore(tmp_path / "runs")
    manager = RunManager(store, client_pool=ClientPool())
    client = ExecutionClient(SupervisedBackend)
    record = manager.new_record(
        directory=tmp_path, target=client.target, backend=client.backend_config,
    )
    record = manager.transition(
        record, state=RunState.RUNNING,
        execution=record.execution.submitted("historical-job"),
    )
    record = manager.transition(record, state=RunState.FINISHED, return_code=0)

    # A new manager has no in-memory knowledge of earlier acknowledgements.
    manager = RunManager(store, client_pool=ClientPool())
    manager.register_client(client)
    acknowledged = []
    monkeypatch.setattr(client, "acknowledge", lambda record: acknowledged.append(record.id))
    monkeypatch.setattr(client, "acknowledge_submission", lambda record: None)

    assert manager.refresh() == ()
    assert acknowledged == []

    assert manager.reconcile_acknowledgements() == (record.id,)
    assert acknowledged == [record.id]


def test_active_status_refresh_does_not_repeat_submission_ack(tmp_path, monkeypatch):
    manager = RunManager(RunStore(tmp_path / "runs"), client_pool=ClientPool())
    client = ExecutionClient(SupervisedBackend)
    manager.register_client(client)
    record = manager.new_record(
        directory=tmp_path, target=client.target, backend=client.backend_config,
    )
    record = manager.transition(
        record, state=RunState.RUNNING,
        execution=record.execution.submitted("active-job"),
    )
    acknowledged = []
    queries = []
    monkeypatch.setattr(client, "acknowledge_submission", lambda record: acknowledged.append(record.id))

    def status(*runs):
        queries.extend(run.execution.backend_id for run in runs)
        return tuple(RunStatus(RunState.RUNNING, backend_id=run.execution.backend_id) for run in runs)

    monkeypatch.setattr(client, "status", status)
    assert manager.refresh()[0].state == RunState.RUNNING
    assert manager.refresh()[0].state == RunState.RUNNING
    assert queries == ["active-job", "active-job"]
    assert acknowledged == []

    assert manager.reconcile_acknowledgements() == ()
    assert acknowledged == [record.id]


def test_recovery_acknowledges_durable_identity_before_status_query(tmp_path, monkeypatch):
    manager = RunManager(
        RunStore(tmp_path / "runs"), client_pool=ClientPool(), submitting_grace=0,
    )
    client = ExecutionClient(SupervisedBackend)
    manager.register_client(client)
    record = manager.new_record(
        directory=tmp_path, target=client.target, backend=client.backend_config,
    )
    record = manager.transition(record, state=RunState.SUBMITTING)
    monkeypatch.setattr(
        client, "recover_submission",
        lambda *args, **kwargs: RecoveryResult.found(JobRef("kkrunner", "recovered-job")),
    )
    acknowledged = []

    def acknowledge(run):
        assert manager.store.get(run.id).execution.backend_id == "recovered-job"
        acknowledged.append(run.id)

    def status(*runs):
        raise OSError("disconnected before status")

    monkeypatch.setattr(client, "acknowledge_submission", acknowledge)
    monkeypatch.setattr(client, "status", status)
    with pytest.raises(OSError, match="disconnected before status"):
        manager.recover_submitting(record)
    assert acknowledged == [record.id]
    assert manager.store.get(record.id).execution.backend_id == "recovered-job"
