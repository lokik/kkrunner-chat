"""Refreshing live runs must not contact backends for historical receipts."""

from kkrunner import RunState, RunStore
from kkrunner.backends import SupervisedBackend
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

    assert manager.refresh() == ()
    assert acknowledged == []

    assert manager.reconcile_acknowledgements() == (record.id,)
    assert acknowledged == [record.id]
