"""Storage layout keeps active reservation checks independent of history."""

import json
from dataclasses import replace

import pytest

from kkrunner import RunManager, RunState, RunStore, ExecutionSpec
from kkrunner.files import FileManifest
from kkrunner.runs import CorruptRunRecord
from kkrunner.runs import records as records_module


def _record(store, directory, name, state, created):
    record = RunManager(store).new_record(
        directory=directory, run_id=name,
        files=FileManifest.from_spec(
            ExecutionSpec(argv=(), directory=directory, output_file=name + ".out"),
        ),
    )
    return replace(record, state=state, created_at=created, updated_at=created)


def _records(store):
    return set(store.directory.rglob("*.json"))


def test_active_queries_and_conflicts_do_not_scan_dated_history(tmp_path, monkeypatch):
    store = RunStore(tmp_path / "runs")
    old = _record(store, tmp_path, "old", RunState.FINISHED, "2025-01-01T10:00:00+00:00")
    running = _record(store, tmp_path, "running", RunState.RUNNING, "2025-01-02T10:00:00+00:00")
    store.save(old)
    store.save(running)
    # Even corrupt historical JSON is irrelevant to live file reservations.
    history = store.directory / "2025-01-01" / "old.json"
    history.write_text("invalid json")
    with pytest.raises(CorruptRunRecord):
        store.runs()

    def unexpected_history_scan(**kwargs):
        pytest.fail("Active-only queries must not scan dated shards")

    monkeypatch.setattr(store, "_day_directories", unexpected_history_scan)
    assert store.runs(active=True) == (running,)
    proposed = replace(running, id="proposed", state=RunState.NEW)
    assert RunManager(store).conflicts(proposed) == (running,)


def test_lifecycle_moves_single_record_and_preserves_history_order(tmp_path):
    store = RunStore(tmp_path / "runs")
    peer = RunStore(store.directory)
    running = store.save(_record(store, tmp_path, "running", RunState.RUNNING,
                                 "2025-01-01T10:00:00+00:00"))
    newer = store.save(_record(store, tmp_path, "newer", RunState.FINISHED,
                               "2025-01-02T10:00:00+00:00"))
    active_path = store.directory / "active" / "running.json"
    history_path = store.directory / "2025-01-01" / "running.json"
    assert active_path.is_file()
    assert not history_path.exists()
    assert peer.runs() == (running, newer)
    assert peer.runs(newest=True, limit=1) == (newer,)
    finished = store.update(running, lambda current: replace(current, state=RunState.FINISHED))
    assert _records(store) == {history_path, store.directory / "2025-01-02" / "newer.json"}
    assert peer.get(running.id) == finished
    assert peer.runs(active=True) == ()
    assert peer.runs(active=False) == (finished, newer)
    with peer.submission_lock():
        resumed = peer.update(finished, lambda current: replace(current, state=RunState.DOWNLOADING))
    assert active_path.is_file()
    assert not history_path.exists()
    assert store.get(running.id) == resumed
    assert store.runs(active=True) == (resumed,)
    assert store.runs(active=False) == (newer,)


def test_legacy_migration_resumes_after_interruption(tmp_path, monkeypatch):
    store = RunStore(tmp_path / "runs")
    old = _record(store, tmp_path, "old", RunState.FINISHED, "2025-01-01T10:00:00+00:00")
    active = _record(store, tmp_path, "running", RunState.UNKNOWN, "2025-01-02T10:00:00+00:00")
    originals = {}
    for record in (old, active):
        path = store.directory / record.created_at[:10] / (record.id + ".json")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(record.to_dict()))
        originals[record.id] = path.read_bytes()
    move = store._move_record

    def interrupt(source, destination):
        move(source, destination)
        raise OSError("interrupted migration")

    with monkeypatch.context() as patch:
        patch.setattr(store, "_move_record", interrupt)
        with pytest.raises(OSError, match="interrupted migration"):
            store.runs(active=True)
    peer = RunStore(store.directory)
    assert peer.runs(active=True) == (active,)
    assert peer.runs() == (old, active)
    assert len(_records(peer)) == 2
    for path in _records(peer):
        assert path.read_bytes() == originals[path.stem]
    assert (peer.directory / "active" / "running.json").is_file()
    monkeypatch.setattr(peer, "_day_directories",
                        lambda **kw: pytest.fail("Repeated migration/history scan"))
    assert peer.runs(active=True) == (active,)


def test_interrupted_archiving_keeps_terminal_record_readable(tmp_path, monkeypatch):
    store = RunStore(tmp_path / "runs")
    running = store.save(_record(store, tmp_path, "running", RunState.RUNNING,
                                 "2025-01-01T10:00:00+00:00"))
    with monkeypatch.context() as patch:
        def interrupt(*args):
            raise OSError("interrupted archive")
        patch.setattr(store, "_move_record", interrupt)
        with pytest.raises(OSError, match="interrupted archive"):
            store.update(running, lambda current: replace(current, state=RunState.FINISHED))
    peer = RunStore(store.directory)
    finished = peer.get(running.id)
    assert finished.state == RunState.FINISHED
    assert peer.runs(active=True) == ()
    assert peer.runs(active=False) == (finished,)
    assert len(_records(peer)) == 1
    assert peer.update(finished, lambda current: current) == finished
    assert _records(peer) == {peer.directory / "2025-01-01" / "running.json"}
    peer.forget(finished)
    assert store.runs() == ()


def test_interrupted_reopening_does_not_hide_active_reservation(tmp_path, monkeypatch):
    store = RunStore(tmp_path / "runs")
    finished = store.save(_record(store, tmp_path, "finished", RunState.FAILED,
                                  "2025-01-01T10:00:00+00:00"))
    with monkeypatch.context() as patch:
        def interrupt(*args, **kwargs):
            raise OSError("interrupted reopening")
        patch.setattr(records_module, "durable_write_json", interrupt)
        with store.submission_lock(), pytest.raises(OSError, match="interrupted reopening"):
            store.update(finished, lambda current: replace(current, state=RunState.DOWNLOADING))
    peer = RunStore(store.directory)
    assert peer.get(finished.id) == finished
    assert peer.runs(active=True) == ()
    assert peer.runs(active=False) == (finished,)
    with peer.submission_lock():
        resumed = peer.update(finished, lambda current: replace(current, state=RunState.DOWNLOADING))
    assert store.runs(active=True) == (resumed,)
    assert len(_records(peer)) == 1
