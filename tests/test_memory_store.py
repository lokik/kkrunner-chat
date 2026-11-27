"""Memory and disk stores share identity rules, not persistence."""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Barrier

import pytest

from kkrunner import MemoryRunStore, RunManager, RunState, RunStore
from kkrunner.files import FileEntry, FileManifest
from kkrunner.runs import DuplicateSubmissionError, ForgottenRunError, RunRecord


@pytest.mark.parametrize("memory", [False, True])
def test_record_owner_and_serialization(tmp_path, memory):
    store = MemoryRunStore() if memory else RunStore(tmp_path / "runs")
    manager = RunManager(store)
    record = manager.new_record(directory=tmp_path)
    assert record.store is store
    saved = store.save(record)
    assert store.get(record.id).store is store
    assert store.runs()[0].store is store
    assert replace(saved, message="test").store is store
    assert "store" not in saved.to_dict()
    restored = RunRecord(**saved.to_dict())
    assert restored == saved and restored.store is None
    other = MemoryRunStore()
    assert other.save(restored).store is other
    with pytest.raises(ValueError):
        store.update(saved, lambda r: replace(r, metadata={"changed": True}))
    store.forget(saved, force=True)
    with pytest.raises(ForgottenRunError):
        store.update(saved, lambda r: r)


def test_atomic_conflicts_between_stores(tmp_path):
    disk, memory = RunStore(tmp_path / "runs"), MemoryRunStore()
    a = RunManager(disk, conflict_stores=[memory])
    b = RunManager(memory, conflict_stores=[disk])
    files = FileManifest(stdout=FileEntry(str(tmp_path / "out")))
    barrier = Barrier(2)

    def reserve(manager):
        record = manager.new_record(directory=tmp_path, files=files)
        barrier.wait(timeout=5)
        try:
            return manager.reserve(record)
        except DuplicateSubmissionError:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(reserve, (a, b)))
    assert sum(r is not None for r in results) == 1
    assert len(disk.runs(active=True)) + len(memory.runs(active=True)) == 1


def test_foreign_record_transition_uses_owner(tmp_path):
    disk, memory = RunStore(tmp_path / "runs"), MemoryRunStore()
    a = RunManager(disk, conflict_stores=[memory])
    b = RunManager(memory, conflict_stores=[disk])
    # Deliberately use the same id in both stores.
    disk_record = a.new_record(directory=tmp_path, run_id="same")
    memory_record = b.new_record(directory=tmp_path, run_id="same")
    disk.save(disk_record)
    memory.save(memory_record)
    updated = a.transition(memory_record, state=RunState.SUBMITTING)
    assert updated.store is memory
    assert disk.get("same").state == RunState.NEW
    assert memory.get("same").state == RunState.SUBMITTING


@pytest.mark.parametrize("first_memory", [False, True])
def test_conflict_recovery_refreshes_foreign_store(tmp_path, monkeypatch, first_memory):
    disk, memory = RunStore(tmp_path / "runs"), MemoryRunStore()
    a = RunManager(disk, conflict_stores=[memory], submitting_grace=0)
    b = RunManager(memory, conflict_stores=[disk], submitting_grace=0)
    first, second = (b, a) if first_memory else (a, b)
    files = FileManifest(stdout=FileEntry(str(tmp_path / "out")))
    old = first.reserve(first.new_record(directory=tmp_path, files=files))
    def recover(record):
        assert record.store is first.store
        return second.transition(record, state=RunState.FAILED)
    monkeypatch.setattr(second, "recover_submitting", recover)
    new = second.reserve(second.new_record(directory=tmp_path, files=files), recover_conflicts=True)
    assert new.store is second.store
    assert first.store.get(old.id).state == RunState.FAILED
    assert second.store.get(new.id).state == RunState.SUBMITTING

def test_inspect_and_forget_foreign_conflict(tmp_path):
    disk, memory = RunStore(tmp_path / "runs"), MemoryRunStore()
    a = RunManager(disk, conflict_stores=[memory])
    b = RunManager(memory, conflict_stores=[disk])
    files = FileManifest(stdout=FileEntry(str(tmp_path / "out")))
    active = b.reserve(b.new_record(directory=tmp_path, files=files))
    proposed = a.new_record(directory=tmp_path, files=files)
    assert a.conflicts(proposed) == (active,)
    a.forget(active, force=True)
    assert a.conflicts(proposed) == ()
    assert disk.runs() == ()

def test_session_record_json_roundtrip_with_file_version(tmp_path):
    import json
    from kkrunner.files.versions import FileVersion
    path = tmp_path / "input"
    path.write_text("test")
    store = MemoryRunStore()
    record = RunManager(store).new_record(
        directory=tmp_path, files=FileManifest(input_versions=(FileVersion.capture(path),)),
    )
    restored = RunRecord(**json.loads(json.dumps(record.to_dict())))
    assert restored == record
    assert restored.store is None

@pytest.mark.parametrize("detached", [False, True])
def test_run_exposes_actual_detached_policy(tmp_path, detached):
    from kkrunner import ExecutionSpec, Run
    from kkrunner.client import ExecutionClient
    client = ExecutionClient()
    spec = ExecutionSpec(["unused"], directory=tmp_path)
    run = client.create_run(spec, tracked=RunManager(MemoryRunStore()), detached=detached)
    assert run.detached is detached
    assert run.record.detached is detached
    assert RunRecord(**run.record.to_dict()).detached is detached
    run.close()


@pytest.fixture(params=["disk", "memory"])
def store(request, tmp_path):
    return RunStore(tmp_path / "store") if request.param == "disk" else MemoryRunStore()


@pytest.mark.parametrize("bad_id", ["", ".", "..", "a/b", "a\\b", 123, None])
def test_store_validates_ids(store, bad_id):
    with pytest.raises(ValueError):
        store.get(bad_id)


def test_store_contract(store, tmp_path):
    from kkrunner.runs import ActiveRunDeletionError
    manager = RunManager(store)
    first = store.save(manager.new_record(directory=tmp_path))
    second = manager.transition(manager.new_record(directory=tmp_path), state=RunState.FAILED)
    assert store.save(first) == first
    assert store.runs(active=True) == (first,)
    assert store.runs(active=False) == (second,)
    assert store.runs(newest=True, limit=1) == (second,)
    assert store.runs(limit=0) == ()
    with pytest.raises(ValueError):
        store.runs(limit=-1)
    with pytest.raises(ActiveRunDeletionError):
        store.forget(first)
    lock = store.observation_lock(first)
    assert lock.acquire(blocking=False)
    try:
        assert not store.observation_lock(first).acquire(blocking=False)
    finally:
        lock.release()
    store.forget(first, force=True)
    with pytest.raises(FileNotFoundError):
        store.get(first.id)


def test_conflicting_ids_have_distinct_file_details(tmp_path):
    a = RunManager(MemoryRunStore())
    b = RunManager(RunStore(tmp_path / "runs"))
    proposed = a.new_record(directory=tmp_path)
    one = a.new_record(directory=tmp_path, run_id="same")
    two = b.new_record(directory=tmp_path, run_id="same")
    error = DuplicateSubmissionError(proposed, (one, two))
    assert set(error.file_conflicts) == {one.key, two.key}


def test_file_operations_follow_record_owner(tmp_path):
    from kkrunner.files import FileVersion
    path = tmp_path / "input"
    path.write_text("test")
    disk, memory = RunStore(tmp_path / "runs"), MemoryRunStore()
    a, b = RunManager(disk), RunManager(memory)
    disk.save(a.new_record(directory=tmp_path, run_id="same"))
    record = b.new_record(directory=tmp_path, run_id="same", files=FileManifest(
        inputs=(FileEntry(str(path)),), input_versions=(FileVersion.capture(path),),
    ))
    record = b.transition(record, state=RunState.FAILED)
    assert a.input_file_is_current(record)
    assert a.retrieve_result_files(record).store is memory
    assert not a.input_file_is_current("same")



def test_history_retrieval_reads_current_bound_record_under_ownership(store, tmp_path, monkeypatch):
    manager = RunManager(store)
    record = manager.new_record(directory=tmp_path)
    stale = manager.transition(record, state=RunState.RUNNING,
                               execution=record.execution.submitted("native"))
    current = manager.transition(stale, state=RunState.FINISHED, return_code=0)
    original_get = store.get
    owner = store.observation_lock(stale)
    reads = []

    def get(run_id):
        assert not owner.acquire(blocking=False), "Read must be under observation ownership"
        reads.append(run_id)
        return original_get(run_id)

    monkeypatch.setattr(store, "get", get)
    assert manager.retrieve_result_files(stale) == current
    assert reads == [stale.id]


def test_foreign_postprocessing_retry_keeps_owner(tmp_path):
    from kkrunner.runs import FailurePhase
    from kkrunner.files.availability import capture_result_files
    disk, memory = RunStore(tmp_path / "runs"), MemoryRunStore()
    a, b = RunManager(disk), RunManager(memory)
    disk.save(a.new_record(directory=tmp_path, run_id="same"))
    out = tmp_path / "out"
    out.write_text("result")
    record = b.new_record(directory=tmp_path, run_id="same", processing_required=True,
                          files=FileManifest(stdout=FileEntry(str(out))))
    record = b.complete(record, state=RunState.FAILED, return_code=0,
                        failure_phase=FailurePhase.POSTPROCESSING,
                        files=capture_result_files(record.files))
    assert a.retry_postprocessing(record, lambda outcome: "parsed") == "parsed"
    assert memory.get(record.id).state == RunState.FINISHED
    assert disk.get(record.id).state == RunState.NEW


def test_foreign_attach_observes_and_completes_in_own_store(tmp_path):
    import sys
    from kkrunner import ExecutionSpec, Run
    from kkrunner.backends import DirectBackend
    from kkrunner.client import ExecutionClient

    disk, memory = RunStore(tmp_path / "runs"), MemoryRunStore()
    a, b = RunManager(disk), RunManager(memory)
    marker, gate = tmp_path / "done", tmp_path / "gate"
    spec = ExecutionSpec(
        (sys.executable, "-c",
         "import pathlib,time; gate=pathlib.Path(%r); deadline=time.monotonic()+8; "
         "exec('while not gate.exists() and time.monotonic()<deadline: time.sleep(.02)'); "
         "pathlib.Path(%r).write_text('done')" % (str(gate), str(marker))),
        directory=str(tmp_path), output_file=str(tmp_path / "out"),
    )
    disk.save(a.new_record(directory=tmp_path, run_id="same"))
    run = ExecutionClient(DirectBackend).create_run(spec, tracked=b,
                     detached=True, run_id="same")
    run.submit()
    try:
        run.stop_observation()
        run.wait()
        record = memory.get(run.id)
        assert record.state == RunState.UNKNOWN and record.detached
        attached = a.attach(record)
        lock = memory.observation_lock(record)
        assert not lock.acquire(blocking=False)
        with pytest.raises(RuntimeError, match="A live client owns this run"):
            b.attach(record)
        assert b.refresh((record,))[0] == memory.get(record.id)
        gate.touch()
        attached.wait()
        assert memory.get("same").state == RunState.FINISHED
        assert disk.get("same").state == RunState.NEW
        assert marker.exists()
        assert lock.acquire(blocking=False)
        lock.release()
    finally:
        gate.touch()



def test_attach_does_not_create_client_until_observation_is_claimed(store, tmp_path, monkeypatch):
    manager = RunManager(store)
    record = manager.new_record(directory=tmp_path)
    stale = manager.transition(record, state=RunState.RUNNING,
                               execution=record.execution.submitted("native"))
    monkeypatch.setattr(manager, "client_for",
                        lambda _: pytest.fail("No observer can be opened"))
    with store.observation_lock(stale):
        with pytest.raises(RuntimeError, match="A live client owns this run"):
            manager.attach(stale)

    manager.transition(stale, state=RunState.FINISHED, return_code=0)
    with pytest.raises(RuntimeError, match="already terminal"):
        manager.attach(stale)
    owner = store.observation_lock(stale)
    assert owner.acquire(blocking=False)
    owner.release()


@pytest.mark.parametrize("memory", [False, True])
def test_failed_direct_attach_releases_observation_claim(tmp_path, monkeypatch, memory):
    from kkrunner.backends import DirectBackend
    from kkrunner.client import ExecutionClient
    store = MemoryRunStore() if memory else RunStore(tmp_path / "runs")
    manager = RunManager(store)
    client = ExecutionClient(DirectBackend)
    record = manager.new_record(directory=tmp_path, target=client.target,
                                backend=client.backend_config, detached=True)
    record = manager.transition(record, state=RunState.RUNNING,
                                execution=record.execution.submitted("native"))
    def fail(*args, **kwargs):
        lock = store.observation_lock(record)
        assert not lock.acquire(blocking=False)
        raise OSError("cannot open streams")
    monkeypatch.setattr(client, "open_run_parts", fail)
    with pytest.raises(OSError, match="cannot open streams"):
        manager.attach(record, client=client)
    lock = store.observation_lock(record)
    assert lock.acquire(blocking=False)
    lock.release()


def test_failed_direct_observer_start_closes_streams_before_releasing_claim(tmp_path, monkeypatch):
    from kkrunner import ExecutionSpec
    from kkrunner.backends import DirectBackend
    from kkrunner.client import ExecutionClient

    store = MemoryRunStore()
    manager = RunManager(store)
    client = ExecutionClient(DirectBackend)
    record = manager.new_record(directory=tmp_path, target=client.target,
                                backend=client.backend_config, detached=True)
    record = manager.transition(record, state=RunState.RUNNING,
                                execution=record.execution.submitted("native"))
    parts = client.create_run_parts(ExecutionSpec(("unused",), directory=tmp_path))
    original_close = parts.files.close
    closed = []

    def fail():
        raise OSError("observer configuration failed")

    def close():
        assert not store.observation_lock(record).acquire(blocking=False)
        original_close()
        closed.append(True)

    monkeypatch.setattr(parts, "configure_observation", fail)
    monkeypatch.setattr(parts.files, "close", close)
    monkeypatch.setattr(client, "open_run_parts", lambda *args, **kwargs: parts)
    with pytest.raises(OSError, match="observer configuration failed"):
        manager.attach(record, client=client)
    assert closed == [True]
    lock = store.observation_lock(record)
    assert lock.acquire(blocking=False)
    lock.release()


def test_discarded_reservation_cannot_be_recreated_by_update(store, tmp_path):
    manager = RunManager(store)
    record = manager.new_record(directory=tmp_path)
    with store.observation_lock(record):
        reserved = manager.reserve(record, preprocessing=True)
        store.discard(reserved)
    with pytest.raises(FileNotFoundError):
        manager.transition(reserved, state=RunState.FAILED)
    assert store.runs() == ()


@pytest.mark.parametrize("field, message", [
    ("submission_key", "Submission key"),
    ("backend_id", "Backend identity"),
    ("target", "Execution target and backend configuration"),
    ("backend", "Execution target and backend configuration"),
])
def test_execution_identity_is_enforced_by_store(store, tmp_path, field, message):
    manager = RunManager(store)
    record = manager.new_record(directory=tmp_path, submission_key="submission")
    record = manager.transition(record, state=RunState.RUNNING,
                                execution=record.execution.submitted("job"))
    from kkrunner.backends.config import BackendConfig
    from kkrunner.targets import SshTarget

    value = {"target": SshTarget("another-host"), "backend": BackendConfig("kkrunner")}.get(
        field, "different",
    )
    invalid = replace(record.execution, **{field: value})
    with pytest.raises(ValueError, match=message):
        store.update(record, lambda current: replace(current, execution=invalid))
    with pytest.raises(ValueError, match=message):
        manager.transition(record, execution=invalid)
    assert store.get(record.id) == record


def test_unchanged_artifact_availability_keeps_history_timestamp(store, tmp_path):
    manager = RunManager(store)
    output = str(tmp_path / "output")
    record = store.save(manager.new_record(
        directory=tmp_path,
        files=FileManifest(stdout=FileEntry(output, availability_state="pending")),
    ))
    failed = manager.update_artifact_availability(record, output, "failed", "missing")
    assert failed.files.stdout.availability_state == "failed"
    assert manager.update_artifact_availability(failed, output, "failed", "missing") == failed
    assert store.get(record.id) == failed
    restored = manager.update_artifact_availability(failed, output, "available")
    assert restored.files.stdout.availability_state == "available"
    assert restored.files.stdout.availability_message is None
    assert store.get(record.id) == restored


def test_first_unchanged_update_creates_record_in_either_store(store, tmp_path):
    record = RunManager(store).new_record(directory=tmp_path)
    assert store.update(record, lambda current: current) == record
    assert store.get(record.id) == record
    assert store.update(record, lambda current: current) == record
