import os
import stat
from pathlib import Path

import pytest

import kkrunner.runs.files as run_files_module
import kkrunner.runs.records as records_module
import kkrunner.support.durability as durability
from kkrunner.client import LocalTarget
from kkrunner.files.manifest import FileEntry, FileManifest
from kkrunner.runs.base import RunState
from kkrunner.runs.manager import RunManager
from kkrunner.runs.records import RunStore

pytestmark = pytest.mark.skipif(os.name != "posix", reason="strong directory fsync semantics are currently POSIX-only")


def test_durable_write_orders_file_fsync_before_replace_and_directory_fsync(tmp_path, monkeypatch):
    path = tmp_path / "state.json"
    from threading import get_ident

    owner_thread = get_ident()
    events = []
    real_fsync = durability.os.fsync
    real_replace = durability.os.replace

    def tracked_fsync(descriptor):
        kind = "directory" if stat.S_ISDIR(os.fstat(descriptor).st_mode) else "file"
        if get_ident() == owner_thread:
            events.append(("fsync", kind))
        return real_fsync(descriptor)

    def tracked_replace(source, destination):
        if get_ident() == owner_thread:
            events.append(("replace", Path(destination).name))
        return real_replace(source, destination)

    monkeypatch.setattr(durability.os, "fsync", tracked_fsync)
    monkeypatch.setattr(durability.os, "replace", tracked_replace)
    durability.durable_write_json(path, {"state": "running"})
    assert events == [("fsync", "file"), ("replace", "state.json"), ("fsync", "directory")]


def test_durable_write_propagates_directory_barrier_failure(tmp_path, monkeypatch):
    path = tmp_path / "state.json"
    path.write_text("old")

    def fail_directory_fsync(_directory):
        raise OSError("directory fsync failed")

    monkeypatch.setattr(durability, "fsync_directory", fail_directory_fsync)
    with pytest.raises(OSError, match="directory fsync failed"):
        durability.durable_write_text(path, "new")
    assert path.read_text() == "new"
    assert not tuple(tmp_path.glob(".*.tmp"))


@pytest.mark.parametrize("already_exists", (False, True))
def test_durable_mkdir_syncs_parent_directory_entries(tmp_path, monkeypatch, already_exists):
    directory = tmp_path / "one"
    if already_exists:
        directory.mkdir()
        target = directory
        expected = [tmp_path]
    else:
        target = directory / "two"
        expected = [tmp_path, directory]
    synced = []
    monkeypatch.setattr(durability, "fsync_directory", lambda parent: synced.append(Path(parent)))
    durability.durable_mkdir(target, parents=True, exist_ok=already_exists)
    assert synced == expected


def test_run_store_uses_durable_json_writer(tmp_path, monkeypatch):
    calls = []
    real_write = records_module.durable_write_json

    def tracked_write(path, data, **options):
        calls.append((Path(path), options))
        return real_write(path, data, **options)

    monkeypatch.setattr(records_module, "durable_write_json", tracked_write)
    store = RunStore(tmp_path / "runs")
    manager = RunManager(store)
    record = manager.new_record(directory=tmp_path, metadata={"test": {"task": "SCF"}}, processing_required=True)
    store.save(record)
    assert len(calls) == 1
    assert calls[0][0].name == f"{record.id}.json"
    assert calls[0][1]["mode"] == 384


def test_reservation_does_not_sync_application_inputs(tmp_path, monkeypatch):
    input_file = tmp_path / "input.inp"
    input_file.write_text("input")
    store = RunStore(tmp_path / "runs")
    manager = RunManager(store)
    record = manager.new_record(
        directory=tmp_path,
        target=LocalTarget(),
        files=FileManifest(inputs=(FileEntry(local_path=str(input_file)),)),
        metadata={"test": {"task": "SCF"}},
        processing_required=True,
    )
    events = []
    monkeypatch.setattr(run_files_module, "sync_file", lambda path: events.append(("sync", Path(path).name)))
    real_transition = manager.transition

    def tracked_transition(current, **changes):
        if changes.get("state") == RunState.SUBMITTING:
            events.append(("state", "submitting"))
        return real_transition(current, **changes)

    monkeypatch.setattr(manager, "transition", tracked_transition)
    reserved = manager.reserve(record)
    assert reserved.state == RunState.SUBMITTING
    assert events == [("state", "submitting")]


def test_artifact_publication_syncs_outputs_before_terminal_record(tmp_path, monkeypatch):
    output_file = tmp_path / "output.out"
    output_file.write_text("result")
    store = RunStore(tmp_path / "runs")
    manager = RunManager(store)
    record = manager.new_record(
        directory=tmp_path,
        files=FileManifest(stdout=FileEntry(local_path=str(output_file))),
        metadata={"test": {"task": "SCF"}},
        processing_required=True,
    )
    store.save(record)
    record = manager.transition(record, state=RunState.RUNNING)
    events = []
    monkeypatch.setattr(run_files_module, "sync_file", lambda path: events.append(("sync", Path(path).name)))
    real_transition = manager.transition

    def tracked_transition(current, **changes):
        if changes.get("state") == RunState.FINISHED:
            events.append(("state", "finished"))
        return real_transition(current, **changes)

    monkeypatch.setattr(manager, "transition", tracked_transition)
    finalized = manager.complete(record, state=RunState.FINISHED, return_code=0, artifacts_available=True)
    assert finalized.state == RunState.FINISHED
    assert events[:2] == [("sync", "output.out"), ("state", "finished")]
    assert len(finalized.files.output_versions) == 1


def test_best_effort_directory_fsync_still_writes_complete_file(tmp_path, monkeypatch):
    path = tmp_path / "state.json"
    path.write_text("old")

    def fail_directory_fsync(_directory):
        raise OSError("directory fsync unsupported")

    monkeypatch.setattr(durability, "fsync_directory", fail_directory_fsync)
    durability.durable_write_json(
        path, {"new": True}, mode=0o600, directory_fsync="best_effort"
    )
    assert path.read_text() == '{"new": true}'
    assert not tuple(tmp_path.glob(".*.tmp"))


def test_best_effort_still_propagates_file_fsync_errors(tmp_path, monkeypatch):
    path = tmp_path / "state.json"
    path.write_text("old")

    def fail_file_fsync(_descriptor):
        raise OSError("file fsync failed")

    monkeypatch.setattr(durability.os, "fsync", fail_file_fsync)
    with pytest.raises(OSError, match="file fsync failed"):
        durability.durable_write_text(path, "new", directory_fsync="best_effort")
    assert path.read_text() == "old"
    assert not tuple(tmp_path.glob(".*.tmp"))


def test_best_effort_still_propagates_replace_errors(tmp_path, monkeypatch):
    path = tmp_path / "state.json"
    path.write_text("old")

    def fail_replace(_source, _destination):
        raise OSError("replace failed")

    monkeypatch.setattr(durability.os, "replace", fail_replace)
    with pytest.raises(OSError, match="replace failed"):
        durability.durable_write_text(path, "new", directory_fsync="best_effort")
    assert path.read_text() == "old"
    assert not tuple(tmp_path.glob(".*.tmp"))


def test_invalid_directory_fsync_rejected_without_touching_destination(tmp_path):
    path = tmp_path / "state.json"
    path.write_text("old")
    with pytest.raises(ValueError, match="directory_fsync"):
        durability.durable_write_text(path, "new", directory_fsync="invalid")
    assert path.read_text() == "old"
    assert not tuple(tmp_path.glob(".*.tmp"))


def test_best_effort_replace_ignores_failures_for_both_parent_directories(tmp_path, monkeypatch):
    source_dir = tmp_path / "source"
    destination_dir = tmp_path / "dest"
    source_dir.mkdir()
    destination_dir.mkdir()
    source = source_dir / "file"
    destination = destination_dir / "file"
    source.write_text("new")
    visited = []

    def fail_directory_fsync(directory):
        visited.append(Path(directory))
        raise OSError("directory sync failed")

    monkeypatch.setattr(durability, "fsync_directory", fail_directory_fsync)
    durability.durable_replace(source, destination, directory_fsync="best_effort")
    assert destination.read_text() == "new"
    assert not source.exists()
    assert visited == [destination_dir, source_dir]


def test_private_mode_remains_enforced_with_permissive_umask(tmp_path):
    path = tmp_path / "private.json"
    previous_umask = os.umask(0)
    try:
        durability.durable_write_json(path, {"private": True}, mode=0o600)
    finally:
        os.umask(previous_umask)
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
