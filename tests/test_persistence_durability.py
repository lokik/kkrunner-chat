import os
import stat
from pathlib import Path

import pytest

import kkrunner.runs.files as run_files_module
import kkrunner.runs.records as records_module
import kkrunner.support.durability as durability
from kkrunner.client import LocalTransport
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


def test_reserve_syncs_inputs_before_submitting_record(tmp_path, monkeypatch):
    input_file = tmp_path / "input.inp"
    input_file.write_text("input")
    store = RunStore(tmp_path / "runs")
    manager = RunManager(store)
    record = manager.new_record(
        directory=tmp_path,
        transport=LocalTransport(),
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
    assert events[:2] == [("sync", "input.inp"), ("state", "submitting")]


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
