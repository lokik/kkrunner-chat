"""Behavioral contracts introduced by the extraction, without application imports."""

from kkrunner.client import EndpointClient, StagingSpec
import json
import subprocess
import sys
from dataclasses import replace
from pathlib import Path
from threading import Event, Thread

import pytest

from kkrunner.runs import CorruptRunRecord
from kkrunner import ExecutionOutcome, ExecutionSpec, RunState, RunStore
from kkrunner.files import FileEntry, FileManifest
from kkrunner.backends import SupervisedBackend
from kkrunner.runs.manager import RunManager
from kkrunner.runs.processing import ProcessingPending


def pending(manager, directory):
    output = directory / "stdout.txt"
    output.write_text("answer")
    record = manager.new_record(
        directory=directory,
        processing_required=True,
        files=FileManifest(stdout=FileEntry(local_path=str(output))),
        metadata={"example": {"label": "answer", "tags": ["one"]}},
    )
    return manager.complete(
        record,
        state=RunState.POSTPROCESSING,
        return_code=0,
        execution=record.execution.submitted("test-backend"),
        artifacts_available=True,
    )


def test_opaque_metadata_is_json_and_recursively_immutable(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = pending(manager, tmp_path)
    with pytest.raises(TypeError):
        record.metadata["example"]["label"] = "changed"
    with pytest.raises(ValueError, match="identity or metadata"):
        manager.store.update(record, lambda current: replace(current, metadata={"other": {}}))
    assert manager.store.get(record.id) == record
    with pytest.raises(ValueError, match="Backend identity"):
        manager.store.update(
            record,
            lambda current: replace(
                current,
                execution=replace(current.execution, backend_id="different"),
            ),
        )
    with pytest.raises(TypeError):
        manager.new_record(directory=tmp_path, metadata={"example": object()})
    with pytest.raises(ValueError):
        manager.new_record(directory=tmp_path, metadata={"example": float("nan")})


def test_corrupt_record_is_not_hidden_during_enumeration(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = pending(manager, tmp_path)
    path = manager.store._path(record)
    data = json.loads(path.read_text())
    data["schema_version"] = 99
    path.write_text(json.dumps(data))
    with pytest.raises(CorruptRunRecord):
        manager.store.runs()


def test_processing_claim_excludes_other_clients(tmp_path):
    store = RunStore(tmp_path / "runs")
    manager = RunManager(store)
    record = pending(manager, tmp_path)
    entered, release = Event(), Event()
    results = []

    def callback(outcome):
        entered.set()
        assert release.wait(5)
        return 42

    thread = Thread(target=lambda: results.append(manager.retry_postprocessing(record.id, callback)))
    thread.start()
    assert entered.wait(5)
    try:
        other = RunManager(RunStore(store.directory))
        with pytest.raises(ProcessingPending):
            other.retry_postprocessing(record.id, lambda outcome: pytest.fail("duplicate callback"))
    finally:
        release.set()
        thread.join(5)
    assert results == [42]
    assert store.get(record.id).state == RunState.FINISHED


def test_process_death_releases_processing_claim(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = pending(manager, tmp_path)
    script = """
import os, sys
from kkrunner.runs.manager import RunManager
from kkrunner.runs.records import RunStore
manager = RunManager(RunStore(sys.argv[1]))
manager.retry_postprocessing(sys.argv[2], lambda outcome: os._exit(17))
"""
    child = subprocess.run([sys.executable, "-c", script, str(manager.store.directory), record.id], timeout=15)
    assert child.returncode == 17
    interrupted = manager.store.get(record.id)
    assert interrupted.state == RunState.POSTPROCESSING
    attempt = interrupted.processing_attempt
    assert manager.retry_postprocessing(record.id, lambda outcome: Path(outcome.output_file).read_text()) == "answer"
    completed = manager.store.get(record.id)
    assert completed.state == RunState.FINISHED
    assert completed.processing_attempt != attempt


def test_modified_artifacts_are_not_reprocessed(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = pending(manager, tmp_path)
    Path(record.files.output_file).write_text("different")
    with pytest.raises(ValueError, match="unchanged historical artifacts"):
        manager.retry_postprocessing(record.id, lambda outcome: pytest.fail("unverified callback"))
    assert manager.store.get(record.id).state == RunState.POSTPROCESSING


def test_managed_execution_needs_no_result_processor(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    manager = RunManager(RunStore(tmp_path / "runs"))
    run = manager.create_run(
        EndpointClient(SupervisedBackend),
        ExecutionSpec(
            [sys.executable, "-c", "print(42)"],
            directory=tmp_path,
            output_file=tmp_path / "stdout.txt",
        ),
    )
    assert isinstance(run.run(), ExecutionOutcome)
    record = manager.store.get(run.id)
    assert record.state == RunState.FINISHED
    assert record.files.captured_at is not None


def test_required_processing_can_be_supplied_after_restart(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    manager = RunManager(RunStore(tmp_path / "runs"))
    run = manager.create_run(
        EndpointClient(SupervisedBackend),
        ExecutionSpec(
            [sys.executable, "-c", "print(42)"],
            directory=tmp_path,
            output_file=tmp_path / "stdout.txt",
        ),
        processing_required=True,
    )
    with pytest.raises(ProcessingPending):
        run.run()
    assert manager.store.get(run.id).state == RunState.POSTPROCESSING
    other = RunManager(RunStore(manager.store.directory))
    attached = other.attach(run.id, process_result=lambda outcome: int(Path(outcome.output_file).read_text()))
    assert attached.wait() == 42
    assert other.store.get(run.id).state == RunState.FINISHED


def test_local_observation_detaches_without_stopping_backend(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    manager = RunManager(RunStore(tmp_path / "runs"))
    run = manager.create_run(
        EndpointClient(SupervisedBackend),
        ExecutionSpec(
            [sys.executable, "-c", "import time; time.sleep(.4); print(42)"],
            directory=tmp_path,
            output_file=tmp_path / "stdout.txt",
        ),
    )
    run.submit()
    identity = run.backend_id
    run.stop_observation()
    run.wait()
    assert manager.store.get(run.id).state == RunState.UNKNOWN
    other = RunManager(RunStore(manager.store.directory))
    attached = other.attach(run.id)
    assert attached.wait().return_code == 0
    assert attached.backend_id == identity
    assert other.store.get(run.id).state == RunState.FINISHED


def test_refresh_captures_artifacts_without_result_processor(tmp_path):
    from kkrunner.runs import RunStatus

    manager = RunManager(RunStore(tmp_path / "runs"))
    output = tmp_path / "stdout.txt"
    output.write_text("42")
    record = manager.new_record(directory=tmp_path, files=FileManifest(stdout=FileEntry(local_path=str(output))))
    manager.store.save(record)
    completed = manager.update(record, RunStatus(RunState.FINISHED, return_code=0))
    assert completed.state == RunState.FINISHED
    assert completed.files.captured_at is not None
    output.write_text("changed")
    assert not manager.output_file_is_current(completed)


@pytest.mark.parametrize(
    "field,value",
    [
        ("execution", "not an execution"),
        ("files", None),
        ("created_at", "yesterday"),
        ("id", "../another-record"),
        ("return_code", "0"),
        ("message", {}),
        ("processing_attempt", 17),
    ],
)
def test_current_schema_rejects_invalid_field_types(tmp_path, field, value):
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = pending(manager, tmp_path)
    path = manager.store._path(record)
    data = json.loads(path.read_text())
    assert data["schema_version"] == 1
    data[field] = value
    path.write_text(json.dumps(data))
    with pytest.raises(CorruptRunRecord) as error:
        manager.store.get(record.id)
    assert str(path) in str(error.value)


def test_failed_download_can_reattach_to_preserved_backend_receipt(tmp_path, monkeypatch):
    from kkrunner.runs import FailurePhase
    from kkrunner.backends import SupervisedBackend
    from kkrunner.client import SshTransport

    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    remote.mkdir()

    # Use a reconstructable, supported SSH transport even though the test
    # executes its remote shell locally. Custom transport subclasses cannot be
    # reattached from durable RunRecords.
    ssh_stub = tmp_path / "ssh-loopback"
    ssh_stub.write_text('#!/bin/sh\nshift\nexec /bin/sh -c "$1"\n')
    ssh_stub.chmod(0o755)
    transport = SshTransport(host="loopback", ssh=str(ssh_stub), python_command=(sys.executable,))
    original_get_file = SshTransport.get_file
    fail_download = True

    async def get_file(self, source, destination):
        if fail_download and str(source).endswith("answer.txt"):
            raise OSError("interrupted download")
        return await original_get_file(self, source, destination)

    monkeypatch.setattr(SshTransport, "get_file", get_file)
    executor = EndpointClient(SupervisedBackend, transport=transport)
    manager = RunManager(RunStore(tmp_path / "runs"))
    run = manager.create_run(
        executor,
        ExecutionSpec(
            [sys.executable, "-c", "from pathlib import Path; Path('answer.txt').write_text('42'); print('done')"],
            directory=local,
            output_file=local / "stdout",
            output_files=(local / "answer.txt",),
            staging=StagingSpec(local_root=local, remote_root=remote),
        ),
    )
    with pytest.raises(OSError, match="interrupted download"):
        run.run()
    failed = manager.store.get(run.id)
    assert failed.failure_phase == FailurePhase.DOWNLOAD
    assert failed.return_code == 0
    fail_download = False
    manager.resume_download(failed)
    attached = manager.attach(run.id, client=executor)
    assert attached.wait().return_code == 0
    assert manager.store.get(run.id).state == RunState.FINISHED
    assert (local / "answer.txt").read_text() == "42"
