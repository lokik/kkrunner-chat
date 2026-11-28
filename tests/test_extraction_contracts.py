"""Independent self-review probes; assertions express the intended contract."""

import json
from dataclasses import replace
from threading import Event, Thread

import pytest

from kkrunner.runs import CorruptRunRecord
from kkrunner import ExecutionSpec, RunState, RunStore
from kkrunner.backends import DirectBackend, SupervisedBackend
from kkrunner.backends.config import BackendConfig
from kkrunner.client import ExecutionClient, LocalTarget, SshTarget, StagingSpec
from kkrunner.client.backend_execution import DirectExecution
from kkrunner.files import FileEntry, FileManifest
from kkrunner.runs.conflicts import submission_conflicts
from kkrunner.runs.manager import RunManager


def manager_at(tmp_path):
    return RunManager(RunStore(tmp_path / "runs"))


def test_save_cannot_replace_immutable_metadata(tmp_path):
    manager = manager_at(tmp_path)
    record = manager.new_record(directory=tmp_path, metadata={"owner": {"id": 1}})
    manager.store.save(record)
    with pytest.raises(ValueError):
        manager.store.save(replace(record, metadata={"owner": {"id": 2}}))
    assert manager.store.get(record.id).metadata == record.metadata


@pytest.mark.parametrize("data", [[], None, 42])
def test_invalid_json_root_has_domain_error(tmp_path, data):
    manager = manager_at(tmp_path)
    record = manager.new_record(directory=tmp_path)
    manager.store.save(record)
    manager.store._path(record).write_text(json.dumps(data))
    with pytest.raises(CorruptRunRecord):
        manager.store.get(record.id)


def test_nested_execution_is_validated(tmp_path):
    manager = manager_at(tmp_path)
    record = manager.new_record(directory=tmp_path)
    manager.store.save(record)
    path = manager.store._path(record)
    data = json.loads(path.read_text())
    data["execution"]["transport"] = []
    path.write_text(json.dumps(data))
    with pytest.raises(CorruptRunRecord):
        manager.store.get(record.id)


def test_fast_execution_does_not_postprocess_until_explicit_wait(tmp_path):
    import sys

    manager = manager_at(tmp_path)
    processed = []
    run = ExecutionClient(SupervisedBackend).create_run(
        ExecutionSpec([sys.executable, "-c", "pass"], directory=tmp_path),
        tracked=manager, process_result=lambda outcome: processed.append(outcome) or 42,
    )
    run.submit()
    assert run.backend_id is not None
    assert processed == []
    assert not manager.store.get(run.id).state.is_terminal
    assert run.wait() == 42
    assert run.wait() == 42
    assert len(processed) == 1
    assert manager.store.get(run.id).state == RunState.FINISHED


def test_cancel_and_failed_submit_do_not_deadlock():
    from .test_cancellation import _controlled_job

    job, execution = _controlled_job(fail_submit=True)
    errors, finished = [], Event()

    def submit():
        try:
            job.run()
        except BaseException as exc:
            errors.append(exc)
        finally:
            finished.set()

    submitting = Thread(target=submit, daemon=True)
    submitting.start()
    try:
        assert execution.entered.wait(3)
        future = job.cancel()
    finally:
        execution.release.set()
    assert finished.wait(3), "submission deadlocked with cancellation"
    submitting.join(3)
    assert not submitting.is_alive()
    assert len(errors) == 1
    assert isinstance(errors[0], RuntimeError)
    assert str(errors[0]) == "submission rejected"
    with pytest.raises(RuntimeError, match="before cancellation was dispatched"):
        future.result(timeout=3)
    assert execution.cancel_calls == 0


@pytest.mark.parametrize(
    "transport, backend",
    [
        (LocalTarget(), BackendConfig("psij")),
        (SshTarget("other-host"), BackendConfig("direct")),
    ],
)
def test_local_publication_conflict_is_independent_of_backend(tmp_path, transport, backend):
    manager = manager_at(tmp_path)
    files = FileManifest(stdout=FileEntry(local_path=str(tmp_path / "same.out")))
    first = manager.new_record(directory=tmp_path, files=files)
    second = manager.new_record(
        directory=tmp_path,
        files=files,
        target=transport,
        backend=backend,
        remote_directory="/remote" if not transport.is_local else None,
    )
    assert submission_conflicts(first, second)


def test_stdin_is_part_of_read_manifest(tmp_path):
    manager = manager_at(tmp_path)
    spec = ExecutionSpec(["reader"], stdin=tmp_path / "data", directory=tmp_path)
    reader = manager.new_record(directory=tmp_path, files=FileManifest.from_spec(spec))
    writer = manager.new_record(
        directory=tmp_path, files=FileManifest(outputs=(FileEntry(local_path=str(tmp_path / "data")),))
    )
    assert submission_conflicts(reader, writer)


def test_remote_preflight_streams_close_on_submit_failure(tmp_path, monkeypatch):
    from .test_transport import _LoopbackSshTransport

    local = tmp_path / "local"
    local.mkdir()
    output = (local / "stdout").open("wb")
    spec = ExecutionSpec(
        ["unused"], directory=local, output_file=output,
        staging=StagingSpec(local_root=local, remote_root=tmp_path / "remote"),
    )
    run = ExecutionClient(DirectBackend, target=_LoopbackSshTransport()).create_run(spec, tracked=False)

    async def rejected(self, request, on_output, *, output_requested=None):
        raise RuntimeError("lost submit response")

    monkeypatch.setattr(DirectExecution, "submit", rejected)
    with pytest.raises(RuntimeError, match="lost submit response"):
        run.run()
    assert output.closed


def test_reused_run_id_is_rejected_before_second_backend_submit(tmp_path, monkeypatch):
    import sys
    from kkrunner.client.backend_execution import ManagedExecution

    manager = manager_at(tmp_path)
    client = ExecutionClient(SupervisedBackend)
    submissions = []
    original = ManagedExecution.submit

    async def submit(self, *args, **kwargs):
        submissions.append(self)
        return await original(self, *args, **kwargs)

    monkeypatch.setattr(ManagedExecution, "submit", submit)
    spec = ExecutionSpec(
        [sys.executable, "-c", "print('written')"], directory=tmp_path,
        output_file=tmp_path / "same-output",
    )
    first = client.create_run(spec, tracked=manager, run_id="same-id")
    second = client.create_run(spec, tracked=manager, run_id="same-id")
    first.submit()
    try:
        with pytest.raises((RuntimeError, ValueError)):
            second.submit()
        assert len(submissions) == 1
    finally:
        assert first.wait().return_code == 0
