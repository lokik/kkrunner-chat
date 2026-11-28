"""Client preprocessing owns its reservation before generating input files."""

import sys
from dataclasses import replace

import pytest

from kkrunner import RunFileState
from kkrunner import ExecutionSpec, RunManager, RunState, RunStore, TaskFile
from kkrunner.client import ExecutionClient
from kkrunner.runs import DuplicateSubmissionError
from kkrunner.runs.base import FailurePhase
from kkrunner.files import FileManifest


def test_preprocessing_reserves_generated_input_before_writing(tmp_path):
    store = tmp_path / "runs"
    manager = RunManager(RunStore(store), submitting_grace=0)
    other = RunManager(RunStore(store), submitting_grace=0)
    input_file = tmp_path / "generated.inp"
    client = ExecutionClient()
    spec = ExecutionSpec(
        (sys.executable, "-c", "from pathlib import Path; print(Path('generated.inp').read_text())"),
        directory=tmp_path, input_file=TaskFile(input_file, write=True), output_file="out",
    )
    competitor_spec = replace(spec, output_file="other.out", input_file=input_file)
    competitor = client.create_run(competitor_spec, tracked=other)

    def preprocess():
        assert run.file_state == RunFileState.PREPROCESSING
        assert run.record.file_state == RunFileState.PREPROCESSING
        assert other.refresh((run.record,))[0].file_state == RunFileState.PREPROCESSING
        assert not input_file.exists()
        with pytest.raises(DuplicateSubmissionError):
            competitor.run()
        input_file.write_text("generated")

    run = client.create_run(spec, tracked=manager, preprocess=preprocess)
    assert not input_file.exists()
    run.run()
    assert run.state == RunState.FINISHED
    assert (tmp_path / "out").read_text().strip() == "generated"
    assert manager.store.get(run.id).files.inputs[0].write


def test_preprocessing_failure_never_submits_and_releases_reservation(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    client = ExecutionClient()
    spec = ExecutionSpec(
        (sys.executable, "-c", "pass"), directory=tmp_path, output_file="out",
        input_files=(TaskFile("generated.inp", write=True),),
    )

    def preprocess():
        raise ValueError("invalid input")

    run = client.create_run(spec, tracked=manager, preprocess=preprocess)
    with pytest.raises(ValueError, match="invalid input"):
        run.run()
    assert run.backend_id is None
    assert run.state == RunState.NEW
    record = manager.store.get(run.id)
    assert record.file_state == RunFileState.FAILED
    assert record.file_failure_phase == FailurePhase.PREPROCESSING
    assert record.files.captured_at is None
    assert record.files.stdout.availability_state == "skipped"
    assert not (tmp_path / "out").exists()
    replacement = client.create_run(replace(spec, input_files=()), tracked=manager)
    replacement.run()
    assert replacement.state == RunState.FINISHED


def test_orphan_preprocessing_is_recovered_without_backend_query(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = manager.new_record(directory=tmp_path)
    with manager.store.observation_lock(record):
        reserved = manager.reserve(record, preprocessing=True)
        assert manager.refresh((reserved,))[0].file_state == RunFileState.PREPROCESSING
    recovered = manager.refresh((reserved,))[0]
    assert recovered.state == RunState.NEW
    assert recovered.file_state == RunFileState.FAILED
    assert recovered.file_failure_phase == FailurePhase.PREPROCESSING
    assert recovered.execution.backend_id is None


def test_memory_preprocessing_conflicts_with_disk_without_disk_history(tmp_path):
    from kkrunner import MemoryRunStore

    disk = RunStore(tmp_path / "runs")
    memory = MemoryRunStore()
    ephemeral = RunManager(memory, conflict_stores=(disk,))
    disk_manager = RunManager(disk, conflict_stores=(memory,))
    spec = ExecutionSpec((sys.executable, "-c", "pass"), directory=tmp_path,
                         output_file="out", input_files=(TaskFile("input", write=True),))
    client = ExecutionClient()
    competitor = client.create_run(replace(spec, input_files=()), tracked=disk_manager)

    def preprocess():
        assert run.record.store is memory
        assert run.file_state == RunFileState.PREPROCESSING
        assert not disk.runs()
        with pytest.raises(DuplicateSubmissionError):
            competitor.run()

    run = client.create_run(spec, tracked=ephemeral, preprocess=preprocess)
    run.run()
    assert run.state == RunState.FINISHED
    assert not disk.runs()
    assert memory.get(run.id).state == RunState.FINISHED


@pytest.mark.parametrize("memory_manager", [False, True])
def test_save_only_reservation_is_memory_only(tmp_path, memory_manager):
    from kkrunner import MemoryRunStore

    disk = RunStore(tmp_path / "runs")
    memory = MemoryRunStore()
    save = RunManager(memory, conflict_stores=(disk,)) if memory_manager else RunManager(disk)
    other = RunManager(RunStore(tmp_path / "independent"))
    spec = ExecutionSpec(argv=(), directory=tmp_path, input_files=(TaskFile("input", write=True),))
    reader = other.new_record(directory=tmp_path, files=FileManifest.from_spec(
        ExecutionSpec(argv=(), directory=tmp_path, input_file="input")))
    with save.preprocessing_reservation(spec):
        assert not memory.runs()
        assert not disk.runs()
        with pytest.raises(DuplicateSubmissionError):
            other.reserve(reader, recover_conflicts=True)
        with pytest.raises(DuplicateSubmissionError):
            with other.preprocessing_reservation(spec):
                pytest.fail("Conflicting save-only operation entered")
        (tmp_path / "input").write_text("prepared")
    assert not other.conflicts(reader)
    assert not memory.runs()
    assert not disk.runs()
    with pytest.raises(ValueError, match="bad input"):
        with save.preprocessing_reservation(spec):
            raise ValueError("bad input")
    assert not other.conflicts(reader)
    other.reserve(reader)


def test_staging_of_preprocessed_run_does_not_appear_as_native_submission(tmp_path, monkeypatch):
    manager = RunManager(RunStore(tmp_path / "runs"), submitting_grace=0)
    other = RunManager(RunStore(tmp_path / "runs"), submitting_grace=0)
    run = ExecutionClient().create_run(ExecutionSpec(
        (sys.executable, "-c", "pass"), directory=tmp_path, input_files=(TaskFile("input", write=True),),
    ), tracked=manager, preprocess=lambda: (tmp_path / "input").write_text("prepared"))
    prepare = run._file_session.prepare_submission_async

    async def observe():
        assert run.record.file_state == RunFileState.PREPROCESSING
        assert other.refresh((run.record,))[0].file_state == RunFileState.PREPROCESSING
        return await prepare()

    monkeypatch.setattr(run._file_session, "prepare_submission_async", observe)
    run.run()
    assert run.state == RunState.FINISHED


def test_save_only_refresh_tolerates_completed_reservation(tmp_path):
    from kkrunner import MemoryRunStore
    manager = RunManager(MemoryRunStore())
    spec = ExecutionSpec(argv=(), directory=tmp_path, input_files=(TaskFile("input", write=True),))
    proposed = manager.new_record(directory=tmp_path, files=FileManifest.from_spec(spec))
    with manager.preprocessing_reservation(spec):
        active = manager.conflicts(proposed)
        assert manager.refresh(active)[0].file_state == RunFileState.PREPROCESSING
        assert manager.refresh() == ()
    # The scoped reservation has been discarded, not moved to history.
    assert manager.refresh(active) == ()


@pytest.mark.parametrize("memory", [False, True])
def test_live_preprocessing_cannot_be_manually_overridden(tmp_path, memory):
    from kkrunner import MemoryRunStore
    store = MemoryRunStore() if memory else RunStore(tmp_path / "runs")
    manager = RunManager(store)
    spec = ExecutionSpec(argv=(), directory=tmp_path, input_files=(TaskFile("input", write=True),))
    record = manager.new_record(directory=tmp_path, files=FileManifest.from_spec(spec))
    proposed = manager.new_record(directory=tmp_path, files=record.files)
    with store.observation_lock(record):
        reserved = manager.reserve(record, preprocessing=True)
        with pytest.raises(RuntimeError, match="live client"):
            manager.mark_failed(reserved)
        with pytest.raises(RuntimeError, match="live client"):
            manager.forget(reserved, force=True)
        assert manager.conflicts(proposed) == (reserved,)
        with pytest.raises(DuplicateSubmissionError):
            manager.reserve(proposed)
        with pytest.raises(RuntimeError, match="live client"):
            manager.complete(reserved, state=RunState.FINISHED)
        assert manager.conflicts(proposed) == (reserved,)
    manager.complete(reserved, state=RunState.FINISHED)
    assert not manager.conflicts(proposed)
    manager.reserve(proposed)


def test_scoped_save_does_not_accumulate_history_or_locks(tmp_path):
    from kkrunner import MemoryRunStore
    store = MemoryRunStore()
    manager = RunManager(store)
    spec = ExecutionSpec(argv=(), directory=tmp_path, input_files=(TaskFile("input", write=True),))
    proposed = manager.new_record(directory=tmp_path, files=FileManifest.from_spec(spec))
    for _ in range(10):
        with manager.preprocessing_reservation(spec):
            assert not store.runs()
            (active,) = manager.conflicts(proposed)
        assert not manager.conflicts(proposed)
        with pytest.raises(FileNotFoundError):
            active.store.get(active.id)
    assert not store.runs()


def test_staging_failure_is_preprocessing_even_without_callback(tmp_path, monkeypatch):
    manager = RunManager(RunStore(tmp_path / "runs"))
    run = ExecutionClient().create_run(ExecutionSpec(
        (sys.executable, "-c", "pass"), directory=tmp_path,
    ), tracked=manager)
    async def reject():
        assert run.record.file_state == RunFileState.PREPROCESSING
        raise OSError("staging failed")
    monkeypatch.setattr(run._file_session, "prepare_submission_async", reject)
    with pytest.raises(OSError, match="staging failed"):
        run.run()
    assert run.record.file_failure_phase == FailurePhase.PREPROCESSING


@pytest.mark.parametrize("outcome", ["success", "processing", "parser-error", "preprocessing-error", "cleanup-error"])
def test_terminal_publication_follows_actual_resource_cleanup(tmp_path, outcome):
    from kkrunner.runs.base import ExecutionResources

    manager = RunManager(RunStore(tmp_path / "runs"))
    other = RunManager(RunStore(tmp_path / "runs"))
    spec = ExecutionSpec((sys.executable, "-c", "print('result')"), directory=tmp_path,
                         output_file="out")
    proposed = other.new_record(directory=tmp_path, files=FileManifest.from_spec(spec))
    seen = []

    class Resource:
        def close(self):
            pytest.fail("Durable execution must preserve recoverable files")

        def detach(self):
            current = manager.store.get(run.id)
            assert current.is_active
            assert not run.file_state.is_terminal
            assert other.conflicts(proposed) == (current,)
            if outcome != "preprocessing-error":
                assert (tmp_path / "out").read_text().strip() == "result"
            seen.append(current.file_state)
            if outcome == "cleanup-error":
                raise OSError("cleanup error")

    def preprocess():
        if outcome == "preprocessing-error":
            raise ValueError("preprocessing error")

    def process(result):
        if outcome == "parser-error":
            raise ValueError("parser error")
        return result

    resource = Resource()
    run = ExecutionClient().create_run(spec, tracked=manager, preprocess=preprocess,
        process_result=process if outcome in ("processing", "parser-error") else None,
        owned_resources=ExecutionResources(resource, on_detach=resource.detach),
    )
    if outcome.endswith("-error"):
        with pytest.raises((ValueError, OSError), match="error"):
            run.run()
    else:
        run.run()
    assert len(seen) == 1
    current = manager.store.get(run.id)
    assert current.state == (RunState.NEW if outcome == "preprocessing-error" else RunState.FINISHED)
    assert current.file_state == (RunFileState.FAILED if outcome.endswith("-error") else
                                  RunFileState.POSTPROCESSED if outcome == "processing" else RunFileState.DONE)
    assert not other.conflicts(proposed)
    if outcome != "preprocessing-error":
        assert current.files.output_versions
        assert (tmp_path / "out").exists()


def test_backend_terminal_observation_cannot_finish_live_run(tmp_path):
    from kkrunner.runs import RunStatus
    from kkrunner.backends import SupervisedBackend

    manager = RunManager(RunStore(tmp_path / "runs"))
    client = ExecutionClient(SupervisedBackend)
    record = manager.new_record(directory=tmp_path, target=client.target,
                                backend=client.backend_config)
    active = manager.transition(record, state=RunState.RUNNING,
                                execution=record.execution.submitted("job"))
    with manager.store.observation_lock(active):
        ended = manager.update(active, RunStatus(RunState.FINISHED, return_code=0))
        assert ended.state == RunState.FINISHED
        assert ended.file_state == active.file_state
        assert manager.update(active, RunStatus(RunState.CANCELLED)) == ended
        assert manager.store.get(active.id) == ended


def test_attachment_cannot_open_files_owned_by_another_client(tmp_path, monkeypatch):
    from kkrunner.backends import SupervisedBackend

    manager = RunManager(RunStore(tmp_path / "runs"))
    client = ExecutionClient(SupervisedBackend)
    record = manager.new_record(directory=tmp_path, target=client.target,
                                backend=client.backend_config)
    active = manager.transition(record, state=RunState.RUNNING,
                                execution=record.execution.submitted("job"))
    monkeypatch.setattr(client, "open_run_parts", lambda *a, **k: pytest.fail("opened owned files"))
    with manager.store.observation_lock(active):
        with pytest.raises(RuntimeError, match="live client"):
            manager.attach(active, client=client)


def test_cleanup_error_is_visible_to_completion_callback(tmp_path):
    from threading import Event
    from kkrunner.runs.base import ExecutionResources

    manager = RunManager(RunStore(tmp_path / "runs"))
    done, seen = Event(), []

    class Resource:
        def close(self):
            raise OSError("cleanup error")

    def completed(result):
        seen.append((run.state, run.record.file_state, run.file_failure_phase))
        done.set()

    run = ExecutionClient().create_run(ExecutionSpec((sys.executable, "-c", "pass"), directory=tmp_path), tracked=manager,
        owned_resources=ExecutionResources(Resource()), callback=completed,
    )
    with pytest.raises(OSError, match="cleanup error"):
        run.run()
    assert done.is_set()
    assert seen == [(RunState.FINISHED, RunFileState.FAILED, FailurePhase.ARTIFACTS)]
    with pytest.raises(OSError, match="cleanup error"):
        run.wait()


def test_memory_run_can_delete_owned_temporary_files_on_completion(tmp_path):
    from kkrunner import MemoryRunStore
    from kkrunner.runs.base import ExecutionResources

    manager = RunManager(MemoryRunStore())
    output = tmp_path / "out"

    class Resource:
        def close(self):
            assert manager.store.get(run.id).is_active
            assert output.read_text().strip() == "result"
            output.unlink()

    run = ExecutionClient().create_run(ExecutionSpec(
            (sys.executable, "-c", "print('result')"), directory=tmp_path, output_file=output,
        ), tracked=manager,
        owned_resources=ExecutionResources(Resource(), on_detach=lambda: pytest.fail("unexpected detach")),
    )
    run.run()
    assert manager.store.get(run.id).state == RunState.FINISHED
    assert manager.store.get(run.id).files.output_versions
    assert not output.exists()


def test_writable_input_aliases_merge_and_stream_flush_is_reserved(tmp_path):
    with (tmp_path / "input").open("w+") as handle:
        spec = ExecutionSpec(
            argv=(), directory=tmp_path, input_file=handle, stdin=handle,
            input_files=(TaskFile("input", write=True),),
        )
        manifest = FileManifest.from_spec(spec)
        assert len(manifest.inputs) == 1
        assert manifest.inputs[0].write is True
        assert manifest.stdin.write is True
        ordinary = FileManifest.from_spec(replace(spec, input_files=()))
        assert ordinary.inputs[0].write is True


@pytest.mark.parametrize("role", ["input_file", "stdin", "output_file", "error_file"])
def test_rejected_run_does_not_flush_or_close_borrowed_file(tmp_path, role):
    path = tmp_path / "shared"
    path.write_text("original")
    manager = RunManager(RunStore(tmp_path / "runs"))
    reserved = manager.new_record(directory=tmp_path, files=FileManifest.from_spec(
        ExecutionSpec(argv=(), directory=tmp_path, input_file="shared")))
    with path.open("r+") as stream:
        stream.write("buffered")
        with manager.store.observation_lock(reserved):
            reserved = manager.reserve(reserved, preprocessing=True)
            run = ExecutionClient().create_run(ExecutionSpec(
                argv=(sys.executable, "-c", "pass"), directory=tmp_path, **{role: stream},
            ), tracked=manager)
            assert path.read_text() == "original"
            with pytest.raises(DuplicateSubmissionError):
                run.run()
            run.close()
            assert not stream.closed
            assert path.read_text() == "original"
        manager.complete(reserved, state=RunState.FAILED)


def test_unbound_project_manager_can_reserve_save_only_files(tmp_path):
    from kkrunner.projects import ProjectRepository, ProjectRunManager

    store = RunStore(tmp_path / "runs")
    manager = ProjectRunManager(project_repository=ProjectRepository(tmp_path / "projects", run_store=store))
    spec = ExecutionSpec(argv=(), directory=tmp_path, input_files=(TaskFile("input", write=True),))
    with manager.preprocessing_reservation(spec):
        assert not store.runs()
        (tmp_path / "input").write_text("prepared")
    assert not store.runs()



def test_save_only_without_default_store_respects_conflict_store(tmp_path):
    disk = RunStore(tmp_path / "runs")
    tracked = RunManager(disk)
    save = RunManager(conflict_stores=(disk,))
    spec = ExecutionSpec(argv=(), directory=tmp_path, input_files=(TaskFile("input", write=True),))
    proposed = save.new_record(directory=tmp_path, files=FileManifest.from_spec(spec))
    active = tracked.reserve(tracked.new_record(directory=tmp_path, files=proposed.files))
    assert save.conflicts(proposed) == (active,)
    bound = tracked.new_record(directory=tmp_path, files=proposed.files)
    assert RunManager().conflicts(bound) == (active,)
    with pytest.raises(DuplicateSubmissionError):
        with save.preprocessing_reservation(spec):
            pytest.fail("Conflicting preprocessing must not be entered")
    tracked.complete(active, state=RunState.FAILED)
    with save.preprocessing_reservation(spec):
        assert len(save.conflicts(proposed)) == 1
        (tmp_path / "input").write_text("prepared")
    assert not save.conflicts(proposed)
    assert (tmp_path / "input").read_text() == "prepared"
    assert len(disk.runs()) == 1


def test_save_only_does_not_recover_conflicting_abandoned_preprocessing(tmp_path):
    from kkrunner import MemoryRunStore

    disk = RunStore(tmp_path / "runs")
    save = RunManager(MemoryRunStore(), conflict_stores=(disk,))
    persistent = RunManager(disk)
    spec = ExecutionSpec(argv=(), directory=tmp_path, input_files=(TaskFile("input", write=True),))
    abandoned = persistent.reserve(persistent.new_record(
        directory=tmp_path, files=FileManifest.from_spec(spec),
    ), preprocessing=True)

    with pytest.raises(DuplicateSubmissionError):
        with save.preprocessing_reservation(spec):
            pytest.fail("Conflicting preprocessing must not be entered")
    assert disk.get(abandoned.id) == abandoned
    assert not save.store.runs()
    assert persistent.refresh((abandoned,))[0].file_state == RunFileState.FAILED
    with save.preprocessing_reservation(spec):
        (tmp_path / "input").write_text("prepared")



def test_untracked_preprocessing_does_not_create_run_history(tmp_path):
    source = tmp_path / "input.txt"
    output = tmp_path / "out.txt"
    spec = ExecutionSpec(
        (sys.executable, "-c", "from pathlib import Path; print(Path('input.txt').read_text())"),
        directory=tmp_path, input_files=(TaskFile("input.txt", write=True),), output_file=output,
    )
    run = ExecutionClient().create_run(spec, preprocess=lambda: source.write_text("prepared"))
    assert run.record is None
    assert run.run().return_code == 0
    assert output.read_text().strip() == "prepared"
    assert run.record is None
