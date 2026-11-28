import pytest
from kkrunner import RunFileState
from kkrunner.targets import LocalTarget
from kkrunner import RunState, RunStore
from kkrunner.runs.manager import RunManager


def _record(manager, tmp_path):
    return manager.new_record(
        directory=tmp_path, target=LocalTarget(), metadata={"test": {"task": "DOS"}}, processing_required=True
    )


def test_stale_transition_cannot_regress_newer_lifecycle_state(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    original = _record(manager, tmp_path)
    submitting = manager.transition(original, state=RunState.SUBMITTING)
    running = manager.transition(submitting, state=RunState.RUNNING, execution=submitting.execution.submitted("123"))
    merged = manager.transition(submitting, state=RunState.QUEUED)
    assert merged.state == RunState.RUNNING
    assert merged.execution.backend_id == "123"
    assert manager.store.get(original.id) == merged
    assert running.execution.backend_id == "123"


def test_none_does_not_erase_known_completion_metadata(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    original = _record(manager, tmp_path)
    stale = manager.transition(original, state=RunState.RUNNING)
    finished = manager.transition(stale, state=RunState.FINISHED, return_code=0, message="complete")
    merged = manager.transition(stale, state=RunState.RUNNING, return_code=None, message=None)
    assert merged.state == RunState.FINISHED
    assert merged.return_code == 0
    assert merged.message == "complete"
    assert manager.store.get(original.id) == merged
    assert finished.state == RunState.FINISHED


def test_late_conflicting_final_state_does_not_replace_known_final_outcome(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    original = _record(manager, tmp_path)
    running = manager.transition(original, state=RunState.RUNNING)
    manager.transition(running, state=RunState.FINISHED, return_code=0)
    merged = manager.transition(running, state=RunState.FAILED, return_code=1)
    assert merged.state == RunState.FINISHED
    assert merged.return_code == 0


def test_late_uncaptured_manifest_cannot_destroy_historical_snapshot(tmp_path):
    from kkrunner.files import FileEntry, FileManifest

    manager = RunManager(RunStore(tmp_path / "runs"))
    output = tmp_path / "out"
    output.write_bytes(b"historical contents")
    original = manager.new_record(
        directory=tmp_path,
        files=FileManifest(stdout=FileEntry(local_path=str(output))),
        metadata={"test": {"task": "DOS"}},
        processing_required=True,
    )
    running = manager.transition(original, state=RunState.RUNNING)
    completed = manager.complete(running, state=RunState.FINISHED, return_code=0, artifacts_available=True)
    assert completed.files.captured_at is not None
    assert len(completed.files.output_versions) == 1
    old_manifest = running.files
    late = manager.transition(running, state=RunState.RUNNING, files=old_manifest)
    assert late.files == completed.files
    assert late.files.output_versions == completed.files.output_versions
    assert manager.store.get(running.id).files == completed.files


def test_late_progress_cannot_publish_exit_code_before_completion(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    running = manager.transition(_record(manager, tmp_path), state=RunState.RUNNING)
    finished = manager.transition(running, state=RunState.FINISHED)
    assert finished.return_code is None

    progress = manager.transition(running, file_state=RunFileState.COLLECTING)
    assert progress.state == finished.state
    assert progress.return_code is None

    completed = manager.transition(running, state=RunState.FAILED, return_code=7)
    assert completed.state == RunState.FAILED
    assert completed.return_code == 7


def test_stale_file_phase_cannot_finish_a_newer_phase(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    waiting = manager.transition(_record(manager, tmp_path), state=RunState.RUNNING,
                                 file_state=RunFileState.WAITING)
    collecting = manager.transition(waiting, state=RunState.FINISHED, return_code=0,
                                    file_state=RunFileState.COLLECTING)
    stale = manager.transition(waiting, file_state=RunFileState.FAILED,
                               file_message="old observer lost output")
    assert stale == collecting
    assert manager.store.get(waiting.id) == collecting


def test_file_state_cannot_skip_processing_or_capture(tmp_path):
    import pytest
    manager = RunManager(RunStore(tmp_path / "runs"))
    collecting = manager.transition(_record(manager, tmp_path), state=RunState.FINISHED,
                                    return_code=0, file_state=RunFileState.COLLECTING)
    for phase in (RunFileState.READY, RunFileState.POSTPROCESSING):
        with pytest.raises(ValueError, match="complete capture"):
            manager.transition(collecting, file_state=phase)
    with pytest.raises(ValueError, match="active postprocessing"):
        manager.transition(collecting, file_state=RunFileState.POSTPROCESSED)
    assert manager.store.get(collecting.id) == collecting


def test_stale_status_cannot_revert_recovered_working_directory(tmp_path):
    from dataclasses import replace
    from kkrunner.runs import RunStatus, WorkingDirectory
    from kkrunner.targets import SshTarget

    manager = RunManager(RunStore(tmp_path / "runs"))
    record = manager.new_record(directory=tmp_path, target=SshTarget("cluster"),
                                remote_directory="~/work")
    stale = manager.transition(record, state=RunState.SUBMITTING,
                               execution=record.execution.submitted("native"))
    recovered = manager.transition(stale, state=RunState.RUNNING,
        execution=replace(stale.execution, directory=WorkingDirectory(str(tmp_path), "/home/user/work")))
    result = manager.update(stale, RunStatus(RunState.RUNNING, backend_id="native"))
    assert result.execution == recovered.execution


def test_stale_status_manifest_cannot_fail_confirmed_collection(tmp_path):
    from kkrunner.files import FileEntry, FileManifest
    from kkrunner.runs import RunStatus

    manager = RunManager(RunStore(tmp_path / "runs"))
    output = str(tmp_path / "out")
    stale = manager.transition(manager.new_record(directory=tmp_path,
        files=FileManifest(stdout=FileEntry(output))), state=RunState.RUNNING,
        file_state=RunFileState.WAITING)
    current = manager.update(stale, RunStatus(RunState.FINISHED, return_code=0))
    late = stale.files.with_file_availability(output, "failed", "old reply")
    assert manager.update(stale, RunStatus(RunState.FINISHED, return_code=0, files=late)) == current


def test_stale_completion_does_not_capture_after_file_phase_changed(tmp_path, monkeypatch):
    from kkrunner import ProcessingPending
    import pytest
    manager = RunManager(RunStore(tmp_path / "runs"))
    waiting = manager.transition(_record(manager, tmp_path), state=RunState.RUNNING,
                                 file_state=RunFileState.WAITING)
    collecting = manager.transition(waiting, state=RunState.FINISHED, return_code=0,
                                    file_state=RunFileState.COLLECTING)
    monkeypatch.setattr(manager, "completion_fields", lambda *a, **k: pytest.fail("stale capture"))
    with pytest.raises(ProcessingPending, match="File progress changed"):
        manager.complete(waiting, state=RunState.FINISHED, return_code=0, artifacts_available=True)
    assert manager.store.get(waiting.id) == collecting


def test_completion_uses_confirmed_success_to_require_outputs(tmp_path):
    from kkrunner.files import FileEntry, FileManifest
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = manager.new_record(directory=tmp_path, files=FileManifest(
        outputs=(FileEntry(str(tmp_path / "missing")),),
    ))
    collecting = manager.transition(record, state=RunState.FINISHED, return_code=0,
                                    file_state=RunFileState.COLLECTING)
    saved = manager.complete(collecting, state=RunState.FAILED, return_code=7,
                             artifacts_available=True)
    assert saved.state == RunState.FINISHED
    assert saved.return_code == 0
    assert saved.file_state == RunFileState.FAILED
    assert saved.files.captured_at is None


def test_late_completion_refines_native_outcome_without_reopening_files(tmp_path, monkeypatch):
    import pytest
    manager = RunManager(RunStore(tmp_path / "runs"))
    waiting = manager.transition(_record(manager, tmp_path), state=RunState.RUNNING,
                                 file_state=RunFileState.WAITING)
    released = manager.mark_failed(waiting, message="manual release")
    monkeypatch.setattr(manager, "completion_fields", lambda *a, **k: pytest.fail("capture after release"))
    ended = manager.complete(waiting, state=RunState.FINISHED, return_code=0,
                             artifacts_available=True)
    assert ended.state == RunState.FINISHED
    assert ended.return_code == 0
    assert ended.file_state == released.file_state == RunFileState.FAILED
    assert ended.file_message == released.file_message
    assert ended.finished_at == released.finished_at


def test_public_file_changes_cannot_bypass_live_owner(tmp_path):
    import pytest
    from kkrunner import MemoryRunStore
    from kkrunner.runs import RunStatus

    for store in (MemoryRunStore(), RunStore(tmp_path / "runs")):
        owner = RunManager(store)
        peer = RunManager(store)
        record = owner.reserve(owner.new_record(directory=tmp_path))
        with store.observation_lock(record):
            with pytest.raises(RuntimeError, match="live client"):
                peer.transition(record, file_state=RunFileState.FAILED)
            with pytest.raises(RuntimeError, match="live client"):
                peer.transition(record, files=record.files)
            ended = peer.update(record, RunStatus(RunState.FINISHED, return_code=0))
            assert ended.state == RunState.FINISHED
            assert ended.file_state == RunFileState.WAITING
        assert peer.collect_files(peer.update(ended, RunStatus(RunState.FINISHED, return_code=0))).file_state == RunFileState.DONE


def test_public_artifact_availability_cannot_change_active_collection(tmp_path):
    import pytest
    from kkrunner.files import FileEntry, FileManifest, ArtifactAvailabilityState
    manager = RunManager(RunStore(tmp_path / "runs"))
    path = tmp_path / "out"
    record = manager.new_record(directory=tmp_path, files=FileManifest(stdout=FileEntry(path)))
    record = manager.transition(record, state=RunState.FINISHED, return_code=0,
                                file_state=RunFileState.COLLECTING)
    with pytest.raises(RuntimeError, match="collection owner"):
        manager.update_artifact_availability(record, path, ArtifactAvailabilityState.AVAILABLE)
    assert manager.store.get(record.id) == record


def test_released_reservation_does_not_allow_historical_retrieval_of_running_job(tmp_path):
    import pytest
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = manager.transition(_record(manager, tmp_path), state=RunState.RUNNING,
                                file_state=RunFileState.WAITING)
    released = manager.mark_failed(record, message="manual release")
    assert released.state == RunState.RUNNING
    assert not released.is_active
    with pytest.raises(RuntimeError, match="not finished"):
        manager.retrieve_result_files(released)


def test_scheduler_can_requeue_running_job(tmp_path):
    from kkrunner.runs import RunStatus

    manager = RunManager(RunStore(tmp_path / "runs"))
    running = manager.transition(_record(manager, tmp_path), state=RunState.RUNNING)
    queued = manager.update(running, RunStatus(RunState.QUEUED))
    assert queued.state == RunState.QUEUED
    assert manager.update(running, RunStatus(RunState.RUNNING)) == queued
    resumed = manager.update(queued, RunStatus(RunState.RUNNING))
    assert resumed.state == RunState.RUNNING


def test_stale_same_state_reply_does_not_replace_native_diagnostic(tmp_path):
    from kkrunner.runs import RunStatus

    manager = RunManager(RunStore(tmp_path / "runs"))
    running = manager.transition(_record(manager, tmp_path), state=RunState.RUNNING)
    lost = manager.update(running, RunStatus(RunState.UNKNOWN, message="connection lost"))
    current = manager.update(lost, RunStatus(RunState.UNKNOWN, message="scheduler unavailable"))
    assert manager.update(lost, RunStatus(RunState.UNKNOWN, message="old error")) == current


def test_file_only_change_does_not_invalidate_native_observation(tmp_path):
    from kkrunner.runs import RunStatus

    manager = RunManager(RunStore(tmp_path / "runs"))
    running = manager.transition(_record(manager, tmp_path), state=RunState.RUNNING,
                                 file_state=RunFileState.WAITING)
    released = manager.mark_failed(running, message="manual release")
    queued = manager.update(running, RunStatus(RunState.QUEUED))
    assert queued.state == RunState.QUEUED
    assert queued.file_state == released.file_state
    assert queued.file_message == released.file_message


def test_completion_does_not_relabel_conflicting_native_evidence(tmp_path):
    from kkrunner.runs import FailurePhase

    manager = RunManager(RunStore(tmp_path / "runs"))
    record = manager.new_record(directory=tmp_path)
    failed = manager.transition(record, state=RunState.FAILED, failure_phase=FailurePhase.EXECUTION,
                                file_state=RunFileState.COLLECTING, message="known failure")
    saved = manager.complete(failed, state=RunState.FINISHED, return_code=0, artifacts_available=True)
    assert saved.state == RunState.FAILED
    assert saved.return_code is None
    assert saved.message == "known failure"
    assert saved.file_state == RunFileState.DONE


def test_long_observation_disconnect_after_file_only_update(tmp_path):
    from kkrunner.runs import RunStatus

    manager = RunManager(RunStore(tmp_path / "runs"))
    running = manager.transition(_record(manager, tmp_path), state=RunState.RUNNING,
                                 file_state=RunFileState.WAITING)
    released = manager.mark_failed(running, message="manual release")
    lost = manager._record_execution(
        released, RunStatus(RunState.UNKNOWN, message="connection lost"),
        observed_from=running,
    )
    assert lost.state == RunState.UNKNOWN
    assert lost.file_state == released.file_state
    assert lost.file_message == released.file_message


def test_long_observation_disconnect_cannot_erase_newer_native_facts(tmp_path):
    from kkrunner.runs import RunStatus

    manager = RunManager(RunStore(tmp_path / "runs"))
    running = manager.transition(_record(manager, tmp_path), state=RunState.RUNNING)
    queued = manager.update(running, RunStatus(RunState.QUEUED))
    lost = manager._record_execution(
        queued, RunStatus(RunState.UNKNOWN, message="old observer lost connection"),
        observed_from=running,
    )
    assert lost == queued


def test_file_mutation_conflicts_with_active_readers_and_writers(tmp_path):
    import pytest
    from kkrunner.files import FileEntry, FileManifest
    from kkrunner.runs import DuplicateSubmissionError

    store = RunStore(tmp_path / "history")
    manager = RunManager(store)
    peer = RunManager(RunStore(store.directory))
    directory = tmp_path / "tree"
    active = manager.reserve(manager.new_record(directory=tmp_path, files=FileManifest(
        inputs=(FileEntry(directory / "input"),),
    )))
    with pytest.raises(DuplicateSubmissionError):
        with peer.file_mutation((FileEntry(directory, kind="directory"),)):
            pytest.fail("Cannot delete an active input's parent")
    with peer.file_mutation((FileEntry(tmp_path / "unrelated"),)):
        pass
    assert store.runs() == (active,)


def test_remote_file_mutation_only_conflicts_in_target_namespace(tmp_path):
    import pytest
    from kkrunner.files import FileEntry, FileManifest, FileMapping
    from kkrunner.runs import DuplicateSubmissionError
    from kkrunner.targets import SshTarget

    manager = RunManager(RunStore(tmp_path / "history"))
    mapping = FileMapping(tmp_path, "/work", filesystem_id="cluster-disk")
    path = "/work/tree/input"
    active = manager.reserve(manager.new_record(
        directory=tmp_path, remote_directory="/work", target=SshTarget("cluster"),
        files=FileManifest(inputs=(FileEntry(remote_path=path),), mapping=mapping),
    ))
    historical = manager.new_record(
        directory=tmp_path, remote_directory="/work", target=SshTarget("alias"),
        files=FileManifest(mapping=mapping),
    )
    with pytest.raises(DuplicateSubmissionError):
        with manager.file_mutation((FileEntry(remote_path="/work/tree", kind="directory"),),
                                   remote_of=historical):
            pytest.fail("Aliased target namespace must be protected")
    with manager.file_mutation((FileEntry(tmp_path / "tree", kind="directory"),)):
        pass
    assert manager.store.runs() == (active,)


@pytest.mark.parametrize("fails", [False, True])
def test_file_mutation_reserves_only_selected_files_until_scope_ends(tmp_path, fails):
    from concurrent.futures import ThreadPoolExecutor
    from contextlib import nullcontext
    from kkrunner.files import FileEntry, FileManifest
    from kkrunner.runs import DuplicateSubmissionError

    store = RunStore(tmp_path / "history")
    manager = RunManager(store)
    peer = RunManager(RunStore(store.directory))
    path = tmp_path / "out"
    proposed = peer.new_record(directory=tmp_path, files=FileManifest(outputs=(FileEntry(path),)))
    unrelated = peer.new_record(directory=tmp_path, files=FileManifest(
        outputs=(FileEntry(tmp_path / "unrelated"),),
    ))

    with ThreadPoolExecutor(max_workers=1) as pool:
        with pytest.raises(OSError, match="mutation failed") if fails else nullcontext():
            with manager.file_mutation((FileEntry(path),)):
                # Neither the disk submission lock nor the global in-memory
                # lock may remain held while a file operation is running.
                assert pool.submit(peer.reserve, unrelated).result(timeout=5).id == unrelated.id
                with pytest.raises(DuplicateSubmissionError):
                    pool.submit(peer.reserve, proposed).result(timeout=5)
                assert store.runs() == (peer.store.get(unrelated.id),)
                if fails:
                    raise OSError("mutation failed")
        assert pool.submit(peer.reserve, proposed).result(timeout=5).id == proposed.id
    assert {record.id for record in store.runs()} == {unrelated.id, proposed.id}


@pytest.mark.parametrize("phase", [RunFileState.READY, RunFileState.POSTPROCESSING])
def test_captured_result_without_processor_cannot_enter_processing(tmp_path, phase):
    from kkrunner.files import FileEntry, FileManifest
    from kkrunner.files.availability import capture_result_files

    manager = RunManager(RunStore(tmp_path / "runs"))
    output = tmp_path / "result"
    output.write_text("answer")
    original = manager.new_record(
        directory=tmp_path, files=FileManifest(stdout=FileEntry(output)),
        processing_required=False,
    )
    collecting = manager.transition(original, state=RunState.FINISHED, return_code=0,
                                    file_state=RunFileState.COLLECTING)
    captured = capture_result_files(collecting.files)
    assert captured.captured_at is not None
    with pytest.raises(ValueError, match="declared processor"):
        manager.transition(collecting, files=captured, file_state=phase)
    assert manager.store.get(collecting.id) == collecting
