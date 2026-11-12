from kkrunner.client.transports import LocalTransport
from kkrunner import RunState, RunStore
from kkrunner.runs.manager import RunManager


def _record(manager, tmp_path):
    return manager.new_record(
        directory=tmp_path, transport=LocalTransport(), metadata={"test": {"task": "DOS"}}, processing_required=True
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
