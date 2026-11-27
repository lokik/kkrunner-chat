"""Administrative project reassignment for completed run records."""
from __future__ import annotations

from dataclasses import replace

import pytest

from kkrunner import RunState
from kkrunner.projects import ProjectRepository, ProjectRepositoryError
from kkrunner.runs.manager import RunManager


def _run(store, directory, *, project_id, state=RunState.FINISHED):
    manager = RunManager(store)
    record = manager.new_record(
        directory=directory, project_id=project_id,
        metadata={"ase2sprkkr": {"task": "SCF"}},
    )
    record = store.save(record)
    return store.update(record, lambda current: replace(current, state=state))


def test_move_completed_run_between_projects_preserves_identity_and_files(tmp_path):
    projects = ProjectRepository(tmp_path / "projects")
    first = projects.create("First")
    second = projects.create("Second")
    store = projects.run_store
    original = _run(store, tmp_path, project_id=first.id)
    moved, = projects.reassign_runs(
        (original.id,), second.id,
        expected_project_ids={original.id: first.id},
    )
    assert moved.project_id == second.id
    assert store.get(original.id).project_id == second.id
    assert moved.id == original.id
    assert moved.created_at == original.created_at
    assert moved.execution == original.execution
    assert moved.files == original.files
    assert moved.metadata == original.metadata


def test_move_running_and_completed_runs_preserves_live_status_updates(tmp_path):
    projects = ProjectRepository(tmp_path / "projects")
    first = projects.create("First")
    second = projects.create("Second")
    store = projects.run_store
    done = _run(store, tmp_path, project_id=first.id)
    running = _run(store, tmp_path, project_id=first.id, state=RunState.RUNNING)

    moved = projects.reassign_runs((done.id, running.id), second.id)
    assert {record.project_id for record in moved} == {second.id}
    assert store.get(running.id).state == RunState.RUNNING

    # A live monitor may hold a stale pre-move snapshot. Its lifecycle update
    # must merge the latest record without reverting the new project identity.
    completed = RunManager(store).transition(
        running, state=RunState.FINISHED, return_code=0
    )
    assert completed.project_id == second.id
    assert completed.state == RunState.FINISHED
    assert store.get(running.id).project_id == second.id


def test_move_prevalidates_stale_selection_before_writing(tmp_path):
    projects = ProjectRepository(tmp_path / "projects")
    first = projects.create("First")
    second = projects.create("Second")
    store = projects.run_store
    done = _run(store, tmp_path, project_id=first.id)
    other = _run(store, tmp_path, project_id=first.id)
    projects.reassign_runs((other.id,), second.id)
    with pytest.raises(ProjectRepositoryError, match="moved by another"):
        projects.reassign_runs(
            (done.id, other.id), first.id,
            expected_project_ids={done.id: first.id, other.id: first.id},
        )
    assert store.get(done.id).project_id == first.id
    assert store.get(other.id).project_id == second.id


def test_move_rejects_stale_project_association(tmp_path):
    projects = ProjectRepository(tmp_path / "projects")
    first = projects.create("First")
    second = projects.create("Second")
    store = projects.run_store
    done = _run(store, tmp_path, project_id=first.id)
    projects.reassign_runs((done.id,), second.id)
    with pytest.raises(ProjectRepositoryError, match="different project|moved by another"):
        projects.reassign_runs(
            (done.id,), first.id,
            expected_project_ids={done.id: first.id},
        )
    assert store.get(done.id).project_id == second.id


def test_move_to_unassigned_and_missing_project(tmp_path):
    projects = ProjectRepository(tmp_path / "projects")
    first = projects.create("First")
    store = projects.run_store
    done = _run(store, tmp_path, project_id=first.id)
    with pytest.raises((FileNotFoundError, KeyError)):
        projects.reassign_runs((done.id,), "123456781234123412341234567890ab")
    assert store.get(done.id).project_id == first.id
    projects.reassign_runs((done.id,), None)
    assert store.get(done.id).project_id is None


def test_move_missing_run_reports_missing_record_and_releases_lock(tmp_path):
    projects = ProjectRepository(tmp_path / "projects")
    first = projects.create("First")
    second = projects.create("Second")
    store = projects.run_store
    original = _run(store, tmp_path, project_id=first.id)

    with pytest.raises(FileNotFoundError):
        projects.reassign_runs((original.id, "missing"), second.id)
    assert store.get(original.id).project_id == first.id
    moved, = projects.reassign_runs((original.id,), second.id)
    assert moved.project_id == second.id


def test_project_move_during_preprocessing_does_not_invalidate_submission(tmp_path):
    projects = ProjectRepository(tmp_path / "projects")
    first = projects.create("First")
    second = projects.create("Second")
    manager = RunManager(projects.run_store)
    record = manager.new_record(directory=tmp_path, project_id=first.id)
    preparing = manager.reserve(record, preprocessing=True)
    projects.reassign_runs((preparing.id,), second.id)

    submitting = manager.begin_submission(preparing)
    assert submitting.state == RunState.SUBMITTING
    assert submitting.project_id == second.id


@pytest.mark.parametrize("fails", [False, True])
def test_project_move_during_result_processing_preserves_outcome(tmp_path, fails):
    from kkrunner.files import FileEntry, FileManifest
    from kkrunner.runs import FailurePhase

    projects = ProjectRepository(tmp_path / "projects")
    first = projects.create("First")
    second = projects.create("Second")
    manager = RunManager(projects.run_store)
    output = tmp_path / "stdout"
    output.write_text("answer")
    record = manager.new_record(
        directory=tmp_path, project_id=first.id, processing_required=True,
        files=FileManifest(stdout=FileEntry(local_path=str(output))),
    )
    pending = manager.complete(
        record, state=RunState.POSTPROCESSING, return_code=0,
        execution=record.execution.submitted("native"), artifacts_available=True,
    )

    def process(outcome):
        projects.reassign_runs((pending.id,), second.id)
        if fails:
            raise ValueError("parser error")
        return "parsed"

    if fails:
        with pytest.raises(ValueError, match="parser error"):
            manager.retry_postprocessing(pending, process)
    else:
        assert manager.retry_postprocessing(pending, process) == "parsed"
    current = manager.store.get(pending.id)
    assert current.project_id == second.id
    assert current.state == (RunState.FAILED if fails else RunState.FINISHED)
    assert current.failure_phase == (FailurePhase.POSTPROCESSING if fails else None)
    assert current.return_code == 0
