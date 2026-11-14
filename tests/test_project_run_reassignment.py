"""Administrative project reassignment for completed run records."""
from __future__ import annotations

from dataclasses import replace

import pytest

from kkrunner import RunState, RunStore
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
    store = RunStore(tmp_path / "projects_runs")
    original = _run(store, tmp_path, project_id=first.id)
    moved, = projects.reassign_runs(
        (original.id,), second.id, store=store,
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
    store = RunStore(tmp_path / "projects_runs")
    done = _run(store, tmp_path, project_id=first.id)
    running = _run(store, tmp_path, project_id=first.id, state=RunState.RUNNING)

    moved = projects.reassign_runs((done.id, running.id), second.id, store=store)
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
    store = RunStore(tmp_path / "projects_runs")
    done = _run(store, tmp_path, project_id=first.id)
    other = _run(store, tmp_path, project_id=first.id)
    projects.reassign_runs((other.id,), second.id, store=store)
    with pytest.raises(ProjectRepositoryError, match="moved by another"):
        projects.reassign_runs(
            (done.id, other.id), first.id, store=store,
            expected_project_ids={done.id: first.id, other.id: first.id},
        )
    assert store.get(done.id).project_id == first.id
    assert store.get(other.id).project_id == second.id


def test_move_rejects_stale_project_association(tmp_path):
    projects = ProjectRepository(tmp_path / "projects")
    first = projects.create("First")
    second = projects.create("Second")
    store = RunStore(tmp_path / "projects_runs")
    done = _run(store, tmp_path, project_id=first.id)
    projects.reassign_runs((done.id,), second.id, store=store)
    with pytest.raises(ProjectRepositoryError, match="different project|moved by another"):
        projects.reassign_runs(
            (done.id,), first.id, store=store,
            expected_project_ids={done.id: first.id},
        )
    assert store.get(done.id).project_id == second.id


def test_move_to_unassigned_and_missing_project(tmp_path):
    projects = ProjectRepository(tmp_path / "projects")
    first = projects.create("First")
    store = RunStore(tmp_path / "projects_runs")
    done = _run(store, tmp_path, project_id=first.id)
    with pytest.raises((FileNotFoundError, KeyError)):
        projects.reassign_runs((done.id,), "123456781234123412341234567890ab", store=store)
    assert store.get(done.id).project_id == first.id
    projects.reassign_runs((done.id,), None, store=store)
    assert store.get(done.id).project_id is None
