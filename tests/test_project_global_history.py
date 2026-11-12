"""Global run history and cross-project namespace regression tests."""

from dataclasses import replace

import pytest

from kkrunner.files import FileEntry, FileManifest
from kkrunner.projects.manager import ProjectRunManager
from kkrunner.projects.repository import ProjectDeleteError, ProjectRepository
from kkrunner.projects.resolution import resolve_project_manager
from kkrunner.runs.base import RunState
from kkrunner.runs.manager import DuplicateSubmissionError
from kkrunner.runs.records import RunStore


def test_different_projects_share_one_store_and_keep_project_identity(tmp_path):
    projects = ProjectRepository(tmp_path / "projects")
    root = projects.create("Root")
    first = projects.create("First", parent_id=root.id)
    second = projects.create("Second", parent_id=root.id)
    a = ProjectRunManager(project_repository=projects, project=first)
    b = ProjectRunManager(project_repository=projects, project=second)
    assert a.store.directory == b.store.directory
    first_run = a.new_record(
        directory=tmp_path / "work", metadata={"test": {"task": "DOS", "dataset": "same"}}, processing_required=True
    )
    second_run = b.new_record(
        directory=tmp_path / "work", metadata={"test": {"task": "DOS", "dataset": "same"}}, processing_required=True
    )
    assert first_run.project_id == first.id
    assert second_run.project_id == second.id
    a.store.save(first_run)
    b.store.save(second_run)
    assert {r.project_id for r in a.store.runs()} == {first.id, second.id}


def test_cross_project_active_conflict_and_project_deletion(tmp_path):
    projects = ProjectRepository(tmp_path / "projects")
    root = projects.create("Root")
    first = projects.create("First", parent_id=root.id)
    second = projects.create("Second", parent_id=root.id)
    a = ProjectRunManager(project_repository=projects, project=first)
    b = ProjectRunManager(project_repository=projects, project=second)
    first_record = a.new_record(
        directory=tmp_path / "work",
        files=FileManifest(stdout=FileEntry(local_path=str(tmp_path / "shared.out"))),
        metadata={"test": {"task": "SCF"}},
        processing_required=True,
    )
    second_record = b.new_record(
        directory=tmp_path / "work",
        files=FileManifest(inputs=(FileEntry(local_path=str(tmp_path / "shared.out")),)),
        metadata={"test": {"task": "DOS"}},
        processing_required=True,
    )
    a.reserve(first_record)
    with pytest.raises(DuplicateSubmissionError):
        b.reserve(second_record)
    with pytest.raises(ProjectDeleteError, match="active"):
        projects.delete(first.id, expected_generation=first.generation)
    a.complete(first_record, state=RunState.FAILED)
    projects.delete(first.id, expected_generation=first.generation)
    assert all((r.project_id != first.id for r in b.store.runs()))


def test_for_project_preserves_explicit_run_store(tmp_path):
    projects = ProjectRepository(tmp_path / "projects")
    project = projects.create("Project")
    custom_store = RunStore(tmp_path / "custom-runs")
    original = ProjectRunManager(store=custom_store, project_repository=projects, submitting_grace=17)
    bound = original.for_project(project)
    assert bound.store is custom_store
    assert bound.submitting_grace == 17
    assert bound.client_pool is original.client_pool
    assert bound.for_project(project) is bound
    assert resolve_project_manager(bound) is bound
    record = bound.new_record(directory=tmp_path / "work", metadata={"test": {"task": "DOS"}}, processing_required=True)
    bound.store.save(record)
    assert custom_store.get(record.id) == record
    assert record.project_id == project.id
    assert not (tmp_path / "projects_runs").exists()


def test_for_project_without_explicit_store_uses_shared_project_store(tmp_path):
    projects = ProjectRepository(tmp_path / "projects")
    project = projects.create("Project")
    unbound = ProjectRunManager(project_repository=projects)
    bound = unbound.for_project(project)
    assert bound.store is unbound.store
    assert bound.project_id == project.id
    assert bound.store.directory == tmp_path / "projects_runs"


def test_unknown_run_prevents_project_deletion_until_terminal(tmp_path):
    """UNKNOWN is recoverable, so deleting its project must not erase history."""
    projects = ProjectRepository(tmp_path / "projects")
    root = projects.create("Root")
    project = projects.create("Child", parent_id=root.id)
    manager = ProjectRunManager(project_repository=projects, project=project)
    record = replace(
        manager.new_record(directory=tmp_path / "work", metadata={"test": {"task": "DOS"}}, processing_required=True),
        state=RunState.UNKNOWN,
    )
    manager.store.save(record)
    with pytest.raises(ProjectDeleteError, match="active"):
        projects.delete(project.id, expected_generation=project.generation)
    assert manager.store.get(record.id).state == RunState.UNKNOWN
    manager.store.update(record, lambda latest: replace(latest, state=RunState.FINISHED))
    projects.delete(project.id, expected_generation=project.generation)
    assert manager.store.runs() == ()
