"""Project-bound manager resolution is independent of applications."""

import pytest

from kkrunner import RunStore
from kkrunner.projects import ProjectRunManager, ProjectRepository
from kkrunner.projects.resolution import resolve_project_manager


def _manager(tmp_path):
    return ProjectRunManager(
        RunStore(tmp_path / "runs"), project_repository=ProjectRepository(tmp_path / "projects")
    )


def test_persistence_requires_an_existing_project(tmp_path):
    manager = _manager(tmp_path)
    assert resolve_project_manager(None) is None
    assert resolve_project_manager(False) is None
    with pytest.raises(ValueError, match="bound to a project"):
        resolve_project_manager(manager)
    with pytest.raises(ValueError, match="require a project"):
        manager.new_record()


def test_project_manager_cannot_be_rebound(tmp_path):
    manager = _manager(tmp_path)
    first = manager.projects.create("First")
    second = manager.projects.create("Second")
    with pytest.raises(ValueError, match="Cannot replace"):
        manager.for_project(first).for_project(second)
