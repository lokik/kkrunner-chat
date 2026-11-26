"""Resolve explicit project-backed execution without global persistence."""

from __future__ import annotations

from kkrunner.projects.manager import ProjectRunManager
from kkrunner.projects.repository import Project
from kkrunner.runs.manager import RunManager


def resolve_project_manager(project):
    """Resolve a project or an explicitly supplied run manager."""
    if project is None or project is False:
        return None
    if isinstance(project, ProjectRunManager):
        if project.project_id is None:
            raise ValueError("ProjectRunManager must be bound to a project")
        return project
    if isinstance(project, RunManager):
        if project.store is None:
            raise ValueError("An explicit RunManager requires a store")
        return project
    if isinstance(project, (Project, str)):
        return ProjectRunManager(project=project)
    raise TypeError("project must be a Project, project ID, or explicit RunManager")
