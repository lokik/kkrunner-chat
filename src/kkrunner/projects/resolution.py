"""Resolve explicit project-backed execution without global persistence."""

from __future__ import annotations

from kkrunner.projects.manager import ProjectRunManager
from kkrunner.projects.repository import Project


def resolve_project_manager(project):
    """Only an existing project enables durable run tracking."""
    if project is None or project is False:
        return None
    if isinstance(project, ProjectRunManager):
        if project.project_id is None:
            raise ValueError("ProjectRunManager must be bound to a project")
        return project
    if isinstance(project, (Project, str)):
        return ProjectRunManager(project=project)
    raise TypeError("project must be a Project, project ID, or project-bound ProjectRunManager")
