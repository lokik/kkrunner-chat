"""Project-bound facade over the shared global run store."""

from __future__ import annotations

from kkrunner.projects.repository import Project, ProjectRepository
from kkrunner.runs.manager import RunManager


class ProjectRunManager(RunManager):
    """Track a run only when it belongs to an existing project.

    All projects share the projects_runs/ RunStore. Each RunRecord contains
    project_id, so reservations are atomic across project boundaries.
    A manager without a bound project cannot create tracked runs.
    """

    def __init__(
        self,
        store=None,
        *,
        client_pool=None,
        submitting_grace=60.0,
        conflict_stores=(),
        project_repository=None,
        project=None,
    ):
        self.projects = (ProjectRepository(run_store=store)
                         if project_repository is None else project_repository)
        if store is not None and store.lock_key != self.projects.run_store.lock_key:
            raise ValueError("Project manager and repository must use the same run store")
        self._project_id = None
        if project is not None:
            identity = project.id if isinstance(project, Project) else str(project)
            current = self.projects.project(identity)
            self._project_id = current.id
        super().__init__(
            self.projects.run_store,
            client_pool=client_pool,
            submitting_grace=submitting_grace,
            conflict_stores=conflict_stores,
        )

    @property
    def project_id(self):
        return self._project_id

    def new_record(self, **kwargs):
        if self._project_id is None:
            raise ValueError("A tracked run must belong to an existing project")
        return super().new_record(project_id=self._project_id, **kwargs)

    def for_project(self, project):
        identity = project.id if isinstance(project, Project) else str(project)
        if self._project_id is not None:
            current = self.projects.project(identity)
            if current.id != self._project_id:
                raise ValueError("Cannot replace the project of a project-bound execution manager")
            return self
        return type(self)(
            client_pool=self.client_pool,
            submitting_grace=self.submitting_grace,
            conflict_stores=self.conflict_stores,
            project_repository=self.projects,
            project=identity,
        )
