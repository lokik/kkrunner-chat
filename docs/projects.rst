Group runs in projects
========================

Projects are optional. :class:`~kkrunner.projects.ProjectRepository` stores project names,
hierarchy and working directories. :class:`~kkrunner.projects.ProjectRunManager`
associates runs with a selected project and uses the same persistence and
reservations as :class:`~kkrunner.RunManager`. Project definitions and run
records have separate stores.

.. testcode::

   import sys
   import tempfile
   from pathlib import Path
   from kkrunner import ExecutionSpec, RunStore
   from kkrunner.backends import SupervisedBackend
   from kkrunner.client import EndpointClient
   from kkrunner.projects import ProjectRunManager, ProjectRepository

   workspace = tempfile.TemporaryDirectory()
   directory = Path(workspace.name)
   projects = ProjectRepository(directory / "projects")
   project = projects.create("Example", directory=directory)
   manager = ProjectRunManager(
       store=RunStore(directory / "runs"),
       project_repository=projects,
       project=project,
   )
   run = manager.create_run(
       EndpointClient(SupervisedBackend),
       ExecutionSpec([sys.executable, "-c", "pass"], directory=directory),
   )
   print(run.run().return_code)
   print(manager.store.get(run.id).project_id == project.id)

.. testoutput::

   0
   True

.. testcleanup::

   workspace.cleanup()

You can also create an unbound manager and call ``manager.for_project(project)``.
It returns a bound manager without replacing an already bound project's identity.
Explicitly supplied stores are shared across projects; file reservations still
prevent conflicting runs in different projects.

Project snapshots are immutable. ``project.modify(...)`` returns an edited
snapshot; persist it using ``repository.save(..., expected_generation=...)``.
Stale generations raise :class:`~kkrunner.projects.ProjectConflictError`. Deleting a root,
a project with children, or a project with active runs raises
:class:`~kkrunner.projects.ProjectDeleteError`. Subscribers receive repository changes
through ``refresh()``; on-disk files remain authoritative.

For application integration, retain the application's existing project repository
location and pass it explicitly. Choosing a new run-record namespace must not
silently move or hide existing project definitions.
