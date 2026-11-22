"""Project tracking is opt-in by project identity, never by a persistent flag."""

from kkrunner.projects.repository import ProjectRepository


def test_project_schema_has_no_run_reference_list(tmp_path):
    projects = ProjectRepository(tmp_path / "projects")
    project = projects.create("test")
    assert not hasattr(project, "persistent_refs")
    text = (tmp_path / "projects" / f"{project.id}.json").read_text()
    assert "persistent_refs" not in text
