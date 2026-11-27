"""User-facing construction paths share the same execution contracts."""

import sys

import pytest

import kkrunner
from kkrunner import (
    DirectBackend, ExecutionClient, ExecutionSpec, LocalTarget, MemoryRunStore,
    ProjectRepository, ProjectRunManager, Run, RunManager, RunStore, SshTarget,
)


def test_one_shot_run_displays_available_unredirected_output(capsys):
    result = kkrunner.run([sys.executable, "-c", "import sys; print(42); print('err', file=sys.stderr)"])
    assert result.return_code == 0
    captured = capsys.readouterr()
    assert captured.out == "42\n"
    assert captured.err == "err\n"


def test_reusable_client_and_named_streams(tmp_path, capsys):
    client = ExecutionClient()
    assert isinstance(client.target, LocalTarget)
    assert isinstance(client.backend, DirectBackend)
    result = client.run(
        [sys.executable, "-c", "print(42)"], directory=tmp_path, output_file="out",
    )
    assert result.return_code == 0
    assert (tmp_path / "out").read_text() == "42\n"
    assert capsys.readouterr().out == ""
    run = client.create_run([sys.executable, "-c", "pass"])
    assert run.client is client
    assert run.record is None
    assert run.run().return_code == 0


def test_create_without_explicit_client():
    run = Run.create(sys.executable)
    assert run.spec.argv == (sys.executable,)
    assert isinstance(run.client.target, LocalTarget)
    assert isinstance(run.client.backend, DirectBackend)
    assert run.record is None
    run.close()


def test_remote_target_and_backend_are_independent(tmp_path):
    spec = ExecutionSpec("program", directory=tmp_path)
    direct = ExecutionClient(target="user@cluster")
    assert direct.target == SshTarget("user@cluster")
    assert direct.backend.name == "direct"
    assert not direct.default_persistent
    client = ExecutionClient("psij", target="user@cluster")
    assert client.target == direct.target
    assert client.backend.options["executor_name"] == "slurm"
    assert client.default_persistent


def test_explicit_store_and_default_policy(tmp_path):
    manager = RunManager(MemoryRunStore())
    with RunManager.as_default(manager):
        run = Run.create([sys.executable, "-c", "pass"], persistent=True)
        assert run.manager is manager
        assert run.id is not None
        assert run.record.state == kkrunner.RunState.NEW
        assert run.run().return_code == 0
        assert manager.store.get(run.id).state == kkrunner.RunState.FINISHED


def test_project_binding_and_version_one_serialization(tmp_path):
    projects = ProjectRepository(tmp_path / "projects")
    project = projects.create("Example")
    manager = ProjectRunManager(project_repository=projects).for_project(project)
    client = ExecutionClient()
    run = client.create_run(
        ExecutionSpec([sys.executable, "-c", "pass"], directory=tmp_path),
        persistent=manager,
    )
    assert run.record.project_id == project.id
    assert run.record.to_dict()["execution"]["transport"] == {"kind": "local"}
    assert "target" not in run.record.to_dict()["execution"]
    assert run.run().return_code == 0
    assert manager.store.get(run.id).execution.target == LocalTarget()


def test_factory_output_listener(tmp_path):
    received = []
    run = Run.create(
        ExecutionSpec([sys.executable, "-c", "print(42)"], directory=tmp_path),
        output_listener=lambda text, kind: received.append((text, kind)),
    )
    assert run.run().return_code == 0
    assert "".join(text for text, kind in received if kind == "out") == "42\n"


def test_untracked_cannot_request_record_metadata_or_preprocessing():
    with pytest.raises(ValueError, match="Record metadata"):
        Run.create("program", persistent=False, metadata={"test": {"name": "example"}})
    with pytest.raises(ValueError, match="Preprocessing"):
        Run.create("program", persistent=False, preprocess=lambda: None)


def test_removed_entry_points_are_not_aliases():
    assert not hasattr(Run, "untracked")
    assert not hasattr(RunManager, "create_run")
    assert not hasattr(ProjectRunManager, "create_run")
    assert not hasattr(ExecutionClient(), "computer")
