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


@pytest.mark.parametrize("submit", [kkrunner.submit, ExecutionClient().submit])
def test_submit_returns_started_run_without_waiting(tmp_path, submit):
    manager = RunManager(MemoryRunStore())
    received = []
    command = """
import time
from pathlib import Path
deadline = time.monotonic() + 10
while not Path("resume").exists():
    if time.monotonic() > deadline:
        raise SystemExit(99)
    time.sleep(0.01)
print(42)
"""
    job = submit(
        [sys.executable, "-c", command], directory=tmp_path,
        output_file="out", tracked=manager,
        output_listener=lambda text, kind: received.append((text, kind)),
    )
    try:
        assert isinstance(job, Run)
        assert not job.state.is_terminal
        assert manager.store.get(job.id).id == job.id
    finally:
        (tmp_path / "resume").touch()
        outcome = job.wait()
    assert outcome.return_code == 0
    assert (tmp_path / "out").read_text() == "42\n"
    assert "".join(text for text, kind in received if kind == "out") == "42\n"
    assert manager.store.get(job.id).state == kkrunner.RunState.FINISHED


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
    client = ExecutionClient("psij", target="user@cluster")
    assert client.target == direct.target
    assert client.backend.options["executor_name"] == "slurm"


@pytest.mark.parametrize("backend, detached", [
    ("direct", False), ("direct", True), ("kkrunner", True), ("psij", True),
])
@pytest.mark.parametrize("tracked", [False, True])
def test_tracking_is_opt_in_independent_of_backend_and_detachment(backend, detached, tracked):
    manager = RunManager(MemoryRunStore())
    client = ExecutionClient(backend)
    kwargs = {"detached": detached}
    if tracked:
        kwargs["tracked"] = True
    with RunManager.as_default(manager):
        jobs = [
            Run.create("program", backend=backend, **kwargs),
            client.create_run("program", **kwargs),
        ]
        for job in jobs:
            try:
                assert (job.id is not None) is tracked
                assert (job.manager is manager) is tracked
                assert job.detached is detached
            finally:
                job.close()


def test_explicit_store_and_default_policy(tmp_path):
    manager = RunManager(MemoryRunStore())
    with RunManager.as_default(manager):
        run = Run.create([sys.executable, "-c", "pass"], tracked=True)
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
        tracked=manager,
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


def test_untracked_cannot_request_record_metadata():
    with pytest.raises(ValueError, match="Record metadata"):
        Run.create("program", tracked=False, metadata={"test": {"name": "example"}})


def test_removed_entry_points_are_not_aliases():
    assert not hasattr(Run, "untracked")
    assert not hasattr(RunManager, "create_run")
    assert not hasattr(ProjectRunManager, "create_run")
    assert not hasattr(ExecutionClient(), "computer")
