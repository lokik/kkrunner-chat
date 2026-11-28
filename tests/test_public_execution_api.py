"""User-facing construction paths share the same execution contracts."""

import sys

import pytest

import kkrunner
from kkrunner import (
    DirectBackend, ExecutionClient, ExecutionSpec, LocalTarget, MemoryRunStore,
    ProjectRepository, ProjectRunManager, Run, RunManager, RunStore, SshTarget, SupervisedBackend,
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


@pytest.mark.parametrize("submit", [
    lambda argv, **kwargs: kkrunner.submit(argv, backend=SupervisedBackend, **kwargs),
    ExecutionClient(SupervisedBackend).submit,
])
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
        assert received == []
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


@pytest.mark.parametrize("asynchronous", [False, True])
def test_caller_driven_run_does_not_create_monitor_threads(tmp_path, asynchronous):
    import asyncio
    import threading
    seen = []
    caller = threading.current_thread()

    def output(text, kind):
        assert threading.current_thread() is caller
        seen.append(text)

    def process(outcome):
        assert threading.current_thread() is caller
        return outcome

    run = ExecutionClient().create_run(
        ExecutionSpec((sys.executable, "-c", "print('caller')"), directory=tmp_path),
        process_result=process, output_listener=output,
    )
    accepted = []
    if asynchronous:
        async def drive():
            loop = asyncio.get_running_loop()

            def submitted(current):
                assert asyncio.get_running_loop() is loop
                assert current.backend_id is not None
                accepted.append(current)

            return await run.run_async(on_submitted=submitted)
        result = asyncio.run(drive())
    else:
        result = run.run(on_submitted=accepted.append)

    assert result.return_code == 0
    assert "".join(seen) == "caller\n"
    assert accepted == [run]
    assert run.wait() == result


def test_independent_submit_does_not_observe_until_wait(tmp_path, monkeypatch):
    from kkrunner.client.backend_execution import ManagedExecution

    original = ManagedExecution.observe
    observations = []

    async def observe(self, *args, **kwargs):
        observations.append(self.ref)
        return await original(self, *args, **kwargs)

    monkeypatch.setattr(ManagedExecution, "observe", observe)
    run = ExecutionClient(SupervisedBackend).create_run(
        ExecutionSpec((sys.executable, "-c", "print(42)"), directory=tmp_path, output_file="out"),
    )
    run.submit()
    assert run.backend_id is not None
    assert observations == []
    assert run.wait().return_code == 0
    assert len(observations) == 1


def test_attached_direct_submit_requires_caller_driven_run(tmp_path):
    from kkrunner.backends.contracts import UnsupportedCapability

    run = ExecutionClient(DirectBackend).create_run(
        ExecutionSpec((sys.executable, "-c", "print(42)"), directory=tmp_path),
    )
    with pytest.raises(UnsupportedCapability, match="requires run"):
        run.submit()
    assert run.state is kkrunner.RunState.NEW
    assert run.run().return_code == 0


@pytest.mark.parametrize("backend", [DirectBackend, SupervisedBackend])
def test_run_and_submit_share_native_launch(tmp_path, monkeypatch, backend):
    from kkrunner.client.backend_execution import DirectExecution, ManagedExecution

    binding = DirectExecution if backend is DirectBackend else ManagedExecution
    launch = binding.submit
    calls = []

    async def submit(self, request, on_output, *, output_requested=None):
        calls.append((self, output_requested.enabled))
        return await launch(self, request, on_output, output_requested=output_requested)

    monkeypatch.setattr(binding, "submit", submit)
    client = ExecutionClient(backend)
    seen = []
    for submit_only in (False, True):
        directory = tmp_path / str(submit_only)
        directory.mkdir()
        spec = ExecutionSpec(
            (sys.executable, "-c", "print(42)"), directory=directory,
            output_file="out", error_file="err",
        )
        run = client.create_run(spec, detached=True,
                                output_listener=lambda text, kind: seen.append(text))
        if submit_only:
            run.submit()
            assert run.backend_id is not None
            assert calls[-1][0]._call is None
            assert seen == []
            result = run.wait()
        else:
            result = run.run()
            assert result.return_code == 0
            seen.clear()
        assert (directory / "out").read_text() == "42\n"
    assert [enabled for _, enabled in calls] == [True, False]


@pytest.mark.parametrize("processed", [False, True])
def test_wait_uses_already_published_terminal_record(tmp_path, monkeypatch, processed):
    from kkrunner.runs.base import ExecutionResources
    from kkrunner.runs.processing import ProcessingPending

    manager = RunManager(MemoryRunStore())
    client = ExecutionClient(SupervisedBackend)
    parser = (lambda outcome: "parsed") if processed else None
    released = []
    resources = ExecutionResources(on_detach=lambda: released.append(True))
    run = client.create_run(
        ExecutionSpec((sys.executable, "-c", "print(42)"), directory=tmp_path, output_file="out"),
        tracked=manager, process_result=parser, owned_resources=resources,
    )
    run.submit()
    other = manager.attach(run.record, process_result=parser)
    try:
        with pytest.raises(RuntimeError, match="A live client owns this run"):
            run.wait()
        assert not resources.closed
        assert released == []
        other.wait()
    finally:
        other.close()
    published = manager.store.get(run.id)
    assert published.state is kkrunner.RunState.FINISHED

    async def unexpected(*args, **kwargs):
        pytest.fail("A published terminal Run must not be observed or finalized again")

    def unexpected_configuration():
        pytest.fail("A published terminal Run needs no observation configuration")

    monkeypatch.setattr(run._parts, "configure_observation", unexpected_configuration)
    monkeypatch.setattr(run._job.execution, "observe", unexpected)
    monkeypatch.setattr(run._file_session, "finalize_async", unexpected)
    if processed:
        with pytest.raises(ProcessingPending, match="Another observer processed"):
            run.wait()
    else:
        assert run.wait().return_code == 0
    assert run.files == published.files
    assert manager.store.get(run.id) == published
    assert resources.closed
    assert released == [True]


def test_tracked_async_receipts_use_callers_loop(tmp_path, monkeypatch):
    import asyncio
    import threading

    from kkrunner.client.pool import ClientPool

    client = ExecutionClient(SupervisedBackend)
    manager = RunManager(MemoryRunStore(), client_pool=ClientPool())
    run = client.create_run(
        ExecutionSpec((sys.executable, "-c", "print(42)"), directory=tmp_path, output_file="out"),
        tracked=manager,
    )
    original = client._invoke
    caller = threading.current_thread()
    receipts = []

    async def exercise():
        loop = asyncio.get_running_loop()

        async def invoke(name, *args, **kwargs):
            if name in ("acknowledge_submission", "acknowledge"):
                assert asyncio.get_running_loop() is loop
                assert threading.current_thread() is caller
                record = manager.store.get(run.id)
                assert record.execution.backend_id is not None
                if name == "acknowledge":
                    assert record.state == kkrunner.RunState.FINISHED
                else:
                    assert not record.state.is_terminal
                receipts.append(name)
            return await original(name, *args, **kwargs)

        def forbid_sync(*args, **kwargs):
            pytest.fail("The native asynchronous Run must not invoke a synchronous ACK adapter")

        monkeypatch.setattr(client, "_invoke", invoke)
        monkeypatch.setattr(client, "acknowledge", forbid_sync)
        monkeypatch.setattr(client, "acknowledge_submission", forbid_sync)
        monkeypatch.setattr(manager, "_acknowledge_result", forbid_sync)
        monkeypatch.setattr(manager, "_acknowledge_submission", forbid_sync)
        return await run.run_async()

    assert asyncio.run(exercise()).return_code == 0
    assert receipts == ["acknowledge_submission", "acknowledge"]


@pytest.mark.parametrize("stale_submission", [False, True])
def test_async_run_recovers_conflicting_reservation_on_its_own_loop(
    tmp_path, monkeypatch, stale_submission,
):
    import asyncio
    from kkrunner.client.pool import ClientPool
    from kkrunner.files import FileManifest

    backend = SupervisedBackend if stale_submission else DirectBackend
    client = ExecutionClient(backend)
    manager = RunManager(MemoryRunStore(), client_pool=ClientPool(), submitting_grace=0)
    spec = ExecutionSpec(
        (sys.executable, "-c", "print('new')"), directory=tmp_path, output_file="out",
    )
    (tmp_path / "out").write_text("previous result\n")
    old = manager.reserve(manager.new_record(
        directory=tmp_path, target=client.target, backend=client.backend_config,
        files=FileManifest.from_spec(spec), submission_key="old-submission",
    ))
    if not stale_submission:
        old = manager.transition(
            old, state=kkrunner.RunState.RUNNING,
            execution=old.execution.submitted("old-native"),
        )
    run = client.create_run(spec, tracked=manager)
    original = client._invoke
    operations = []

    async def exercise():
        loop = asyncio.get_running_loop()

        async def invoke(name, *args, **kwargs):
            assert asyncio.get_running_loop() is loop
            if name == "recover" and args[0] == "old-submission":
                operations.append(name)
                return {"remote_id": "old-native", "directory": str(tmp_path)}
            if name == "status" and args[0] == "old-native":
                operations.append(name)
                return {"state": "finished", "return_code": None}
            if name in ("acknowledge_submission", "acknowledge") and args[0] in (
                "old-submission", "old-native",
            ):
                operations.append(name)
                return {"acknowledged": True}
            return await original(name, *args, **kwargs)

        def forbid_sync(*args, **kwargs):
            pytest.fail("Conflict recovery must not invoke a synchronous network adapter")

        monkeypatch.setattr(client, "_invoke", invoke)
        monkeypatch.setattr(client, "status", forbid_sync)
        monkeypatch.setattr(client, "recover_submission", forbid_sync)
        monkeypatch.setattr(manager, "refresh", forbid_sync)
        return await run.run_async()

    assert asyncio.run(exercise()).return_code == 0
    assert manager.store.get(old.id).state is kkrunner.RunState.FINISHED
    assert manager.store.get(run.id).state is kkrunner.RunState.FINISHED
    assert (tmp_path / "out").read_text() == "new\n"
    if stale_submission:
        assert operations == ["recover", "acknowledge_submission", "status", "acknowledge"]
    else:
        assert operations == ["status"]
