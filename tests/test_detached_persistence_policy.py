"""Behavioral regression coverage for orthogonal persistence and detachment."""

import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

from kkrunner import ExecutionSpec, Run, RunManager, RunState, RunStore
from kkrunner.client import ExecutionClient, SshTarget, StagingSpec
from kkrunner.backends import SupervisedBackend
from kkrunner.backends import DirectBackend
from kkrunner.client.contracts import Operation
from kkrunner.targets.implementations.ssh.rpc import operation_command
WORKER_SOURCE = operation_command(Operation.for_backend(DirectBackend, "stream"), framed="execute")[-1]


def test_explicit_and_contextual_persistence(tmp_path):
    direct = ExecutionClient(DirectBackend)
    spec = ExecutionSpec((sys.executable, "-c", "print(42)"), directory=str(tmp_path))
    assert direct.create_run(spec).record is None

    manager = RunManager(RunStore(tmp_path / "records"))
    tracked = direct.create_run(spec, tracked=manager)
    assert tracked.manager is manager
    assert tracked.record.execution.submission_key
    assert tracked.record.execution.backend_id is None
    with RunManager.as_default(manager):
        assert RunManager.get_default() is manager
        assert direct.create_run(spec, tracked=True).manager is manager
    assert RunManager.get_default() is not manager

    # A backend's natural recovery capability never forces tracked records.
    untracked_supervised = ExecutionClient(SupervisedBackend).create_run(spec, tracked=False)
    assert untracked_supervised.record is None
    assert untracked_supervised._parts.submission_builder is not None


def test_tracked_direct_completes_without_recoverable_handle(tmp_path):
    spec = ExecutionSpec(
        (sys.executable, "-c", "print('hello')"),
        directory=str(tmp_path), output_file="out.txt",
    )
    manager = RunManager(RunStore(tmp_path / "records"))
    run = ExecutionClient(DirectBackend).create_run(spec, tracked=manager)
    assert run.run().return_code == 0
    assert run.state == RunState.FINISHED
    assert manager.store.get(run.id).state == RunState.FINISHED
    assert (tmp_path / "out.txt").read_text().strip() == "hello"


def _eventually_exists(path, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return True
        time.sleep(0.05)
    return path.exists()


def test_direct_local_detached_outlives_creating_python_process(tmp_path):
    marker = tmp_path / "marker"
    script = """
from pathlib import Path
import sys
from kkrunner import ExecutionSpec, Run
from kkrunner.client import ExecutionClient
from kkrunner.backends import DirectBackend
root, marker = sys.argv[1:]
spec = ExecutionSpec(
    (sys.executable, "-c",
     "import pathlib,time; time.sleep(.6); pathlib.Path(%r).write_text('survived')" % marker),
    directory=root, output_file=str(Path(root) / "stdout.txt"),
)
ExecutionClient(DirectBackend).create_run(spec, detached=True).submit()
"""
    child = subprocess.run(
        [sys.executable, "-c", script, str(tmp_path), str(marker)],
        capture_output=True, timeout=10, text=True,
    )
    assert child.returncode == 0, child.stderr
    assert _eventually_exists(marker)
    assert marker.read_text() == "survived"


def test_raw_ssh_worker_detached_does_not_require_agent_or_client_streams(tmp_path):
    started = tmp_path / "remote-started"
    marker = tmp_path / "remote-result"
    child_script = (
        "import pathlib,time; pathlib.Path(%r).write_text('started'); "
        "time.sleep(.6); pathlib.Path(%r).write_text('remote')"
    ) % (str(started), str(marker))
    request = {
        "directory": str(tmp_path),
        "argv": [sys.executable, "-c", child_script],
        "detached": True,
        "output_file": str(tmp_path / "worker.out"),
    }
    worker = subprocess.Popen(
        [sys.executable, "-u", "-c", WORKER_SOURCE, json.dumps({"args": [request], "kwargs": {}})],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert _eventually_exists(started), "target child was not launched"
        # Simulate a broken SSH worker. The child owns separate process and
        # file descriptors and must complete despite its parent's death.
        worker.kill()
        worker.wait(timeout=5)
        assert _eventually_exists(marker)
        assert marker.read_text() == "remote"
    finally:
        if worker.poll() is None:
            worker.kill()
            worker.wait(timeout=5)
        worker.stdin.close()
        worker.stdout.close()
        worker.stderr.close()


def test_direct_ssh_detached_waits_and_retrieves_declared_streams(tmp_path):
    """An attached observer still knows exit code and obtains target files."""
    from .test_transport import _LoopbackSshTransport

    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    spec = ExecutionSpec(
        (sys.executable, "-c",
         "import sys; print('out', flush=True); print('err', file=sys.stderr, flush=True)"),
        directory=str(local),
        output_file=str(local / "stdout.txt"),
        error_file=str(local / "stderr.txt"),
        staging=StagingSpec(local_root=local, remote_root=remote),
    )
    run = ExecutionClient(DirectBackend, target=_LoopbackSshTransport()).create_run(spec, detached=True)
    result = run.run()
    assert result.return_code == 0
    assert run.state == RunState.FINISHED
    assert (local / "stdout.txt").read_text() == "out\n"
    assert (local / "stderr.txt").read_text() == "err\n"
    assert (remote / "stdout.txt").read_text() == "out\n"


def test_supervised_job_never_owns_client_manifest(tmp_path):
    job = ExecutionClient(SupervisedBackend).create_run_parts(
        ExecutionSpec((sys.executable, "-c", "print('ok')"), directory=str(tmp_path)),
    )
    job = job.job
    assert not hasattr(job, "set_manifest")
    assert not hasattr(job, "files")


@pytest.mark.parametrize("tracked", [False, True])
@pytest.mark.parametrize("entrypoint", ["run", "run_async"])
def test_single_submission_path_with_and_without_store(tmp_path, tracked, entrypoint):
    manager = RunManager(RunStore(tmp_path / "stored"))
    spec = ExecutionSpec(
        (sys.executable, "-c", "print('finished')"),
        output_file=str(tmp_path / "out.txt"),
        directory=str(tmp_path),
    )
    run = ExecutionClient(DirectBackend).create_run(spec, tracked=manager if tracked else False)
    if entrypoint == "run_async":
        import asyncio
        result = asyncio.run(run.run_async())
    else:
        result = run.run()
    assert result.return_code == 0
    assert run.backend_id is not None
    assert run.state == RunState.FINISHED
    assert (tmp_path / "out.txt").read_text() == "finished\n"
    if tracked:
        assert manager.store.get(run.id).state == RunState.FINISHED
    else:
        assert run.id is None


def test_detached_local_delivers_named_output_on_request(tmp_path):
    spec = ExecutionSpec(
        (sys.executable, "-u", "-c",
         "import sys,time; print('first', flush=True); time.sleep(.15); print('second', file=sys.stderr, flush=True)"),
        directory=str(tmp_path),
        output_file=str(tmp_path / "out.txt"),
        error_file=str(tmp_path / "err.txt"),
    )
    run = ExecutionClient(DirectBackend).create_run(spec, detached=True)
    received = []
    run.add_output_listener(lambda data, kind: received.append((kind, data)))
    assert run.run().return_code == 0
    assert ("out", "first\n") in received
    assert ("err", "second\n") in received
    assert (tmp_path / "out.txt").read_text() == "first\n"
    assert (tmp_path / "err.txt").read_text() == "second\n"


@pytest.mark.parametrize("tracked", [False, True])
def test_detached_direct_can_stop_observing_without_stopping_process(tmp_path, tracked):
    from kkrunner import MemoryRunStore
    marker = tmp_path / "completed"
    gate = tmp_path / "continue"
    spec = ExecutionSpec(
        (sys.executable, "-c",
         "import pathlib,time; gate=pathlib.Path(%r); "
         "deadline=time.monotonic()+5; "
         "exec('while not gate.exists() and time.monotonic()<deadline: time.sleep(.02)'); "
         "pathlib.Path(%r).write_text('done')" % (str(gate), str(marker))),
        directory=str(tmp_path), output_file=str(tmp_path / "out.txt"),
    )
    manager = RunManager(MemoryRunStore()) if tracked else False
    run = ExecutionClient(DirectBackend).create_run(spec, tracked=manager, detached=True)
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    accepted = Event()
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(run.run, on_submitted=lambda _: accepted.set())
        try:
            assert accepted.wait(5)
            with pytest.raises(RuntimeError, match="Cannot close resources"):
                run.close()
            run.stop_observation()
            assert future.result(timeout=5).return_code is None
        finally:
            run.stop_observation()
    try:
        assert run.wait().return_code is None
        assert run.state == RunState.UNKNOWN
        if tracked:
            before = manager.store.get(run.id)
            assert before.execution.backend_id
        run.close()
        run.close()
        if tracked:
            assert manager.store.get(run.id) == before
        assert not marker.exists()
    finally:
        gate.touch()
    assert _eventually_exists(marker)


@pytest.mark.parametrize("backend", [DirectBackend, SupervisedBackend])
def test_attached_stops_listeners_but_drains_output(tmp_path, backend, monkeypatch):
    from threading import Event

    gate = tmp_path / "continue"
    spec = ExecutionSpec(
        (sys.executable, "-u", "-c",
         "import pathlib,time; print('ready', flush=True); "
         "gate=pathlib.Path(%r); deadline=time.monotonic()+5; "
         "exec('while not gate.exists() and time.monotonic()<deadline: time.sleep(.02)'); "
         "print('finished', flush=True)" % str(gate)),
        directory=str(tmp_path), output_file=str(tmp_path / "out.txt"),
    )
    monkeypatch.setenv("KKRunner_REMOTE_STATE_DIR", str(tmp_path / "state"))
    run = ExecutionClient(backend).create_run(spec, detached=False, tracked=False)
    ready = Event()
    received = []

    def listener(data, kind):
        received.append(data)
        if "ready" in data:
            ready.set()

    from concurrent.futures import ThreadPoolExecutor
    run.add_output_listener(listener)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(run.run)
        try:
            assert ready.wait(5)
            run.stop_observation()
            run.stop_observation()
        finally:
            gate.touch()
        assert future.result(timeout=5).return_code == 0
    assert run.state == RunState.FINISHED
    assert "finished" not in "".join(received)
    assert (tmp_path / "out.txt").read_text() == "ready\nfinished\n"


def test_local_direct_submit_survives_event_loop_and_gc(tmp_path):
    import gc

    gate = tmp_path / "gate"
    marker = tmp_path / "marker"
    spec = ExecutionSpec(
        (sys.executable, "-c",
         "import pathlib,time; gate=pathlib.Path('gate'); "
         "deadline=time.monotonic()+10; "
         "exec('while not gate.exists() and time.monotonic()<deadline: time.sleep(.02)'); "
         "pathlib.Path('marker').touch()"),
        directory=tmp_path, output_file="out", error_file="err",
    )
    run = ExecutionClient(DirectBackend).create_run(spec, detached=True)
    try:
        run.submit()
        gc.collect()
        assert not marker.exists()
        gate.touch()
        assert _eventually_exists(marker), "Closing the submission loop killed the detached process"
        result = run.wait()
        assert run.state == RunState.FINISHED
        assert result.return_code is None
    finally:
        gate.touch()
        run.close()


@pytest.mark.parametrize("use_pidfd", [False, True])
def test_detached_native_wait_returns_exit_code_without_worker(tmp_path, monkeypatch, use_pidfd):
    import asyncio
    from kkrunner.backends.implementations.direct.process import wait_detached_process

    if use_pidfd:
        if not sys.platform.startswith("linux") or not hasattr(os, "pidfd_open"):
            pytest.skip("pidfd is Linux-specific")
        try:
            fd = os.pidfd_open(os.getpid())
        except OSError:
            pytest.skip("pidfd unavailable on this kernel")
        os.close(fd)
    else:
        monkeypatch.delattr(os, "pidfd_open", raising=False)

    async def exercise():
        child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(.2); raise SystemExit(7)"])
        try:
            if use_pidfd:
                loop = asyncio.get_running_loop()
                readers = []
                original = loop.add_reader

                def add_reader(fd, *args):
                    readers.append(fd)
                    return original(fd, *args)

                monkeypatch.setattr(loop, "add_reader", add_reader)
            assert await asyncio.wait_for(wait_detached_process(child), 5) == 7
            if use_pidfd:
                assert len(readers) == 1
                with pytest.raises(OSError):
                    os.fstat(readers[0])
        finally:
            if child.poll() is None:
                child.kill()
                child.wait()

    asyncio.run(exercise())


def test_detached_native_wait_cancellation_releases_only_observation():
    import asyncio
    from kkrunner.backends.implementations.direct.process import wait_detached_process

    async def exercise():
        child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(10)"])
        try:
            waiter = asyncio.create_task(wait_detached_process(child))
            await asyncio.sleep(0)
            waiter.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiter
            assert child.poll() is None
        finally:
            child.kill()
            child.wait()

    asyncio.run(exercise())


@pytest.mark.parametrize("operation", ["run", "submit"])
def test_detached_direct_rejects_descriptorless_stdin_before_launch(tmp_path, operation):
    import io
    from kkrunner.backends.contracts import SubmissionNotStarted

    spec = ExecutionSpec(
        (sys.executable, "-c", "from pathlib import Path; Path('launched').touch()"),
        directory=tmp_path, stdin=io.BytesIO(b"x" * (256 * 1024)),
        output_file="out", error_file="err",
    )
    run = ExecutionClient(DirectBackend).create_run(spec, detached=True)
    with pytest.raises(SubmissionNotStarted, match="Detached stdin requires a file descriptor"):
        (run.run if operation == "run" else run.submit)()
    assert run.backend_id is None
    assert not (tmp_path / "launched").exists()


def test_attached_direct_preserves_descriptorless_stdin(tmp_path):
    import io

    data = b"x" * (256 * 1024)
    spec = ExecutionSpec(
        (sys.executable, "-c", "import sys; print(len(sys.stdin.buffer.read()))"),
        directory=tmp_path, stdin=io.BytesIO(data), output_file="out", error_file="err",
    )
    run = ExecutionClient(DirectBackend).create_run(spec, detached=False)
    assert run.run().return_code == 0
    assert (tmp_path / "out").read_text().strip() == str(len(data))


@pytest.mark.parametrize("operation", ["run", "submit"])
def test_detached_direct_accepts_named_open_files(tmp_path, operation):
    import asyncio

    if operation == "submit" and not sys.platform.startswith("linux"):
        pytest.skip("This test requires a verifiable Linux process identity")
    source = tmp_path / "input"
    source.write_bytes(b"input bytes")
    stdout, stderr = tmp_path / "out", tmp_path / "err"
    with source.open("rb") as stdin, stdout.open("wb") as out, stderr.open("wb") as err:
        spec = ExecutionSpec(
            (sys.executable, "-c",
             "import sys; sys.stdout.buffer.write(sys.stdin.buffer.read()); print('error', file=sys.stderr)"),
            directory=tmp_path, stdin=stdin, output_file=out, error_file=err,
        )
        run = ExecutionClient(DirectBackend).create_run(spec, detached=True)
        if operation == "submit":
            run.submit()
            assert out.closed and err.closed
            asyncio.run(asyncio.wait_for(run.wait_async(), timeout=5))
        else:
            assert run.run().return_code == 0
        assert run.state == RunState.FINISHED
        assert out.closed and err.closed
    assert stdout.read_bytes() == b"input bytes"
    assert stderr.read_bytes() == b"error\n"


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux process identity")
def test_detached_direct_accepts_identity_of_already_exited_child(tmp_path, monkeypatch):
    import asyncio

    original = subprocess.Popen
    children = []

    def launch(*args, **kwargs):
        child = original(*args, **kwargs)
        if args and list(args[0]) == [sys.executable, "-c", "pass"]:
            children.append(child)
            # Ensure it has exited but leave its PID/start ticks available.
            os.waitid(os.P_PID, child.pid, os.WEXITED | os.WNOWAIT)
        return child

    monkeypatch.setattr(subprocess, "Popen", launch)
    run = ExecutionClient(DirectBackend).create_run(
        ExecutionSpec((sys.executable, "-c", "pass"), directory=tmp_path),
        detached=True,
    )
    try:
        run.submit()
        assert len(children) == 1
        assert run.backend_id.startswith("direct:")
        asyncio.run(asyncio.wait_for(run.wait_async(), timeout=5))
        assert run.state == RunState.FINISHED
    finally:
        for child in children:
            child.wait()
        run.close()



@pytest.mark.parametrize("operation", ["run", "submit"])
def test_detached_direct_cancel_during_acceptance_preserves_native_outcome(tmp_path, operation):
    if operation == "submit" and sys.platform not in ("linux", "darwin"):
        pytest.skip("Explicit wait requires a verifiable process identity")
    import asyncio

    async def scenario():
        run = ExecutionClient(DirectBackend).create_run(
            ExecutionSpec(
                (sys.executable, "-c", "import time; time.sleep(20)"),
                directory=tmp_path,
            ),
            detached=True,
        )
        requests = []
        try:
            def accepted(current):
                requests.append(current.cancel())

            if operation == "submit":
                await asyncio.wait_for(run.submit_async(on_submitted=accepted), 5)
                result = await asyncio.wait_for(run.wait_async(), 5)
            else:
                result = await asyncio.wait_for(run.run_async(on_submitted=accepted), 5)
            assert len(requests) == 1
            reply = await asyncio.wait_for(asyncio.wrap_future(requests[0]), 5)
            assert reply.state == RunState.CANCELLED
            assert run.state == RunState.CANCELLED
            assert result.return_code == reply.return_code
        finally:
            if (sys.platform.startswith("linux") and run.backend_id is not None
                    and DirectBackend.status(run.backend_id)["state"] == "running"):
                await asyncio.to_thread(DirectBackend.cancel, run.backend_id)
            run.close()

    asyncio.run(scenario())
