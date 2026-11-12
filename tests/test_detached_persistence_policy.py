"""Behavioral regression coverage for orthogonal persistence and detachment."""

import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

from kkrunner import ExecutionSpec, Run, RunManager, RunState, RunStore
from kkrunner.client import EndpointClient, SshTransport, StagingSpec
from kkrunner.backends import SupervisedBackend
from kkrunner.backends import DirectBackend
from kkrunner.client.contracts import Operation
from kkrunner.client.transports.rpc import operation_command
WORKER_SOURCE = operation_command(Operation.for_backend(DirectBackend, "stream"), streaming=True)[-1]


def test_explicit_and_contextual_persistence(tmp_path):
    direct = EndpointClient(DirectBackend)
    spec = ExecutionSpec((sys.executable, "-c", "print(42)"), directory=str(tmp_path))
    assert Run.create(direct, spec).record is None

    manager = RunManager(RunStore(tmp_path / "records"))
    tracked = Run.create(direct, spec, persistent=manager)
    assert tracked.manager is manager
    assert tracked.record.execution.submission_key
    assert tracked.record.execution.backend_id is None
    with RunManager.as_default(manager):
        assert RunManager.get_default() is manager
        assert Run.create(direct, spec, persistent=True).manager is manager
    assert RunManager.get_default() is not manager

    # A backend's natural recovery capability never forces persistent records.
    untracked_supervised = Run.create(EndpointClient(SupervisedBackend), spec, persistent=False)
    assert untracked_supervised.record is None
    assert untracked_supervised._parts.submission_builder is not None


def test_tracked_direct_completes_without_recoverable_handle(tmp_path):
    spec = ExecutionSpec(
        (sys.executable, "-c", "print('hello')"),
        directory=str(tmp_path), output_file="out.txt",
    )
    manager = RunManager(RunStore(tmp_path / "records"))
    run = Run.create(EndpointClient(DirectBackend), spec, persistent=manager)
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
from kkrunner.client import EndpointClient
from kkrunner.backends import DirectBackend
root, marker = sys.argv[1:]
spec = ExecutionSpec(
    (sys.executable, "-c",
     "import pathlib,time; time.sleep(.6); pathlib.Path(%r).write_text('survived')" % marker),
    directory=root, output_file=str(Path(root) / "stdout.txt"),
)
Run.create(EndpointClient(DirectBackend), spec, detached=True).submit()
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
    run = Run.create(EndpointClient(DirectBackend, transport=_LoopbackSshTransport()), spec, detached=True)
    result = run.run()
    assert result.return_code == 0
    assert run.state == RunState.FINISHED
    assert (local / "stdout.txt").read_text() == "out\n"
    assert (local / "stderr.txt").read_text() == "err\n"
    assert (remote / "stdout.txt").read_text() == "out\n"


def test_supervised_job_never_owns_client_manifest(tmp_path):
    job = EndpointClient(SupervisedBackend).create_run_parts(
        ExecutionSpec((sys.executable, "-c", "print('ok')"), directory=str(tmp_path)),
    )
    job = job.job
    assert not hasattr(job, "set_manifest")
    assert not hasattr(job, "files")


@pytest.mark.parametrize("persistent", [False, True])
def test_single_submission_path_with_and_without_store(tmp_path, persistent):
    manager = RunManager(RunStore(tmp_path / "stored"))
    spec = ExecutionSpec(
        (sys.executable, "-c", "print('finished')"),
        output_file=str(tmp_path / "out.txt"),
        directory=str(tmp_path),
    )
    run = Run.create(EndpointClient(DirectBackend), spec, persistent=manager if persistent else False)
    result = run.run()
    assert result.return_code == 0
    assert run.state == RunState.FINISHED
    assert (tmp_path / "out.txt").read_text() == "finished\n"
    if persistent:
        assert manager.store.get(run.id).state == RunState.FINISHED
    else:
        assert run.id is None


def test_detached_local_uses_shared_file_tail_for_live_observation(tmp_path):
    spec = ExecutionSpec(
        (sys.executable, "-u", "-c",
         "import sys,time; print('first', flush=True); time.sleep(.15); print('second', file=sys.stderr, flush=True)"),
        directory=str(tmp_path),
        output_file=str(tmp_path / "out.txt"),
        error_file=str(tmp_path / "err.txt"),
    )
    run = Run.create(EndpointClient(DirectBackend), spec, detached=True)
    received = []
    run.add_output_listener(lambda data, kind: received.append((kind, data)))
    assert run.run().return_code == 0
    assert ("out", "first\n") in received
    assert ("err", "second\n") in received
    assert (tmp_path / "out.txt").read_text() == "first\n"
    assert (tmp_path / "err.txt").read_text() == "second\n"
