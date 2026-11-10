"""Behavioral regression coverage for orthogonal persistence and detachment."""

import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

from kkrunner import ExecutionSpec, Run, RunManager, RunState, RunStore
from kkrunner.backends import ProcessBackend, SupervisedProcessBackend
from kkrunner.client.ssh_direct.worker import WORKER_SOURCE


def test_explicit_and_contextual_persistence(tmp_path):
    direct = ProcessBackend()
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
    untracked_supervised = Run.create(SupervisedProcessBackend(), spec, persistent=False)
    assert untracked_supervised.record is None
    assert untracked_supervised._job._run_id


def test_tracked_direct_completes_without_recoverable_handle(tmp_path):
    spec = ExecutionSpec(
        (sys.executable, "-c", "print('hello')"),
        directory=str(tmp_path), output_file="out.txt",
    )
    manager = RunManager(RunStore(tmp_path / "records"))
    run = Run.create(ProcessBackend(), spec, persistent=manager)
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
from kkrunner.backends import ProcessBackend
root, marker = sys.argv[1:]
spec = ExecutionSpec(
    (sys.executable, "-c",
     "import pathlib,time; time.sleep(.6); pathlib.Path(%r).write_text('survived')" % marker),
    directory=root, output_file=str(Path(root) / "stdout.txt"),
)
Run.create(ProcessBackend(), spec, detached=True).submit()
"""
    child = subprocess.run(
        [sys.executable, "-c", script, str(tmp_path), str(marker)],
        capture_output=True, timeout=10, text=True,
    )
    assert child.returncode == 0, child.stderr
    assert _eventually_exists(marker)
    assert marker.read_text() == "survived"


def test_raw_ssh_worker_detached_does_not_require_agent_or_client_streams(tmp_path):
    marker = tmp_path / "remote-result"
    request = {
        "directory": str(tmp_path),
        "argv": [sys.executable, "-c",
                 "import pathlib,time; time.sleep(.6); pathlib.Path(%r).write_text('remote')" % str(marker)],
        "detached": True,
        "output_file": str(tmp_path / "worker.out"),
    }
    child = subprocess.run(
        [sys.executable, "-u", "-c", WORKER_SOURCE, json.dumps(request)],
        input=b"", capture_output=True, timeout=10,
    )
    assert child.returncode == 0, child.stderr
    assert _eventually_exists(marker)
    assert marker.read_text() == "remote"
