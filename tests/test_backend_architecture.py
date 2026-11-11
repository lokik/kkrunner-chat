"""Behavioral checks for backend/location independence and portable direct I/O."""

import asyncio
import json
import os
import signal
import subprocess
import sys
import threading
import time
from dataclasses import asdict

import psutil
import pytest

from kkrunner import ExecutionSpec, Run
from kkrunner.backends import DirectBackend, PsijBackend, SupervisedBackend
from kkrunner.client import LocalRunEndpoint, SshRunEndpoint, SshTransport
from kkrunner.client.endpoint import ExecutionEndpointConfig, describe_endpoint, endpoint_from_config
from kkrunner.client.operations import BackendOperation, operation_command, operation_payload


class LoopbackSshTransport(SshTransport):
    """Execute the actual SSH payload locally, without replacing its protocol."""

    def __init__(self):
        super().__init__("loopback", python_command=(sys.executable,))

    def _command(self, argv, cwd=None):
        assert cwd is None
        return argv


@pytest.mark.parametrize("backend", (DirectBackend, SupervisedBackend, PsijBackend))
@pytest.mark.parametrize("location", ("local", "ssh"))
def test_backend_and_location_round_trip_independently(backend, location):
    endpoint = (LocalRunEndpoint(backend) if location == "local" else
                SshRunEndpoint(SshTransport("example"), backend=backend))
    config = describe_endpoint(endpoint)
    loaded = ExecutionEndpointConfig(**json.loads(json.dumps(asdict(config))))
    restored = endpoint_from_config(loaded)

    assert config.location == location
    assert config.backend == backend.name
    assert hash(config) == hash(loaded)
    assert restored.backend is backend
    assert restored.default_detached == backend.default_detached
    assert restored.default_persistent == backend.default_persistent
    assert restored.supports_reattachment == (not backend.session_bound)


@pytest.mark.parametrize("backend", (DirectBackend, SupervisedBackend))
def test_same_ssh_endpoint_runs_selected_backend(tmp_path, monkeypatch, backend):
    monkeypatch.setenv("KKRunner_REMOTE_STATE_DIR", str(tmp_path / "state"))
    local, remote = tmp_path / "client", tmp_path / "target"
    local.mkdir()
    from kkrunner.runs import LaunchSpec

    endpoint = SshRunEndpoint(LoopbackSshTransport(), backend=backend)
    spec = ExecutionSpec(
        (sys.executable, "-c", "import sys; print('out'); print('err', file=sys.stderr)"),
        directory=str(local), output_file="stdout", error_file="stderr",
        launch=LaunchSpec(local_root=str(local), remote_root=str(remote)),
    )
    run = Run.direct(endpoint, spec)
    assert run.run().return_code == 0
    assert (local / "stdout").read_bytes() == (remote / "stdout").read_bytes() == b"out\n"
    assert (local / "stderr").read_bytes() == (remote / "stderr").read_bytes() == b"err\n"
    assert bool(run.backend_id) == (not backend.session_bound)


def _alive(pid):
    try:
        return psutil.Process(pid).status() not in (psutil.STATUS_ZOMBIE, psutil.STATUS_DEAD)
    except psutil.NoSuchProcess:
        return False


@pytest.mark.skipif(os.name != "posix", reason="Direct process groups and pipe readiness require POSIX")
@pytest.mark.parametrize("descriptor", (1, 2))
def test_cancel_stops_calculation_while_remote_output_is_blocked(tmp_path, descriptor):
    """No output consumer may prevent the independent control reader running."""
    code = (
        "import os; from pathlib import Path; Path('pid').write_text(str(os.getpid()));\n"
        f"while True: os.write({descriptor}, b'x' * 65536)"
    )
    command = operation_command(
        BackendOperation.for_backend(DirectBackend, "stream"),
        python=(sys.executable, "-I"), streaming=True,
    )
    payload = operation_payload(({
        "argv": [sys.executable, "-u", "-c", code], "directory": str(tmp_path),
    },), {})
    process = subprocess.Popen((*command, payload), stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    child_pid = None
    readers = []
    try:
        deadline = time.monotonic() + 5
        pid_file = tmp_path / "pid"
        while child_pid is None:
            try:
                child_pid = int(pid_file.read_text())
            except (FileNotFoundError, ValueError):
                assert time.monotonic() < deadline, "Calculation did not start"
                time.sleep(.01)
        time.sleep(.2)  # Let the finite pipe capacity apply backpressure.
        process.stdin.write(b"cancel\n")
        process.stdin.flush()
        deadline = time.monotonic() + 3
        while _alive(child_pid) and time.monotonic() < deadline:
            time.sleep(.01)
        assert not _alive(child_pid), "Blocked output stalled the cancellation control reader"

        def discard(source):
            while source.read(65536):
                pass

        # Pending output is still delivered; cancellation does not discard it.
        for source in (process.stdout, process.stderr):
            reader = threading.Thread(target=discard, args=(source,), daemon=True)
            reader.start()
            readers.append(reader)
        assert process.wait(timeout=5) in (128 + signal.SIGTERM, 128 + signal.SIGKILL)
    finally:
        if process.poll() is None:
            process.kill()
        if child_pid is not None:
            try:
                os.killpg(child_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        process.wait()
        process.stdin.close()
        for reader in readers:
            reader.join(timeout=2)
        process.stdout.close()
        process.stderr.close()


def test_native_scheduler_status_contains_only_execution_facts(monkeypatch):
    from kkrunner.backends.contracts import JobStatus
    from kkrunner.native.psij import slurm

    monkeypatch.setattr(slurm, "_slurm_details", lambda identity: {"JobState": "COMPLETED", "ExitCode": "7:0"})
    status = slurm.status("42")
    assert isinstance(status, JobStatus)
    assert status.state.value == "failed"
    assert status.return_code == 7
    assert not hasattr(status, "files")
    assert not hasattr(status, "execution")


def test_local_direct_preserves_client_streams_and_working_directory(tmp_path):
    from io import BytesIO

    before = (os.getcwd(), sys.stdout, sys.stderr, os.get_blocking(1), os.get_blocking(2))
    output, error = BytesIO(), BytesIO()

    async def execute():
        return await DirectBackend.stream(
            {"argv": [sys.executable, "-c", "print('local')"], "directory": str(tmp_path)},
            stdout=output, stderr=error, control=asyncio.StreamReader(),
        )

    assert asyncio.run(execute()) == 0
    assert output.getvalue() == b"local\n"
    assert (os.getcwd(), sys.stdout, sys.stderr, os.get_blocking(1), os.get_blocking(2)) == before


def test_native_psij_status_contains_only_execution_facts(monkeypatch):
    from types import SimpleNamespace
    from kkrunner.backends.contracts import JobStatus
    from kkrunner.native.psij import executor

    native_status = SimpleNamespace(state=SimpleNamespace(name="COMPLETED"), exit_code=7, message="native")
    job = SimpleNamespace(status=native_status)
    monkeypatch.setattr(executor, "_load_psij", lambda: SimpleNamespace(Job=lambda: job))
    runtime = executor.PsijExecutor("local", job_executor=SimpleNamespace(attach=lambda job, identity: None))
    status = runtime.status("42")
    assert isinstance(status, JobStatus)
    assert status.state.value == "failed"
    assert status.return_code == 7
    assert status.message == "native"
    assert not hasattr(status, "execution")
    assert not hasattr(status, "files")
