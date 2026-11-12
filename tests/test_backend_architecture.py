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
from kkrunner.client import EndpointClient, LocalTransport, SshTransport, StagingSpec
from kkrunner.backends.config import BackendConfig
from kkrunner.client.contracts import Operation
from kkrunner.client.transports.rpc import operation_command, operation_payload


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
    transport = LocalTransport() if location == "local" else SshTransport("example")
    client = EndpointClient(backend, transport=transport)
    config = (client.transport.to_dict(), asdict(client.backend_config))
    restored = EndpointClient.from_config(*json.loads(json.dumps(config)))

    assert client.transport.kind == location
    assert client.backend.name == backend.name
    assert restored.backend.name == backend.name
    assert restored.backend.options == client.backend.options
    assert restored.default_detached == backend().default_detached
    assert restored.default_persistent == (backend().execution_mode == "managed")
    assert restored.backend.supports == backend().supports


@pytest.mark.parametrize("backend", (DirectBackend, SupervisedBackend))
def test_same_ssh_endpoint_runs_selected_backend(tmp_path, monkeypatch, backend):
    monkeypatch.setenv("KKRunner_REMOTE_STATE_DIR", str(tmp_path / "state"))
    local, remote = tmp_path / "client", tmp_path / "target"
    local.mkdir()

    client = EndpointClient(backend, transport=LoopbackSshTransport())
    spec = ExecutionSpec(
        (sys.executable, "-c", "import sys; print('out'); print('err', file=sys.stderr)"),
        directory=str(local), output_file="stdout", error_file="stderr",
        staging=StagingSpec(local_root=str(local), remote_root=str(remote)),
    )
    run = Run.untracked(client, spec)
    assert run.run().return_code == 0
    assert (local / "stdout").read_bytes() == (remote / "stdout").read_bytes() == b"out\n"
    assert (local / "stderr").read_bytes() == (remote / "stderr").read_bytes() == b"err\n"
    # Both backends expose an opaque identity; only managed jobs guarantee reattachment.
    assert isinstance(run.backend_id, str) and run.backend_id


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
        Operation.for_backend(DirectBackend, "stream"),
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
        # The RPC helper reports the cancelled child in its reply and exits cleanly.
        assert process.wait(timeout=5) == 0
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
    from kkrunner.backends.psij import slurm

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

    assert asyncio.run(execute())["state"] == "finished"
    assert output.getvalue() == b"local\n"
    assert (os.getcwd(), sys.stdout, sys.stderr, os.get_blocking(1), os.get_blocking(2)) == before


def test_native_psij_status_contains_only_execution_facts(monkeypatch):
    from types import SimpleNamespace
    from kkrunner.backends.psij import backend as psij_ops
    from kkrunner.remote.handles import encode_handle

    native_status = SimpleNamespace(state=SimpleNamespace(name="COMPLETED"), exit_code=7, message="native")
    monkeypatch.setattr(psij_ops, "_attach_job", lambda identity: SimpleNamespace(status=native_status))
    remote_id = encode_handle({"kind": "psij", "executor_name": "pbs", "url": None, "native_id": "42"})
    facts = PsijBackend.status(remote_id)
    assert facts["state"] == "failed"
    assert facts["return_code"] == 7
    assert facts["error"] == "native"
    assert "files" not in facts
    assert "execution" not in facts
