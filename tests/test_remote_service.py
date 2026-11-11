import base64
import io

import pytest

from kkrunner.runs import FailurePhase, RunStatus
from kkrunner import RunState
from kkrunner.remote.handles import decode_handle, encode_handle
from kkrunner.backends.supervised import mailbox as remote_mailbox
from kkrunner.backends.supervised import support as process_operations
from kkrunner.backends.supervised import streams as supervisor
from kkrunner.backends.supervised import worker as remote_process_worker
from kkrunner.backends.psij import slurm as psij_slurm
from kkrunner.backends.psij import support as scheduler_operations
from kkrunner.backends.support import messages
from kkrunner.remote import service as remote_service


def _process_handle(tmp_path):
    outcome = tmp_path / "remote-outcomes" / ("a" * 32 + ".json")
    handle = {
        "kind": "process",
        "identity": {"pid": 123, "start_time": 456, "time_basis": "linux-boot"},
        "receipt_id": "a" * 32,
        "socket_path": "/tmp/test-supervisor.sock",
    }
    return (handle, encode_handle(handle), outcome)


def _psij_handle(tmp_path):
    handle = {"kind": "psij", "backend": "slurm", "url": None, "native_id": "42"}
    return (handle, encode_handle(handle))


def _write_outcome(path, state, return_code, failure_phase=None):
    from kkrunner.backends.supervised import mailbox as remote_mailbox

    return remote_mailbox.publish_outcome("a" * 32, state, return_code, failure_phase)


def test_remote_id_is_opaque_backend_handle_without_lifecycle_state(tmp_path):
    handle, remote_id, _ = _process_handle(tmp_path)
    assert remote_id != str(handle)
    decoded = decode_handle(remote_id)
    assert decoded["kind"] == "process"
    assert decoded["identity"]["pid"] == 123
    assert "state" not in decoded
    assert "failure_phase" not in decoded
    assert set(decoded) == {"kind", "identity", "receipt_id", "socket_path"}


@pytest.mark.parametrize(
    "state, return_code, phase, error",
    [
        (RunState.UNKNOWN, None, None, None),
        (RunState.RUNNING, None, None, None),
        (RunState.FINISHED, 0, None, None),
        (RunState.CANCELLED, -15, None, None),
        (RunState.FAILED, 127, FailurePhase.LAUNCH, "spawn failed"),
        (RunState.FAILED, 7, FailurePhase.EXECUTION, "job failed"),
    ],
)
def test_remote_status_wire_reply_is_shared_between_process_and_psij(
    monkeypatch, tmp_path, state, return_code, phase, error
):
    _, process_id, _ = _process_handle(tmp_path)
    _, psij_id = _psij_handle(tmp_path)
    monkeypatch.setattr(process_operations, "_process_outcome", lambda receipt_id: (state, return_code, phase, error))
    monkeypatch.setattr(
        psij_slurm,
        "status",
        lambda native_id: RunStatus(
            state, backend_id=native_id, return_code=return_code, failure_phase=phase, message=error
        ),
    )
    expected = {
        "state": state.value,
        "return_code": return_code,
        "error": error,
        "failure_phase": None if phase is None else phase.value,
    }
    assert remote_service.status_remote(process_id) == expected
    assert remote_service.status_remote(psij_id) == expected


@pytest.mark.parametrize(
    "payload",
    [
        "!",
        "%%%invalid",
        base64.urlsafe_b64encode(b"not JSON").decode(),
        base64.urlsafe_b64encode(b"\xff").decode(),
        base64.urlsafe_b64encode(b"[]").decode(),
        base64.urlsafe_b64encode(b'{"kind":"process"}').decode(),
        base64.urlsafe_b64encode(b'{"kind":"psij","backend":"slurm"}').decode(),
        base64.urlsafe_b64encode(b'{"kind":"unknown"}').decode(),
    ],
)
def test_malformed_remote_id_is_rejected_at_decode_boundary(payload):
    with pytest.raises(ValueError, match="Invalid remote execution id"):
        decode_handle(payload)


def test_remote_id_rejects_wrong_field_types():
    for handle in (
        {
            "kind": "process",
            "identity": {"pid": True, "start_time": 10, "time_basis": "linux-boot"},
            "receipt_id": "a" * 32,
            "socket_path": "/tmp/socket",
        },
        {
            "kind": "process",
            "identity": {"pid": 11, "start_time": 10, "time_basis": "linux-boot"},
            "receipt_id": "../bad",
            "socket_path": "/tmp/socket",
        },
        {"kind": "psij", "backend": "", "native_id": "10", "url": None},
        {"kind": "psij", "backend": "slurm", "native_id": None, "url": None},
    ):
        with pytest.raises(ValueError, match="Invalid remote execution id"):
            decode_handle(encode_handle(handle))


def test_mailbox_receipt_survives_until_client_ack(tmp_path, monkeypatch):
    monkeypatch.setenv("KKRunner_REMOTE_STATE_DIR", str(tmp_path))
    _, remote_id, outcome = _process_handle(tmp_path)
    monkeypatch.setattr(process_operations.ProcessIdentity, "is_running", lambda self: False)
    assert remote_service.status_remote(remote_id)["state"] == RunState.UNKNOWN.value
    _write_outcome(outcome, RunState.FINISHED, 0)
    assert remote_service.status_remote(remote_id)["state"] == RunState.FINISHED.value
    assert remote_service.status_remote(remote_id)["state"] == RunState.FINISHED.value
    assert remote_service.acknowledge_remote(remote_id) == {"acknowledged": True}
    assert remote_service.acknowledge_remote(remote_id) == {"acknowledged": False}
    assert not outcome.exists()


def test_status_distinguishes_launch_and_execution_failure(tmp_path, monkeypatch):
    _, remote_id, outcome = _process_handle(tmp_path)
    monkeypatch.setattr(process_operations.ProcessIdentity, "is_running", lambda self: False)
    monkeypatch.setenv("KKRunner_REMOTE_STATE_DIR", str(tmp_path))
    _write_outcome(outcome, RunState.FAILED, 127, FailurePhase.LAUNCH)
    launch = remote_service.status_remote(remote_id)
    assert launch["state"] == RunState.FAILED.value
    assert launch["failure_phase"] == FailurePhase.LAUNCH.value
    remote_service.acknowledge_remote(remote_id)
    _write_outcome(outcome, RunState.FAILED, 3, FailurePhase.EXECUTION)
    execution = remote_service.status_remote(remote_id)
    assert execution["state"] == RunState.FAILED.value
    assert execution["return_code"] == 3
    assert execution["failure_phase"] == FailurePhase.EXECUTION.value


def test_slurm_status_is_native_and_does_not_use_psij_job_cache(tmp_path, monkeypatch):
    _, remote_id = _psij_handle(tmp_path)
    monkeypatch.setattr(
        scheduler_operations, "_create_executor", lambda opts: pytest.fail("local Slurm status must not initialize PSI/J")
    )
    monkeypatch.setattr(psij_slurm, "_slurm_details", lambda native_id: {"JobState": "RUNNING", "ExitCode": "0:0"})
    status = remote_service.status_remote(remote_id)
    assert status["state"] == RunState.RUNNING.value
    assert status["return_code"] is None
    assert status["failure_phase"] is None


@pytest.mark.parametrize(
    "accounting_state, code, expected_state, expected_phase",
    [
        ("COMPLETED", 0, RunState.FINISHED, None),
        ("FAILED", 7, RunState.FAILED, FailurePhase.EXECUTION),
        ("COMPLETED", 7, RunState.FAILED, FailurePhase.EXECUTION),
    ],
)
def test_slurm_accounting_status_maps_state_and_exit_code(
    tmp_path, monkeypatch, accounting_state, code, expected_state, expected_phase
):
    _, remote_id = _psij_handle(tmp_path)
    monkeypatch.setattr(psij_slurm, "_slurm_details", lambda _native_id: {})
    monkeypatch.setattr(psij_slurm, "_scheduler_output", lambda argv: f"42|{accounting_state}|{code}:0|\n")
    status = remote_service.status_remote(remote_id)
    assert status["state"] == expected_state.value
    assert status["return_code"] == code
    assert status["failure_phase"] == (None if expected_phase is None else expected_phase.value)


def test_psij_cancellation_does_not_invent_terminal_state(monkeypatch, tmp_path):
    _, remote_id = _psij_handle(tmp_path)
    calls = []

    class Job:
        def cancel(self):
            calls.append("cancel")

    class Backend:
        def attach_native_job(self, native_id):
            calls.append(native_id)
            return Job()

    monkeypatch.setattr(scheduler_operations, "_psij_executor", lambda _: Backend())
    monkeypatch.setattr(
        scheduler_operations,
        "_status_impl",
        lambda _: {"state": RunState.RUNNING.value, "return_code": None, "failure_phase": None},
    )
    status = remote_service.cancel_remote(remote_id)
    assert status["state"] == RunState.RUNNING.value
    assert calls == ["42", "cancel"]


def _stub_terminal_psij_job(monkeypatch, state, return_code, observed):
    from types import SimpleNamespace

    class Job:
        status = SimpleNamespace(state=SimpleNamespace(name=state), exit_code=return_code, message=None)

        def wait(self):
            observed.append("wait")
            return self.status

    class Backend:
        def attach_native_job(self, native_id):
            observed.append(("attach", native_id))
            return Job()

    monkeypatch.setattr(scheduler_operations, "_psij_executor", lambda _: Backend())


def test_follow_waits_for_psij_terminal_without_repeated_status_probes(tmp_path, monkeypatch):
    _, remote_id = _psij_handle(tmp_path)
    observed = []
    _stub_terminal_psij_job(monkeypatch, "COMPLETED", 0, observed)
    checks = []

    def status(_remote_id):
        checks.append(_remote_id)
        return {"state": RunState.FINISHED.value, "return_code": 0}

    monkeypatch.setattr(scheduler_operations, "_status_impl", status)
    outcome = remote_service.follow_remote(remote_id, io.BytesIO(), io.BytesIO())
    assert outcome["state"] == RunState.FINISHED.value
    assert observed == [("attach", "42"), "wait"]
    assert checks == [remote_id]


def test_follow_uses_native_terminal_when_status_query_is_transient_unknown(tmp_path, monkeypatch):
    _, remote_id = _psij_handle(tmp_path)
    observed = []
    _stub_terminal_psij_job(monkeypatch, "FAILED", 7, observed)
    checks = []

    def status(_remote_id):
        checks.append(_remote_id)
        return {"state": RunState.UNKNOWN.value}

    monkeypatch.setattr(scheduler_operations, "_status_impl", status)
    outcome = remote_service.follow_remote(remote_id, io.BytesIO(), io.BytesIO())
    assert outcome["state"] == RunState.FAILED.value
    assert outcome["return_code"] == 7
    assert outcome["failure_phase"] == FailurePhase.EXECUTION.value
    assert observed == [("attach", "42"), "wait"]
    assert checks == [remote_id]


def test_follow_fallback_treats_completed_nonzero_psij_as_failed(tmp_path, monkeypatch):
    _, remote_id = _psij_handle(tmp_path)
    observed = []
    _stub_terminal_psij_job(monkeypatch, "COMPLETED", 7, observed)
    monkeypatch.setattr(scheduler_operations, "_status_impl", lambda _remote_id: {"state": RunState.UNKNOWN.value})
    outcome = remote_service.follow_remote(remote_id, io.BytesIO(), io.BytesIO())
    assert outcome == {
        "state": RunState.FAILED.value,
        "return_code": 7,
        "failure_phase": FailurePhase.EXECUTION.value,
        "error": None,
    }
    assert observed == [("attach", "42"), "wait"]


def test_cancel_requests_supervisor_without_inventing_terminal_state(tmp_path, monkeypatch):
    _, remote_id, outcome = _process_handle(tmp_path)
    monkeypatch.setenv("KKRunner_REMOTE_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(process_operations.ProcessIdentity, "is_running", lambda self: True)
    sent = []

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def settimeout(self, timeout):
            pass

        def connect(self, path):
            assert path == "/tmp/test-supervisor.sock"

        def sendall(self, data):
            sent.append(data)

        def recv(self, count):
            return b'{"accepted":true}\n'

    monkeypatch.setattr(process_operations.socket, "socket", lambda *args: Connection())
    status = remote_service.cancel_remote(remote_id)
    assert sent == [b'{"command":"cancel"}\n']
    assert status["state"] == RunState.RUNNING.value
    assert not outcome.exists()


def test_cancel_after_natural_completion_keeps_backend_outcome(tmp_path, monkeypatch):
    _, remote_id, _ = _process_handle(tmp_path)
    monkeypatch.setenv("KKRunner_REMOTE_STATE_DIR", str(tmp_path))
    _write_outcome(None, RunState.FINISHED, 0)

    def forbidden(*args):
        raise AssertionError("Terminal process must not receive cancellation")

    monkeypatch.setattr(process_operations.socket, "socket", forbidden)
    status = remote_service.cancel_remote(remote_id)
    assert status["state"] == RunState.FINISHED.value
    assert status["return_code"] == 0


def test_supervisor_requires_parent_launch_release(monkeypatch):
    import os

    read_fd, write_fd = os.pipe()
    os.close(write_fd)
    monkeypatch.setenv(remote_process_worker._START_FD_ENV, str(read_fd))
    with pytest.raises(RuntimeError, match="not released"):
        remote_process_worker._wait_for_parent_release()


def test_uncertain_submit_uses_only_temporary_identity_ticket(tmp_path, monkeypatch):
    from uuid import uuid4

    monkeypatch.setenv("KKRunner_REMOTE_STATE_DIR", str(tmp_path))
    run_id = uuid4().hex
    handle = encode_handle(
        {
            "kind": "process",
            "identity": {"pid": 123, "start_time": 456, "time_basis": "linux-boot"},
            "receipt_id": "a" * 32,
            "socket_path": "/tmp/fake-socket",
        }
    )
    reply = {"remote_id": handle, "directory": str(tmp_path), "state": "running"}
    remote_mailbox.publish_submission(run_id, reply)
    recovered = remote_service.recover_remote(run_id, {"executor": "process", "directory": str(tmp_path)})
    assert recovered["remote_id"] == handle
    assert remote_service.acknowledge_submission("process", run_id) == {"acknowledged": True}
    assert remote_service.recover_remote(run_id, {"executor": "process"}) is None
    assert remote_service.acknowledge_submission("process", run_id) == {"acknowledged": False}


def test_direct_follow_uses_shared_socket_protocol(tmp_path, monkeypatch):
    """The target command and the supervisor must agree on frames and offsets."""
    import asyncio
    import os
    import sys

    async def exercise():
        monkeypatch.setattr(remote_process_worker, "_mailbox_publish_outcome", lambda *args, **kwargs: None)
        rd, wr = os.pipe()
        spec = {
            "argv": [
                sys.executable,
                "-c",
                "import sys,time; print('stdout', flush=True); print('stderr', file=sys.stderr, flush=True); time.sleep(.1)",
            ],
            "directory": str(tmp_path),
            "output_file": str(tmp_path / "stdout"),
            "error_file": str(tmp_path / "stderr"),
            "receipt_id": "a" * 32,
            "socket_fd": wr,
            "detached": True,
        }
        worker = asyncio.create_task(remote_process_worker._run(spec))
        with os.fdopen(rd, "rb") as source:
            socket_path = (await asyncio.to_thread(source.readline)).decode().strip()
        remote_id = encode_handle(
            {
                "kind": "process",
                "socket_path": socket_path,
                "identity": process_operations.ProcessIdentity.read(os.getpid()).to_dict(),
                "receipt_id": "a" * 32,
            }
        )
        out, err = (io.BytesIO(), io.BytesIO())
        terminal = await asyncio.to_thread(
            remote_service.follow_remote,
            remote_id,
            out,
            err,
            stdout_path=spec["output_file"],
            stderr_path=spec["error_file"],
        )
        assert terminal["state"] == "finished"
        assert out.getvalue() == b"stdout\n"
        assert err.getvalue() == b"stderr\n"
        assert await worker == 0

    asyncio.run(exercise())


def test_direct_follow_without_stream_files_is_live_only(tmp_path, monkeypatch):
    import asyncio
    import os
    import sys

    async def exercise():
        monkeypatch.setattr(remote_process_worker, "_mailbox_publish_outcome", lambda *args, **kwargs: None)
        rd, wr = os.pipe()
        spec = {
            "argv": [sys.executable, "-c", "import time; time.sleep(.15); print('live', flush=True); time.sleep(.1)"],
            "directory": str(tmp_path),
            "output_file": None,
            "error_file": None,
            "receipt_id": "b" * 32,
            "socket_fd": wr,
            "detached": True,
        }
        worker = asyncio.create_task(remote_process_worker._run(spec))
        with os.fdopen(rd, "rb") as source:
            socket_path = (await asyncio.to_thread(source.readline)).decode().strip()
        remote_id = encode_handle(
            {
                "kind": "process",
                "socket_path": socket_path,
                "identity": process_operations.ProcessIdentity.read(os.getpid()).to_dict(),
                "receipt_id": "b" * 32,
            }
        )
        out, err = (io.BytesIO(), io.BytesIO())
        terminal = await asyncio.to_thread(
            remote_service.follow_remote, remote_id, out, err, stdout_offset=0, stderr_offset=0
        )
        assert terminal["state"] == "finished"
        assert out.getvalue() == b"live\n"
        assert err.getvalue() == b""
        assert await worker == 0
        assert list(tmp_path.iterdir()) == []

    asyncio.run(exercise())


def test_ssh_psij_cleanup_removes_declared_outputs_before_submit(tmp_path, monkeypatch):
    from kkrunner.remote import service as remote_service

    output = tmp_path / "previous.dat"
    kept = tmp_path / "cache.pot"
    output.write_bytes(b"stale")
    kept.write_bytes(b"restart")
    from kkrunner.backends.psij.submission import PsijSubmission
    from kkrunner.backends.psij import operations as scheduler_operations

    class Runner(PsijSubmission):
        def __init__(self, spec):
            self.spec = spec
            self._stdout_path = None
            self._stderr_path = None

        backend_id = "native-123"
        state = RunState.RUNNING
        return_code = None
        failure_phase = None
        result = type("Result", (), {"output_file": None, "error_file": None})()

        def submit_native(self):
            self._prepare_io()
            assert not output.exists()
            assert kept.read_bytes() == b"restart"

    class Backend:
        backend = "slurm"
        url = None

        def create_native_job(self, spec, *, submission_key=None):
            return Runner(spec)

    monkeypatch.setattr(scheduler_operations, "_psij_executor", lambda options: Backend())
    request = {
        "id": "d" * 32,
        "executor": "psij",
        "directory": str(tmp_path),
        "argv": ["true"],
        "output_files": [str(output)],
        "cache_files": [str(kept)],
        "input_file": None,
        "output_file": None,
    }
    response = remote_service.submit_remote(request)
    assert response["remote_id"]


def test_scheduler_request_protects_remote_cache_wildcards(tmp_path):
    """PSI/J cleanup must see cache globs, not just locally staged cache files."""
    request = {
        "directory": str(tmp_path),
        "argv": ["program"],
        "input_file": "case.inp",
        "input_files": ["case.inp"],
        "output_file": "case.out",
        "output_files": ["*.pot", "case.out"],
        "cache_files": ["known-restart.pot"],
        "client_sync": {
            "cache_patterns": ["restart-*.pot"],
            "cache_files": [{"path": "external-cache.pot", "before": None}],
        },
    }
    from kkrunner.remote.messages import request_to_spec
    spec = request_to_spec(request)
    assert "restart-*.pot" in spec.cache_files
    assert "external-cache.pot" in spec.cache_files
    assert "*.pot" in spec.output_files
    assert "case.inp" in spec.input_files


@pytest.mark.skipif(__import__("os").name != "posix", reason="Supervisor needs POSIX process groups and Unix sockets")
def test_failed_submission_ticket_cannot_leave_calculation_running(tmp_path, monkeypatch):
    """A ticket write failure happens while the worker is still blocked."""
    from kkrunner.runs.base import SubmissionNotStarted

    marker = tmp_path / "must-not-run"
    request = {
        "id": "a" * 32,
        "executor": "process",
        "detached": True,
        "directory": str(tmp_path),
        "argv": [
            __import__("sys").executable,
            "-c",
            "from pathlib import Path; Path('must-not-run').write_text('bad')",
        ],
        "input_file": None,
        "output_file": str(tmp_path / "run.out"),
        "error_file": None,
    }

    def reject(_run_id, _reply):
        raise OSError("ticket store unavailable")

    monkeypatch.setattr(remote_mailbox, "publish_submission", reject)
    with pytest.raises(SubmissionNotStarted, match="ticket store unavailable"):
        remote_service.submit_remote(request)
    assert not marker.exists()


def test_remote_cleanup_preserves_cache_matches_while_removing_stale_outputs(tmp_path):
    from kkrunner.runs.base import ExecutionSpec

    work = tmp_path / "calculation"
    work.mkdir()
    cache = work / "restart.out"
    stale = work / "obsolete.out"
    stdout = work / "run.log"
    input_file = work / "main.inp"
    for file in (cache, stale, stdout, input_file):
        file.write_text(file.name)
    spec = ExecutionSpec(
        argv=(),
        directory=str(work),
        input_file="main.inp",
        output_file="run.log",
        output_files=("*.out",),
        cache_files=("restart.out",),
    )
    process_operations._clean_request_outputs(spec, work, str(stdout))
    assert cache.read_text() == "restart.out"
    assert input_file.read_text() == "main.inp"
    assert not stale.exists()
    assert not stdout.exists()


def test_remote_cleanup_checks_collisions_before_deleting_any_output(tmp_path):
    from kkrunner.runs.base import ExecutionSpec

    work = tmp_path / "calculation"
    work.mkdir()
    cache = work / "restart.dat"
    stale = work / "old.dat"
    cache.write_text("restart")
    stale.write_text("old")
    spec = ExecutionSpec(
        argv=(),
        directory=str(work),
        input_file=None,
        output_file=None,
        output_files=("old.dat", "restart.dat"),
        cache_files=("restart.dat",),
    )
    with pytest.raises(ValueError, match="overlaps protected"):
        process_operations._clean_request_outputs(spec, work)
    assert stale.read_text() == "old"
    assert cache.read_text() == "restart"


@pytest.mark.parametrize("failure", ("stream_start", "handshake", "startup_cleanup"))
def test_supervisor_prelaunch_failure_publishes_durable_receipt_and_closes_fd(tmp_path, monkeypatch, failure):
    import asyncio
    import os

    from kkrunner.backends.supervised import mailbox as remote_mailbox

    monkeypatch.setenv("KKRunner_REMOTE_STATE_DIR", str(tmp_path / "state"))
    read_fd, write_fd = os.pipe()
    receipt = "c" * 32
    spec = {
        "argv": ["this-command-must-not-launch"],
        "directory": str(tmp_path),
        "output_file": None,
        "error_file": None,
        "receipt_id": receipt,
        "socket_fd": write_fd,
        "detached": True,
    }
    if failure == "stream_start":

        async def broken_start(_self):
            raise OSError("failed to initialize socket")

        monkeypatch.setattr(remote_process_worker.SupervisorStreams, "start", broken_start)
    elif failure == "startup_cleanup":
        original_finish = remote_process_worker.SupervisorStreams.finish

        async def fail_socket_bind(*args, **kwargs):
            raise OSError("socket bind failed")

        async def cleanup_then_fail(self):
            await original_finish(self)
            raise OSError("cleanup failed")

        monkeypatch.setattr(asyncio, "start_unix_server", fail_socket_bind)
        monkeypatch.setattr(remote_process_worker.SupervisorStreams, "finish", cleanup_then_fail)
    else:
        os.close(read_fd)
        read_fd = None
    try:
        assert asyncio.run(remote_process_worker._run(spec)) == 127
        if read_fd is not None:
            with os.fdopen(read_fd, "rb") as reader:
                assert reader.read() == b""
            read_fd = None
        with pytest.raises(OSError):
            os.fstat(write_fd)
        outcome = remote_mailbox.read_outcome(receipt)
        assert outcome["state"] == RunState.FAILED.value
        assert outcome["failure_phase"] == FailurePhase.LAUNCH.value
        assert outcome["return_code"] == 127
        assert "socket" in outcome["error"].lower() or "pipe" in outcome["error"].lower()
        if failure == "startup_cleanup":
            assert "socket bind failed" in outcome["error"]
            assert "cleanup failed" in outcome["error"]
    finally:
        if read_fd is not None:
            os.close(read_fd)


@pytest.mark.parametrize("launch_failure", [False, True])
def test_supervisor_stdin_close_failure_preserves_terminal_receipt(tmp_path, monkeypatch, launch_failure):
    """A failing close() cannot suppress a previous launch error or receipt."""
    import asyncio
    import os
    import sys

    from kkrunner.backends.supervised import mailbox as remote_mailbox

    original_input = tmp_path / "input"
    original_input.write_bytes(b"input")
    from builtins import open as builtin_open

    class FailingInput:
        def __init__(self):
            self.stream = builtin_open(original_input, "rb")

        def fileno(self):
            return self.stream.fileno()

        def close(self):
            self.stream.close()
            raise OSError("stdin close failed")

    opened = []

    def open_input(path, mode):
        assert path == str(original_input)
        assert mode == "rb"
        obj = FailingInput()
        opened.append(obj)
        return obj

    monkeypatch.setattr(remote_process_worker, "open", open_input, raising=False)
    if launch_failure:

        async def broken_spawn(*args, **kwargs):
            raise FileNotFoundError("calculation executable missing")

        monkeypatch.setattr(remote_process_worker.asyncio, "create_subprocess_exec", broken_spawn)
    monkeypatch.setenv("KKRunner_REMOTE_STATE_DIR", str(tmp_path / "state"))
    reader, writer = os.pipe()
    receipt_id = "d" * 32 if launch_failure else "e" * 32
    spec = {
        "argv": [sys.executable, "-c", "pass"],
        "directory": str(tmp_path),
        "stdin": str(original_input),
        "output_file": None,
        "error_file": None,
        "receipt_id": receipt_id,
        "socket_fd": writer,
        "detached": True,
    }
    try:
        return_code = asyncio.run(remote_process_worker._run(spec))
    finally:
        with os.fdopen(reader, "rb") as pipe:
            assert pipe.readline().endswith(b"/socket\n")
    receipt = remote_mailbox.read_outcome(receipt_id)
    assert opened and opened[0].stream.closed
    assert receipt["state"] == RunState.FAILED.value
    if launch_failure:
        assert return_code == 127
        assert receipt["failure_phase"] == FailurePhase.LAUNCH.value
        assert "calculation executable missing" in receipt["error"]
        assert "stdin close failed" not in receipt["error"]
    else:
        assert return_code == 0
        assert receipt["failure_phase"] == FailurePhase.EXECUTION.value
        assert "stdin close failed" in receipt["error"]


@pytest.mark.parametrize("failure_point", ("socket_directory", "first_pipe", "second_pipe"))
def test_remote_submit_ipc_setup_failure_closes_every_fd(tmp_path, monkeypatch, failure_point):
    """No supervisor can have started; all owned prelaunch IPC must be released."""
    import os

    from kkrunner.runs.base import SubmissionNotStarted

    directory = tmp_path / "socket-directory"
    created = []
    real_pipe = os.pipe
    attempts = []

    def directory_factory(*, prefix):
        assert prefix.startswith("kkrunner-follow-")
        if failure_point == "socket_directory":
            raise OSError("cannot allocate socket directory")
        directory.mkdir()
        return str(directory)

    def pipe():
        attempts.append(True)
        if failure_point == "first_pipe" or (failure_point == "second_pipe" and len(attempts) == 2):
            raise OSError("cannot allocate pipe")
        pair = real_pipe()
        created.extend(pair)
        return pair

    monkeypatch.setattr(process_operations.tempfile, "mkdtemp", directory_factory)
    monkeypatch.setattr(process_operations.os, "pipe", pipe)
    monkeypatch.setattr(
        process_operations.subprocess, "Popen", lambda *args, **kwargs: pytest.fail("supervisor must not start")
    )
    with pytest.raises(SubmissionNotStarted, match="cannot allocate"):
        process_operations._local_submit({"argv": ("never-executed",), "directory": str(tmp_path), "detached": True})
    assert not directory.exists()
    assert len(created) == {"socket_directory": 0, "first_pipe": 0, "second_pipe": 2}[failure_point]
    for fd in created:
        with pytest.raises(OSError):
            os.fstat(fd)


def test_remote_submit_json_setup_failure_cleans_pipes_and_directory(tmp_path, monkeypatch):
    """Even a non-I/O failure before Popen cannot strand raw pipe ends."""
    import os

    directory = tmp_path / "socket-directory"
    real_pipe = os.pipe
    created = []

    def directory_factory(*, prefix):
        directory.mkdir()
        return str(directory)

    def pipe():
        pair = real_pipe()
        created.extend(pair)
        return pair

    def broken_json(*args, **kwargs):
        raise TypeError("invalid worker specification")

    monkeypatch.setattr(process_operations.tempfile, "mkdtemp", directory_factory)
    monkeypatch.setattr(process_operations.os, "pipe", pipe)
    monkeypatch.setattr(process_operations.json, "dumps", broken_json)
    monkeypatch.setattr(
        process_operations.subprocess, "Popen", lambda *args, **kwargs: pytest.fail("supervisor must not start")
    )
    with pytest.raises(TypeError, match="invalid worker specification"):
        process_operations._local_submit({"argv": ("never-executed",), "directory": str(tmp_path), "detached": True})
    assert not directory.exists()
    assert len(created) == 4
    for fd in created:
        with pytest.raises(OSError):
            os.fstat(fd)


def test_legacy_tick_identity_is_not_reinterpreted_as_seconds():
    handle = {
        "kind": "process",
        "pid": 42,
        "start_time": "123456",
        "receipt_id": "a" * 32,
        "socket_path": "/missing/socket",
    }
    with pytest.raises(ValueError, match="Invalid remote execution id"):
        decode_handle(encode_handle(handle))


def test_unverifiable_identity_prevents_releasing_calculation(tmp_path, monkeypatch):
    from kkrunner.runs.base import SubmissionNotStarted

    monkeypatch.setattr(process_operations.ProcessIdentity, "read", classmethod(lambda cls, pid: None))
    marker = tmp_path / "must-not-run"
    with pytest.raises(SubmissionNotStarted, match="Unable to verify"):
        remote_service.submit_remote(
            {
                "directory": str(tmp_path),
                "argv": [__import__("sys").executable, "-c", "from pathlib import Path; Path('must-not-run').touch()"],
                "detached": True,
            }
        )
    assert not marker.exists()


@pytest.mark.parametrize(
    "wire, state, phase, recover",
    [
        (b"", RunState.FAILED, FailurePhase.LAUNCH, True),
        (b"", RunState.FAILED, FailurePhase.EXECUTION, False),
        (b"", RunState.RUNNING, None, False),
        (b"", RunState.UNKNOWN, None, False),
        (b"", RunState.FINISHED, None, False),
        (b"A2S", RunState.FAILED, FailurePhase.LAUNCH, False),
        (b"A2SFOLL1", RunState.FAILED, FailurePhase.LAUNCH, False),
        (b"A2SFOLL1O\x00", RunState.FAILED, FailurePhase.LAUNCH, False),
        (b"A2SFOLL1" + b"O" + b"\x00" * 15 + b"\x01", RunState.FAILED, FailurePhase.LAUNCH, False),
    ],
)
def test_follow_recovers_only_empty_handshake_with_launch_failure_receipt(
    tmp_path, monkeypatch, wire, state, phase, recover
):
    import asyncio

    from kkrunner.backends.supervised import streams as supervisor

    _, remote_id, _ = _process_handle(tmp_path)
    facts = messages.status_reply(state, 127, failure_phase=phase)
    monkeypatch.setattr(process_operations, "_status_impl", lambda handle: facts)

    async def closed_socket(path, offsets, on_chunk):
        reader = asyncio.StreamReader()
        reader.feed_data(wire)
        reader.feed_eof()
        async for kind, offset, data in supervisor.receive(reader):
            on_chunk(kind, offset, data)

    monkeypatch.setattr(process_operations, "follow_socket", closed_socket)
    out = io.BytesIO()
    if recover:
        assert remote_service.follow_remote(remote_id, out, io.BytesIO()) == facts
        assert out.getvalue() == b""
    else:
        with pytest.raises(asyncio.IncompleteReadError):
            remote_service.follow_remote(remote_id, out, io.BytesIO())


def test_malformed_follow_greeting_is_not_hidden_by_launch_failure_receipt(tmp_path, monkeypatch):
    import asyncio

    from kkrunner.backends.supervised import streams as supervisor

    _, remote_id, _ = _process_handle(tmp_path)
    monkeypatch.setattr(
        process_operations, "_status_impl",
        lambda handle: messages.status_reply(RunState.FAILED, 127, failure_phase=FailurePhase.LAUNCH),
    )

    async def malformed_socket(*args):
        reader = asyncio.StreamReader()
        reader.feed_data(b"BADMAGIC")
        reader.feed_eof()
        return await supervisor.receive(reader).__anext__()

    monkeypatch.setattr(process_operations, "follow_socket", malformed_socket)
    with pytest.raises(ValueError, match="Unknown supervisor stream protocol"):
        remote_service.follow_remote(remote_id, io.BytesIO(), io.BytesIO())


@pytest.mark.parametrize("offset", [-1, 1, False])
def test_launch_failure_receipt_does_not_mask_rejected_follow_offset(tmp_path, monkeypatch, offset):
    from kkrunner.backends.supervised import streams as supervisor

    _, remote_id, _ = _process_handle(tmp_path)
    monkeypatch.setattr(
        process_operations, "_status_impl",
        lambda handle: messages.status_reply(RunState.FAILED, 127, failure_phase=FailurePhase.LAUNCH),
    )

    async def closed_socket(*args):
        raise supervisor.SupervisorClosedBeforeFollow(b"", len(supervisor.MAGIC))

    monkeypatch.setattr(process_operations, "follow_socket", closed_socket)
    with pytest.raises(supervisor.SupervisorClosedBeforeFollow):
        remote_service.follow_remote(remote_id, io.BytesIO(), io.BytesIO(), stdout_offset=offset)
