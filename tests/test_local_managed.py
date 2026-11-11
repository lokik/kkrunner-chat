from tests._helpers import open_job
"""Local projects and SSH use the same detached supervisor and submission ticket."""

from kkrunner.client.local import LocalRunEndpoint
import sys
import time
from dataclasses import replace

import pytest

from kkrunner.runs import Execution, RunRecord, WorkingDirectory
from kkrunner import ExecutionSpec, RunState
from kkrunner.backends import SupervisedBackend
from kkrunner.backends.contracts import RecoveryDisposition
from kkrunner.client.contracts import SubmissionContext
from kkrunner.client.contracts import JobAttachment
from kkrunner.client.endpoint import ExecutionEndpointConfig
from kkrunner.files import FileEntry, FileManifest
from kkrunner.files.availability import local_manifest
from kkrunner.backends.supervised.mailbox import recover_submission
from kkrunner.remote.handles import decode_handle, encode_handle
from kkrunner.backends.supervised import SupervisedBackend
from kkrunner.support.process_identity import ProcessIdentity
from kkrunner.runs.base import FailurePhase, RunStatus

RUN_ID = "b" * 32
pytestmark = pytest.mark.skipif(
    __import__("os").name != "posix", reason="The shared supervisor requires POSIX process groups and Unix sockets"
)


@pytest.fixture(autouse=True)
def private_backend_state(tmp_path, monkeypatch):
    monkeypatch.setenv("KKRunner_REMOTE_STATE_DIR", str(tmp_path / "backend-state"))


def _spec(tmp_path, command=None, *, out="out.txt", err="err.txt"):
    return ExecutionSpec(
        argv=command or (sys.executable, "-c", "print('output', flush=True)"),
        input_file=None,
        output_file=None if out is None else str(tmp_path / out),
        error_file=None if err is None else str(tmp_path / err),
        directory=str(tmp_path),
    )


def _record(tmp_path, backend_id, *, stdout=True, stderr=True):
    return RunRecord(
        id=RUN_ID,
        state=RunState.RUNNING,
        created_at="2026-10-13T08:00:00+00:00",
        updated_at="2026-10-13T08:00:00+00:00",
        execution=Execution(ExecutionEndpointConfig(backend="process"), WorkingDirectory(str(tmp_path)), backend_id),
        files=FileManifest(
            stdout=FileEntry(local_path=str(tmp_path / "out.txt")) if stdout else None,
            stderr=FileEntry(local_path=str(tmp_path / "err.txt")) if stderr else None,
        ),
        metadata={"test": {"task": "DOS"}},
        processing_required=True,
    )


def _attachment(record):
    return JobAttachment(record.execution, record.files)


def test_project_launch_reconnect_and_shared_ticket(tmp_path):
    executor = LocalRunEndpoint(SupervisedBackend)
    runner = executor.create_job(_spec(tmp_path), submission_key=RUN_ID)
    runner.submit()
    handle = runner.backend_id
    assert decode_handle(handle)["kind"] == "process"
    assert recover_submission(RUN_ID)["remote_id"] == handle
    assert runner.wait().return_code == 0
    assert runner.state == RunState.FINISHED
    assert not ProcessIdentity.from_dict({"pid": 1, "start_time": 0, "time_basis": "epoch"}).is_running()
    assert (tmp_path / "out.txt").read_text() == "output\n"
    assert executor.status(_attachment(_record(tmp_path, handle)))[0].state == RunState.FINISHED
    assert executor.recover_submission(RUN_ID, SubmissionContext(_record(tmp_path, None).execution)).ref.token == handle
    executor.acknowledge_submission(_record(tmp_path, handle))
    assert recover_submission(RUN_ID) is None
    assert executor.status(_attachment(_record(tmp_path, handle)))[0].state == RunState.FINISHED
    seen = []
    attached = open_job(executor, _attachment(_record(tmp_path, handle)), output_listener=lambda text, kind: seen.append((text, kind)))
    assert attached.spec.is_partial
    assert attached.spec.argv is None
    assert attached.wait().return_code == 0
    assert seen == [("output\n", "out")]
    executor.acknowledge(_record(tmp_path, handle))
    assert runner.state == RunState.FINISHED
    assert runner.return_code == 0


def test_project_file_objects_are_not_serialized(tmp_path):
    spec = _spec(tmp_path)
    with (tmp_path / "extra.out").open("w+b") as extra, (tmp_path / "cache.pot").open("w+b") as cache:
        spec = replace(spec, output_files=(extra,), cache_files=(cache,))
        runner = LocalRunEndpoint(SupervisedBackend).create_job(spec, submission_key=RUN_ID)
        runner.submit()
        assert runner.wait().return_code == 0


def test_process_failure_preserves_execution_phase(tmp_path):
    runner = LocalRunEndpoint(SupervisedBackend).create_job(
        _spec(tmp_path, (sys.executable, "-c", "import sys; sys.exit(7)")), submission_key=RUN_ID
    )
    runner.submit()
    assert runner.wait().return_code == 7
    assert runner.state == RunState.FAILED
    assert runner.failure_phase == FailurePhase.EXECUTION


def test_cancel_is_supervisor_owned_and_nonblocking(tmp_path):
    runner = LocalRunEndpoint(SupervisedBackend).create_job(
        _spec(tmp_path, (sys.executable, "-c", "import time; print('started', flush=True); time.sleep(30)")),
        submission_key=RUN_ID,
    )
    runner.submit()
    started = time.monotonic()
    runner.cancel()
    assert time.monotonic() - started < 2
    runner.wait()
    assert runner.state == RunState.CANCELLED
    assert SupervisedBackend.status(runner.backend_id)["state"] == "cancelled"


def test_missing_ticket_keeps_submission_uncertain(tmp_path):
    assert LocalRunEndpoint(SupervisedBackend).recover_submission(
        RUN_ID, SubmissionContext(_record(tmp_path, None).execution)
    ).disposition is RecoveryDisposition.INDETERMINATE


def test_missing_supervisor_outcome_is_unknown(tmp_path):
    handle = encode_handle(
        {
            "kind": "process",
            "identity": {"pid": 9999999, "start_time": 0, "time_basis": "linux-boot"},
            "receipt_id": "a" * 32,
            "socket_path": "/missing/socket",
        }
    )
    assert LocalRunEndpoint(SupervisedBackend).status(_attachment(_record(tmp_path, handle)))[0].state == RunState.UNKNOWN


def test_wildcards_track_concrete_file_availability(tmp_path):
    (tmp_path / "one.dat").write_text("x")
    files = FileManifest(outputs=(FileEntry(local_path=str(tmp_path / "*.dat"), availability_state="pending"),))
    status = RunStatus(RunState.FINISHED, backend_id="test")
    updated = local_manifest(files, status)
    assert len(updated.outputs) == 2
    assert updated.outputs[0].availability_state == "available"
    assert updated.outputs[1].source_pattern == str(tmp_path / "*.dat")
    assert updated.outputs[1].availability_state == "available"


def test_failed_launch_clears_stale_stdout(tmp_path):
    out = tmp_path / "out.txt"
    out.write_bytes(b"previous")
    runner = LocalRunEndpoint(SupervisedBackend).create_job(_spec(tmp_path, (str(tmp_path / "missing-program"),)), submission_key=RUN_ID)
    runner.submit()
    runner.wait()
    assert runner.state == RunState.FAILED
    assert runner.failure_phase == FailurePhase.LAUNCH
    assert out.read_bytes() == b""


def test_none_stream_paths_are_live_only(tmp_path):
    runner = LocalRunEndpoint(SupervisedBackend).create_job(_spec(tmp_path, out=None, err=None), submission_key=RUN_ID)
    seen = []
    runner.add_output_listener(lambda text, kind: seen.append((text, kind)))
    runner.submit()
    assert runner.wait().return_code == 0
    assert not (tmp_path / "out.txt").exists()
    assert not (tmp_path / "err.txt").exists()
    attached = open_job(LocalRunEndpoint(SupervisedBackend), _attachment(_record(tmp_path, runner.backend_id, stdout=False, stderr=False)), start=False)
    assert attached.backend_id == runner.backend_id
    assert attached.result.output_file is None
    assert attached.result.error_file is None
    assert attached.state == RunState.FINISHED


def test_terminal_without_ticket_is_still_accessible_by_handle(tmp_path):
    runner = LocalRunEndpoint(SupervisedBackend).create_job(_spec(tmp_path), submission_key=RUN_ID)
    runner.submit()
    runner.wait()
    assert SupervisedBackend.acknowledge_submission(RUN_ID)["acknowledged"]
    assert LocalRunEndpoint(SupervisedBackend).status(
        _attachment(_record(tmp_path, runner.backend_id))
    )[0].state == RunState.FINISHED


def test_local_prelaunch_error_is_known_submission_failure(tmp_path, monkeypatch):
    from kkrunner.runs import FailurePhase
    from kkrunner.backends.supervised import backend as process_operations
    from kkrunner.runs.base import SubmissionNotStarted

    def failed_launch(*args, **kwargs):
        raise OSError("cannot spawn supervisor")

    monkeypatch.setattr(process_operations.subprocess, "Popen", failed_launch)
    runner = LocalRunEndpoint(SupervisedBackend).create_job(_spec(tmp_path), submission_key=RUN_ID)
    with pytest.raises(SubmissionNotStarted, match="cannot spawn supervisor"):
        runner.submit()
    assert runner.backend_id is None
    assert runner.state == RunState.FAILED
    assert runner.failure_phase == FailurePhase.SUBMISSION
    assert not recover_submission(RUN_ID)


def test_cancelled_run_preserves_partial_stream_availability(tmp_path):
    out = tmp_path / "out.txt"
    out.write_bytes(b"partial output\n")
    files = FileManifest(stdout=FileEntry(local_path=str(out), availability_state="pending"))
    manifest = local_manifest(files, RunStatus(RunState.CANCELLED))
    assert manifest.stdout.availability_state == "available"
    assert out.read_bytes() == b"partial output\n"


def test_terminal_local_runner_never_regresses_to_running(tmp_path, monkeypatch):

    runner = LocalRunEndpoint(SupervisedBackend).create_job(_spec(tmp_path), submission_key=RUN_ID)
    runner._backend_id = "opaque-handle"
    runner._state = RunState.FINISHED
    runner._return_code = 0
    runner.result.return_code = 0
    monkeypatch.setattr(runner.context, "_status", lambda *args: RunStatus(RunState.RUNNING))
    assert runner.state == RunState.FINISHED
    assert runner.return_code == 0


def test_project_request_flushes_each_declared_file_once(tmp_path):

    class NamedFile:
        def __init__(self, name):
            self.name = str(tmp_path / name)
            self.flush_count = 0

        def flush(self):
            self.flush_count += 1

    output = NamedFile("supplement.out")
    inp = NamedFile("supplement.in")
    cache = NamedFile("restart.pot")
    spec = replace(
        _spec(tmp_path),
        output_files=(None, output, "a.dat"),
        input_files=(None, inp, "b.inp"),
        cache_files=(None, cache, "restart.pot"),
    )
    runner = LocalRunEndpoint(SupervisedBackend).create_job(spec, submission_key=RUN_ID)
    runner.prepare_submission()
    request = runner._request
    assert request["output_files"] == [output.name, str(tmp_path / "a.dat")]
    assert request["input_files"] == [inp.name, str(tmp_path / "b.inp")]
    assert request["cache_files"] == [cache.name, str(tmp_path / "restart.pot")]
    assert (output.flush_count, inp.flush_count, cache.flush_count) == (1, 1, 1)


def test_submission_cleanup_uses_parsed_manifest_and_protects_caches(tmp_path):
    from kkrunner.backends.supervised.support import _clean_request_outputs
    from kkrunner.backends.support.messages import request_to_spec

    for name in ("stale.dat", "input.dat", "stdin.dat", "cache.dat", "sync-cache.dat"):
        (tmp_path / name).write_text(name)
    request = {
        "directory": str(tmp_path),
        "argv": [sys.executable, "-c", "pass"],
        "input_file": "input.dat",
        "stdin": "stdin.dat",
        "output_files": ["*.dat"],
        "cache_files": ["cache.dat"],
        "client_sync": {"cache_files": [{"path": "sync-cache.dat", "before": None}]},
    }
    spec = request_to_spec(request)
    _clean_request_outputs(spec, tmp_path)
    assert not (tmp_path / "stale.dat").exists()
    for name in ("input.dat", "stdin.dat", "cache.dat", "sync-cache.dat"):
        assert (tmp_path / name).read_text() == name


def test_local_project_request_uses_common_command_fields(tmp_path):
    from kkrunner.runs import LaunchSpec
    from kkrunner.backends.support.messages import request_to_spec

    spec = replace(
        _spec(tmp_path), mpi_argv=("mpi-program", "input.inp"), mpi_uses_stdin=False, launch=LaunchSpec.from_mpi(3)
    )
    runner = LocalRunEndpoint(SupervisedBackend).create_job(spec, submission_key=RUN_ID)
    runner.prepare_submission()
    request = runner._request
    for key, value in spec.command_request().items():
        assert request[key] == __import__("json").loads(__import__("json").dumps(value))
    reconstructed = request_to_spec(request)
    assert reconstructed.mpi_uses_stdin is False
    assert reconstructed.mpi_argv == spec.mpi_argv


def test_terminal_project_runner_does_not_query_backend(monkeypatch, tmp_path):
    from kkrunner.client.job import Job
    from kkrunner.runs.base import RunState

    endpoint = LocalRunEndpoint(SupervisedBackend)
    runner = Job(
        endpoint, _spec(tmp_path),
        backend_id="old-supervisor", remote_directory=str(tmp_path),
        status={"state": "finished", "return_code": 0},
    )
    async def unexpected_status(*args, **kwargs):
        pytest.fail("A terminal job must not query the backend")
    monkeypatch.setattr(endpoint, "_status", unexpected_status)
    assert runner.state is RunState.FINISHED



def test_local_project_invalid_output_is_known_non_submission(tmp_path):
    """Output/cache collisions fail preflight without destroying existing files."""
    from kkrunner.runs.base import SubmissionNotStarted

    existing = tmp_path / "out.txt"
    extra = tmp_path / "other.out"
    existing.write_text("restart cache")
    extra.write_text("old output")
    spec = replace(_spec(tmp_path), cache_files=(str(existing),), output_files=(str(extra),))
    runner = LocalRunEndpoint(SupervisedBackend).create_job(spec, submission_key=RUN_ID)
    with pytest.raises(SubmissionNotStarted, match="conflicts with input/cache"):
        runner.submit()
    assert runner.state is RunState.FAILED
    assert runner.failure_phase is FailurePhase.SUBMISSION
    assert runner.backend_id is None
    assert existing.read_text() == "restart cache"
    assert extra.read_text() == "old output"
    assert recover_submission(RUN_ID) is None


def test_new_client_process_reopens_store_and_reconnects_to_running_calculation(tmp_path):
    import subprocess

    submit = """
import os, sys
from pathlib import Path
from kkrunner import ExecutionSpec, RunManager, RunStore
from kkrunner.backends import SupervisedBackend
from kkrunner.client.local import LocalRunEndpoint
root = Path(sys.argv[1])
manager = RunManager(RunStore(root / 'runs'))
command = '''
import time
from pathlib import Path
deadline = time.monotonic() + 20
while not Path('resume').exists():
    if time.monotonic() > deadline:
        raise SystemExit(99)
    time.sleep(0.01)
print(42)
'''
run = manager.create_run(LocalRunEndpoint(SupervisedBackend), ExecutionSpec(
    [sys.executable, '-c', command], directory=root, output_file='run.out'))
run.submit()
print(run.id, flush=True)
# Simulate abrupt client termination without running monitor cleanup.
os._exit(0)
"""
    reconnect = """
import sys
from pathlib import Path
from kkrunner import RunManager, RunState, RunStore
root = Path(sys.argv[1])
manager = RunManager(RunStore(root / 'runs'))
record = manager.store.get(sys.argv[2])
assert manager.refresh([record])[0].state == RunState.RUNNING
run = manager.attach(record.id)
(root / 'resume').touch()
assert run.wait().return_code == 0
assert manager.store.get(record.id).state == RunState.FINISHED
assert (root / 'run.out').read_text() == '42\\n'
"""
    try:
        submitted = subprocess.run(
            [sys.executable, "-c", submit, str(tmp_path)], capture_output=True, text=True, check=True, timeout=10
        )
        run_id = submitted.stdout.strip()
        assert len(run_id) == 32, submitted
        subprocess.run([sys.executable, "-c", reconnect, str(tmp_path), run_id], check=True, timeout=25)
    finally:
        (tmp_path / "resume").touch()


def test_managed_monitor_preserves_malformed_protocol_error(tmp_path, monkeypatch):
    """A terminal backend does not erase the local observation failure."""
    import asyncio
    from kkrunner.remote.stream import StreamProtocolError

    runner = LocalRunEndpoint(SupervisedBackend).create_job(_spec(tmp_path), submission_key=RUN_ID)
    runner._backend_id = "opaque-target-id"
    runner._remote_directory = str(tmp_path)

    async def follow(*args, **kwargs):
        raise StreamProtocolError("Unknown supervisor stream protocol")

    async def status(*args, **kwargs):
        return {"state": "finished", "return_code": 0}

    monkeypatch.setattr(runner.context, "_follow", follow)
    monkeypatch.setattr(runner.context, "_status", status)
    asyncio.run(runner._monitor())
    assert runner.state is RunState.FINISHED
    assert isinstance(runner.output_error, StreamProtocolError)


def test_unnameable_file_declared_for_local_managed_is_known_prelaunch_error(tmp_path):
    from kkrunner.runs.base import SubmissionNotStarted

    spec = replace(_spec(tmp_path), output_files=(object(),))
    job = LocalRunEndpoint(SupervisedBackend).create_job(spec, submission_key=RUN_ID)
    with pytest.raises(SubmissionNotStarted, match="named file"):
        job.submit()
    assert job.backend_id is None
    assert job.state is RunState.FAILED


def test_local_follow_uses_psij_backend_with_original_job_identity(tmp_path, monkeypatch):
    import asyncio
    from kkrunner.backends import PsijBackend
    from kkrunner.client.local import LocalRunEndpoint
    from kkrunner.remote.handles import encode_handle

    calls = []

    async def follow(job_ref, stdout, stderr, **options):
        calls.append((job_ref, options))
        return {"state": "running", "return_code": None}

    monkeypatch.setattr(PsijBackend, "follow", staticmethod(follow))
    endpoint = LocalRunEndpoint(PsijBackend, backend_options={"executor_name": "pbs", "url": "local"})
    identity = encode_handle({"kind": "psij", "executor_name": "pbs", "url": "local", "native_id": "native-id"})
    result = asyncio.run(endpoint._follow(
        str(tmp_path), identity, on_output=lambda *_: None,
        offsets={"out": 4, "err": None}, stdout_path=str(tmp_path / "run.out"),
    ))
    assert result["state"] == "running"
    assert calls == [(identity, {
        "stdout_offset": 4, "stderr_offset": None,
        "stdout_path": str(tmp_path / "run.out"), "stderr_path": None,
    })]


def test_local_monitor_uses_in_band_follow_terminal_before_scheduler_status(tmp_path, monkeypatch):
    import asyncio
    from kkrunner.client.local import LocalRunEndpoint
    from kkrunner.backends import SupervisedBackend

    job = LocalRunEndpoint(SupervisedBackend).create_job(_spec(tmp_path))
    job._backend_id = "opaque-psij-id"
    job._remote_directory = str(tmp_path)

    async def follow(*args, **kwargs):
        return {"state": "finished", "return_code": 0}

    async def never_poll(*args, **kwargs):
        raise AssertionError("FOLLOW already supplied authoritative terminal facts")

    monkeypatch.setattr(job.context, "_follow", follow)
    monkeypatch.setattr(job.context, "_status", never_poll)
    asyncio.run(job._monitor())
    assert job.state is RunState.FINISHED
    assert job.return_code == 0
