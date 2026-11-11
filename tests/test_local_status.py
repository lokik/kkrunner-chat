"""Process backend status and recoverable submission identity."""

from types import SimpleNamespace

import pytest

from kkrunner import RunState
from kkrunner.backends import SupervisedProcessBackend
from kkrunner.backends.contracts import RecoveryDisposition, SubmissionContext
from kkrunner.client.contracts import JobAttachment
from kkrunner.remote.handles import encode_handle
from kkrunner.client import local_supervised as local_project
from kkrunner.backends import supervised as process_operations
from kkrunner.client.endpoint import ExecutionEndpointConfig
from kkrunner.files import FileEntry, FileManifest
from kkrunner.runs import Execution, FailurePhase, RunRecord, WorkingDirectory


def test_disappeared_local_process_is_unknown(monkeypatch):
    executor = SupervisedProcessBackend()
    handle = encode_handle({
        "kind": "process",
        "identity": {"pid": 12345, "start_time": 0, "time_basis": "linux-boot"},
        "receipt_id": "a" * 32,
        "socket_path": "/missing/socket",
    })
    record = RunRecord(
        id="run",
        state=RunState.RUNNING,
        created_at="2026-10-13T08:00:00+00:00",
        updated_at="2026-10-13T08:00:00+00:00",
        execution=Execution(ExecutionEndpointConfig(executor="local-supervised"), WorkingDirectory("/tmp"), handle),
        files=FileManifest(),
        metadata={"test": {"task": "SCF"}},
        processing_required=True,
    )
    monkeypatch.setattr(process_operations, "_process_outcome", lambda receipt_id: None)
    monkeypatch.setattr(process_operations.ProcessIdentity, "read", classmethod(lambda cls, pid: None))
    from kkrunner.client.run_parts import backend_job_context
    (status,) = executor.status(backend_job_context(JobAttachment(record.execution, record.files), "process"))
    assert status.state == RunState.UNKNOWN


def test_lost_is_not_a_run_state():
    with pytest.raises(ValueError):
        RunState("lost")


@pytest.mark.parametrize(
    "state, phase, stream_state",
    [
        (RunState.RUNNING, None, None),
        (RunState.UNKNOWN, None, None),
        (RunState.FINISHED, None, "available"),
        (RunState.CANCELLED, None, "available"),
        (RunState.FAILED, FailurePhase.EXECUTION, "available"),
        (RunState.FAILED, FailurePhase.LAUNCH, "skipped"),
        (RunState.FAILED, FailurePhase.ARTIFACTS, "failed"),
    ],
)
def test_local_status_projection_and_identity_recovery_are_independent(
    monkeypatch, tmp_path, state, phase, stream_state,
):
    output = tmp_path / "run.out"
    output.write_bytes(b"partial output")
    files = FileManifest(stdout=FileEntry(local_path=str(output), availability_state="pending"))
    facts = {
        "state": state.value,
        "return_code": 0 if state == RunState.FINISHED else 7,
        "error": "backend message",
        "failure_phase": None if phase is None else phase.value,
    }
    backend_id = "opaque-backend-reference"
    monkeypatch.setattr(
        local_project,
        "status_remote",
        lambda identity: facts if identity == backend_id else pytest.fail("Unexpected backend identity"),
    )
    queried = local_project.local_status(backend_id)
    from kkrunner.files.availability import local_manifest
    projected = local_manifest(files, queried) if state.is_terminal else None
    monkeypatch.setattr(
        process_operations, "_recover_impl",
        lambda run_id, request: (
            {"remote_id": backend_id, "directory": str(tmp_path)}
            if run_id == "test-run" and request == {"executor": "process", "directory": str(tmp_path)}
            else pytest.fail("Unexpected recovery request")
        ),
    )
    submission = JobAttachment(
        Execution(ExecutionEndpointConfig(executor="local-supervised"), WorkingDirectory(str(tmp_path))),
        files,
    )
    recovered = SupervisedProcessBackend().recover_submission(
        "test-run", SubmissionContext(submission.execution),
    )
    assert recovered.disposition is RecoveryDisposition.FOUND
    assert recovered.ref.backend == "process"
    assert recovered.ref.token == backend_id
    assert queried.backend_id == backend_id
    assert queried.message == "backend message"
    assert queried.failure_phase == phase
    if stream_state is None:
        assert queried.files is None
        assert files.stdout.availability_state == "pending"
    else:
        assert queried.files is None
        assert projected.stdout.availability_state == stream_state
        assert files.stdout.availability_state == "pending"


def test_local_status_without_manifest_keeps_backend_facts(monkeypatch):
    facts = {"state": "finished", "return_code": 0, "error": None, "failure_phase": None}
    monkeypatch.setattr(local_project, "status_remote", lambda backend_id: facts)
    status = local_project.local_status("opaque")
    assert status.state == RunState.FINISHED
    assert status.backend_id == "opaque"
    assert status.files is None
