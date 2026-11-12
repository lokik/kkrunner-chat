"""Process backend status and recoverable submission identity."""

from kkrunner.client import EndpointClient, LocalTransport
from kkrunner.backends.config import BackendConfig
from types import SimpleNamespace

import pytest

from kkrunner import RunState
from kkrunner.backends import SupervisedBackend
from kkrunner.backends.contracts import RecoveryDisposition
from kkrunner.client.contracts import SubmissionContext
from kkrunner.client.contracts import JobAttachment
from kkrunner.remote.handles import encode_handle
from kkrunner.backends.supervised import backend as process_operations
from kkrunner.files import FileEntry, FileManifest
from kkrunner.runs import ExecutionMetadata, FailurePhase, RunRecord, WorkingDirectory


def test_disappeared_local_process_is_unknown(monkeypatch):
    executor = EndpointClient(SupervisedBackend)
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
        execution=ExecutionMetadata(LocalTransport(), WorkingDirectory("/tmp"), handle, backend=BackendConfig("process")),
        files=FileManifest(),
        metadata={"test": {"task": "SCF"}},
        processing_required=True,
    )
    monkeypatch.setattr(process_operations, "_process_outcome", lambda receipt_id: None)
    monkeypatch.setattr(process_operations.ProcessIdentity, "read", classmethod(lambda cls, pid: None))
    (status,) = executor.status(JobAttachment(record.execution, record.files))
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
        (RunState.FAILED, FailurePhase.OUTPUT, "failed"),
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
        SupervisedBackend,
        "status",
        staticmethod(lambda identity: facts if identity == backend_id else pytest.fail("Unexpected backend identity")),
    )
    endpoint = EndpointClient(SupervisedBackend)
    queried = endpoint.status(JobAttachment(
        ExecutionMetadata(LocalTransport(), WorkingDirectory(str(tmp_path)), backend_id, backend=BackendConfig("process")), files,
    ))[0]
    from kkrunner.files.availability import local_manifest
    projected = local_manifest(files, queried) if state.is_terminal else None
    def recover(run_id, request, **options):
        assert run_id == "test-run"
        assert request == {
            "backend": "process",
            "backend_options": {},
            "directory": str(tmp_path),
        }
        return {"remote_id": backend_id, "directory": str(tmp_path)}

    monkeypatch.setattr(SupervisedBackend, "recover", staticmethod(recover))
    submission = JobAttachment(
        ExecutionMetadata(LocalTransport(), WorkingDirectory(str(tmp_path)), backend=BackendConfig("process")),
        files,
    )
    recovered = EndpointClient(SupervisedBackend).recover_submission(
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


def test_local_status_without_manifest_keeps_backend_facts(monkeypatch, tmp_path):
    facts = {"state": "finished", "return_code": 0, "error": None, "failure_phase": None}
    monkeypatch.setattr(SupervisedBackend, "status", staticmethod(lambda backend_id: facts))
    attachment = JobAttachment(
        ExecutionMetadata(LocalTransport(), WorkingDirectory(str(tmp_path)), "opaque", backend=BackendConfig("process")),
        FileManifest(),
    )
    status = EndpointClient(SupervisedBackend).status(attachment)[0]
    assert status.state == RunState.FINISHED
    assert status.backend_id == "opaque"
    assert status.files is None
