"""Execution outcome and file availability are independent persisted facts."""

import pytest

from kkrunner.files import ArtifactAvailabilityState, FileEntry, FileManifest
from kkrunner.runs import FailurePhase
from kkrunner import RunState, RunStore
from kkrunner.runs.manager import RunManager
from kkrunner.targets import LocalTarget, SshTarget


def _record(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = manager.new_record(
        remote_directory="/remote",
        target=SshTarget("cluster"),
        directory=tmp_path,
        files=FileManifest(stdout=FileEntry(local_path=str(tmp_path / "run.out"), availability_state="pending")),
        metadata={"test": {"task": "DOS"}},
        processing_required=False,
    )
    record = manager.transition(record, state=RunState.DOWNLOADING)
    return (manager, record)


@pytest.mark.parametrize(
    "requested_state, return_code, phase, backend_error, transfer_error, artifacts_available, expected_state, expected_phase",
    [
        pytest.param(
            RunState.FINISHED,
            0,
            None,
            None,
            "SFTP connection lost",
            True,
            RunState.FAILED,
            FailurePhase.DOWNLOAD,
            id="successful-backend-failed-transfer",
        ),
        pytest.param(
            RunState.FAILED,
            17,
            FailurePhase.EXECUTION,
            "SPR-KKR returned 17",
            "diagnostic files unavailable",
            False,
            RunState.FAILED,
            FailurePhase.EXECUTION,
            id="execution-failure-overrides-transfer-error",
        ),
        pytest.param(
            RunState.CANCELLED,
            None,
            None,
            None,
            "download interrupted",
            False,
            RunState.CANCELLED,
            None,
            id="cancellation-preserves-outcome",
        ),
    ],
)
def test_backend_outcome_and_transfer_availability_are_independent(
    tmp_path,
    requested_state,
    return_code,
    phase,
    backend_error,
    transfer_error,
    artifacts_available,
    expected_state,
    expected_phase,
):
    manager, record = _record(tmp_path)
    failed_files = record.files.with_file_availability(str(tmp_path / "run.out"), "failed", transfer_error)
    saved = manager.complete(
        record,
        state=requested_state,
        return_code=return_code,
        failure_phase=phase,
        message=backend_error,
        artifacts_available=artifacts_available,
        files=failed_files,
    )
    assert manager.store.get(record.id) == saved
    assert saved.state is expected_state
    assert saved.failure_phase is expected_phase
    assert saved.return_code == return_code
    assert saved.message == (transfer_error if expected_phase is FailurePhase.DOWNLOAD else backend_error)
    assert saved.files.availability_state == ArtifactAvailabilityState.FAILED
    assert saved.files.availability_message == transfer_error
    assert saved.files.captured_at is None


def test_successful_run_captures_available_files(tmp_path):
    manager, record = _record(tmp_path)
    (tmp_path / "run.out").write_text("complete")
    available = record.files.with_file_availability(str(tmp_path / "run.out"), "available")
    saved = manager.complete(record, state=RunState.FINISHED, return_code=0, artifacts_available=True, files=available)
    assert saved.state is RunState.FINISHED
    assert saved.return_code == 0
    assert saved.files.availability_state == ArtifactAvailabilityState.AVAILABLE.value
    assert saved.files.captured_at is not None
    assert len(saved.files.output_versions) == 1


def test_resume_download_rejects_execution_failure_and_nonzero_code(tmp_path):
    manager, record = _record(tmp_path)
    bad = manager.complete(
        record, state=RunState.FAILED, return_code=17, failure_phase=FailurePhase.EXECUTION, artifacts_available=False
    )
    with pytest.raises(ValueError, match="Only failed downloads"):
        manager.resume_download(bad.id)


def test_retry_failed_download_preserves_backend_then_captures_clean_snapshot(tmp_path):
    manager, initial = _record(tmp_path)
    output = tmp_path / "run.out"
    output.write_bytes(b"partial")
    initial = manager.transition(
        initial, state=RunState.DOWNLOADING, execution=initial.execution.submitted("existing-backend")
    )
    failed_files = initial.files.with_file_availability(str(output), "failed", "interrupted transfer")
    failed = manager.complete(
        initial,
        state=RunState.FAILED,
        return_code=0,
        failure_phase=FailurePhase.DOWNLOAD,
        message="interrupted transfer",
        files=failed_files,
        artifacts_available=False,
    )
    resumed = manager.resume_download(initial.id)
    assert resumed.state is RunState.DOWNLOADING
    assert resumed.execution == failed.execution
    assert resumed.execution.backend_id == "existing-backend"
    assert resumed.files.stdout.availability_state == "pending"
    assert resumed.files.stdout.availability_message is None
    assert resumed.finished_at is None
    assert resumed.failure_phase is None
    assert output.read_bytes() == b"partial"
    assert manager.store.get(initial.id) == resumed
    output.write_bytes(b"complete")
    available = resumed.files.with_file_availability(str(output), "available")
    complete = manager.complete(
        resumed, state=RunState.FINISHED, return_code=0, files=available, artifacts_available=True
    )
    assert complete.state is RunState.FINISHED
    assert complete.files.failed_artifacts == ()
    assert complete.files.captured_at is not None
    assert len(complete.files.output_versions) == 1
    assert manager.output_file_is_current(complete)


def test_resume_download_retains_previously_available_artifacts(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    output = tmp_path / "stdout"
    other = tmp_path / "other"
    record = manager.new_record(
        directory=tmp_path,
        files=FileManifest(
            stdout=FileEntry(local_path=str(output), availability_state="available"),
            outputs=(
                FileEntry(
                    local_path=str(other), availability_state="failed", availability_message="temporary transport error"
                ),
            ),
        ),
        metadata={"test": {"task": "DOS"}},
        processing_required=True,
    )
    record = manager.transition(
        record, state=RunState.DOWNLOADING, execution=record.execution.submitted("existing-backend")
    )
    manager.complete(
        record, state=RunState.FAILED, return_code=0, failure_phase=FailurePhase.DOWNLOAD, artifacts_available=False
    )
    retried = manager.resume_download(record.id)
    assert retried.files.stdout.availability_state == "available"
    assert retried.files.outputs[0].availability_state == "pending"
    assert retried.files.outputs[0].availability_message is None



@pytest.mark.parametrize("state, code, phase", [
    (RunState.FAILED, 17, FailurePhase.EXECUTION),
    (RunState.CANCELLED, None, None),
])
def test_refresh_captures_local_diagnostics(tmp_path, monkeypatch, state, code, phase):
    from kkrunner.backends import SupervisedBackend
    from kkrunner.client import ExecutionClient

    manager = RunManager(RunStore(tmp_path / "runs"))
    client = ExecutionClient(SupervisedBackend)
    manager.register_client(client)
    path = tmp_path / "run.out"
    path.write_text("diagnostic")
    record = manager.new_record(directory=tmp_path, backend=client.backend_config,
        files=FileManifest(stdout=FileEntry(local_path=str(path), availability_state="pending")))
    record = manager.transition(record, state=RunState.RUNNING,
        execution=record.execution.submitted("native-job"))
    monkeypatch.setattr(SupervisedBackend, "status", staticmethod(lambda token: {
        "state": state.value, "return_code": code,
        "failure_phase": None if phase is None else phase.value,
    }))
    monkeypatch.setattr(SupervisedBackend, "acknowledge", staticmethod(lambda token: None))
    monkeypatch.setattr(SupervisedBackend, "acknowledge_submission", staticmethod(lambda token: None))
    (saved,) = manager.refresh((record,))
    assert saved.state is state
    assert saved.failure_phase is phase
    assert saved.return_code == code
    assert saved.files.captured_at is not None
    assert len(saved.files.output_versions) == 1


@pytest.mark.parametrize("state, code, phase", [
    (RunState.FAILED, 17, FailurePhase.EXECUTION),
    (RunState.CANCELLED, None, None),
])
def test_diagnostic_capture_error_preserves_native_outcome(tmp_path, monkeypatch, state, code, phase):
    from kkrunner.files import availability

    manager, record = _record(tmp_path)
    path = tmp_path / "run.out"
    path.write_text("diagnostic")
    files = record.files.with_file_availability(str(path), "available")

    def fail_sync(path):
        raise OSError("cannot synchronize diagnostic")

    monkeypatch.setattr(availability, "sync_file", fail_sync)
    saved = manager.complete(record, state=state, return_code=code, failure_phase=phase,
        message="native outcome", files=files, artifacts_available=True)
    assert saved.state is state
    assert saved.return_code == code
    assert saved.failure_phase is phase
    assert saved.message == "native outcome"
    assert saved.files.captured_at is None
    assert saved.files.stdout.availability_state == "available"


@pytest.mark.parametrize("operation", ("update", "refresh"))
def test_failed_launch_is_terminal_without_result_collection(tmp_path, monkeypatch, operation):
    from kkrunner.backends import SupervisedBackend
    from kkrunner.client import ExecutionClient, SshTarget
    from kkrunner.runs.base import RunStatus

    manager = RunManager(RunStore(tmp_path / "runs"))
    client = ExecutionClient(SupervisedBackend, target=SshTarget("cluster"))
    manager.register_client(client)
    output = tmp_path / "stale.out"
    output.write_text("old output")
    record = manager.new_record(
        directory=tmp_path, remote_directory="/remote",
        target=client.target, backend=client.backend_config,
        files=FileManifest(stdout=FileEntry(str(output), "/remote/stale.out")).initialize_availability(),
    )
    record = manager.transition(record, state=RunState.RUNNING,
                                execution=record.execution.submitted("launch-failed"))
    status = RunStatus(RunState.FAILED, return_code=127, failure_phase=FailurePhase.LAUNCH,
                       message="Executable not found", backend_id="launch-failed")
    monkeypatch.setattr(client, "status", lambda *args: (status,))
    monkeypatch.setattr(client, "acknowledge", lambda *args: None)
    monkeypatch.setattr(client, "collect_ended_files",
                        lambda *args: pytest.fail("No execution artifacts to retrieve"))
    saved = manager.update(record, status) if operation == "update" else manager.refresh((record,), retrieve_ended=True)[0]
    assert saved.state == RunState.FAILED
    assert saved.failure_phase == FailurePhase.LAUNCH
    assert saved.files.stdout.availability_state == "skipped"
    assert saved.files.captured_at is None
    assert not saved.files.output_versions
    assert output.read_text() == "old output"
    assert not manager.output_file_is_current(saved)
    output.unlink()
    monkeypatch.setattr(client, "resolve_result_files",
                        lambda *args: pytest.fail("No failed-launch output to restore"))
    assert manager.retrieve_result_files(saved) == saved


@pytest.mark.parametrize("mode, expected_phase", [
    ("local", FailurePhase.ARTIFACTS),
    ("shared", FailurePhase.ARTIFACTS),
    ("ssh", FailurePhase.DOWNLOAD),
])
def test_completion_failure_phase_follows_file_transfer_capability(tmp_path, mode, expected_phase):
    from kkrunner.files.manifest import FileMapping

    manager = RunManager(RunStore(tmp_path / "runs"))
    target = LocalTarget() if mode == "local" else SshTarget("cluster")
    mapping = None if mode == "local" else FileMapping(
        str(tmp_path), "/work", shared_filesystem=mode == "shared",
    )
    files = FileManifest(
        stdout=FileEntry(str(tmp_path / "stdout"), availability_state="failed",
                         availability_message="output unavailable"),
        mapping=mapping,
    )
    record = manager.new_record(
        directory=tmp_path, remote_directory=None if mode == "local" else "/work",
        target=target, files=files,
    )
    saved = manager.complete(record, state=RunState.FINISHED, return_code=0, files=files)
    assert saved.state == RunState.FAILED
    assert saved.failure_phase == expected_phase
    assert saved.message == "output unavailable"
