from subprocess import CalledProcessError

from unittest.mock import AsyncMock

import pytest

from kkrunner.files import FileEntry, FileManifest
from kkrunner import RunState, RunStore
from kkrunner.runs import RunStatus
from kkrunner.runs.manager import RunManager
from kkrunner.backends import DirectBackend, SupervisedBackend
from kkrunner.client import ExecutionClient

from .transfer_helpers import LoopbackFileTransfer


@pytest.mark.parametrize("optional", [False, True])
def test_missing_output_respects_optionality(tmp_path, optional):
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = manager.new_record(
        directory=tmp_path,
        files=FileManifest(outputs=(FileEntry(local_path=str(tmp_path / "never-created"), optional=optional),)),
    )
    record = manager.complete(record, state=RunState.FINISHED, return_code=0, artifacts_available=True)
    assert record.state == (RunState.FINISHED if optional else RunState.FAILED)


@pytest.mark.parametrize("terminal_state", [RunState.FINISHED, RunState.FAILED, RunState.CANCELLED])
def test_refresh_acknowledges_terminal_receipt_after_commit(tmp_path, monkeypatch, terminal_state):
    manager = RunManager(RunStore(tmp_path / "runs"))
    calls = []
    client = ExecutionClient(SupervisedBackend)

    async def acknowledge(record):
        assert manager.store.get(record.id).state == terminal_state
        calls.append(record.id)

    monkeypatch.setattr(client, "acknowledge_async", acknowledge)
    manager.register_client(client)
    record = manager.new_record(
        directory=tmp_path, target=client.target, backend=client.backend_config,
    )
    record = manager.transition(
        record, state=RunState.RUNNING, execution=record.execution.submitted("receipt"),
    )
    status = RunStatus(terminal_state, backend_id="receipt", return_code=0)
    monkeypatch.setattr(client, "status_async", AsyncMock(side_effect=lambda *runs: (status,)))
    monkeypatch.setattr(client, "acknowledge_submission_async", AsyncMock(side_effect=lambda run: None))
    (finished,) = manager.refresh((record,))
    assert finished.state == terminal_state
    assert calls == [record.id]


def test_completion_callback_can_read_its_own_finished_result(tmp_path):
    from threading import Event

    from kkrunner import ExecutionSpec, Run

    ended, results = Event(), []
    run = None

    def completed(outcome):
        results.append(run.wait())
        ended.set()

    run = ExecutionClient(DirectBackend).create_run(
        ExecutionSpec(["true"], directory=tmp_path), tracked=False,
        callback=completed,
    )
    run.run()
    assert ended.is_set()
    assert results == [run.result]


@pytest.mark.parametrize("error", [OSError("temporarily disconnected"), CalledProcessError(255, ["ssh"])])
def test_failed_ack_is_reconciled_without_changing_terminal_outcome(tmp_path, monkeypatch, error):
    manager = RunManager(RunStore(tmp_path / "runs"))
    calls = []
    client = ExecutionClient(SupervisedBackend)

    async def acknowledge(record):
        calls.append(record.id)
        if len(calls) == 1:
            raise error

    monkeypatch.setattr(client, "acknowledge_async", acknowledge)
    manager.register_client(client)
    record = manager.new_record(
        directory=tmp_path, target=client.target, backend=client.backend_config,
    )
    record = manager.complete(
        record, state=RunState.FINISHED, return_code=0, execution=record.execution.submitted("receipt")
    )
    assert record.state == RunState.FINISHED
    assert manager.reconcile_acknowledgements() == (record.id,)
    assert calls == [record.id, record.id]
    assert manager.store.get(record.id) == record


def test_failed_artifact_observation_prevents_processing_success(tmp_path):
    from kkrunner.files import ArtifactAvailabilityState

    # A callback cannot publish FINISHED after a concurrent artifact failure.
    manager = RunManager(RunStore(tmp_path / "runs"))
    path = tmp_path / "out"
    path.write_text("answer")
    record = manager.new_record(
        directory=tmp_path, processing_required=True, files=FileManifest(stdout=FileEntry(str(path)))
    )
    record = manager.complete(record, state=RunState.POSTPROCESSING, return_code=0, artifacts_available=True)

    def process(outcome):
        manager.update_artifact_availability(record, path, ArtifactAvailabilityState.FAILED, "lost artifact")
        return 42

    with pytest.raises(OSError, match="lost artifact"):
        manager.retry_postprocessing(record.id, process)
    assert manager.store.get(record.id).state != RunState.FINISHED


def test_download_retry_retains_backend_receipt_until_success(tmp_path, monkeypatch):
    from kkrunner.runs import FailurePhase

    manager = RunManager(RunStore(tmp_path / "runs"))
    acknowledgements = []
    client = ExecutionClient(SupervisedBackend)
    monkeypatch.setattr(client, "acknowledge_async", AsyncMock(side_effect=lambda record: acknowledgements.append(record.id)))
    manager.register_client(client)
    path = tmp_path / "out"
    record = manager.new_record(
        directory=tmp_path,
        target=client.target, backend=client.backend_config,
        remote_directory="/remote",
        files=FileManifest(stdout=FileEntry(str(path), "/remote/out", "failed", "download interrupted")),
    )
    failed = manager.complete(
        record,
        state=RunState.FAILED,
        return_code=0,
        failure_phase=FailurePhase.DOWNLOAD,
        execution=record.execution.submitted("receipt"),
    )
    assert acknowledgements == []
    assert manager.reconcile_acknowledgements() == ()
    downloading = manager.resume_download(failed)
    path.write_text("retried output")
    completed = manager.complete(downloading, state=RunState.FINISHED, return_code=0, artifacts_available=True)
    assert completed.state == RunState.FINISHED
    assert acknowledgements == [record.id]


@pytest.mark.parametrize("state", [RunState.FINISHED, RunState.FAILED, RunState.CANCELLED])
def test_attach_rejects_durable_terminal_record_before_backend_access(tmp_path, state):
    manager = RunManager(RunStore(tmp_path / "runs"))
    stale = manager.new_record(directory=tmp_path)
    manager.complete(stale, state=state, return_code=0)
    with pytest.raises(RuntimeError, match="already terminal"):
        manager.attach(stale)


def test_completed_durable_result_cannot_be_postprocessed_again(tmp_path):
    from kkrunner import ProcessingPending

    manager = RunManager(RunStore(tmp_path / "runs"))
    record = manager.new_record(directory=tmp_path, processing_required=True)
    record = manager.transition(record, state=RunState.RUNNING)
    completed = manager.complete(record, state=RunState.FINISHED, return_code=0)
    assert completed.state == RunState.FINISHED

    called = []
    with pytest.raises(ValueError, match="Only postprocessing failures"):
        manager.retry_postprocessing(completed.id, lambda outcome: called.append(outcome))
    assert called == []
    assert manager.store.get(completed.id).state == RunState.FINISHED


def _tracked_test_run(tmp_path, monkeypatch, *, process_result=None):
    """Use real Run/Job/FileSession, intercepting only the native submit boundary."""
    from kkrunner import ExecutionSpec
    client = ExecutionClient(SupervisedBackend)
    monkeypatch.setattr(client, "acknowledge_submission_async", AsyncMock(side_effect=lambda record: None))
    monkeypatch.setattr(client, "acknowledge_async", AsyncMock(side_effect=lambda record: None))
    manager = RunManager(RunStore(tmp_path / "runs"))
    run = client.create_run(ExecutionSpec(("instant",), directory=tmp_path), tracked=manager,
        process_result=process_result,
    )
    return run, manager


def _accept_job(job, *, identity="native-1", state=RunState.FINISHED):
    """Simulate backend acceptance without replacing the client Job itself."""
    from kkrunner.backends.contracts import JobRef, JobStatus
    job.execution.ref = JobRef("process", identity)
    job._apply_status(JobStatus(
        state, return_code=0 if state == RunState.FINISHED else None,
    ))


def test_confirmed_identity_is_durable_before_observation(tmp_path, monkeypatch):
    from kkrunner.backends.contracts import JobStatus

    run, manager = _tracked_test_run(tmp_path, monkeypatch, process_result=lambda outcome: outcome)
    job = run._job
    observed = []

    async def submit(request, on_output, *, output_requested=None):
        _accept_job(job)
        return job.status()

    async def observe(on_output, *, output_requested=None):
        observed.append(manager.store.get(run.id))
        return JobStatus(RunState.FINISHED, return_code=0)

    monkeypatch.setattr(job.execution, "submit", submit)
    monkeypatch.setattr(job.execution, "observe", observe)
    run.run()
    assert len(observed) == 1
    assert observed[0].state == RunState.FINISHING
    assert observed[0].execution.backend_id == "native-1"
    assert manager.store.get(run.id).state == RunState.FINISHED


def test_observation_interrupted_after_acceptance_is_not_submission_failure(tmp_path, monkeypatch):
    import asyncio

    run, manager = _tracked_test_run(tmp_path, monkeypatch,
                                     process_result=lambda outcome: pytest.fail("No confirmed outcome"))
    callbacks = []
    run.callback = callbacks.append

    async def submit(*args, **kwargs):
        _accept_job(run._job, state=RunState.RUNNING)
        return run._job.status()

    async def observe(*args, **kwargs):
        assert manager.store.get(run.id).execution.backend_id == "native-1"
        raise asyncio.CancelledError("observation interrupted")

    monkeypatch.setattr(run._job.execution, "submit", submit)
    monkeypatch.setattr(run._job.execution, "observe", observe)
    with pytest.raises(asyncio.CancelledError, match="observation interrupted"):
        run.run()
    saved = manager.store.get(run.id)
    assert saved.execution.backend_id == "native-1"
    assert saved.state == RunState.UNKNOWN
    assert saved.failure_phase is None
    assert callbacks == [None]
    owner = manager.store.observation_lock(saved)
    assert owner.acquire(blocking=False)
    owner.release()


def test_missing_submit_identity_does_not_start_observation(tmp_path, monkeypatch):
    from kkrunner.backends.contracts import ObservationUnavailable

    run, manager = _tracked_test_run(tmp_path, monkeypatch)

    async def submit(request, on_output, *, output_requested=None):
        raise ObservationUnavailable("Backend accepted no execution identity")

    async def observe(*args, **kwargs):
        pytest.fail("No identity to observe")

    monkeypatch.setattr(run._job.execution, "submit", submit)
    monkeypatch.setattr(run._job.execution, "observe", observe)
    with pytest.raises(ObservationUnavailable, match="no execution identity"):
        run.run()
    assert not run._started
    saved = manager.store.get(run.id)
    assert saved.state == RunState.SUBMITTING
    assert saved.execution.backend_id is None


def test_known_backend_identity_survives_failed_client_submission(tmp_path, monkeypatch):
    run, manager = _tracked_test_run(tmp_path, monkeypatch)

    async def accepted_but_reply_lost(*args, **kwargs):
        _accept_job(run._job)
        raise ConnectionError("client could not start observer")

    monkeypatch.setattr(run._job.execution, "submit", accepted_but_reply_lost)
    with pytest.raises(ConnectionError, match="could not start observer"):
        run.submit()
    saved = manager.store.get(run.id)
    assert saved.execution.backend_id == "native-1"
    assert saved.state == RunState.UNKNOWN
    assert not saved.state.is_terminal


@pytest.mark.parametrize("operation", ["run", "submit"])
def test_submission_cleanup_failure_cannot_hide_pre_submit_error_or_skip_persistence(
    tmp_path, monkeypatch, caplog, operation,
):
    from kkrunner.runs.base import FailurePhase, SubmissionNotStarted

    original = SubmissionNotStarted("submission never reached backend")
    run, manager = _tracked_test_run(tmp_path, monkeypatch)
    close_calls = []

    def failed_cleanup():
        close_calls.append(True)
        raise OSError("cleanup also failed")

    async def failed_submit(*args, **kwargs):
        raise original

    monkeypatch.setattr(run._job.execution, "submit", failed_submit)
    monkeypatch.setattr(run._parts, "close", failed_cleanup)
    with pytest.raises(SubmissionNotStarted) as caught:
        (run.run if operation == "run" else run.submit)()
    assert "cleanup also failed" in caplog.text
    assert caught.value is original
    assert len(close_calls) == 1
    saved = manager.store.get(run.id)
    assert saved.state == RunState.FAILED
    assert saved.failure_phase == FailurePhase.SUBMISSION
    assert "submission never reached backend" in saved.message


@pytest.mark.parametrize("operation", ["download", "processing"])
def test_historical_retry_uses_the_same_conflict_reservation(tmp_path, operation):
    from kkrunner import MemoryRunStore
    from kkrunner.runs import DuplicateSubmissionError, FailurePhase

    disk, memory = RunStore(tmp_path / "runs"), MemoryRunStore()
    manager = RunManager(disk, conflict_stores=(memory,))
    other = RunManager(memory, conflict_stores=(disk,))
    path = tmp_path / "out"
    path.write_text("answer")
    manifest = FileManifest(stdout=FileEntry(str(path)))
    record = manager.new_record(directory=tmp_path, files=manifest,
                                processing_required=operation == "processing")
    failed = manager.complete(
        record, state=RunState.FAILED, return_code=0,
        failure_phase=FailurePhase.POSTPROCESSING if operation == "processing" else FailurePhase.DOWNLOAD,
        execution=record.execution.submitted("receipt"), artifacts_available=operation == "processing",
    )
    active = other.reserve(other.new_record(directory=tmp_path, files=manifest), preprocessing=True)
    # A live conflicting reservation must not be recovered away.
    with memory.observation_lock(active):
        with pytest.raises(DuplicateSubmissionError):
            if operation == "processing":
                manager.retry_postprocessing(failed, lambda outcome: pytest.fail("parser started"))
            else:
                manager.resume_download(failed)
        assert disk.get(failed.id) == failed
    other.complete(active, state=RunState.FAILED)
    if operation == "processing":
        assert manager.retry_postprocessing(failed, lambda outcome: "parsed") == "parsed"
    else:
        assert manager.resume_download(failed).state == RunState.DOWNLOADING


def test_uncertain_submission_relinquishes_application_resources(tmp_path, monkeypatch):
    from kkrunner.runs.base import ExecutionResources

    run, manager = _tracked_test_run(tmp_path, monkeypatch)
    calls = []
    resources = ExecutionResources(on_detach=lambda: calls.append("detached"))
    run._owned_resources = resources

    async def accepted_but_reply_lost(*args, **kwargs):
        _accept_job(run._job)
        raise ConnectionError("lost reply")

    monkeypatch.setattr(run._job.execution, "submit", accepted_but_reply_lost)
    with pytest.raises(ConnectionError, match="lost reply"):
        run.submit()
    assert calls == ["detached"]
    assert resources.closed
    assert manager.store.get(run.id).state == RunState.UNKNOWN


@pytest.mark.parametrize("failure", ["capture", "persist"])
def test_completion_failure_relinquishes_handles_without_removing_evidence(tmp_path, monkeypatch, failure):
    from kkrunner.runs.base import ExecutionResources

    run, manager = _tracked_test_run(tmp_path, monkeypatch)
    calls = []
    resources = ExecutionResources(on_detach=lambda: calls.append("detach"))
    run._owned_resources = resources
    record = manager.reserve(run.record, preprocessing=True)
    path = tmp_path / "out"
    path.write_text("answer")
    manifest = FileManifest(stdout=FileEntry(str(path)))

    def fail(*args, **kwargs):
        raise OSError("history unavailable")

    if failure == "capture":
        monkeypatch.setattr(manager, "completion_fields", fail)
    else:
        monkeypatch.setattr(manager, "transition", fail)
    with pytest.raises(OSError, match="history unavailable"):
        run._persist_completion(record, state=RunState.FINISHED, return_code=0,
                             files=manifest, artifacts_available=True)
    assert calls == ["detach"]
    assert resources.closed
    assert path.read_text() == "answer"


@pytest.mark.parametrize("historical_input", [False, True])
def test_historical_retrieval_cannot_write_into_active_run(tmp_path, monkeypatch, historical_input):
    from kkrunner.runs import DuplicateSubmissionError
    from kkrunner.files import FileMapping
    from kkrunner.client import LocalTarget, SshTarget

    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    path = local / "data"
    source = remote / "data"
    source.write_text("historical")
    monkeypatch.setattr(SshTarget, "expand_files", LocalTarget.expand_files)
    monkeypatch.setattr(SshTarget, "get_file", LoopbackFileTransfer.get_file)
    client = ExecutionClient(target=SshTarget("loopback"))
    manager = RunManager(RunStore(tmp_path / "runs"))
    manager.register_client(client)
    entry = FileEntry(str(path), str(source))
    manifest = FileManifest(
        inputs=(entry,) if historical_input else (),
        stdout=None if historical_input else entry,
        mapping=FileMapping(str(local), str(remote)),
    )
    historical = manager.new_record(
        directory=local, remote_directory=remote, target=client.target,
        backend=client.backend_config, files=manifest,
    )
    historical = manager.complete(historical, state=RunState.FAILED,
                                  execution=historical.execution.submitted("historical"),
                                  artifacts_available=False)
    active = manager.new_record(directory=local, files=FileManifest(inputs=(FileEntry(str(path)),)))
    with manager.store.observation_lock(active):
        manager.reserve(active, preprocessing=True)
        with pytest.raises(DuplicateSubmissionError):
            manager.retrieve_result_files(historical)
        assert not path.exists()
        assert not tuple(local.iterdir())
        path.write_text("already available")
        assert manager.retrieve_result_files(historical) == historical
    assert path.read_text() == "already available"


@pytest.mark.parametrize("backend", ("direct", "psij"))
def test_receipt_reconciliation_does_not_create_client_for_backend_without_receipts(tmp_path, monkeypatch, backend):
    from kkrunner.backends.config import BackendConfig

    manager = RunManager(RunStore(tmp_path / "runs"))

    def unexpected(*args, **kwargs):
        pytest.fail("No backend receipt exists to acknowledge")

    monkeypatch.setattr(manager, "client_for", unexpected)
    record = manager.new_record(directory=tmp_path, backend=BackendConfig(backend))
    record = manager.complete(
        record, state=RunState.FINISHED, return_code=0,
        execution=record.execution.submitted("native-job"),
    )
    assert manager.reconcile_acknowledgements() == ()
    assert manager.store.get(record.id) == record


def test_failed_submission_ack_does_not_prevent_status_refresh(tmp_path, monkeypatch):
    manager = RunManager(RunStore(tmp_path / "runs"))
    client = ExecutionClient(SupervisedBackend)
    manager.register_client(client)
    record = manager.new_record(
        directory=tmp_path, target=client.target, backend=client.backend_config,
    )
    record = manager.transition(record, state=RunState.QUEUED,
                                execution=record.execution.submitted("receipt"))
    monkeypatch.setattr(client, "status_async", AsyncMock(side_effect=lambda *runs: (RunStatus(RunState.RUNNING, backend_id="receipt"),)))
    attempts = []

    async def acknowledge(run):
        attempts.append(run.id)
        if len(attempts) == 1:
            raise CalledProcessError(255, ["ssh"])

    monkeypatch.setattr(client, "acknowledge_submission_async", acknowledge)
    (running,) = manager.refresh((record,))
    assert running.state == RunState.RUNNING
    assert manager.refresh((running,))[0].state == RunState.RUNNING
    assert attempts == [record.id, record.id]
