import pytest

from kkrunner.files import FileEntry, FileManifest
from kkrunner import RunState, RunStore
from kkrunner.runs import RunStatus
from kkrunner.runs.manager import RunManager
from kkrunner.backends import DirectBackend, SupervisedBackend
from kkrunner.client import ExecutionClient


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

    def acknowledge(record):
        assert manager.store.get(record.id).state == terminal_state
        calls.append(record.id)

    monkeypatch.setattr(client, "acknowledge", acknowledge)
    manager.register_client(client)
    record = manager.new_record(
        directory=tmp_path, target=client.target, backend=client.backend_config,
    )
    record = manager.transition(
        record, state=RunState.RUNNING, execution=record.execution.submitted("receipt"),
    )
    status = RunStatus(terminal_state, backend_id="receipt", return_code=0)
    monkeypatch.setattr(client, "status", lambda *runs: (status,))
    monkeypatch.setattr(client, "acknowledge_submission", lambda run: None)
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
        ExecutionSpec(["true"], directory=tmp_path), persistent=False,
        callback=completed,
    )
    run.submit()
    assert ended.wait(3), "completion callback deadlocked waiting for its own completion event"
    assert results == [run.result]


def test_failed_ack_is_reconciled_without_changing_terminal_outcome(tmp_path, monkeypatch):
    manager = RunManager(RunStore(tmp_path / "runs"))
    calls = []
    client = ExecutionClient(SupervisedBackend)

    def acknowledge(record):
        calls.append(record.id)
        if len(calls) == 1:
            raise OSError("temporarily disconnected")

    monkeypatch.setattr(client, "acknowledge", acknowledge)
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
    monkeypatch.setattr(client, "acknowledge", lambda record: acknowledgements.append(record.id))
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
    monkeypatch.setattr(client, "acknowledge_submission", lambda record: None)
    monkeypatch.setattr(client, "acknowledge", lambda record: None)
    manager = RunManager(RunStore(tmp_path / "runs"))
    run = client.create_run(ExecutionSpec(("instant",), directory=tmp_path), persistent=manager,
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


@pytest.mark.parametrize("operation", ("submit", "start"))
def test_raw_terminal_state_is_not_persisted_before_run_completion(tmp_path, monkeypatch, operation):
    run, manager = _tracked_test_run(
        tmp_path, monkeypatch, process_result=lambda outcome: outcome,
    )
    job = run._job
    monkeypatch.setattr(job, "wait", lambda: job.result)
    if operation == "submit":
        monkeypatch.setattr(job, "submit", lambda: _accept_job(job))
    else:
        # A reattached Run has already committed a job reference.
        manager.store.save(run.record)
        run._record = manager.transition(
            run.record, state=RunState.RUNNING,
            execution=run.record.execution.submitted("native-1"),
        )
        _accept_job(job, state=RunState.RUNNING)
        monkeypatch.setattr(job, "start", lambda: _accept_job(job))
    # Hold the client completion monitor, then drive its result manually.
    monkeypatch.setattr(run, "_start_completion", lambda: setattr(run, "_started", True))
    getattr(run, operation)()
    assert manager.store.get(run.id).state == RunState.RUNNING
    run._finish()
    assert manager.store.get(run.id).state == RunState.FINISHED


def test_confirmed_identity_is_durable_before_client_monitor_starts(tmp_path, monkeypatch):
    run, manager = _tracked_test_run(tmp_path, monkeypatch)
    monkeypatch.setattr(run._job, "submit", lambda: _accept_job(run._job))
    observed = []

    def start_monitor():
        observed.append(manager.store.get(run.id))
        run._started = True

    monkeypatch.setattr(run, "_start_completion", start_monitor)
    run.submit()
    assert len(observed) == 1
    assert observed[0].state == RunState.SUBMITTING
    assert observed[0].execution.backend_id == "native-1"
    assert manager.store.get(run.id).state == RunState.RUNNING


def test_missing_submit_identity_does_not_start_completion(tmp_path, monkeypatch):
    from kkrunner.backends.contracts import JobStatus

    run, manager = _tracked_test_run(tmp_path, monkeypatch)
    monkeypatch.setattr(
        run._job, "submit",
        lambda: run._job._apply_status(JobStatus(RunState.UNKNOWN)),
    )
    monkeypatch.setattr(
        run, "_start_completion",
        lambda: pytest.fail("Completion must not start without a job identity"),
    )
    with pytest.raises(RuntimeError, match="without a job identity"):
        run.submit()
    assert not run._started
    saved = manager.store.get(run.id)
    assert saved.state == RunState.SUBMITTING
    assert saved.execution.backend_id is None


def test_failed_monitor_thread_start_keeps_confirmed_identity(tmp_path, monkeypatch):
    class FailingThread:
        def __init__(self, *, target, daemon):
            pass

        def start(self):
            raise RuntimeError("client thread unavailable")

    run, manager = _tracked_test_run(tmp_path, monkeypatch)
    monkeypatch.setattr(run._job, "submit", lambda: _accept_job(run._job))
    monkeypatch.setattr("kkrunner.runs.run.Thread", FailingThread)
    with pytest.raises(RuntimeError, match="client thread unavailable"):
        run.submit()
    assert not run._started
    assert run._thread is None
    saved = manager.store.get(run.id)
    assert saved.state == RunState.UNKNOWN
    assert saved.execution.backend_id == "native-1"


def test_known_backend_identity_survives_failed_client_submission(tmp_path, monkeypatch):
    run, manager = _tracked_test_run(tmp_path, monkeypatch)

    def accepted_but_reply_lost():
        _accept_job(run._job)
        raise ConnectionError("client could not start observer")

    monkeypatch.setattr(run._job, "submit", accepted_but_reply_lost)
    with pytest.raises(ConnectionError, match="could not start observer"):
        run.submit()
    saved = manager.store.get(run.id)
    assert saved.execution.backend_id == "native-1"
    assert saved.state == RunState.UNKNOWN
    assert not saved.state.is_terminal


def test_submission_cleanup_failure_cannot_hide_pre_submit_error_or_skip_persistence(
    tmp_path, monkeypatch, caplog,
):
    from kkrunner.runs.base import FailurePhase, SubmissionNotStarted

    original = SubmissionNotStarted("submission never reached backend")
    run, manager = _tracked_test_run(tmp_path, monkeypatch)
    close_calls = []

    def failed_cleanup():
        close_calls.append(True)
        raise OSError("cleanup also failed")

    def failed_submit():
        raise original

    monkeypatch.setattr(run._job, "submit", failed_submit)
    monkeypatch.setattr(run._parts, "close", failed_cleanup)
    with pytest.raises(SubmissionNotStarted) as caught:
        run.submit()
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

    def accepted_but_reply_lost():
        _accept_job(run._job)
        raise ConnectionError("lost reply")

    monkeypatch.setattr(run._job, "submit", accepted_but_reply_lost)
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
        run._complete_record(record, state=RunState.FINISHED, return_code=0,
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
    monkeypatch.setattr(SshTarget, "get_file", LocalTarget.get_file)
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
