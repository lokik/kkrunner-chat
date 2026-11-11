import pytest

from kkrunner.files import FileEntry, FileManifest
from kkrunner import RunState, RunStore
from kkrunner.runs import RunStatus
from kkrunner.runs.manager import RunManager


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

    class Backend:
        def acknowledge(self, record):
            assert manager.store.get(record.id).state == terminal_state
            calls.append(record.id)

    monkeypatch.setattr(manager, "executor_for", lambda record: Backend())
    record = manager.new_record(directory=tmp_path)
    manager.store.save(record)
    finished = manager.update(record, RunStatus(terminal_state, backend_id="receipt", return_code=0))
    assert finished.state == terminal_state
    assert calls == [record.id]


def test_completion_callback_can_read_its_own_finished_result(tmp_path):
    from threading import Event

    from kkrunner import ExecutionSpec, Run
    from kkrunner.client.local import LocalRunEndpoint

    ended, results = Event(), []
    run = None

    def completed(outcome):
        results.append(run.wait())
        ended.set()

    run = Run.direct(LocalRunEndpoint(), ExecutionSpec(["true"], directory=tmp_path), callback=completed)
    run.submit()
    assert ended.wait(3), "completion callback deadlocked waiting for its own completion event"
    assert results == [run.result]


def test_failed_ack_is_reconciled_without_changing_terminal_outcome(tmp_path, monkeypatch):
    manager = RunManager(RunStore(tmp_path / "runs"))
    calls = []

    class Backend:
        def acknowledge(self, record):
            calls.append(record.id)
            if len(calls) == 1:
                raise OSError("temporarily disconnected")

    monkeypatch.setattr(manager, "executor_for", lambda record: Backend())
    record = manager.new_record(directory=tmp_path)
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
    backend = type("Backend", (), {"acknowledge": lambda self, record: acknowledgements.append(record.id)})()
    monkeypatch.setattr(manager, "executor_for", lambda record: backend)
    path = tmp_path / "out"
    record = manager.new_record(
        directory=tmp_path,
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


class _ImmediatelyFinishedJob:
    """Fake backend that finishes before Run persists its submitted identity."""

    state = RunState.NEW
    backend_id = None
    exception = None
    failure_phase = None
    failure_message = None
    return_code = 0
    output_error = None

    def __init__(self):
        from kkrunner import ExecutionSpec
        self.spec = ExecutionSpec(("instant",))
        self.result = None

    def submit(self):
        self.backend_id = "native-1"
        self.state = RunState.FINISHED

    def start(self):
        self.state = RunState.FINISHED

    def wait(self):
        from kkrunner import ExecutionOutcome
        return ExecutionOutcome(return_code=0)


@pytest.mark.parametrize("operation", ["submit", "start"])
def test_raw_terminal_state_is_not_persisted_before_run_completion(tmp_path, monkeypatch, operation):
    from kkrunner import Run

    class Backend:
        name = "instant"

        def acknowledge(self, record):
            pass

    manager = RunManager(RunStore(tmp_path / "runs"))
    backend = Backend()
    job = _ImmediatelyFinishedJob()
    if operation == "start":
        job.backend_id = "native-1"
    record = manager.new_record(directory=tmp_path, processing_required=True)
    if operation == "start":
        record = manager.transition(
            record, state=RunState.RUNNING,
            execution=record.execution.submitted(job.backend_id),
        )
    manager.register_executor(backend, record.execution.endpoint)
    run = Run(
        job, backend=backend, manager=manager, record=record,
        process_result=lambda outcome: outcome,
    )
    # Freeze the monitor at the point where its creation normally races with
    # persistence, then explicitly deliver its result after checking the store.
    monkeypatch.setattr(run, "_start_completion", lambda: setattr(run, "_started", True))
    getattr(run, operation)()
    assert manager.store.get(run.id).state == RunState.RUNNING
    run._finish()
    assert manager.store.get(run.id).state == RunState.FINISHED


def test_confirmed_identity_is_durable_before_client_monitor_starts(tmp_path, monkeypatch):
    from kkrunner import Run

    class Backend:
        name = "instant"

    manager = RunManager(RunStore(tmp_path / "runs"))
    backend = Backend()
    job = _ImmediatelyFinishedJob()
    record = manager.new_record(directory=tmp_path)
    manager.register_executor(backend, record.execution.endpoint)
    run = Run(job, backend=backend, manager=manager, record=record)
    seen = []

    def start_monitor():
        seen.append(manager.store.get(run.id))
        run._started = True

    monkeypatch.setattr(run, "_start_completion", start_monitor)
    run.submit()
    assert len(seen) == 1
    assert seen[0].state == RunState.SUBMITTING
    assert seen[0].execution.backend_id == "native-1"
    assert manager.store.get(run.id).state == RunState.RUNNING


def test_missing_submit_identity_does_not_start_completion(tmp_path, monkeypatch):
    from kkrunner import Run

    class Backend:
        name = "unconfirmed"

    class Job:
        state = RunState.NEW
        backend_id = None
        exception = None
        failure_phase = None
        failure_message = None

        def submit(self):
            self.state = RunState.UNKNOWN

    manager = RunManager(RunStore(tmp_path / "runs"))
    backend = Backend()
    record = manager.new_record(directory=tmp_path)
    manager.register_executor(backend, record.execution.endpoint)
    run = Run(Job(), backend=backend, manager=manager, record=record)
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
    from kkrunner import Run

    class Backend:
        name = "monitor-error"

    class FailingThread:
        def __init__(self, *, target, daemon):
            pass

        def start(self):
            raise RuntimeError("client thread unavailable")

    manager = RunManager(RunStore(tmp_path / "runs"))
    backend = Backend()
    record = manager.new_record(directory=tmp_path)
    manager.register_executor(backend, record.execution.endpoint)
    run = Run(_ImmediatelyFinishedJob(), backend=backend, manager=manager, record=record)

    monkeypatch.setattr("kkrunner.runs.run.Thread", FailingThread)
    with pytest.raises(RuntimeError, match="client thread unavailable"):
        run.submit()
    assert not run._started
    assert run._thread is None
    saved = manager.store.get(run.id)
    assert saved.state == RunState.UNKNOWN
    assert saved.execution.backend_id == "native-1"


def test_known_backend_identity_survives_failed_client_submission(tmp_path):
    from kkrunner import Run

    class Backend:
        name = "interrupted"

    class Job:
        state = RunState.NEW
        backend_id = None
        exception = None
        failure_phase = None
        failure_message = None

        def submit(self):
            self.backend_id = "accepted-job"
            self.state = RunState.FINISHED
            raise ConnectionError("client could not start observer")

    manager = RunManager(RunStore(tmp_path / "runs"))
    backend = Backend()
    manager.register_executor(backend)
    record = manager.new_record(directory=tmp_path)
    run = Run(Job(), backend=backend, manager=manager, record=record)
    with pytest.raises(ConnectionError, match="could not start observer"):
        run.submit()
    saved = manager.store.get(record.id)
    assert saved.execution.backend_id == "accepted-job"
    assert saved.state == RunState.UNKNOWN
    assert not saved.state.is_terminal


def test_submission_cleanup_failure_cannot_hide_pre_submit_error_or_skip_persistence(tmp_path, caplog):
    from kkrunner import ExecutionSpec, Run
    from kkrunner.files import FileManifest
    from kkrunner.runs.base import FailurePhase, SubmissionNotStarted

    original = SubmissionNotStarted("submission never reached backend")

    class Backend:
        name = "preflight"

    class Job:
        state = RunState.NEW
        backend_id = None
        failure_phase = None
        failure_message = None
        exception = None

        def configure_submission(self, request, **observation):
            pass

        def submit(self):
            raise original

    class Files:
        def __init__(self):
            self.spec = ExecutionSpec(("unused",), directory=tmp_path)
            self.files = FileManifest()
            self.close_calls = 0

        def prepare_submission(self):
            pass

        def execution(self, backend_id):
            return None

        def prepare_job(self):
            return {"directory": str(tmp_path)}

        def observation(self):
            return {"stream_offsets": {}, "stream_writer": None, "stream_paths": (None, None)}

        def close(self):
            self.close_calls += 1
            raise OSError("cleanup also failed")

    manager = RunManager(RunStore(tmp_path / "runs"))
    backend = Backend()
    manager.register_executor(backend)
    record = manager.new_record(directory=tmp_path)
    files = Files()
    run = Run(Job(), backend=backend, manager=manager, record=record, file_session=files)

    with pytest.raises(SubmissionNotStarted) as caught:
        run.submit()
    assert "cleanup also failed" in caplog.text
    assert caught.value is original
    assert files.close_calls == 1
    saved = manager.store.get(run.id)
    assert saved.state == RunState.FAILED
    assert saved.failure_phase == FailurePhase.SUBMISSION
    assert "submission never reached backend" in saved.message
