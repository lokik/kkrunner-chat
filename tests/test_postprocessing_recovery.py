"""Durable completion and retry ownership without application result readers."""

from threading import Event
from types import SimpleNamespace

import pytest

from kkrunner import ExecutionOutcome, RunState, RunStore
from kkrunner.runs import FailurePhase
from kkrunner.client import EndpointClient
from kkrunner.backends import SupervisedBackend
from kkrunner.backends.contracts import JobStatus
from kkrunner import ExecutionSpec
from kkrunner.runs.manager import RunManager


class _RawRunner:
    def __init__(self):
        self.state = RunState.NEW
        self.backend_id = None
        self.result = ExecutionOutcome(return_code=0)
        self.exception = None
        self.failure_phase = None
        self.failure_message = None
        self.output_error = None

    @property
    def return_code(self):
        return self.result.return_code

    @property
    def requires_stream_retrieval(self):
        return False

    def configure_detached(self, detached):
        pass

    def configure_submission(self, request, **observation):
        pass

    def status(self):
        return JobStatus(self.state, return_code=self.return_code)

    def submit(self):
        self.backend_id = "job-1"
        self.state = RunState.FINISHED

    def wait(self):
        return self.result

    def cancel(self):
        self.state = RunState.CANCELLED

    def add_output_listener(self, callback):
        pass

    def add_output_error_listener(self, callback):
        pass


class _Executor(EndpointClient):
    def __init__(self):
        super().__init__(SupervisedBackend)

    def acknowledge_submission(self, record):
        pass

    def acknowledge(self, record):
        pass

    def create_run_parts(self, spec, *, submission_key=None, callback=None):
        parts = super().create_run_parts(spec, submission_key=submission_key, callback=callback)
        self.runner = _RawRunner()
        self.runner.spec = spec
        parts.job = self.runner
        return parts


def test_finished_run_is_persisted_before_callback_without_wait(tmp_path):
    store = RunStore(tmp_path / "runs")
    manager = RunManager(store)
    finished = Event()
    run = manager.create_run(
        _Executor(),
        ExecutionSpec(argv=("unused",), directory=str(tmp_path)),
        process_result=lambda outcome: outcome,
        callback=lambda result: finished.set(),
        metadata={"example": {"task": "DOS".upper()}},
    )
    run.submit()
    assert finished.wait(timeout=5), "completion callback was not called"
    record = store.get(run.id)
    assert record.state == RunState.FINISHED
    assert record.finished_at is not None


def test_result_processor_failure_is_postprocessing_failure(tmp_path):
    store = RunStore(tmp_path / "runs")
    manager = RunManager(store)
    executor = _Executor()

    def fail(_outcome):
        raise ValueError("result parsing failed")

    run = manager.create_run(
        executor,
        ExecutionSpec(argv=("unused",), directory=str(tmp_path)),
        process_result=fail,
        metadata={"example": {"task": "DOS".upper()}},
    )
    run.submit()
    with pytest.raises(ValueError, match="result parsing failed"):
        run.wait()
    record = store.get(run.id)
    assert record.state == RunState.FAILED
    assert record.failure_phase == FailurePhase.POSTPROCESSING
    assert record.message == "result parsing failed"
    assert record.finished_at is not None


@pytest.mark.parametrize("during_retry", ["finished", "failed", "refreshed"])
def test_retry_does_not_overwrite_concurrent_record_update(tmp_path, during_retry):
    from kkrunner.files import FileEntry, FileManifest

    manager = RunManager(RunStore(tmp_path / "runs"))
    output = tmp_path / "run.out"
    output.write_text("saved data")
    record = manager.new_record(
        directory=tmp_path,
        files=FileManifest(stdout=FileEntry(local_path=str(output))),
        metadata={"example": {"task": "DOS".upper()}},
        processing_required=True,
    )
    record = manager.transition(
        record, state=RunState.RUNNING, execution=record.execution.submitted("existing-backend")
    )
    failed = manager.complete(
        record,
        state=RunState.FAILED,
        return_code=0,
        failure_phase=FailurePhase.POSTPROCESSING,
        artifacts_available=True,
    )

    def concurrent_change(outcome):
        current = manager.store.get(failed.id)
        if during_retry == "refreshed":
            manager.transition(current, state=RunState.POSTPROCESSING, message="external refresh")
        else:
            manager.complete(
                current,
                state=RunState.FINISHED if during_retry == "finished" else RunState.FAILED,
                return_code=0,
                message="external terminal update",
                failure_phase=None if during_retry == "finished" else FailurePhase.POSTPROCESSING,
            )
        return outcome

    # A competing observer cannot replace the processing owner's state,
    # even if it attempts to publish a terminal result mid-callback.
    manager.retry_postprocessing(failed.id, concurrent_change)
    latest = manager.store.get(failed.id)
    assert latest.state == RunState.FINISHED
    assert latest.message is None


def test_retry_exception_does_not_overwrite_concurrent_completion(tmp_path):
    from kkrunner.files import FileEntry, FileManifest

    manager = RunManager(RunStore(tmp_path / "runs"))
    output = tmp_path / "run.out"
    output.write_text("saved data")
    record = manager.new_record(
        directory=tmp_path,
        files=FileManifest(stdout=FileEntry(local_path=str(output))),
        metadata={"example": {"task": "DOS".upper()}},
        processing_required=True,
    )
    record = manager.transition(
        record, state=RunState.RUNNING, execution=record.execution.submitted("existing-backend")
    )
    failed = manager.complete(
        record,
        state=RunState.FAILED,
        return_code=0,
        failure_phase=FailurePhase.POSTPROCESSING,
        artifacts_available=True,
    )

    def interrupted(outcome):
        manager.complete(
            manager.store.get(failed.id), state=RunState.FINISHED, return_code=0, message="external success"
        )
        raise ValueError("parser error")

    with pytest.raises(ValueError, match="parser error"):
        manager.retry_postprocessing(failed.id, interrupted)
    current = manager.store.get(failed.id)
    assert current.state == RunState.FAILED
    assert current.failure_phase == FailurePhase.POSTPROCESSING
    assert current.message == "parser error"


@pytest.mark.parametrize("input_change", ("overwritten", "missing"))
def test_postprocessing_retry_accepts_unchanged_output_with_stale_input(tmp_path, input_change):
    """Captured output remains retryable when the original input is stale."""
    from kkrunner.files import FileEntry, FileManifest, FileState

    manager = RunManager(RunStore(tmp_path / "runs"))
    output = tmp_path / "result.out"
    source = tmp_path / "original.inp"
    output.write_bytes(b"captured result")
    source.write_bytes(b"original input")
    record = manager.new_record(
        directory=tmp_path,
        files=FileManifest(inputs=(FileEntry(local_path=str(source)),), stdout=FileEntry(local_path=str(output))),
        metadata={"example": {"task": "DOS".upper()}},
        processing_required=True,
    )
    prepared = manager.transition(
        record, state=RunState.RUNNING, execution=record.execution.submitted("existing-backend")
    )
    failed = manager.complete(
        prepared,
        state=RunState.FAILED,
        return_code=0,
        failure_phase=FailurePhase.POSTPROCESSING,
        artifacts_available=True,
    )
    assert failed.files.output_versions
    assert failed.files.input_versions
    if input_change == "missing":
        source.unlink()
    else:
        source.write_bytes(b"changed input")
    assert all(item.state() is FileState.CURRENT for item in failed.files.output_versions)
    assert any(item.state() is not FileState.CURRENT for item in failed.files.input_versions)
    seen = []

    def parse(outcome):
        seen.append(outcome)
        return output.read_bytes()

    assert manager.retry_postprocessing(failed.id, parse) == b"captured result"
    assert len(seen) == 1
    assert seen[0].input_file is None
    assert seen[0].output_file == str(output)
    current = manager.store.get(failed.id)
    assert current.state is RunState.FINISHED
    assert current.files == failed.files
