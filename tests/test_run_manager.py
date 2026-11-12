from threading import Event
import pytest

from kkrunner.runs import DuplicateSubmissionError, FailurePhase, RunStatus
from kkrunner.client.contracts import JobAttachment
from kkrunner.client.run_parts import RunParts
from kkrunner import ExecutionOutcome, RunState, RunStore
from kkrunner.backends import SupervisedBackend
from kkrunner.backends.contracts import JobStatus
from kkrunner.client import EndpointClient
from kkrunner.client.pool import ClientPool
from kkrunner import ExecutionSpec
from kkrunner.runs.manager import RunManager


class _RawRunner:
    def __init__(self, on_submit=None):
        self.on_submit = on_submit
        self.state = RunState.NEW
        self.backend_id = None
        self.result = ExecutionOutcome(return_code=None)
        self.exception = None
        self.failure_phase = None
        self.failure_message = None
        self._done = Event()

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
        return JobStatus(self.state, return_code=self.return_code,
                         failure_phase=self.failure_phase, message=self.failure_message)

    def submit(self):
        if self.on_submit:
            self.on_submit()
        self.backend_id = "native-1"
        self.state = RunState.QUEUED

    def wait(self):
        self._done.wait()
        return self.result

    def finish(self, return_code=0):
        self.result.return_code = return_code
        if return_code == 0:
            self.state = RunState.FINISHED
            self.failure_phase = None
            self.failure_message = None
        else:
            self.state = RunState.FAILED
            self.failure_phase = FailurePhase.EXECUTION
            self.failure_message = f"process exited with {return_code}"
        self._done.set()

    def cancel(self):
        self.state = RunState.CANCELLED
        self._done.set()

    def add_output_listener(self, callback):
        pass

    def add_output_error_listener(self, callback):
        pass


class _Executor(EndpointClient):
    def __init__(self):
        super().__init__(SupervisedBackend)
        self.runners = []
        self.status_calls = []
        self.on_submit = None

    def create_run_parts(self, spec, *, submission_key=None, callback=None):
        runner = _RawRunner(on_submit=self.on_submit)
        runner.spec = spec
        self.runners.append(runner)
        parts = super().create_run_parts(spec, submission_key=submission_key, callback=callback)
        parts.job = runner
        return parts

    def status(self, *runs):
        self.status_calls.append(runs)
        return tuple((RunStatus(RunState.RUNNING, backend_id=run.execution.backend_id) for run in runs))


def _spec(tmp_path, *, dataset="case"):
    return ExecutionSpec(
        argv=("test-executable",),
        directory=str(tmp_path),
        input_file=None,
        output_file=str(tmp_path / f"{dataset}.out"),
    )


def _manager(tmp_path):
    return RunManager(RunStore(tmp_path / "runs"), client_pool=ClientPool())


def test_submit_persists_submitting_before_backend_call(tmp_path):
    manager = _manager(tmp_path)
    executor = _Executor()
    run = manager.create_run(
        executor,
        _spec(tmp_path),
        process_result=lambda outcome: outcome,
        metadata={"test": {"task": "ARPES", "dataset": "case"}},
    )
    seen = []
    executor.on_submit = lambda: seen.append(manager.store.get(run.id).state)
    executor.runners[0].on_submit = executor.on_submit
    run.submit()
    assert seen == [RunState.SUBMITTING]
    stored = manager.store.get(run.id)
    assert stored.state == RunState.QUEUED
    assert stored.finished_at is None
    assert stored.execution.backend_id == executor.runners[0].backend_id
    assert stored.metadata["test"]["task"] == "ARPES"
    assert stored.metadata["test"]["dataset"] == "case"


@pytest.mark.parametrize(
    "return_code, expected_state, expected_phase",
    [
        pytest.param(0, RunState.FINISHED, None, id="postprocess-success"),
        pytest.param(7, RunState.FAILED, FailurePhase.EXECUTION, id="skip-processing-on-failure"),
    ],
)
def test_completion_postprocesses_only_successful_execution(tmp_path, return_code, expected_state, expected_phase):
    manager = _manager(tmp_path)
    executor = _Executor()
    processed = []
    run = manager.create_run(
        executor,
        _spec(tmp_path),
        process_result=lambda outcome: processed.append(outcome) or outcome,
        metadata={"test": {"task": "DOS"}},
    )
    run.submit()
    (tmp_path / "case.out").write_text("result")
    executor.runners[0].finish(return_code)
    result = run.wait()
    assert result.return_code == return_code
    assert processed == ([result] if return_code == 0 else [])
    stored = manager.store.get(run.id)
    assert stored.state == expected_state
    assert stored.failure_phase == expected_phase
    if return_code == 0:
        assert stored.finished_at is not None
    else:
        assert stored.return_code == return_code


def test_refresh_batches_status_queries(tmp_path):
    manager = _manager(tmp_path)
    executor = _Executor()
    manager.register_client(executor)
    records = []
    for index in range(2):
        record = manager.new_record(
            directory=tmp_path / str(index),
            transport=executor.transport, backend=executor.backend_config,
            metadata={"test": {"task": "DOS"}},
            processing_required=True,
        )
        record = manager.transition(record, state=RunState.QUEUED, execution=record.execution.submitted(str(index)))
        records.append(record)
    refreshed = manager.refresh(records)
    assert [record.state for record in refreshed] == [RunState.RUNNING, RunState.RUNNING]
    assert executor.status_calls == [tuple(JobAttachment(record.execution, record.files) for record in records)]


def test_duplicate_submission_is_blocked_by_persisted_namespace(tmp_path):
    manager = _manager(tmp_path)
    executor = _Executor()
    first = manager.create_run(
        executor,
        _spec(tmp_path),
        process_result=lambda outcome: outcome,
        metadata={"test": {"task": "DOS", "dataset": "case"}},
    )
    first.submit()
    second = manager.create_run(
        executor,
        _spec(tmp_path),
        process_result=lambda outcome: outcome,
        metadata={"test": {"task": "DOS", "dataset": "case"}},
    )
    with pytest.raises(DuplicateSubmissionError):
        second.submit()
