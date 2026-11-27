from subprocess import CalledProcessError
from threading import Event
import pytest

from kkrunner.runs import DuplicateSubmissionError, FailurePhase, RunStatus
from kkrunner.client.contracts import JobAttachment
from kkrunner.client.run_parts import RunParts
from kkrunner import ExecutionOutcome, RunState, RunStore
from kkrunner.backends import SupervisedBackend
from kkrunner.backends.contracts import JobStatus
from kkrunner.client import ExecutionClient
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
        self.output_error = None
        self.failure_phase = None
        self.failure_message = None
        self._done = Event()

    @property
    def return_code(self):
        return self.result.return_code

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


class _Executor(ExecutionClient):
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
    run = executor.create_run(
        _spec(tmp_path), tracked=manager,
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
    run = executor.create_run(
        _spec(tmp_path), tracked=manager,
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
            target=executor.target, backend=executor.backend_config,
            metadata={"test": {"task": "DOS"}},
            processing_required=True,
        )
        record = manager.transition(record, state=RunState.QUEUED, execution=record.execution.submitted(str(index)))
        records.append(record)
    # Duplicate input occurrences retain their positions in the result, but
    # must not trigger repeated queries or completion of the same execution.
    refreshed = manager.refresh((*records, records[0]))
    assert [record.state for record in refreshed] == [RunState.RUNNING] * 3
    assert refreshed[0] == refreshed[2]
    assert executor.status_calls == [tuple(JobAttachment(record.execution, record.files) for record in records)]


def test_duplicate_submission_is_blocked_by_persisted_namespace(tmp_path):
    manager = _manager(tmp_path)
    executor = _Executor()
    first = executor.create_run(
        _spec(tmp_path), tracked=manager,
        process_result=lambda outcome: outcome,
        metadata={"test": {"task": "DOS", "dataset": "case"}},
    )
    first.submit()
    second = executor.create_run(
        _spec(tmp_path), tracked=manager,
        process_result=lambda outcome: outcome,
        metadata={"test": {"task": "DOS", "dataset": "case"}},
    )
    with pytest.raises(DuplicateSubmissionError):
        second.submit()


@pytest.mark.parametrize("error", [OSError("unreachable"), CalledProcessError(255, ["ssh"])])
def test_refresh_preserves_successful_statuses_when_one_query_fails(tmp_path, monkeypatch, error):
    manager = _manager(tmp_path)
    client = ExecutionClient("psij")
    manager.register_client(client)
    records = []
    for identity in ("first", "unreachable", "last"):
        record = manager.new_record(
            directory=tmp_path / identity, target=client.target, backend=client.backend_config,
        )
        records.append(manager.transition(
            record, state=RunState.QUEUED, execution=record.execution.submitted(identity),
        ))

    async def invoke(operation, identity):
        assert operation == "status"
        if identity == "unreachable":
            raise error
        return {"state": "running" if identity == "first" else "finished",
                "return_code": None if identity == "first" else 0}

    monkeypatch.setattr(client, "_invoke", invoke)
    refreshed = manager.refresh(records)
    assert [record.state for record in refreshed] == [
        RunState.RUNNING, RunState.UNKNOWN, RunState.FINISHED,
    ]
    assert "Cannot query execution status" in refreshed[1].message
    assert refreshed[2].return_code == 0


def test_failed_status_query_does_not_replace_a_newer_observation(tmp_path, monkeypatch):
    manager = _manager(tmp_path)
    client = ExecutionClient(SupervisedBackend)
    manager.register_client(client)
    record = manager.new_record(
        directory=tmp_path, target=client.target, backend=client.backend_config,
    )
    record = manager.transition(record, state=RunState.QUEUED,
                                execution=record.execution.submitted("native"))

    async def invoke(operation, identity):
        assert operation == "status"
        manager.transition(record, state=RunState.RUNNING)
        raise OSError("connection lost")

    monkeypatch.setattr(client, "_invoke", invoke)
    (refreshed,) = manager.refresh((record,))
    assert refreshed.state == RunState.RUNNING
    assert refreshed.message is None


def test_refresh_uses_backend_identity_assigned_after_snapshot(tmp_path):
    manager = _manager(tmp_path)
    executor = _Executor()
    manager.register_client(executor)
    record = manager.new_record(
        directory=tmp_path, target=executor.target, backend=executor.backend_config,
    )
    stale = manager.transition(record, state=RunState.SUBMITTING)
    current = manager.transition(stale, state=RunState.QUEUED,
                                 execution=stale.execution.submitted("native"))
    refreshed, = manager.refresh((stale,))
    assert refreshed.state == RunState.RUNNING
    assert executor.status_calls == [(JobAttachment(current.execution, current.files),)]


def test_refresh_uses_assigned_identity_without_preliminary_store_read(tmp_path, monkeypatch):
    manager = _manager(tmp_path)
    executor = _Executor()
    manager.register_client(executor)
    record = manager.new_record(
        directory=tmp_path, target=executor.target, backend=executor.backend_config,
        processing_required=True,
    )
    stale = manager.transition(record, state=RunState.RUNNING,
                               execution=record.execution.submitted("native"))
    current = manager.transition(stale, state=RunState.POSTPROCESSING, return_code=0)
    status = executor.status
    get = manager.store.get
    reads = []

    def read(run_id):
        reads.append(run_id)
        return get(run_id)

    def query(*attachments):
        assert not reads
        return status(*attachments)

    monkeypatch.setattr(manager.store, "get", read)
    monkeypatch.setattr(executor, "status", query)
    assert manager.refresh((stale,)) == (current,)
    assert executor.status_calls == [(JobAttachment(stale.execution, stale.files),)]
