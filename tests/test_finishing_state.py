from threading import Event
from types import SimpleNamespace

from kkrunner import ExecutionOutcome, RunState, RunStore
from kkrunner.runs import FailurePhase, RunStatus
from kkrunner.client.pool import BackendPool
from kkrunner.runs.run import Run
from kkrunner.runs.manager import RunManager


class _Executor:
    name = "postprocessing-test"

    def __init__(self, status):
        self.status_value = status

    def status(self, *runs):
        return tuple(
            (
                RunStatus(
                    backend_id=run.execution.backend_id,
                    state=self.status_value,
                    return_code=0 if self.status_value == RunState.FINISHED else 1,
                    failure_phase=FailurePhase.EXECUTION if self.status_value == RunState.FAILED else None,
                )
                for run in runs
            )
        )


class _RawRunner:
    def __init__(self):
        self.result = ExecutionOutcome(return_code=0)
        self.state = RunState.NEW
        self.backend_id = "job-1"
        self.exception = None
        self.failure_phase = None
        self._done = Event()

    def submit(self):
        self.state = RunState.RUNNING

    def wait(self):
        self._done.wait()
        self.state = RunState.FINISHED
        return self.result

    def complete(self):
        self._done.set()

    def cancel(self):
        self.state = RunState.CANCELLED
        self._done.set()

    def add_output_listener(self, callback):
        pass

    def add_output_error_listener(self, callback):
        pass


def _record(tmp_path, executor, state=RunState.RUNNING):
    manager = RunManager(RunStore(tmp_path / "runs"), backend_pool=BackendPool())
    endpoint = manager.register_backend(executor)
    record = manager.new_record(
        directory=str(tmp_path), endpoint=endpoint, metadata={"test": {"task": "DOS"}}, processing_required=True
    )
    record = manager.transition(record, state=state, execution=record.execution.submitted("job-1"))
    return (manager, record)


def test_refresh_maps_raw_success_to_postprocessing(tmp_path):
    executor = _Executor(RunState.FINISHED)
    manager, record = _record(tmp_path, executor)
    (refreshed,) = manager.refresh((record,))
    assert refreshed.state == RunState.POSTPROCESSING
    assert refreshed.return_code == 0
    assert refreshed in manager.store.runs(active=True)


def test_refresh_keeps_raw_execution_failure_terminal(tmp_path):
    executor = _Executor(RunState.FAILED)
    manager, record = _record(tmp_path, executor)
    (refreshed,) = manager.refresh((record,))
    assert refreshed.state == RunState.FAILED
    assert refreshed.failure_phase == FailurePhase.EXECUTION
    assert refreshed.return_code == 1
    assert refreshed in manager.store.runs(active=False)


def test_postprocessing_is_observable_until_processor_finishes():
    raw = _RawRunner()
    entered = Event()
    release = Event()

    def process(outcome):
        entered.set()
        release.wait()
        return SimpleNamespace(return_code=outcome.return_code)

    runner = Run(raw, process_result=process)
    runner.submit()
    raw.complete()
    assert entered.wait(1)
    assert runner.state == RunState.POSTPROCESSING
    release.set()
    result = runner.wait()
    assert result.return_code == 0
    assert runner.state == RunState.FINISHED


def test_failed_wait_keeps_original_error_when_backend_status_is_unavailable():

    class InaccessibleRunner:
        backend_id = "job-1"
        exception = None
        failure_phase = None
        result = None

        def __init__(self):
            self.started = False

        @property
        def state(self):
            if not self.started:
                return RunState.NEW
            raise OSError("backend status unavailable")

        def submit(self):
            self.started = True

        def wait(self):
            raise ValueError("original execution failure")

    runner = Run(InaccessibleRunner(), process_result=lambda outcome: outcome)
    runner.submit()
    try:
        import pytest

        with pytest.raises(ValueError, match="original execution failure"):
            runner.wait()
        assert runner.state == RunState.UNKNOWN
        assert runner.failure_phase is None
        assert runner.failure_message == "original execution failure"
    finally:
        assert runner._done.wait(2), "completion worker must not escape exception handling"

def test_manager_refresh_owns_remote_transfer_boundary(tmp_path):
    from kkrunner.runs.model import WorkingDirectory, Execution
    from dataclasses import replace

    executor = _Executor(RunState.FINISHED)
    manager, record = _record(tmp_path, executor)
    record = manager.transition(
        record,
        execution=Execution(
            record.execution.endpoint, WorkingDirectory(str(tmp_path), "/remote/job"),
            record.execution.backend_id,
        ),
    )
    (refreshed,) = manager.refresh((record,))
    assert refreshed.state == RunState.DOWNLOADING
    assert refreshed.return_code == 0
    assert refreshed in manager.store.runs(active=True)


def test_backend_failure_on_remote_job_still_preserves_transfer_stage(tmp_path):
    from kkrunner.runs.model import WorkingDirectory, Execution

    executor = _Executor(RunState.FAILED)
    manager, record = _record(tmp_path, executor)
    record = manager.transition(
        record,
        execution=Execution(
            record.execution.endpoint, WorkingDirectory(str(tmp_path), "/remote/job"),
            record.execution.backend_id,
        ),
    )
    (refreshed,) = manager.refresh((record,))
    assert refreshed.state == RunState.DOWNLOADING
    assert refreshed in manager.store.runs(active=True)
