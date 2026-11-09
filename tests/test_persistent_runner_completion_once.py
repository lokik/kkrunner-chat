"""Durable Run completion: passive status reads, once-only commits and recovery."""

from threading import Event

from kkrunner import ExecutionOutcome, Run, RunState, RunStore
from kkrunner.runs.manager import RunManager


class _Job:
    backend_id = "backend-job"
    exception = None
    failure_phase = None
    return_code = 0

    def __init__(self):
        self.state = RunState.RUNNING
        self.failure_message = None
        self._complete = Event()
        self.result = ExecutionOutcome(return_code=0)

    def wait(self):
        assert self._complete.wait(5), "fake backend never completed"
        return self.result

    def finish(self, state, message=None):
        self.state = state
        self.failure_message = message
        self._complete.set()


class _Backend:
    name = "fake"

    def __init__(self):
        self.acknowledgements = []

    def acknowledge(self, record):
        self.acknowledgements.append(record.id)


def _run(tmp_path, *, manager=None, backend=None, record=None, job=None, callback=None):
    manager = manager or RunManager(RunStore(tmp_path / "runs"))
    backend = backend or _Backend()
    if record is None:
        record = manager.new_record(directory=tmp_path)
        record = manager.transition(
            record, state=RunState.RUNNING,
            execution=record.execution.submitted("backend-job"),
        )
    manager.register_backend(backend, record.execution.endpoint)
    job = job or _Job()
    return Run(job, manager=manager, backend=backend, record=record, callback=callback), job, manager, backend


def test_reading_state_does_not_write_to_store(tmp_path, monkeypatch):
    run, _, manager, _ = _run(tmp_path)
    def unexpected_transition(*args, **kwargs):
        raise AssertionError("Reading state must not persist anything")
    monkeypatch.setattr(manager, "transition", unexpected_transition)
    assert run.state == RunState.RUNNING
    assert run.record.state == RunState.RUNNING


def test_finalization_is_once_even_when_wait_is_repeated(tmp_path, monkeypatch):
    run, job, manager, backend = _run(tmp_path)
    completed = []
    original = manager.complete

    def record_completion(*args, **kwargs):
        completed.append(kwargs["state"])
        return original(*args, **kwargs)

    monkeypatch.setattr(manager, "complete", record_completion)
    run.start()
    job.finish(RunState.FINISHED)
    result = run.wait()
    assert run.wait() is result
    run._sync_completion()
    assert manager.store.get(run.id).state == RunState.FINISHED
    assert completed == [RunState.FINISHED]
    assert backend.acknowledgements == [run.id]


def test_unknown_observation_does_not_finalize_execution(tmp_path, monkeypatch):
    run, job, manager, backend = _run(tmp_path)
    completed = []
    original = manager.complete

    def record_completion(*args, **kwargs):
        completed.append(kwargs["state"])
        return original(*args, **kwargs)

    monkeypatch.setattr(manager, "complete", record_completion)
    run.start()
    job.finish(RunState.UNKNOWN, "scheduler disconnected")
    run.wait()
    run._sync_completion()
    stored = manager.store.get(run.id)
    assert stored.state == RunState.UNKNOWN
    assert stored.message == "scheduler disconnected"
    assert completed == []
    assert backend.acknowledgements == []


def test_new_observer_can_complete_after_unknown_observation(tmp_path):
    delivered = []
    first, job, manager, backend = _run(tmp_path, callback=delivered.append)
    first.start()
    job.finish(RunState.UNKNOWN, "connection lost")
    first.wait()
    assert len(delivered) == 1
    assert manager.store.get(first.id).state == RunState.UNKNOWN

    second, second_job, _, _ = _run(
        tmp_path, manager=manager, backend=backend,
        record=manager.store.get(first.id), callback=delivered.append,
    )
    second.start()
    second_job.finish(RunState.FINISHED)
    second.wait()

    assert len(delivered) == 2
    assert manager.store.get(first.id).state == RunState.FINISHED
    assert backend.acknowledgements == [first.id]
