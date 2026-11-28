"""Durable Run completion: passive reads, once-only commits and observation recovery."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from threading import Event

from unittest.mock import AsyncMock

import pytest

from kkrunner import RunFileState
from kkrunner import ExecutionSpec, Run, RunState, RunStore
from kkrunner.backends import SupervisedBackend
from kkrunner.backends.contracts import JobRef, JobStatus
from kkrunner.client import ExecutionClient
from kkrunner.client.job import Job
from kkrunner.runs.manager import RunManager


def _run(tmp_path, *, manager=None, client=None, record=None, callback=None):
    """Use a real client Job and FileSession, controlling only backend observation."""
    manager = manager or RunManager(RunStore(tmp_path / "runs"))
    client = client or ExecutionClient(SupervisedBackend)
    acknowledgements = getattr(client, "_test_acknowledgements", None)
    if acknowledgements is None:
        acknowledgements = []
        client._test_acknowledgements = acknowledgements
        async def acknowledge(record):
            acknowledgements.append(record.id)

        async def acknowledge_submission(record):
            pass

        client.acknowledge_async = acknowledge
        client.acknowledge_submission_async = acknowledge_submission
    manager.register_client(client)

    spec = ExecutionSpec(("true",), directory=str(tmp_path))
    if record is None:
        record = manager.new_record(
            directory=tmp_path, target=client.target, backend=client.backend_config,
        )
        record = manager.transition(
            record, state=RunState.RUNNING, file_state=RunFileState.WAITING,
            execution=record.execution.submitted("backend-job"),
        )
    parts = client.create_run_parts(spec)
    job = parts.job
    assert type(job) is Job
    job.execution.ref = JobRef("kkrunner", record.execution.backend_id)
    job._apply_status(JobStatus(RunState.RUNNING))
    complete = Event()

    async def wait_for_backend():
        assert await asyncio.to_thread(complete.wait, 5), "fake backend never completed"
        return job.result

    def finish(state, message=None):
        job._apply_status(JobStatus(
            state,
            return_code=0 if state == RunState.FINISHED else None,
            message=message,
        ))
        complete.set()

    job.wait_async = wait_for_backend
    job.finish = finish
    run = Run(
        parts, client=client, manager=manager, record=record, callback=callback,
    )
    return run, job, manager, client


def test_reading_state_does_not_write_to_store(tmp_path, monkeypatch):
    run, _, manager, _ = _run(tmp_path)

    def unexpected_transition(*args, **kwargs):
        raise AssertionError("Reading state must not persist anything")

    monkeypatch.setattr(manager.store, "update", unexpected_transition)
    assert run.state == RunState.RUNNING
    assert run.record.state == RunState.RUNNING


def test_finalization_is_once_even_when_wait_is_repeated(tmp_path):
    completed = []
    run, job, manager, client = _run(tmp_path, callback=completed.append)
    job.finish(RunState.FINISHED)
    result = run.wait()
    saved = manager.store.get(run.id)
    assert saved.state == RunState.FINISHED
    assert saved.file_state == RunFileState.DONE
    assert run.wait() is result
    assert manager.store.get(run.id) == saved
    assert completed == [result]
    assert client._test_acknowledgements == [run.id]


def test_unknown_observation_does_not_finalize_execution(tmp_path):
    run, job, manager, client = _run(tmp_path)
    job.finish(RunState.UNKNOWN, "scheduler disconnected")
    run.wait()
    stored = manager.store.get(run.id)
    assert stored.state == RunState.UNKNOWN
    assert stored.message == "scheduler disconnected"
    assert stored.file_state == RunFileState.WAITING
    assert stored.finished_at is None
    assert client._test_acknowledgements == []



@pytest.mark.parametrize("return_code", [0, 7])
def test_refresh_of_settled_run_exposes_current_terminal_facts(tmp_path, monkeypatch, return_code):
    from kkrunner.runs import FailurePhase, RunStatus

    run, job, manager, client = _run(tmp_path)
    job.finish(RunState.UNKNOWN, "connection lost")
    run.wait()
    assert run.failure_message == "connection lost"
    assert run.return_code is None

    state = RunState.FINISHED if return_code == 0 else RunState.FAILED
    phase = None if return_code == 0 else FailurePhase.EXECUTION
    message = None if return_code == 0 else "program failed"
    monkeypatch.setattr(client, "status_async", AsyncMock(side_effect=lambda *records: tuple(
        RunStatus(state, backend_id=record.execution.backend_id,
                  return_code=return_code, failure_phase=phase, message=message)
        for record in records
    )))
    assert run.refresh() == state
    assert run.record == manager.store.get(run.id)
    assert run.return_code == return_code
    assert run.failure_phase == phase
    assert run.failure_message == message
    assert run.files.captured_at is not None
    assert run.files == run.record.files

    monkeypatch.setattr(job, "cancel", lambda: pytest.fail("Native execution already finished"))
    assert run.cancel().result(timeout=1) is None


def test_new_observer_can_complete_after_unknown_observation(tmp_path):
    delivered = []
    first, job, manager, client = _run(tmp_path, callback=delivered.append)
    job.finish(RunState.UNKNOWN, "connection lost")
    first.wait()
    assert len(delivered) == 1
    assert manager.store.get(first.id).state == RunState.UNKNOWN

    second, second_job, _, _ = _run(
        tmp_path, manager=manager, client=client,
        record=manager.store.get(first.id), callback=delivered.append,
    )
    second_job.finish(RunState.FINISHED)
    second.wait()

    assert len(delivered) == 2
    assert manager.store.get(first.id).state == RunState.FINISHED
    assert client._test_acknowledgements == [first.id]


@pytest.mark.parametrize("terminal", [RunState.FINISHED, RunState.CANCELLED])
def test_tracked_run_publishes_finalization_after_native_exit(tmp_path, monkeypatch, terminal):
    run, job, manager, _ = _run(tmp_path)
    entered, release = Event(), Event()
    finalize = run._file_session.finalize_async

    async def blocked_finalize(status, *, on_resolved=None):
        entered.set()
        assert await asyncio.to_thread(release.wait, 5), "Finalization was never released"
        return await finalize(status, on_resolved=on_resolved)

    monkeypatch.setattr(run._file_session, "finalize_async", blocked_finalize)
    job.finish(terminal)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(run.wait)
        try:
            assert entered.wait(5), "Native completion was not observed"
            assert run.state == terminal
            assert run.file_state == RunFileState.COLLECTING
            stored = manager.store.get(run.id)
            assert stored.state == terminal
            assert stored.file_state == RunFileState.COLLECTING
            assert stored.return_code == (0 if terminal == RunState.FINISHED else None)
            assert stored.finished_at is None
        finally:
            release.set()
        future.result(timeout=5)
    assert manager.store.get(run.id).state == terminal


def test_refresh_is_authoritative_while_original_observer_is_running(tmp_path, monkeypatch):
    from kkrunner.runs import RunStatus

    run, job, manager, client = _run(tmp_path)
    entered = Event()
    wait = job.wait_async

    async def observing():
        entered.set()
        return await wait()

    job.wait_async = observing
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(run.wait)
        try:
            assert entered.wait(5)
            for state in (RunState.RUNNING, RunState.UNKNOWN, RunState.RUNNING, RunState.FINISHED):
                monkeypatch.setattr(client, "status_async", AsyncMock(side_effect=lambda *records: tuple(
                    RunStatus(state, backend_id=record.execution.backend_id,
                              return_code=0 if state == RunState.FINISHED else None)
                    for record in records
                )))
                expected = state
                assert run.refresh() == expected
                assert run.record.state == expected
                assert run.state == expected
                assert run.record.is_active
                assert run.record.file_state == RunFileState.WAITING
        finally:
            job.finish(RunState.FINISHED)
        future.result(timeout=5)
    assert run.state == RunState.FINISHED


def test_result_ack_does_not_hold_file_ownership(tmp_path, monkeypatch):
    run, job, manager, client = _run(tmp_path)
    entered, release = Event(), Event()

    async def acknowledge(record):
        assert manager.store.get(record.id).file_state == RunFileState.DONE
        entered.set()
        assert await asyncio.to_thread(release.wait, 5)

    monkeypatch.setattr(client, "acknowledge_async", acknowledge)
    job.finish(RunState.FINISHED)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(run.wait)
        try:
            assert entered.wait(5)
            # Completion is already committed and an unrelated network
            # acknowledgement must not retain the client's file lock.
            lock = manager.store.observation_lock(run.record)
            assert lock.acquire(blocking=False)
            lock.release()
        finally:
            release.set()
        assert future.result(timeout=5).state == RunState.FINISHED
