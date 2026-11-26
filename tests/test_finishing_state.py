"""Execution status projection and observable client postprocessing."""

import sys
from subprocess import CalledProcessError
from threading import Event

import pytest

from kkrunner import ExecutionSpec, RunState, RunStore
from kkrunner.backends import DirectBackend, SupervisedBackend
from kkrunner.client import EndpointClient, SshTransport
from kkrunner.runs import FailurePhase, RunStatus
from kkrunner.client.pool import ClientPool
from kkrunner.runs.run import Run
from kkrunner.runs.manager import RunManager


class _Executor(EndpointClient):
    def __init__(self, status, *, remote=False, backend=SupervisedBackend):
        super().__init__(
            backend,
            transport=SshTransport("cluster") if remote else None,
        )
        self.status_value = status

    def acknowledge(self, record):
        # This suite verifies state projection, not remote receipt transport.
        return None

    def acknowledge_submission(self, record):
        return None

    def status(self, *runs):
        return tuple(
            RunStatus(
                backend_id=run.execution.backend_id,
                state=self.status_value,
                return_code=0 if self.status_value == RunState.FINISHED else 1,
                failure_phase=FailurePhase.EXECUTION if self.status_value == RunState.FAILED else None,
            )
            for run in runs
        )


def _record(tmp_path, executor, state=RunState.RUNNING):
    manager = RunManager(RunStore(tmp_path / "runs"), client_pool=ClientPool())
    manager.register_client(executor)
    record = manager.new_record(
        directory=str(tmp_path),
        transport=executor.transport,
        backend=executor.backend_config,
        remote_directory="/remote/job" if not executor.transport.is_local else None,
        metadata={"test": {"task": "DOS"}},
        processing_required=True,
    )
    record = manager.transition(
        record, state=state, execution=record.execution.submitted("job-1"),
    )
    return manager, record


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


def test_postprocessing_is_observable_until_processor_finishes(tmp_path):
    entered = Event()
    release = Event()

    def process(outcome):
        entered.set()
        assert release.wait(5), "Postprocessing was never released"
        return outcome

    run = Run.untracked(
        EndpointClient(DirectBackend),
        ExecutionSpec((sys.executable, "-c", "print('complete')"), directory=str(tmp_path)),
        process_result=process,
    )
    try:
        run.submit()
        assert entered.wait(5), "Postprocessing did not begin"
        assert run.state == RunState.POSTPROCESSING
    finally:
        release.set()
    assert run.wait().return_code == 0
    assert run.state == RunState.FINISHED


def test_failed_wait_keeps_original_error_when_backend_status_is_unavailable(tmp_path, monkeypatch):
    run = Run.untracked(
        EndpointClient(DirectBackend),
        ExecutionSpec((sys.executable, "-c", "pass"), directory=str(tmp_path)),
    )

    def fail_wait():
        raise ValueError("original execution failure")

    monkeypatch.setattr(run._job, "wait", fail_wait)
    run.submit()
    with pytest.raises(ValueError, match="original execution failure"):
        run.wait()
    assert run.state == RunState.UNKNOWN
    assert run.failure_phase is None
    assert run.failure_message == "original execution failure"
    assert run._done.wait(2), "Completion worker must not escape exception handling"


def test_manager_refresh_owns_remote_transfer_boundary(tmp_path):
    executor = _Executor(RunState.FINISHED, remote=True)
    manager, record = _record(tmp_path, executor)
    (refreshed,) = manager.refresh((record,))
    assert refreshed.state == RunState.DOWNLOADING
    assert refreshed.return_code == 0
    assert refreshed in manager.store.runs(active=True)


def test_backend_failure_on_remote_job_still_preserves_transfer_stage(tmp_path):
    executor = _Executor(RunState.FAILED, remote=True)
    manager, record = _record(tmp_path, executor)
    (refreshed,) = manager.refresh((record,))
    assert refreshed.state == RunState.DOWNLOADING
    assert refreshed in manager.store.runs(active=True)


def test_cancelled_remote_job_keeps_transfer_reservation(tmp_path):
    executor = _Executor(RunState.CANCELLED, remote=True)
    manager, record = _record(tmp_path, executor)
    (refreshed,) = manager.refresh((record,))
    assert refreshed.state == RunState.DOWNLOADING
    assert refreshed in manager.store.runs(active=True)


@pytest.mark.parametrize("transfer_error", [None, OSError("remote stream unavailable"),
                                            CalledProcessError(255, ("ssh", "cluster"))])
def test_direct_remote_cancellation_preserves_confirmed_outcome(tmp_path, monkeypatch, transfer_error):
    executor = _Executor(RunState.FINISHED, remote=True, backend=DirectBackend)
    manager, record = _record(tmp_path, executor)
    calls = []

    monkeypatch.setattr(executor, "cancel", lambda attachment: RunStatus(
        RunState.CANCELLED, backend_id=attachment.execution.backend_id,
    ))

    def collect(current, status):
        assert current.id == record.id
        assert status.state == RunState.CANCELLED
        assert manager.store.get(record.id).state == RunState.RUNNING
        calls.append(current.id)
        if transfer_error is not None:
            raise transfer_error
        return current.files

    monkeypatch.setattr(executor, "collect_ended_direct_files", collect)
    cancelled = manager.cancel(record.id)
    assert calls == [record.id]
    assert cancelled.state == RunState.CANCELLED
    if transfer_error is not None:
        assert str(transfer_error) in cancelled.message
    (refreshed,) = manager.refresh((cancelled,))
    assert refreshed.state == RunState.CANCELLED
    assert calls == [record.id]
