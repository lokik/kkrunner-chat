"""Execution status projection and observable client postprocessing."""

import sys
from threading import Event

import pytest

from kkrunner import RunFileState
from kkrunner import ExecutionSpec, RunState, RunStore
from kkrunner.backends import DirectBackend, SupervisedBackend
from kkrunner.client import ExecutionClient, SshTarget
from kkrunner.runs import FailurePhase, RunStatus
from kkrunner.client.pool import ClientPool
from kkrunner.runs.run import Run
from kkrunner.runs.manager import RunManager


class _Executor(ExecutionClient):
    def __init__(self, status, *, remote=False, backend=SupervisedBackend, failure_phase=FailurePhase.EXECUTION):
        super().__init__(
            backend,
            target=SshTarget("cluster") if remote else None,
        )
        self.status_value = status
        self.failure_phase = failure_phase

    async def acknowledge_async(self, record):
        # This suite verifies state projection, not remote receipt transport.
        return None

    async def acknowledge_submission_async(self, record):
        return None

    async def status_async(self, *runs):
        return tuple(
            RunStatus(
                backend_id=run.execution.backend_id,
                state=self.status_value,
                return_code=(0 if self.status_value == RunState.FINISHED else
                             1 if self.status_value in (RunState.FAILED, RunState.CANCELLED) else None),
                failure_phase=self.failure_phase if self.status_value == RunState.FAILED else None,
            )
            for run in runs
        )


def _record(tmp_path, executor, state=RunState.RUNNING):
    manager = RunManager(RunStore(tmp_path / "runs"), client_pool=ClientPool())
    manager.register_client(executor)
    record = manager.new_record(
        directory=str(tmp_path),
        target=executor.target,
        backend=executor.backend_config,
        remote_directory="/remote/job" if not executor.target.is_local else None,
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
    assert refreshed.state == RunState.FINISHED
    assert refreshed.file_state == RunFileState.READY
    assert refreshed.return_code == 0
    assert refreshed in manager.store.runs(active=True)


def test_refresh_keeps_raw_execution_failure_terminal(tmp_path):
    executor = _Executor(RunState.FAILED)
    manager, record = _record(tmp_path, executor)
    (refreshed,) = manager.refresh((record,))
    assert refreshed.state == RunState.FAILED
    assert refreshed.failure_phase == FailurePhase.EXECUTION
    assert refreshed.return_code == 1
    assert refreshed.file_state == RunFileState.READY
    assert refreshed in manager.store.runs(active=True)


def test_postprocessing_is_observable_until_processor_finishes(tmp_path):
    entered = Event()
    release = Event()

    def process(outcome):
        entered.set()
        assert release.wait(5), "Postprocessing was never released"
        return outcome

    run = ExecutionClient(DirectBackend).create_run(
        ExecutionSpec((sys.executable, "-c", "print('complete')"), directory=str(tmp_path)), tracked=False,
        process_result=process,
    )
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(run.run)
        try:
            assert entered.wait(5), "Postprocessing did not begin"
            assert run.state == RunState.FINISHED
            assert run.file_state == RunFileState.POSTPROCESSING
        finally:
            release.set()
        assert future.result(timeout=5).return_code == 0
    assert run.state == RunState.FINISHED


def test_failed_wait_keeps_original_error_when_backend_status_is_unavailable(tmp_path, monkeypatch):
    run = ExecutionClient(DirectBackend).create_run(
        ExecutionSpec((sys.executable, "-c", "pass"), directory=str(tmp_path)), tracked=False,
    )

    async def fail_wait():
        raise ValueError("original execution failure")

    async def fail_run(*, on_submitted=None):
        run._job._state = RunState.RUNNING
        await on_submitted()
        run._job._observation_lost("original execution failure")
        await fail_wait()
    monkeypatch.setattr(run._job, "run_async", fail_run)
    with pytest.raises(ValueError, match="original execution failure"):
        run.run()
    assert run.state == RunState.UNKNOWN
    assert run.failure_phase is None
    assert run.failure_message == "original execution failure"
    with pytest.raises(ValueError, match="original execution failure"):
        run.wait()


def test_manager_refresh_owns_remote_transfer_boundary(tmp_path):
    executor = _Executor(RunState.FINISHED, remote=True)
    manager, record = _record(tmp_path, executor)
    (refreshed,) = manager.refresh((record,))
    assert refreshed.state == RunState.FINISHED
    assert refreshed.file_state == RunFileState.COLLECTING
    assert refreshed.return_code == 0
    assert refreshed in manager.store.runs(active=True)


@pytest.mark.parametrize("phase,expected_state", (
    (FailurePhase.EXECUTION, RunFileState.COLLECTING),
    (FailurePhase.PREPROCESSING, RunFileState.DONE),
    (FailurePhase.SUBMISSION, RunFileState.DONE),
    (FailurePhase.LAUNCH, RunFileState.DONE),
))
def test_remote_failure_only_downloads_after_execution_started(tmp_path, phase, expected_state):
    executor = _Executor(RunState.FAILED, remote=True, failure_phase=phase)
    manager, record = _record(tmp_path, executor)
    (refreshed,) = manager.refresh((record,))
    assert refreshed.state == RunState.FAILED
    assert refreshed.file_state == expected_state
    assert refreshed in manager.store.runs(active=not expected_state.is_terminal)


def test_cancelled_remote_job_keeps_transfer_reservation(tmp_path):
    executor = _Executor(RunState.CANCELLED, remote=True)
    manager, record = _record(tmp_path, executor)
    (refreshed,) = manager.refresh((record,))
    assert refreshed.state == RunState.CANCELLED
    assert refreshed.file_state == RunFileState.COLLECTING
    assert refreshed in manager.store.runs(active=True)


@pytest.mark.parametrize("transfer_error", [None, OSError("remote stream unavailable")])
def test_direct_remote_cancellation_preserves_confirmed_outcome(tmp_path, monkeypatch, transfer_error):
    from kkrunner.client.file_access import FileAccess

    executor = _Executor(RunState.FINISHED, remote=True, backend=DirectBackend)
    manager, record = _record(tmp_path, executor)
    calls = []

    async def cancel(attachment):
        return RunStatus(RunState.CANCELLED, backend_id=attachment.execution.backend_id)
    monkeypatch.setattr(executor, "cancel_async", cancel)

    async def collect(access, current, status, *, on_resolved=None):
        assert access.target == record.execution.target
        assert current.id == record.id
        assert status.state == RunState.CANCELLED
        assert manager.store.get(record.id).state == RunState.CANCELLED
        calls.append(current.id)
        from kkrunner.files.availability import capture_result_files
        files = current.files if transfer_error is not None else capture_result_files(current.files, require_outputs=False)
        return files, transfer_error

    monkeypatch.setattr(FileAccess, "collect_ended_files_async", collect)
    cancelled = manager.cancel(record.id)
    assert calls == [record.id]
    assert cancelled.state == RunState.CANCELLED
    if transfer_error is not None:
        assert str(transfer_error) in cancelled.file_message
        assert cancelled.file_state == RunFileState.FAILED
    else:
        assert cancelled.file_state == RunFileState.READY
    (refreshed,) = manager.refresh((cancelled,))
    assert refreshed.state == RunState.CANCELLED
    assert calls == [record.id]


@pytest.mark.parametrize("observe", ["update", "refresh"])
def test_stale_backend_status_does_not_replace_postprocessing(tmp_path, observe):
    executor = _Executor(RunState.FAILED)
    manager, stale = _record(tmp_path, executor)
    current = manager.complete(stale, state=RunState.FINISHED,
                               file_state=RunFileState.POSTPROCESSING, return_code=0,
                               artifacts_available=True)
    if observe == "update":
        result = manager.update(stale, RunStatus(RunState.FAILED, return_code=1))
    else:
        (result,) = manager.refresh((stale,))
    assert result == current
    assert manager.store.get(stale.id) == current


@pytest.mark.parametrize("state", [RunState.FAILED, RunState.CANCELLED])
def test_update_reserves_local_diagnostics_until_refresh(tmp_path, state):
    from kkrunner.files import FileEntry, FileManifest

    executor = _Executor(state)
    manager, record = _record(tmp_path, executor)
    output = tmp_path / "run.out"
    output.write_text("diagnostic output")
    record = manager.transition(
        record, files=FileManifest(stdout=FileEntry(local_path=str(output))),
    )
    status, = executor.status(record)

    updated = manager.update(record, status)
    assert updated.state == state
    assert updated.file_state == RunFileState.COLLECTING
    assert updated.files.captured_at is None
    assert updated in manager.store.runs(active=True)

    refreshed, = manager.refresh((updated,))
    assert refreshed.state == state
    assert refreshed.files.captured_at is not None
    assert len(refreshed.files.output_versions) == 1
    assert refreshed.file_state == RunFileState.READY
    assert refreshed in manager.store.runs(active=True)


def test_lost_supervisor_cannot_retract_confirmed_native_exit(tmp_path):
    executor = _Executor(RunState.UNKNOWN)
    manager, record = _record(tmp_path, executor, state=RunState.FINISHED)
    record = manager.transition(record, file_state=RunFileState.COLLECTING)
    (refreshed,) = manager.refresh((record,))
    assert refreshed.state == RunState.FINISHED
    assert refreshed.file_state == RunFileState.READY
    assert refreshed in manager.store.runs(active=True)


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Direct status requires a verifiable process identity")
@pytest.mark.parametrize("backend", (DirectBackend, SupervisedBackend))
@pytest.mark.parametrize("tracked", (False, True))
def test_refresh_observes_native_exit_while_listener_is_stalled(tmp_path, monkeypatch, backend, tracked):
    import time

    monkeypatch.setenv("KKRunner_REMOTE_STATE_DIR", str(tmp_path / "state"))
    gate = tmp_path / "start"
    entered = Event()
    release = Event()

    def listener(text, kind):
        if "ready" in text:
            entered.set()
            assert release.wait(10), "Output listener was not released"

    client = ExecutionClient(backend)
    manager = RunManager(RunStore(tmp_path / "runs")) if tracked else None
    run = client.create_run(
        ExecutionSpec((sys.executable, "-c",
                       "from pathlib import Path; import time, sys\n"
                       "while not Path('start').exists(): time.sleep(.01)\n"
                       "print('ready', flush=True); sys.exit(7)"),
                      directory=tmp_path, output_file="stdout"),
        tracked=manager if tracked else False, detached=False, output_listener=listener,
    )
    from concurrent.futures import ThreadPoolExecutor
    accepted = Event()
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(run.run, on_submitted=lambda _: accepted.set())
        try:
            assert accepted.wait(5)
            assert run.refresh() == RunState.RUNNING
            gate.touch()
            assert entered.wait(5)
            deadline = time.monotonic() + 5
            while not run.refresh().is_terminal:
                assert time.monotonic() < deadline, "Native process exit was not observable"
                time.sleep(.01)
            assert run.state.is_terminal
            assert run.file_state == RunFileState.WAITING
            if tracked:
                assert run.record.state.is_terminal
        finally:
            gate.touch()
            release.set()
        outcome = future.result(timeout=5)
    assert outcome.return_code == 7
    assert run.state == RunState.FAILED
    assert run.refresh() == RunState.FAILED


def test_live_job_facts_are_not_masked_by_previous_run_refresh(tmp_path, monkeypatch):
    from kkrunner.backends.contracts import JobRef, JobStatus
    from kkrunner.client import ExecutionClient
    from kkrunner.backends import DirectBackend

    run = ExecutionClient(DirectBackend).create_run(
        ExecutionSpec((sys.executable, "-c", "pass"), directory=tmp_path), tracked=False,
    )
    job = run._job
    job.execution.ref = JobRef("direct", "native")
    job._apply_status(JobStatus(RunState.UNKNOWN, message="old connection error"))

    async def unknown():
        return JobStatus(RunState.UNKNOWN, message="old connection error")

    monkeypatch.setattr(job.execution, "status", unknown)
    assert run.refresh() == RunState.UNKNOWN
    assert run.failure_message == "old connection error"
    job._apply_status(JobStatus(RunState.FAILED, return_code=7, failure_phase="execution",
                                message="process exited with 7"))
    assert run.state == RunState.FAILED
    assert run.return_code == 7
    assert run.failure_phase == FailurePhase.EXECUTION
    assert run.failure_message == "process exited with 7"
    run.close()


@pytest.mark.parametrize("tracked", [False, True])
def test_prelaunch_failure_is_not_replaced_by_refresh(tmp_path, tracked):
    manager = RunManager(RunStore(tmp_path / "runs")) if tracked else False

    def preprocess():
        raise ValueError("invalid input")

    run = ExecutionClient(DirectBackend).create_run(
        ExecutionSpec((sys.executable, "-c", "pass"), directory=tmp_path),
        tracked=manager, preprocess=preprocess,
    )
    with pytest.raises(ValueError, match="invalid input"):
        run.run()
    assert run.refresh() == RunState.NEW
    assert run.file_state == RunFileState.FAILED
    assert run.file_failure_phase == FailurePhase.PREPROCESSING
    assert run.file_message == "invalid input"
    assert run.failure_message is None
