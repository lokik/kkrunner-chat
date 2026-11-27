"""Cancellation races at the current Job/Run ownership boundary."""

import asyncio
from threading import Event, Thread

import pytest

from kkrunner import ExecutionSpec, RunState
from kkrunner.client.run_parts import RunParts
from kkrunner.backends.contracts import JobRef, JobStatus, SubmissionNotStarted
from kkrunner.client import ExecutionClient
from kkrunner.client.job import Job
from kkrunner.runs.base import ExecutionResources
from kkrunner.runs.run import Run


class _Resource:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class _ControlledExecution:
    """Minimal backend binding with a controlled asynchronous submit boundary."""

    def __init__(self, *, fail_submit=False, already_running=False):
        self.ref = JobRef("process", "job-1") if already_running else None
        self.snapshot = JobStatus(RunState.RUNNING if already_running else RunState.NEW)
        self.output_error = None
        self.fail_submit = fail_submit
        self.entered = Event()
        self.release = Event()
        self.cancelled = Event()
        self.cancel_calls = 0
        self.observation_calls = 0

    def configure_detached(self, detached):
        pass

    def configure_observation(self, *, stream_offsets, stream_paths):
        pass

    async def submit(self, request, on_output, *, output_requested=None):
        self.entered.set()
        if not await asyncio.to_thread(self.release.wait, 5):
            raise TimeoutError("test did not release submit")
        if self.fail_submit:
            raise RuntimeError("submission rejected")
        self.ref = JobRef("process", "job-1")
        return JobStatus(RunState.QUEUED)

    async def observe(self, on_output, *, output_requested=None):
        self.observation_calls += 1
        if not await asyncio.to_thread(self.cancelled.wait, 5):
            raise TimeoutError("test did not cancel job")
        return JobStatus(RunState.CANCELLED)

    async def status(self):
        return self.snapshot

    async def cancel(self):
        self.cancel_calls += 1
        self.cancelled.set()

    async def stop_observation(self):
        pass

    async def close(self):
        pass


def _controlled_job(*, fail_submit=False, already_running=False):
    execution = _ControlledExecution(fail_submit=fail_submit, already_running=already_running)
    spec = ExecutionSpec(("program",))
    job = Job(execution, spec)
    if not already_running:
        job.configure_submission(
            {"argv": ["program"]}, stream_offsets={}, stream_paths={},
        )
    return job, execution


def test_public_run_latches_cancel_before_submit(tmp_path):
    job, execution = _controlled_job()
    resource = _Resource()
    client = ExecutionClient()
    run = Run(RunParts(job, client.file_session(job.spec)), client=client,
              owned_resources=ExecutionResources(resource))
    run.cancel().result(timeout=3)
    assert run.state is RunState.CANCELLED
    assert resource.closed
    assert not execution.entered.is_set()
    with pytest.raises(asyncio.CancelledError):
        run.submit()


def test_cancel_during_submit_is_dispatched_after_submission():
    job, execution = _controlled_job()
    errors = []

    def submit():
        try:
            job.submit()
        except BaseException as exc:
            errors.append(exc)

    thread = Thread(target=submit, daemon=True)
    thread.start()
    try:
        assert execution.entered.wait(5)
        future = job.cancel()
    finally:
        execution.release.set()
    thread.join(5)
    assert not thread.is_alive()
    assert not errors
    assert future.result(timeout=5) is None
    job.wait()
    assert job.state is RunState.CANCELLED
    assert job.cancel().result(timeout=5) is None
    assert execution.cancel_calls == 1


def test_cancel_started_run_does_not_close_owned_resources_before_monitor_finishes():
    job, execution = _controlled_job(already_running=True)
    resource = _Resource()
    client = ExecutionClient()
    resources = ExecutionResources(resource)
    run = Run(RunParts(job, client.file_session(job.spec)), client=client, owned_resources=resources)
    future = run.cancel()
    future.result(timeout=5)
    assert not resources.closed
    assert not resource.closed
    assert execution.cancel_calls == 1


def test_attached_job_can_start_after_thread_creation_failure(monkeypatch):
    job, execution = _controlled_job(already_running=True)

    def fail(_thread):
        raise RuntimeError("cannot start thread")

    with monkeypatch.context() as patch:
        patch.setattr(Thread, "start", fail)
        with pytest.raises(RuntimeError, match="cannot start thread"):
            job.start()
        assert not job.observing

    job.start()
    try:
        job.start()
        future = job.cancel()
        future.result(timeout=5)
        assert job.wait().state == RunState.CANCELLED
        assert job.cancel().result(timeout=5) is None
        assert execution.observation_calls == 1
        assert execution.cancel_calls == 1
    finally:
        execution.cancelled.set()


@pytest.mark.parametrize("observe", [False, True])
@pytest.mark.parametrize("first_fails", [False, True])
def test_cancel_can_be_retried_while_job_remains_active(observe, first_fails):
    job, execution = _controlled_job(already_running=True)
    entered, release = Event(), Event()

    async def cancel():
        execution.cancel_calls += 1
        if execution.cancel_calls == 1:
            entered.set()
            if not await asyncio.to_thread(release.wait, 5):
                raise TimeoutError("test did not release cancellation")
            if first_fails:
                raise OSError("connection lost")
            return JobStatus(RunState.RUNNING)
        execution.cancelled.set()
        return JobStatus(RunState.CANCELLED)

    execution.cancel = cancel
    if observe:
        job.start()
    try:
        first = job.cancel()
        assert entered.wait(5)
        assert job.cancel() is first
        release.set()
        if first_fails:
            with pytest.raises(OSError, match="connection lost"):
                first.result(timeout=5)
        else:
            assert first.result(timeout=5).state == RunState.RUNNING
        second = job.cancel()
        assert second is not first
        assert second.result(timeout=5).state == RunState.CANCELLED
        assert execution.cancel_calls == 2
        if observe:
            assert job.wait().state == RunState.CANCELLED
            job.cancel().result(timeout=5)
            assert execution.cancel_calls == 2
    finally:
        release.set()
        execution.cancelled.set()


def test_cancel_while_observation_closes_reports_undispatched_request():
    job, execution = _controlled_job(already_running=True)
    closing, release_close = Event(), Event()

    async def observe(on_output, *, output_requested=None):
        return JobStatus(RunState.UNKNOWN)

    async def close():
        closing.set()
        if not await asyncio.to_thread(release_close.wait, 5):
            raise TimeoutError("test did not release close")

    execution.observe = observe
    execution.close = close
    job.start()
    try:
        assert closing.wait(5)
        future = job.cancel()
        release_close.set()
        with pytest.raises(RuntimeError, match="before cancellation was dispatched"):
            future.result(timeout=5)
        assert job.wait().state == RunState.UNKNOWN
        assert execution.cancel_calls == 0
        job.cancel().result(timeout=5)
        assert execution.cancel_calls == 1
    finally:
        release_close.set()


def test_cancel_without_execution_reference_does_not_leave_unresolved_future():
    execution = _ControlledExecution()
    execution.snapshot = JobStatus(RunState.UNKNOWN)
    job = Job(execution, ExecutionSpec(("program",)))
    with pytest.raises(RuntimeError, match="No execution reference"):
        job.cancel().result(timeout=5)
    assert execution.cancel_calls == 0


def test_cancel_during_submission_survives_detaching_observation():
    job, execution = _controlled_job()
    execution.can_stop_observation = True
    thread = Thread(target=job.submit, daemon=True)
    thread.start()
    try:
        assert execution.entered.wait(5)
        future = job.cancel()
        job.stop_observation()
        execution.release.set()
        future.result(timeout=5)
        assert job.wait().state == RunState.UNKNOWN
        assert execution.cancel_calls == 1
        assert execution.observation_calls == 0
    finally:
        execution.release.set()
        execution.cancelled.set()
        thread.join(5)
