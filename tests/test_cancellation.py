"""Cancellation races at the current Job/Run ownership boundary."""

import asyncio
from threading import Event, Thread

import pytest

from kkrunner import ExecutionSpec, RunState
from kkrunner.backends.contracts import JobRef, JobStatus, SubmissionNotStarted
from kkrunner.client import EndpointClient
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

    requires_stream_retrieval = False

    def __init__(self, *, fail_submit=False, already_running=False):
        self.ref = JobRef("process", "job-1") if already_running else None
        self.snapshot = JobStatus(RunState.RUNNING if already_running else RunState.NEW)
        self.fail_submit = fail_submit
        self.entered = Event()
        self.release = Event()
        self.cancelled = Event()
        self.cancel_calls = 0

    def configure_detached(self, detached):
        pass

    def configure_observation(self, *, stream_offsets, stream_paths):
        pass

    async def submit(self, request, on_output):
        self.entered.set()
        if not await asyncio.to_thread(self.release.wait, 5):
            raise TimeoutError("test did not release submit")
        if self.fail_submit:
            raise RuntimeError("submission rejected")
        self.ref = JobRef("process", "job-1")
        return JobStatus(RunState.QUEUED)

    async def observe(self, on_output):
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
            {"argv": ["program"]}, stream_offsets={}, stream_paths={}, stream_writer=None,
        )
    return job, execution


def test_public_run_latches_cancel_before_submit(tmp_path):
    job, execution = _controlled_job()
    resource = _Resource()
    client = EndpointClient()
    run = Run(job, client=client, file_session=client.file_session(job.spec),
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
    job.cancel()
    assert execution.cancel_calls == 1


def test_cancel_started_run_does_not_close_owned_resources_before_monitor_finishes():
    job, execution = _controlled_job(already_running=True)
    resource = _Resource()
    client = EndpointClient()
    resources = ExecutionResources(resource)
    run = Run(job, client=client, file_session=client.file_session(job.spec), owned_resources=resources)
    future = run.cancel()
    future.result(timeout=5)
    assert not resources.closed
    assert not resource.closed
    assert execution.cancel_calls == 1
