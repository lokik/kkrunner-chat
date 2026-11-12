"""Shared monitor waiting contract for local-process and remote backend jobs."""

import asyncio

import pytest

from kkrunner.client.job import Job
from kkrunner.client.backend_execution import DirectExecution
from kkrunner.client.transports import LocalTransport
from kkrunner.backends import DirectBackend
from kkrunner.runs.base import ExecutionSpec, RunState


@pytest.mark.parametrize(
    "state, message",
    (
        (RunState.NEW, "Run has not been submitted"),
        (RunState.RUNNING, "Attached run has not been started"),
        (RunState.UNKNOWN, "Attached run has not been started"),
        (RunState.FINISHED, "Attached run has not been started"),
    ),
)
def test_wait_without_started_monitor_fails_immediately(tmp_path, state, message):
    spec = ExecutionSpec(argv=(), directory=str(tmp_path))
    job = Job(DirectExecution(DirectBackend(), LocalTransport()), spec)
    job._state = state
    with pytest.raises(RuntimeError, match=message):
        job.wait()


def test_wait_and_run_async_return_completed_monitor_result(tmp_path):
    spec = ExecutionSpec(argv=(), directory=str(tmp_path))
    job = Job(DirectExecution(DirectBackend(), LocalTransport()), spec)
    job._state = RunState.FINISHED
    job._thread = object()
    job._done.set()
    assert job.wait() is job.result
    assert asyncio.run(job.run_async()) is job.result
