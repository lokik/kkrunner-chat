"""Shared Job lifecycle across local and remote targets."""

import sys

import pytest

from kkrunner import ExecutionSpec, RunState
from kkrunner.backends import DirectBackend
from kkrunner.client import ExecutionClient, SshTarget
from kkrunner.client.job import Job


def test_direct_local_and_direct_remote_share_job_class(tmp_path):
    spec = ExecutionSpec((sys.executable, "-c", "print('ready')"), directory=str(tmp_path))
    local = ExecutionClient(DirectBackend).create_run_parts(spec).job
    remote = ExecutionClient(DirectBackend, target=SshTarget("cluster")).create_run_parts(spec).job
    assert type(local) is type(remote) is Job
    assert local.state is remote.state is RunState.NEW
    assert local.execution.target.is_local
    assert not remote.execution.target.is_local


def test_direct_cancel_before_submit_is_final(tmp_path):
    spec = ExecutionSpec((sys.executable, "-c", "pass"), directory=str(tmp_path))
    job = ExecutionClient(DirectBackend).create_run_parts(spec).job
    job.cancel()
    assert job.state is RunState.CANCELLED
    assert job.wait().return_code is None
    with pytest.raises(RuntimeError, match="cancelled"):
        job.run()


def test_direct_failed_launch_retains_launch_failure(tmp_path):
    from kkrunner.runs.base import SubmissionNotStarted, FailurePhase
    spec = ExecutionSpec(("this-command-does-not-exist-kkrunner",), directory=str(tmp_path))
    parts = ExecutionClient(DirectBackend).create_run_parts(spec)
    parts.files.prepare_submission()
    parts.prepare_job()
    job = parts.job
    with pytest.raises(SubmissionNotStarted):
        job.run()
    assert job.state is RunState.FAILED
    assert job.failure_phase is FailurePhase.LAUNCH
