"""Shared Job lifecycle across local and remote transports."""

import sys

import pytest

from kkrunner import ExecutionSpec, RunState
from kkrunner.backends import DirectBackend, PsijBackend, SupervisedBackend
from kkrunner.client import EndpointClient, LocalTransport, SshTransport
from kkrunner.client.job import Job


def test_direct_local_and_direct_remote_share_job_class(tmp_path):
    spec = ExecutionSpec((sys.executable, "-c", "print('ready')"), directory=str(tmp_path))
    local = EndpointClient(DirectBackend).create_run_parts(spec).job
    remote = EndpointClient(DirectBackend, transport=SshTransport("cluster")).create_run_parts(spec).job
    assert type(local) is type(remote) is Job
    assert local.state is remote.state is RunState.NEW
    assert local.execution.transport.is_local
    assert not remote.execution.transport.is_local


def test_supervised_local_and_remote_share_managed_job_class(tmp_path):
    spec = ExecutionSpec(("true",), directory=str(tmp_path))
    local = EndpointClient(SupervisedBackend).create_run_parts(spec).job
    remote = EndpointClient(SupervisedBackend, transport=SshTransport("cluster")).create_run_parts(spec).job
    assert type(local) is type(remote) is Job
    assert local.execution.backend.execution_mode == remote.execution.backend.execution_mode == 'managed'


def test_psij_uses_shared_job_without_constructing_native_scheduler(tmp_path):
    spec = ExecutionSpec(("true",), directory=str(tmp_path))
    client = EndpointClient(PsijBackend, backend_options={"executor_name": "slurm"})
    job = client.create_run_parts(spec, submission_key="key").job
    assert type(job) is Job
    assert isinstance(job.execution.backend, PsijBackend)
    assert job.state is RunState.NEW
    assert job.backend_id is None


def test_direct_cancel_before_submit_is_final(tmp_path):
    spec = ExecutionSpec((sys.executable, "-c", "pass"), directory=str(tmp_path))
    job = EndpointClient(DirectBackend).create_run_parts(spec).job
    job.cancel()
    assert job.state is RunState.CANCELLED
    assert job.wait().return_code is None
    with pytest.raises(RuntimeError, match="cancelled"):
        job.submit()


def test_direct_failed_launch_retains_launch_failure(tmp_path):
    from kkrunner.runs.base import SubmissionNotStarted, FailurePhase
    spec = ExecutionSpec(("this-command-does-not-exist-kkrunner",), directory=str(tmp_path))
    parts = EndpointClient(DirectBackend).create_run_parts(spec)
    parts.prepare_submission()
    parts.prepare_job()
    job = parts.job
    with pytest.raises(SubmissionNotStarted):
        job.submit()
    assert job.state is RunState.FAILED
    assert job.failure_phase is FailurePhase.LAUNCH
