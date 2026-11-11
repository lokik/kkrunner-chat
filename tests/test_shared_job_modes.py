"""Direct, supervised, PSI/J and agent use one client Job lifecycle."""

import sys
from types import SimpleNamespace

import pytest

from kkrunner import ExecutionSpec, RunState
from kkrunner.client.local import LocalRunEndpoint
from kkrunner.backends import SupervisedBackend
from kkrunner.client.job import Job
from kkrunner.client.transport import LocalTransport
from kkrunner.client.ssh import SshRunEndpoint


def test_direct_local_and_direct_ssh_create_the_same_job_class(tmp_path):
    spec = ExecutionSpec((sys.executable, "-c", "print('ready')"), directory=str(tmp_path))
    local = LocalRunEndpoint().create_job(spec)
    remote = SshRunEndpoint(LocalTransport()).create_run_parts(spec).job
    assert type(local) is type(remote) is Job
    assert local.adapter.expose_pid
    assert not remote.adapter.expose_pid
    assert local.state is remote.state is RunState.NEW


def test_supervised_and_remote_agent_create_the_same_job_class(tmp_path):
    spec = ExecutionSpec(("true",), directory=str(tmp_path))
    supervised = LocalRunEndpoint(SupervisedBackend).create_job(spec)
    agent = SshRunEndpoint(LocalTransport()).create_run_parts(spec).job
    assert type(supervised) is type(agent) is Job
    assert not supervised._direct
    assert not agent._direct


def test_psij_client_uses_the_same_job_class(monkeypatch, tmp_path):
    import kkrunner.backends.psij as module

    from kkrunner.client.local import LocalRunEndpoint
    from kkrunner.backends import PsijBackend
    spec = ExecutionSpec(("true",), directory=str(tmp_path))
    job = LocalRunEndpoint(PsijBackend, backend_options={"executor_name": "slurm"}).create_job(spec, submission_key="key")
    assert type(job) is Job
    assert job.context.backend == "psij"
    assert job.state is RunState.NEW


def test_direct_confirmed_exit_is_distinct_from_unknown_channel(tmp_path):
    from kkrunner.client.direct_channels import LocalDirectChannel

    class UncertainChannel(LocalDirectChannel):
        expose_pid = False
        uncertain_after_launch = True

        def exit_status(self, code):
            assert code == 0
            return RunState.UNKNOWN, None, "control channel lost"

    job = Job.direct(
        ExecutionSpec((sys.executable, "-c", "pass"), directory=str(tmp_path)),
        adapter=UncertainChannel(),
    )
    job.run()
    assert job.state is RunState.UNKNOWN
    assert job.backend_id is None
    assert job.return_code is None


def test_direct_cancel_before_submit_never_launches(tmp_path):
    job = Job.direct(ExecutionSpec((sys.executable, "-c", "pass"), directory=str(tmp_path)))
    job.cancel()
    assert job.state is RunState.CANCELLED
    assert job.wait().return_code is None
    with pytest.raises(RuntimeError, match="cancelled"):
        job.submit()


def test_direct_session_is_private_resource_owner_without_second_job_state(tmp_path):
    from kkrunner.client.direct_session import DirectSession

    spec = ExecutionSpec((sys.executable, "-c", "print('ok')"), directory=str(tmp_path))
    job = LocalRunEndpoint().create_job(spec)
    assert isinstance(job._direct_session, DirectSession)
    assert job._direct_session.adapter is job.adapter
    assert not hasattr(job._direct_session, "state")
    assert not hasattr(job._direct_session, "result")
    assert job.run().return_code == 0
    assert job.state is RunState.FINISHED
    assert job._direct_session._process is None
    assert job._direct_session._loop is None
    assert job._direct_session.pid is not None


def test_direct_session_preserves_cancel_before_launch(tmp_path):
    spec = ExecutionSpec((sys.executable, "-c", "raise SystemExit(12)"), directory=str(tmp_path))
    job = LocalRunEndpoint().create_job(spec)
    job.cancel()
    assert job.state is RunState.CANCELLED
    assert job._direct_session._cancel_requested.is_set()
    assert job.wait().return_code is None
    with pytest.raises(RuntimeError, match="cancelled"):
        job.submit()


def test_direct_session_does_not_relabel_failed_launch(tmp_path):
    spec = ExecutionSpec(("this-command-does-not-exist-kkrunner",), directory=str(tmp_path))
    job = LocalRunEndpoint().create_job(spec)
    from kkrunner.runs.base import SubmissionNotStarted, FailurePhase
    with pytest.raises(SubmissionNotStarted):
        job.submit()
    assert job.state is RunState.FAILED
    assert job.failure_phase is FailurePhase.LAUNCH
    assert job._direct_session._process is None
    assert job._direct_session._loop is None
