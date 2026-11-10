"""Direct, supervised, PSI/J and agent use one client Job lifecycle."""

import sys
from types import SimpleNamespace

import pytest

from kkrunner import ExecutionSpec, RunState
from kkrunner.backends import DirectProcessBackend, SupervisedProcessBackend
from kkrunner.client.job import Job
from kkrunner.client.transport import LocalTransport
from kkrunner.client.agent import AgentRunEndpoint
from kkrunner.client.ssh_direct.endpoint import SshDirectRunEndpoint


def test_direct_local_and_direct_ssh_create_the_same_job_class(tmp_path):
    spec = ExecutionSpec((sys.executable, "-c", "print('ready')"), directory=str(tmp_path))
    local = DirectProcessBackend().create_job(spec)
    remote = SshDirectRunEndpoint(LocalTransport()).create_run_parts(spec).job
    assert type(local) is type(remote) is Job
    assert local.adapter.expose_pid
    assert not remote.adapter.expose_pid
    assert local.state is remote.state is RunState.NEW


def test_supervised_and_remote_agent_create_the_same_job_class(tmp_path):
    spec = ExecutionSpec(("true",), directory=str(tmp_path))
    supervised = SupervisedProcessBackend().create_job(spec)
    agent = AgentRunEndpoint(LocalTransport()).create_run_parts(spec).job
    assert type(supervised) is type(agent) is Job
    assert not supervised._direct
    assert not agent._direct


def test_psij_client_uses_the_same_job_class(monkeypatch, tmp_path):
    import kkrunner.backends.psij.backend as module

    class NativeExecutor:
        @classmethod
        def get_instance(cls, backend, url=None, config=None):
            return object()

    monkeypatch.setattr(module, "_load_psij", lambda: SimpleNamespace(JobExecutor=NativeExecutor))
    spec = ExecutionSpec(("true",), directory=str(tmp_path))
    job = module.PsijBackend("slurm").create_job(spec, submission_key="key")
    assert type(job) is Job
    assert job.context.backend == "psij"
    assert job.state is RunState.NEW


def test_direct_confirmed_exit_is_distinct_from_unknown_channel(tmp_path):
    from kkrunner.backends.process.direct.local import LocalProcessAdapter

    class UncertainChannel(LocalProcessAdapter):
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
