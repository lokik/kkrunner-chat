"""Backend contract invariants, independent of backend implementation."""

from dataclasses import FrozenInstanceError

import pytest

from kkrunner.runs.states import RunState

from kkrunner.backends.contracts import (
    JobFailurePhase,
    JobRef,
    JobStatus,
    RecoveryDisposition,
    RecoveryResult,
)


@pytest.mark.parametrize("state", list(RunState))
def test_only_real_terminal_states_are_terminal(state):
    assert state.is_terminal == (state in {RunState.FINISHED, RunState.FAILED, RunState.CANCELLED})


@pytest.mark.parametrize("state", (RunState.SUBMITTING, RunState.DOWNLOADING, RunState.POSTPROCESSING))
def test_client_phases_are_not_backend_statuses(state):
    with pytest.raises(ValueError, match="client lifecycle phase"):
        JobStatus(state)



def test_job_ref_contains_backend_scoped_identity():
    ref = JobRef("psij", "opaque-native-job")
    assert ref.backend == "psij"
    assert ref.token == "opaque-native-job"
    with pytest.raises(FrozenInstanceError):
        ref.token = "replacement"
    with pytest.raises(ValueError, match="nonempty"):
        JobRef("", "a")
    with pytest.raises(ValueError, match="nonempty"):
        JobRef("process", "")


def test_recovery_result_requires_job_identity_only_if_found():
    ref = JobRef("process", "opaque")
    result = RecoveryResult.found(ref)
    assert result.disposition is RecoveryDisposition.FOUND
    assert result.ref == ref
    for result in (RecoveryResult.absent(), RecoveryResult.indeterminate(), RecoveryResult.unsupported()):
        assert result.ref is None
    with pytest.raises(ValueError, match="Exactly FOUND"):
        RecoveryResult(RecoveryDisposition.FOUND)
    with pytest.raises(ValueError, match="Exactly FOUND"):
        RecoveryResult(RecoveryDisposition.ABSENT, ref)


def test_status_validates_execution_facts_without_client_phases():
    status = JobStatus("failed", return_code=7, failure_phase="execution", message="exit 7")
    assert status.state is RunState.FAILED
    assert status.failure_phase is JobFailurePhase.EXECUTION
    with pytest.raises(ValueError):
        JobStatus("downloading")
    with pytest.raises(TypeError):
        JobStatus(RunState.FINISHED, return_code=True)


def test_backends_are_namespaces_and_endpoints_own_jobs(tmp_path):
    from kkrunner.backends import DirectBackend, SupervisedBackend, PsijBackend
    from kkrunner.backends.contracts import Backend
    from kkrunner.client.contracts import JobProtocol
    from kkrunner.client import LocalRunEndpoint
    from kkrunner.client.job import Job
    from kkrunner.client.contracts import RunEndpoint
    from kkrunner.runs.base import ExecutionSpec
    for backend in (DirectBackend, SupervisedBackend, PsijBackend):
        assert issubclass(backend, Backend)
        assert not hasattr(backend, "create_job")
        assert not hasattr(backend, "create_run_parts")
        endpoint = LocalRunEndpoint(backend)
        assert isinstance(endpoint, RunEndpoint)
        job = endpoint.create_run_parts(ExecutionSpec(("true",), directory=str(tmp_path))).job
        assert type(job) is Job
        assert isinstance(job, JobProtocol)
    assert DirectBackend.session_bound
    assert not SupervisedBackend.session_bound
    assert not SupervisedBackend.session_bound
    assert not PsijBackend.session_bound


def test_direct_rejects_reattachment_without_resubmission(tmp_path):
    from kkrunner.client import LocalRunEndpoint
    from kkrunner.backends.contracts import UnsupportedCapability
    with pytest.raises(UnsupportedCapability, match="durable identity"):
        LocalRunEndpoint().open_run_parts(object())


def test_local_and_ssh_managed_endpoints_use_one_client_job_class(tmp_path):
    from kkrunner.backends import SupervisedBackend
    from kkrunner.client import LocalRunEndpoint, SshRunEndpoint, SshTransport
    from kkrunner.client.job import Job
    from kkrunner.runs.base import ExecutionSpec

    spec = ExecutionSpec(("true",), directory=str(tmp_path))
    local = LocalRunEndpoint(SupervisedBackend).create_run_parts(spec).job
    remote = SshRunEndpoint(SshTransport("example"), backend=SupervisedBackend).create_run_parts(spec).job
    assert type(local) is type(remote) is Job

def test_managed_job_does_not_expose_direct_detached_configuration(tmp_path):
    from kkrunner.client.job import Job
    from kkrunner.client.run_parts import RunParts
    from kkrunner.runs.base import ExecutionSpec
    class Context:
        backend = "process"
    managed = Job(Context(), ExecutionSpec(("true",), directory=str(tmp_path)))
    RunParts(managed).configure_detached(True, default=True)
    with pytest.raises(ValueError):
        RunParts(managed).configure_detached(False, default=True)


def test_client_context_provides_backend_identity_for_job_ref(tmp_path):
    from types import SimpleNamespace
    from kkrunner.backends import PsijBackend
    from kkrunner.client.job import Job
    from kkrunner.runs.base import ExecutionSpec

    context = SimpleNamespace(backend=PsijBackend)
    job = Job(context, ExecutionSpec(("true",), directory=str(tmp_path)), backend_id="opaque-id")
    assert job.ref == JobRef("psij", "opaque-id")
