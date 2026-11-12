"""Backend contract invariants, independent of backend implementation."""

from dataclasses import FrozenInstanceError

import pytest

from kkrunner.runs.states import RunState

from kkrunner.backends.contracts import (
    BackendCapability,
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
    from kkrunner.client import EndpointClient
    from kkrunner.client.job import Job
    from kkrunner.runs.base import ExecutionSpec

    for backend in (DirectBackend, SupervisedBackend, PsijBackend):
        assert issubclass(backend, Backend)
        assert not hasattr(backend, "create_job")
        assert not hasattr(backend, "create_run_parts")
        endpoint = EndpointClient(backend)
        assert callable(endpoint.create_run_parts)
        job = endpoint.create_run_parts(
            ExecutionSpec(("true",), directory=str(tmp_path))
        ).job
        assert type(job) is Job
        assert job.execution.backend is endpoint.backend
        assert job.execution.transport is endpoint.transport
        assert all(hasattr(job, name) for name in ("submit", "wait", "cancel", "status", "ref"))

    assert DirectBackend().execution_mode == "direct"
    assert SupervisedBackend().execution_mode == PsijBackend().execution_mode == "managed"
    managed = frozenset({
        BackendCapability.REATTACH, BackendCapability.RECOVER, BackendCapability.STATUS,
    })
    assert DirectBackend().supports == frozenset()
    assert SupervisedBackend().supports == managed
    assert PsijBackend().supports == managed


def test_direct_rejects_reattachment_without_resubmission():
    from kkrunner.backends import DirectBackend
    from kkrunner.backends.contracts import UnsupportedCapability
    from kkrunner.client import EndpointClient

    with pytest.raises(UnsupportedCapability, match="reattachment"):
        EndpointClient(DirectBackend).open_run_parts(object())


def test_local_and_ssh_managed_endpoints_use_one_client_job_class(tmp_path):
    from kkrunner.backends import SupervisedBackend
    from kkrunner.client import EndpointClient, SshTransport, StagingSpec
    from kkrunner.client.job import Job
    from kkrunner.runs.base import ExecutionSpec

    spec = ExecutionSpec(
        ("true",), directory=str(tmp_path),
        staging=StagingSpec(local_root=str(tmp_path), remote_root=str(tmp_path / "remote")),
    )
    local = EndpointClient(SupervisedBackend).create_run_parts(spec).job
    remote = EndpointClient(SupervisedBackend, transport=SshTransport("example")).create_run_parts(spec).job
    assert type(local) is type(remote) is Job
    assert local.execution.transport.is_local
    assert not remote.execution.transport.is_local
    assert local.execution.backend.name == remote.execution.backend.name == "process"


def test_managed_job_rejects_direct_detached_configuration(tmp_path):
    from kkrunner.backends import SupervisedBackend
    from kkrunner.client import EndpointClient
    from kkrunner.runs.base import ExecutionSpec

    parts = EndpointClient(SupervisedBackend).create_run_parts(
        ExecutionSpec(("true",), directory=str(tmp_path))
    )
    parts.configure_detached(True)
    with pytest.raises(ValueError, match="fixed by the selected backend"):
        parts.configure_detached(False)


def test_client_execution_binding_provides_backend_identity_for_job_ref(tmp_path):
    from kkrunner.backends import PsijBackend
    from kkrunner.client import EndpointClient
    from kkrunner.client.backend_execution import bind_execution
    from kkrunner.client.job import Job
    from kkrunner.runs.base import ExecutionSpec

    endpoint = EndpointClient(PsijBackend)
    execution = bind_execution(
        endpoint.backend, endpoint.transport,
        ref=JobRef("psij", "opaque-id"), status={"state": "running"},
    )
    job = Job(execution, ExecutionSpec(("true",), directory=str(tmp_path)))
    assert job.ref == JobRef("psij", "opaque-id")
    assert job.backend_id == "opaque-id"
    assert job.status().state is RunState.RUNNING
