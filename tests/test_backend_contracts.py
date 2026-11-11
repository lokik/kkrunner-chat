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


def test_backend_and_client_contracts_have_distinct_owners():
    from kkrunner.backends.contracts import Backend, BackendJobContext, SubmissionContext
    from kkrunner.client.contracts import JobAttachment, RunEndpoint, ReopenableRunEndpoint
    from kkrunner.client import AgentRunEndpoint, SshDirectRunEndpoint, LocalTransport
    from kkrunner.backends import DirectProcessBackend, SupervisedProcessBackend, PsijBackend
    from kkrunner.client.run_parts import run_endpoint_for_executor

    assert isinstance(DirectProcessBackend(), Backend)
    assert isinstance(SupervisedProcessBackend(), Backend)
    assert not isinstance(AgentRunEndpoint(LocalTransport()), Backend)
    assert not isinstance(SshDirectRunEndpoint(LocalTransport()), Backend)
    assert not isinstance(SshDirectRunEndpoint(LocalTransport()), ReopenableRunEndpoint)
    assert not hasattr(AgentRunEndpoint, "create_job")
    assert not hasattr(SshDirectRunEndpoint, "create_job")
    assert run_endpoint_for_executor(DirectProcessBackend()).backend.__class__ is DirectProcessBackend
    assert JobAttachment.__module__ == "kkrunner.client.contracts"
    assert Backend.__module__ == "kkrunner.backends.contracts"
    assert BackendJobContext.__module__ == "kkrunner.backends.contracts"
    assert SubmissionContext.__module__ == "kkrunner.backends.contracts"
    assert RunEndpoint.__module__ == "kkrunner.client.contracts"


def test_native_backend_attach_receives_only_job_context(tmp_path):
    from kkrunner.backends.contracts import BackendJobContext, JobRef, SubmissionContext
    from kkrunner.client.contracts import JobAttachment
    from kkrunner.client.run_parts import backend_job_context
    from kkrunner.client.endpoint import ExecutionEndpointConfig
    from kkrunner.runs.model import Execution, WorkingDirectory
    from kkrunner.files.manifest import FileManifest, FileEntry

    execution = Execution(ExecutionEndpointConfig(executor="psij"), WorkingDirectory(str(tmp_path)), "native-1")
    files = FileManifest(stdout=FileEntry(local_path=str(tmp_path / "output")))
    client = JobAttachment(execution, files)
    backend = backend_job_context(client, "psij")
    assert isinstance(backend, BackendJobContext)
    assert backend.ref == JobRef("psij", "native-1")
    assert backend.output_file == files.output_file
    assert not hasattr(backend, "files")
    assert not hasattr(SubmissionContext(execution), "files")


def test_raw_ssh_and_local_share_native_job_factory(monkeypatch, tmp_path):
    from kkrunner.backends import DirectProcessBackend
    from kkrunner.client import SshDirectRunEndpoint, LocalTransport
    from kkrunner.runs.base import ExecutionSpec

    calls = []
    original = DirectProcessBackend.create_job

    def observed(self, spec, *, submission_key=None, callback=None):
        calls.append((self._adapter_factory is not None, submission_key))
        return original(self, spec, submission_key=submission_key, callback=callback)

    monkeypatch.setattr(DirectProcessBackend, "create_job", observed)
    spec = ExecutionSpec(("true",), directory=str(tmp_path))
    DirectProcessBackend().create_job(spec)
    SshDirectRunEndpoint(LocalTransport()).create_run_parts(spec, submission_key="token")
    assert calls == [(False, None), (True, "token")]


def test_every_native_backend_has_explicit_nominal_contract():
    from kkrunner.backends.contracts import Backend, ReopenableBackend, RecoverableBackend
    from kkrunner.backends import DirectProcessBackend, SupervisedProcessBackend, PsijBackend
    from kkrunner.client.contracts import RunEndpoint, ReopenableRunEndpoint
    from kkrunner.client import AgentRunEndpoint, SshDirectRunEndpoint
    from kkrunner.client.run_parts import BackendRunEndpoint, ReopenableBackendRunEndpoint

    assert Backend in DirectProcessBackend.__mro__
    assert RecoverableBackend in SupervisedProcessBackend.__mro__
    assert RecoverableBackend in PsijBackend.__mro__
    assert not issubclass(SupervisedProcessBackend, DirectProcessBackend)
    assert not issubclass(DirectProcessBackend, ReopenableBackend)
    assert DirectProcessBackend.name != SupervisedProcessBackend.name
    assert issubclass(BackendRunEndpoint, RunEndpoint)
    assert issubclass(ReopenableBackendRunEndpoint, ReopenableRunEndpoint)
    assert issubclass(AgentRunEndpoint, ReopenableRunEndpoint)
    assert issubclass(SshDirectRunEndpoint, RunEndpoint)
    assert not issubclass(SshDirectRunEndpoint, ReopenableRunEndpoint)


def test_job_is_a_structural_contract_not_an_inherited_abstract_property(tmp_path):
    from kkrunner.backends.contracts import JobProtocol
    from kkrunner.backends import DirectProcessBackend
    from kkrunner.runs.base import ExecutionSpec

    spec = ExecutionSpec(("true",), directory=str(tmp_path))
    job = DirectProcessBackend().create_job(spec)
    assert isinstance(job, JobProtocol)
    assert job.spec is spec


def test_protocol_definitions_are_independent_of_implementations():
    import kkrunner.backends as backends
    import kkrunner.client as client
    from kkrunner.backends.contracts import Backend, JobProtocol, RecoverableBackend
    from kkrunner.client.contracts import RunEndpoint, ReopenableRunEndpoint, RunningCommand

    assert Backend.__module__ == "kkrunner.backends.contracts"
    assert RecoverableBackend.__module__ == "kkrunner.backends.contracts"
    assert JobProtocol.__module__ == "kkrunner.backends.contracts"
    assert RunEndpoint.__module__ == "kkrunner.client.contracts"
    assert ReopenableRunEndpoint.__module__ == "kkrunner.client.contracts"
    assert RunningCommand.__module__ == "kkrunner.client.contracts"
    assert not hasattr(backends, "ProcessBackend")
    assert not hasattr(backends, "ProtocolBackend")
    assert not hasattr(client, "BackendPool")
    assert not hasattr(client, "ExecutionEndpoint")


def test_supervised_and_agent_use_one_client_job_class(tmp_path):
    from kkrunner.backends import SupervisedProcessBackend
    from kkrunner.client.job import Job
    from kkrunner.client.agent import AgentRunEndpoint
    from kkrunner.client.transport import LocalTransport
    from kkrunner.runs.base import ExecutionSpec

    spec = ExecutionSpec(("true",), directory=str(tmp_path))
    local = SupervisedProcessBackend().create_job(spec)
    remote = AgentRunEndpoint(LocalTransport(), backend="process").create_run_parts(spec).job
    assert type(local) is Job
    assert type(remote) is Job


def test_managed_job_does_not_expose_direct_detached_configuration(tmp_path):
    from kkrunner.client.job import Job
    from kkrunner.client.run_parts import RunParts
    from kkrunner.runs.base import ExecutionSpec

    class Context:
        backend = "process"

    managed = Job(Context(), ExecutionSpec(("true",), directory=str(tmp_path)))
    parts = RunParts(managed)
    parts.configure_detached(True, default=True)
    assert not managed._direct
    assert managed._direct_session is None
    try:
        parts.configure_detached(False, default=True)
    except ValueError:
        pass
    else:
        raise AssertionError("Managed backend detached policy was incorrectly configurable")


def test_agent_context_provides_backend_identity_for_job_ref(tmp_path):
    from types import SimpleNamespace

    from kkrunner.backends.contracts import JobRef
    from kkrunner.client.agent import _AgentRunContext
    from kkrunner.client.job import Job
    from kkrunner.runs.base import ExecutionSpec, LaunchSpec

    endpoint = SimpleNamespace(backend="psij", transport=object())
    context = _AgentRunContext(endpoint, LaunchSpec())
    job = Job(context, ExecutionSpec(("true",), directory=str(tmp_path)),
              backend_id="opaque-id")
    assert job.ref == JobRef("psij", "opaque-id")
