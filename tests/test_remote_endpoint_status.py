"""Backend execution status and recovery remain separate from client file transfer."""

import pytest

from kkrunner import RunState
from kkrunner.backends.contracts import RecoveryDisposition
from kkrunner.client.contracts import JobAttachment
from kkrunner.client import EndpointClient, SshTransport
from kkrunner.backends.config import BackendConfig
from kkrunner.files import FileManifest
from kkrunner.runs import ExecutionMetadata, FailurePhase, RunRecord, WorkingDirectory


def _run(native_id, *, backend="psij"):
    transport = SshTransport("cluster")
    return RunRecord(
        id=native_id or "unconfirmed",
        state=RunState.RUNNING,
        created_at="2026-10-13T08:00:00+00:00",
        updated_at="2026-10-13T08:00:00+00:00",
        execution=ExecutionMetadata(transport, WorkingDirectory("/local/case", "/work/case"), native_id, backend=BackendConfig(backend)),
        files=FileManifest(),
        metadata={"test": {"task": "DOS"}},
        processing_required=True,
    )


def _attachment(record):
    return JobAttachment(record.execution, record.files)


def test_remote_endpoint_status_queries_opaque_handles_individually():
    executor = EndpointClient("psij", transport=SshTransport("cluster"))
    calls = []
    replies = {
        "job-1": {"state": "running", "return_code": None},
        "job-2": {
            "state": "failed",
            "failure_phase": "launch",
            "error": "missing executable",
        },
    }

    async def request(action, *args, **kwargs):
        calls.append((action, args, kwargs))
        return replies[args[0]]

    executor._invoke = request
    statuses = executor.status(*(_attachment(_run(ref)) for ref in ("job-1", "job-2")))
    assert calls == [("status", ("job-1",), {}), ("status", ("job-2",), {})]
    assert [status.backend_id for status in statuses] == ["job-1", "job-2"]
    assert [status.state for status in statuses] == [RunState.RUNNING, RunState.FAILED]
    assert statuses[1].failure_phase == FailurePhase.LAUNCH
    assert "missing executable" in statuses[1].message


def test_remote_terminal_status_does_not_claim_client_completion():
    executor = EndpointClient("process", transport=SshTransport("cluster"))
    calls = []

    async def request(action, *args, **kwargs):
        calls.append((action, args, kwargs))
        return {"state": "finished", "return_code": 0}

    executor._invoke = request
    (status,) = executor.status(_attachment(_run("opaque-process", backend="process")))
    assert status.state == RunState.FINISHED
    assert calls == [("status", ("opaque-process",), {})]


def test_remote_endpoint_status_rejects_reference_for_other_backend():
    executor = EndpointClient("process", transport=SshTransport("cluster"))
    with pytest.raises(ValueError, match="different transport or backend"):
        executor.status(_attachment(_run("job", backend="psij")))


def test_recovery_proves_identity_without_claiming_execution_status():
    executor = EndpointClient("process", transport=SshTransport("cluster"))
    calls = []

    async def request(action, *args, **kwargs):
        calls.append((action, args, kwargs))
        return {
            "state": "running", "remote_id": "opaque-job", "directory": "/work/case",
            "output_file": "/work/case/run.out", "error_file": "/work/case/run.err",
        }

    executor._invoke = request
    result = executor.recover_submission(
        "run-1", _attachment(_run(None, backend="process")),
        submitted_at="2026-10-13T08:00:00+02:00",
    )
    assert calls == [(
        "recover",
        ("run-1", {"backend": "process", "backend_options": {}, "directory": "/work/case"}),
        {"submitted_at": "2026-10-13T08:00:00+02:00"},
    )]
    assert result.disposition == RecoveryDisposition.FOUND
    assert result.ref.backend == "process"
    assert result.ref.token == "opaque-job"
    assert result.directory == "/work/case"
    assert result.output_file == "/work/case/run.out"
    assert result.error_file == "/work/case/run.err"
    assert not hasattr(result, "state")


@pytest.mark.parametrize(
    "reply,expected",
    [
        (None, RecoveryDisposition.INDETERMINATE),
        ({"unsupported": True, "error": "identity cannot be reconstructed"}, RecoveryDisposition.UNSUPPORTED),
    ],
)
def test_remote_recovery_does_not_guess_non_submission(reply, expected):
    executor = EndpointClient("process", transport=SshTransport("cluster"))

    async def request(action, *args, **kwargs):
        assert action == "recover"
        return reply

    executor._invoke = request
    result = executor.recover_submission("run-1", _attachment(_run(None, backend="process")))
    assert result.disposition == expected
    assert result.ref is None


@pytest.mark.parametrize(
    "facts, expected_state",
    [
        ({"state": "running"}, RunState.RUNNING),
        ({"state": "finished", "return_code": 0}, RunState.FINISHED),
        ({"state": "failed", "return_code": 7, "failure_phase": "execution"}, RunState.FAILED),
        ({"state": "failed", "failure_phase": "launch"}, RunState.FAILED),
        ({"state": "failed", "failure_phase": "submission"}, RunState.FAILED),
        ({"state": "cancelled", "return_code": 130}, RunState.CANCELLED),
        ({"state": "unknown"}, RunState.UNKNOWN),
    ],
)
def test_status_reports_backend_facts_without_claiming_client_transfer(facts, expected_state):
    executor = EndpointClient("process", transport=SshTransport("cluster"))

    async def request(action, *args, **kwargs):
        assert action == "status"
        assert args == ("opaque-job",)
        return dict(facts)

    executor._invoke = request
    (status,) = executor.status(_attachment(_run("opaque-job", backend="process")))
    assert status.state == expected_state
    assert status.backend_id == "opaque-job"
    assert status.return_code == facts.get("return_code")
    assert status.failure_phase == (
        None if facts.get("failure_phase") is None else FailurePhase(facts["failure_phase"])
    )
