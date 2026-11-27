"""Backend outcome and client file delivery have independent lifecycles."""

import asyncio
import subprocess

import pytest

from kkrunner import ExecutionSpec, Run, RunState
from kkrunner.client.run_parts import RunParts
from kkrunner.backends import SupervisedBackend
from kkrunner.backends.contracts import JobRef
from kkrunner.client import ExecutionClient, StagingSpec
from kkrunner.client.backend_execution import ManagedExecution
from kkrunner.client.file_context import FileContext
from kkrunner.client.file_session import FileSession
from kkrunner.client.job import Job
from kkrunner.targets import LocalTarget
from kkrunner.files.manifest import FileEntry, FileManifest, FileSyncPolicy
from kkrunner.runs.base import FailurePhase


class _Transport(LocalTarget):
    """Remote-like observation over real local files, with no SSH daemon."""

    is_local = False

    def __init__(self, state=RunState.FINISHED):
        super().__init__()
        object.__setattr__(self, "backend_state", RunState(state))
        object.__setattr__(self, "status_error", None)
        object.__setattr__(self, "follow_error", None)
        object.__setattr__(self, "follow_calls", 0)

    def facts(self):
        return {
            "state": self.backend_state.value,
            "remote_id": "opaque-handle",
            "return_code": 17 if self.backend_state == RunState.FAILED else (
                0 if self.backend_state == RunState.FINISHED else None
            ),
            "failure_phase": "execution" if self.backend_state == RunState.FAILED else None,
            "error": "application failed" if self.backend_state == RunState.FAILED else None,
        }

    async def call(self, operation, *args, **kwargs):
        if operation.method == "status":
            if self.status_error is not None:
                raise self.status_error
            return self.facts()
        return await super().call(operation, *args, **kwargs)

    async def follow(self, operation, identity, on_output, **options):
        object.__setattr__(self, "follow_calls", self.follow_calls + 1)
        if self.follow_error is not None:
            raise self.follow_error
        if self.status_error is not None:
            from kkrunner.client.contracts import Operation
            await self.call(Operation.for_backend(SupervisedBackend, "status"), identity)
        on_output(b"streamed\n", "out")
        return self.facts()


def _session(tmp_path, transport, spec, *, files=None):
    client = ExecutionClient(SupervisedBackend, target=transport)
    context = FileContext(
        client, StagingSpec(local_root=tmp_path, remote_root=tmp_path,
                            output_sync=FileSyncPolicy.CHANGED),
    )
    return client, FileSession(
        context, spec, files=files, remote_directory=str(tmp_path),
    )


def _run(tmp_path, state=RunState.FINISHED):
    destination = tmp_path / "calculation.out"
    remote = tmp_path / "authoritative.out"
    remote.write_bytes(b"streamed\n")
    destination.write_bytes(b"old local output")
    spec = ExecutionSpec(argv=(), output_file=destination, directory=tmp_path)
    files = FileManifest(stdout=FileEntry(str(destination), str(remote))).initialize_availability()
    transport = _Transport(state)
    client, session = _session(tmp_path, transport, spec, files=files)
    execution = ManagedExecution(
        client.backend, transport, ref=JobRef("kkrunner", "opaque-handle"),
        status={"state": "running", "remote_id": "opaque-handle"},
    )
    job = Job(execution, spec)
    return Run(RunParts(job, session), client=client), transport, destination


def test_terminal_backend_state_triggers_file_synchronization(tmp_path):
    run, transport, destination = _run(tmp_path)
    run.start()
    assert run.wait().return_code == 0
    assert run.state == RunState.FINISHED
    assert destination.read_bytes() == b"streamed\n"
    assert run.files.availability_state == "available"
    assert transport.follow_calls == 1


def test_unknown_backend_state_preserves_existing_output_without_downloading(tmp_path):
    run, _, destination = _run(tmp_path, RunState.UNKNOWN)

    async def fail_sync(*, required):
        raise AssertionError("Unknown execution must not download results")

    run._file_session._synchronize = fail_sync
    run.start()
    outcome = run.wait()
    assert outcome.output_file == str(destination)
    assert run.state == RunState.UNKNOWN
    assert destination.read_bytes() == b"old local output"


@pytest.mark.parametrize(
    "state, expected_phase",
    [
        (RunState.FINISHED, FailurePhase.DOWNLOAD),
        (RunState.FAILED, FailurePhase.EXECUTION),
        (RunState.CANCELLED, None),
    ],
)
def test_diagnostic_transfer_error_preserves_backend_outcome(tmp_path, state, expected_phase):
    run, _, _ = _run(tmp_path, state)

    async def unavailable(*, required):
        raise OSError("download unavailable")

    run._file_session._synchronize = unavailable
    run.start()
    if state == RunState.FINISHED:
        with pytest.raises(OSError, match="download unavailable"):
            run.wait()
        assert run.state == RunState.FAILED
    else:
        run.wait()
        assert run.state == state
    assert run.failure_phase == expected_phase


@pytest.mark.parametrize("kind", ("follow", "status"))
def test_remote_transport_failure_is_recoverable_unknown(tmp_path, kind):
    run, transport, _ = _run(tmp_path, RunState.RUNNING)
    if kind == "follow":
        object.__setattr__(transport, "follow_error", ConnectionError("SSH disconnected"))
    else:
        object.__setattr__(transport, "status_error", OSError("SSH status unavailable"))
    run.start()
    result = run.wait()
    assert result.return_code is None
    assert run.state == RunState.UNKNOWN
    assert run.failure_phase is None


def test_lost_follow_with_confirmed_terminal_status_still_downloads_files(tmp_path):
    run, transport, destination = _run(tmp_path, RunState.FINISHED)
    object.__setattr__(
        transport, "follow_error", subprocess.CalledProcessError(17, ("ssh",)),
    )
    run.start()
    assert run.wait().return_code == 0
    assert run._job.state == RunState.FINISHED
    assert isinstance(run._job.output_error, subprocess.CalledProcessError)
    assert destination.read_bytes() == b"streamed\n"
    assert run.state == RunState.FINISHED
    assert run.failure_phase is None
