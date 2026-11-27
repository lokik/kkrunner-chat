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

from .transfer_helpers import LoopbackFileTransfer


class _Transport(LoopbackFileTransfer):
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
        client.target, StagingSpec(local_root=tmp_path, remote_root=tmp_path,
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


@pytest.mark.parametrize("terminal", (RunState.FINISHED, RunState.FAILED, RunState.CANCELLED))
def test_untracked_refresh_after_observation_loss_reports_pending_download(tmp_path, terminal):
    run, transport, destination = _run(tmp_path, RunState.RUNNING)
    object.__setattr__(transport, "follow_error", ConnectionError("SSH disconnected"))
    run.start()
    run.wait()
    assert run.state == RunState.UNKNOWN

    for native, expected in (
        (RunState.RUNNING, RunState.RUNNING),
        (RunState.FINISHING, RunState.FINISHING),
        (RunState.UNKNOWN, RunState.UNKNOWN),
        (terminal, RunState.DOWNLOADING),
    ):
        object.__setattr__(transport, "backend_state", native)
        assert run.refresh() == expected
        assert run.state == expected
        assert destination.read_bytes() == b"old local output"
        assert run.files.stdout.availability_state == "pending"
    assert run.return_code == (0 if terminal == RunState.FINISHED else 17 if terminal == RunState.FAILED else None)
    assert run.refresh() == RunState.DOWNLOADING
    assert transport.follow_calls == 1


@pytest.mark.parametrize("storage", ("local", "remote", "shared"))
@pytest.mark.parametrize("phase", (FailurePhase.LAUNCH, FailurePhase.SUBMISSION))
def test_no_execution_does_not_finalize_stale_outputs(tmp_path, storage, phase, monkeypatch):
    from kkrunner.runs.base import RunStatus

    local = tmp_path / "local"
    remote = tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    for path in (local / "out", local / "result.dat", remote / "out", remote / "result.dat"):
        path.write_text("not produced by this run")
    spec = ExecutionSpec(
        argv=(), directory=local, output_file="out", output_files=("result.dat",),
    )
    client = ExecutionClient(target=LocalTarget() if storage == "local" else _Transport())
    context = FileContext(client.target, StagingSpec(
        local_root=local, remote_root=remote, shared_filesystem=storage == "shared",
    ))
    files = asyncio.run(context.map_manifest(FileManifest.from_spec(spec))).initialize_availability()
    session = FileSession(context, spec, files=files)

    async def unexpected_transfer(*args, **kwargs):
        pytest.fail("A failed launch must not synchronize result files")

    monkeypatch.setattr(session, "_synchronize", unexpected_transfer)
    final = session.finalize(RunStatus(RunState.FAILED, return_code=127, failure_phase=phase))
    assert final.captured_at is None
    assert not final.output_versions
    assert not final.cache_versions
    assert all(entry.availability_state == "skipped" for entry in final.artifacts)
    assert (local / "result.dat").read_text() == "not produced by this run"
