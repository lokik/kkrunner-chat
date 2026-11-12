"""Backend outcome and client file delivery have independent lifecycles."""

import asyncio
import subprocess

import pytest

from kkrunner import ExecutionSpec, Run, RunState
from kkrunner.backends import SupervisedBackend
from kkrunner.backends.contracts import JobRef
from kkrunner.client import EndpointClient, StagingSpec
from kkrunner.client.backend_execution import ManagedExecution
from kkrunner.client.file_context import FileContext
from kkrunner.client.file_session import FileSession
from kkrunner.client.job import Job
from kkrunner.client.transports import LocalTransport
from kkrunner.files.manifest import FileEntry, FileManifest, FileSyncPolicy
from kkrunner.files.streams import OutputStreams
from kkrunner.runs.base import FailurePhase
from kkrunner.support.file_locks import FileLock


class _Transport(LocalTransport):
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
    client = EndpointClient(SupervisedBackend, transport=transport)
    context = FileContext(
        client, StagingSpec(local_root=tmp_path, remote_root=tmp_path,
                            output_sync=FileSyncPolicy.NEVER),
    )
    return client, FileSession(
        context, spec, files=files, remote_directory=str(tmp_path),
    )


def _run(tmp_path, state=RunState.FINISHED):
    destination = tmp_path / "calculation.out"
    remote = tmp_path / "authoritative.out"
    remote.write_bytes(b"streamed\n")
    output = destination.open("wb")
    spec = ExecutionSpec(argv=(), output_file=output, directory=tmp_path)
    files = FileManifest(stdout=FileEntry(str(destination), str(remote))).initialize_availability()
    transport = _Transport(state)
    client, session = _session(tmp_path, transport, spec, files=files)
    session._streams = OutputStreams(output, None)
    session._stream_positions = {"out": 0, "err": None}
    execution = ManagedExecution(
        client.backend, transport, ref=JobRef("process", "opaque-handle"),
        status={"state": "running", "remote_id": "opaque-handle"},
    )
    job = Job(execution, spec)
    return Run(job, client=client, file_session=session), transport, destination, output


def test_terminal_backend_state_triggers_file_synchronization(tmp_path):
    run, transport, destination, output = _run(tmp_path)
    run.start()
    assert run.wait().return_code == 0
    assert run.state == RunState.FINISHED
    assert destination.read_bytes() == b"streamed\n"
    assert output.closed
    assert run.files.availability_state == "available"
    assert transport.follow_calls == 1


def test_unknown_backend_state_preserves_partial_output_without_downloading(tmp_path):
    run, _, destination, output = _run(tmp_path, RunState.UNKNOWN)

    async def fail_sync(*args):
        raise AssertionError("Unknown execution must not download results")

    run._file_session._synchronize = fail_sync
    run.start()
    outcome = run.wait()
    assert outcome.output_file == str(destination)
    assert run.state == RunState.UNKNOWN
    assert destination.read_bytes() == b"streamed\n"
    assert output.closed


@pytest.mark.parametrize(
    "state, expected_phase",
    [
        (RunState.FINISHED, FailurePhase.DOWNLOAD),
        (RunState.FAILED, FailurePhase.EXECUTION),
        (RunState.CANCELLED, None),
    ],
)
def test_diagnostic_transfer_error_preserves_backend_outcome(tmp_path, state, expected_phase):
    run, _, _, output = _run(tmp_path, state)

    async def unavailable(*args):
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
    assert output.closed


def test_follow_write_is_idempotent_across_attached_clients(tmp_path):
    destination = tmp_path / "output"
    destination.write_bytes(b"abc")
    transport = _Transport(RunState.RUNNING)
    a_stream = destination.open("ab")
    b_stream = destination.open("ab")
    try:
        spec_a = ExecutionSpec(argv=(), output_file=a_stream, directory=tmp_path)
        spec_b = ExecutionSpec(argv=(), output_file=b_stream, directory=tmp_path)
        _, a = _session(tmp_path, transport, spec_a)
        _, b = _session(tmp_path, transport, spec_b)
        a._streams = OutputStreams(a_stream, None)
        b._streams = OutputStreams(b_stream, None)
        a._stream_positions = {"out": 3}
        b._stream_positions = {"out": 3}
        a._write_output(b"def", "out")
        b._write_output(b"def", "out")
        assert destination.read_bytes() == b"abcdef"
        assert a._stream_positions["out"] == b._stream_positions["out"] == 6
        b._stream_positions["out"] = 3
        with pytest.raises(RuntimeError, match="Conflicting FOLLOW bytes"):
            b._write_output(b"wrong", "out")
        assert destination.read_bytes() == b"abcdef"
        lock_path = destination.with_name(f".{destination.name}.follow.lock")
        with FileLock(lock_path) as lock:
            assert lock.acquired
    finally:
        a_stream.close()
        b_stream.close()


@pytest.mark.parametrize("kind", ("follow", "status"))
def test_remote_transport_failure_is_recoverable_unknown(tmp_path, kind):
    run, transport, _, output = _run(tmp_path, RunState.RUNNING)
    if kind == "follow":
        object.__setattr__(transport, "follow_error", ConnectionError("SSH disconnected"))
    else:
        object.__setattr__(transport, "status_error", OSError("SSH status unavailable"))
    run.start()
    result = run.wait()
    assert result.return_code is None
    assert run.state == RunState.UNKNOWN
    assert run.failure_phase is None
    assert output.closed


def test_unframed_backend_follow_failure_does_not_invent_terminal_state(tmp_path):
    run, transport, _, output = _run(tmp_path, RunState.FINISHED)
    object.__setattr__(
        transport, "follow_error", subprocess.CalledProcessError(17, ("ssh",)),
    )
    run.start()
    with pytest.raises(subprocess.CalledProcessError):
        run.wait()
    assert run._job.state == RunState.FINISHED
    assert run.result.return_code == 0
    assert isinstance(run._job.output_error, subprocess.CalledProcessError)
    assert run.state == RunState.FAILED
    assert run.failure_phase is FailurePhase.OUTPUT
    assert output.closed
