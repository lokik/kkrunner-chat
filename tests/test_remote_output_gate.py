"""Backend outcome and client file delivery have independent lifecycles."""

import subprocess

import pytest

from kkrunner import ExecutionSpec, Run, RunState
from kkrunner.backends import SupervisedBackend
from kkrunner.backends.contracts import JobRef, JobStatus
from kkrunner.client import EndpointClient, StagingSpec
from kkrunner.client.file_context import FileContext
from kkrunner.client.job import Job
from kkrunner.client.file_session import FileSession
from kkrunner.support.file_locks import FileLock
from kkrunner.client.transports import LocalTransport
from kkrunner.files.manifest import FileEntry, FileManifest, FileSyncPolicy
from kkrunner.runs.base import FailurePhase


class _LoopbackRemoteTransport(LocalTransport):
    """Use local file primitives while exercising the remote file lifecycle."""

    is_local = False


def _file_context(tmp_path):
    client = EndpointClient(SupervisedBackend, transport=_LoopbackRemoteTransport())
    return FileContext(
        client,
        StagingSpec(local_root=tmp_path, remote_root=tmp_path, output_sync=FileSyncPolicy.NEVER),
    )


class _BackendExecution:
    """An attached backend binding: native state and output are independent."""

    def __init__(self, state=RunState.FINISHED):
        self.backend_state = RunState(state)
        self.ref = JobRef("process", "opaque-handle")
        self.snapshot = JobStatus(RunState.RUNNING)
        self.status_error = None
        self.follow_error = None
        self.follow_calls = 0

    @property
    def requires_stream_retrieval(self):
        return False

    def configure_observation(self, *, stream_offsets, stream_paths):
        self.stream_offsets = stream_offsets
        self.stream_paths = stream_paths

    async def observe(self, on_output):
        self.follow_calls += 1
        if self.follow_error is not None:
            raise self.follow_error
        on_output(b"streamed\\n", "out")
        return await self.status()

    async def status(self):
        if self.status_error is not None:
            raise self.status_error
        return JobStatus(
            self.backend_state,
            return_code=17 if self.backend_state is RunState.FAILED else (
                0 if self.backend_state is RunState.FINISHED else None
            ),
            failure_phase="execution" if self.backend_state is RunState.FAILED else None,
            message="application failed" if self.backend_state is RunState.FAILED else None,
        )

    async def stop_observation(self):
        pass

    async def close(self):
        pass


def _run(tmp_path, state=RunState.FINISHED):
    destination = tmp_path / "calculation.out"
    remote = tmp_path / "authoritative.out"
    remote.write_bytes(b"streamed\n")
    output = destination.open("wb")
    spec = ExecutionSpec(argv=(), output_file=output, directory=tmp_path)
    files = FileManifest(
        stdout=FileEntry(str(destination), str(remote)),
    ).initialize_availability()
    execution = _BackendExecution(state)
    session = FileSession(_file_context(tmp_path), spec, files=files, remote_directory="/work/case")
    from kkrunner.files.streams import OutputStreams
    session._streams = OutputStreams(output, None)
    session._stream_positions = {"out": 0, "err": None}
    job = Job(execution, spec)
    return Run(job, file_session=session), execution, destination, output


def test_terminal_backend_state_triggers_file_synchronization(tmp_path):
    run, context, destination, output = _run(tmp_path)
    run.start()
    assert run.wait().return_code == 0
    assert run.state == RunState.FINISHED
    assert destination.read_bytes() == b"streamed\n"
    assert output.closed
    assert run.files.availability_state == "available"
    assert context.follow_calls == 1


def test_unknown_backend_state_preserves_partial_output_without_downloading(tmp_path):
    run, _, destination, output = _run(tmp_path, RunState.UNKNOWN)
    def fail_sync(*args):
        raise AssertionError("Unknown execution must not download results")
    run._file_session.finalize = fail_sync
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
    def unavailable(*args):
        raise OSError("download unavailable")
    run._file_session.finalize = unavailable
    if state == RunState.FINISHED:
        run.start()
        with pytest.raises(OSError, match="download unavailable"):
            run.wait()
        assert run.state == RunState.FAILED
    else:
        run.start()
        run.wait()
        assert run.state == state
    assert run.failure_phase == expected_phase
    assert output.closed


def test_follow_write_is_idempotent_across_attached_clients(tmp_path):
    destination = tmp_path / "output"
    destination.write_bytes(b"abc")
    a_stream = destination.open("ab")
    b_stream = destination.open("ab")
    try:
        context = _file_context(tmp_path)
        a = FileSession(context, ExecutionSpec(argv=(), output_file=a_stream, directory=tmp_path))
        b = FileSession(context, ExecutionSpec(argv=(), output_file=b_stream, directory=tmp_path))
        from kkrunner.files.streams import OutputStreams
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
        # A failed overlap must release the same portable lock used by FOLLOW.
        lock_path = destination.with_name(f".{destination.name}.follow.lock")
        with FileLock(lock_path) as lock:
            assert lock.acquired
    finally:
        a_stream.close()
        b_stream.close()


@pytest.mark.parametrize("kind", ("follow", "status"))
def test_remote_transport_failure_is_recoverable_unknown(tmp_path, kind):
    run, context, _, output = _run(tmp_path, RunState.RUNNING)
    if kind == "follow":
        context.follow_error = ConnectionError("SSH disconnected")
    else:
        context.status_error = OSError("SSH status unavailable")
    run.start()
    result = run.wait()
    assert result.return_code is None
    assert run.state == RunState.UNKNOWN
    assert run.failure_phase is None
    assert output.closed


def test_unframed_backend_follow_failure_does_not_disguise_client_failure(tmp_path):
    """A failed FOLLOW transport can coexist with confirmed native success."""
    execution = _BackendExecution(RunState.FINISHED)
    execution.follow_error = subprocess.CalledProcessError(17, ("ssh", "follow"))
    spec = ExecutionSpec(argv=("true",), directory=str(tmp_path))
    job = Job(execution, spec)
    run = Run(job)
    run.start()
    with pytest.raises(subprocess.CalledProcessError):
        run.wait()
    assert run.result.return_code == 0
    assert job.state is RunState.FINISHED
    assert job.failure_phase is None
    assert isinstance(job.output_error, subprocess.CalledProcessError)
    assert run.state is RunState.FAILED
    assert run.failure_phase is FailurePhase.OUTPUT
