"""Backend outcome and client file delivery have independent lifecycles."""

import asyncio
from types import SimpleNamespace

import pytest

from kkrunner import ExecutionSpec, Run, RunState
from kkrunner.client.remote_job import RemoteJob
from kkrunner.client.remote_files import RemoteFileSession
from kkrunner.client.transport import LocalTransport
from kkrunner.files.manifest import FileEntry, FileManifest, FileSyncPolicy
from kkrunner.runs.base import FailurePhase


class _Follow:
    terminal = None

    def __init__(self, output=b"streamed\n"):
        self.stdout = asyncio.StreamReader()
        self.stderr = asyncio.StreamReader()
        self.stdout.feed_data(output)
        self.stdout.feed_eof()
        self.stderr.feed_eof()

    async def wait(self):
        return 0

    def terminate(self):
        pass


class _Context:
    backend = "process"
    output_files = FileSyncPolicy.NEVER

    def __init__(self, state=RunState.FINISHED):
        self.backend_state = RunState(state)
        self.transport = LocalTransport()
        self.status_error = None
        self.follow_error = None
        self.follow_calls = 0

    async def _follow(self, *args, **kwargs):
        self.follow_calls += 1
        if self.follow_error is not None:
            raise self.follow_error
        return _Follow()

    async def _status(self, *args, **kwargs):
        if self.status_error is not None:
            raise self.status_error
        return {
            "state": self.backend_state.value,
            "return_code": 17 if self.backend_state == RunState.FAILED else (
                0 if self.backend_state == RunState.FINISHED else None
            ),
            "failure_phase": "execution" if self.backend_state == RunState.FAILED else None,
            "error": "application failed" if self.backend_state == RunState.FAILED else None,
        }


def _run(tmp_path, state=RunState.FINISHED):
    destination = tmp_path / "calculation.out"
    remote = tmp_path / "authoritative.out"
    remote.write_bytes(b"streamed\n")
    output = destination.open("wb")
    spec = ExecutionSpec(argv=(), output_file=output, directory=tmp_path)
    files = FileManifest(
        stdout=FileEntry(str(destination), str(remote)),
    ).initialize_availability()
    context = _Context(state)
    session = RemoteFileSession(context, spec, files=files, remote_directory="/work/case")
    job = RemoteJob(
        context, spec, backend_id="opaque-handle", remote_directory="/work/case",
        status={"state": "running"},
    )
    return Run(job, file_session=session), context, destination, output


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
    run._file_session.synchronize = fail_sync
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
    run._file_session.synchronize = unavailable
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
    context = _Context(RunState.RUNNING)
    a_stream = destination.open("ab")
    b_stream = destination.open("ab")
    try:
        a = RemoteFileSession(context, ExecutionSpec(argv=(), output_file=a_stream, directory=tmp_path))
        b = RemoteFileSession(context, ExecutionSpec(argv=(), output_file=b_stream, directory=tmp_path))
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
