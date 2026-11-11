import asyncio
import sys

import pytest

from kkrunner.client.ssh import SshRunEndpoint
from kkrunner.remote.stream import STREAM_MAGIC, StreamProtocolError, encode_stream_frame, encode_terminal_frame
from kkrunner.client.observation import follow_framed_command
from kkrunner.client.transport import LocalTransport
from kkrunner.runs.base import ExecutionSpec, RunState

from .test_remote import _launch, _processed_run, _OperationLoopbackTransport
from kkrunner.backends import SupervisedBackend


def _reader_with(data):
    stream = asyncio.StreamReader()
    stream.feed_data(data)
    stream.feed_eof()
    return stream


class _FinishedCommand:
    def __init__(self, stdout, stderr=b"", returncode=0):
        self.stdout = _reader_with(stdout)
        self.stderr = _reader_with(stderr)
        self.returncode = None
        self._returncode = returncode
        self.terminated = False

    async def wait(self):
        self.returncode = self._returncode
        return self.returncode

    def terminate(self):
        self.terminated = True


def test_framed_command_separates_calculation_and_transport_stderr():

    async def check():
        process = _FinishedCommand(
            STREAM_MAGIC + encode_stream_frame("out", b"stdout\n") + encode_stream_frame("err", b"stderr\n")
            + encode_terminal_frame({"state": "finished", "return_code": 0}),
            b"Warning: transport diagnostic\n",
        )
        received = []
        terminal = await follow_framed_command(process, lambda data, kind: received.append((kind, data)))
        assert terminal == {"state": "finished", "return_code": 0}
        assert received == [("out", b"stdout\n"), ("err", b"stderr\n")]
        assert process.returncode == 0

    asyncio.run(check())


def test_framed_command_reports_helper_failure_with_diagnostic():

    async def check():
        process = _FinishedCommand(
            STREAM_MAGIC + encode_terminal_frame({"state": "unknown"}),
            b"remote helper traceback\n", returncode=2,
        )
        with pytest.raises(StreamProtocolError, match="status 2.*remote helper traceback"):
            await follow_framed_command(process, lambda *_: None)

    asyncio.run(check())


class _DiagnosticProcess:
    """Proxy a real subprocess while replacing its transport diagnostic channel."""

    def __init__(self, process, diagnostic):
        self._process = process
        self.stdout = process.stdout
        self.stderr = diagnostic

    @property
    def returncode(self):
        return self._process.returncode

    async def wait(self):
        return await self._process.wait()

    async def communicate(self, input=None):
        stdout, stderr = await self._process.communicate(input)
        diagnostic = await self.stderr.read()
        return (stdout, (stderr or b"") + diagnostic)

    def terminate(self):
        self._process.terminate()

    def kill(self):
        self._process.kill()


class _DiagnosticTransport(_OperationLoopbackTransport):
    """Inject an SSH-like diagnostic into the transport control channel."""

    async def start(self, argv, *, cwd=None, stdin=None):
        process = await super().start(argv, cwd=cwd, stdin=stdin)
        diagnostic = asyncio.StreamReader()
        diagnostic.feed_data(b"Warning: Permanently added test host\n")
        diagnostic.feed_eof()
        return _DiagnosticProcess(process, diagnostic)


def test_remote_transport_stderr_never_enters_calculation_files(tmp_path):
    local_root = tmp_path / "local"
    workdir = local_root / "calculation"
    workdir.mkdir(parents=True)
    input_file = (workdir / "input.txt").open("w+b")
    output_file = (workdir / "output.txt").open("wb")
    error_file = (workdir / "output.err").open("wb")
    remote_root = tmp_path / "remote"
    executor = SshRunEndpoint(_DiagnosticTransport(), backend=SupervisedBackend)
    spec = ExecutionSpec(
        argv=(sys.executable, "-c", "import sys; print('stdout'); print('stderr', file=sys.stderr)"),
        input_file=input_file,
        output_file=output_file,
        error_file=error_file,
        directory=str(workdir),
        launch=_launch(local_root, remote_root),
    )
    result = _processed_run(executor, spec).run()
    assert result.return_code == 0
    assert result.output == b"stdout\n"
    assert result.error == b"stderr\n"
    assert (workdir / "output.txt").read_bytes() == b"stdout\n"
    assert (workdir / "output.err").read_bytes() == b"stderr\n"
    assert b"Permanently added" not in (workdir / "output.txt").read_bytes()
    assert b"Permanently added" not in (workdir / "output.err").read_bytes()


def test_framed_command_delivers_terminal_result_in_band():
    from kkrunner.remote.stream import encode_terminal_frame

    async def check():
        received = []
        terminal = await follow_framed_command(
            _FinishedCommand(
                STREAM_MAGIC
                + encode_stream_frame("out", b"hello")
                + encode_terminal_frame(
                    {"state": "failed", "return_code": 4, "failure_phase": "execution", "error": "calculation failed"}
                )
            ), lambda data, kind: received.append((kind, data)),
        )
        assert received == [("out", b"hello")]
        assert terminal["return_code"] == 4
        assert terminal["failure_phase"] == "execution"

    asyncio.run(check())


@pytest.mark.parametrize(
    "payload", (b"null", b"[]", b"42", b'{"missing":"state"}', b'{"state":"invalid"}', b"{invalid")
)
def test_remote_terminal_frame_rejects_invalid_outcomes(payload):
    from kkrunner.remote.stream import _HEADER, read_stream_frames

    async def check():
        reader = _reader_with(STREAM_MAGIC + _HEADER.pack(b"T", len(payload)) + payload)
        with pytest.raises(StreamProtocolError, match="terminal frame"):
            await read_stream_frames(reader).__anext__()

    asyncio.run(check())


def test_malformed_terminal_frame_terminates_helper_and_preserves_protocol_error():
    from kkrunner.remote.stream import _HEADER

    async def check():
        payload = b"null"
        process = _FinishedCommand(STREAM_MAGIC + _HEADER.pack(b"T", len(payload)) + payload)
        with pytest.raises(StreamProtocolError, match="Invalid terminal frame outcome"):
            await follow_framed_command(process, lambda *_: None)
        assert process.returncode == 0

    asyncio.run(check())


@pytest.mark.parametrize("state", tuple(RunState))
def test_terminal_frame_accepts_every_run_state(state):
    from kkrunner.remote.stream import _HEADER, read_stream_frames

    async def check():
        payload = ('{"state":"' + state.value + '"}').encode()
        reader = _reader_with(STREAM_MAGIC + _HEADER.pack(b"T", len(payload)) + payload)
        assert await read_stream_frames(reader).__anext__() == ("terminal", {"state": state.value})

    asyncio.run(check())


def test_framed_follow_rejects_clean_exit_without_terminal_receipt():
    """Native success cannot establish that FOLLOW delivered the last bytes."""
    async def check():
        received = []
        with pytest.raises(StreamProtocolError, match="without terminal frame"):
            await follow_framed_command(
                _FinishedCommand(STREAM_MAGIC + encode_stream_frame("out", b"partial")),
                lambda data, kind: received.append((kind, data)),
            )
        assert received == [("out", b"partial")]

    asyncio.run(check())


def test_framed_follow_rejects_data_after_terminal_frame():
    async def check():
        process = _FinishedCommand(
            STREAM_MAGIC
            + encode_terminal_frame({"state": "finished", "return_code": 0})
            + encode_stream_frame("out", b"late bytes")
        )
        with pytest.raises(StreamProtocolError, match="data after terminal frame"):
            await follow_framed_command(process, lambda *_: None)
        assert process.returncode == 0

    asyncio.run(check())
