"""Framed FOLLOW transport keeps native output and SSH diagnostics separate."""

import asyncio
import sys

import pytest

from kkrunner import ExecutionSpec, Run
from kkrunner.backends import SupervisedBackend
from kkrunner.client import EndpointClient, SshTransport, StagingSpec
from kkrunner.client.transports.follow import follow_framed_command
from kkrunner.execution import RunState
from kkrunner.remote.stream import (
    STREAM_MAGIC, StreamProtocolError, encode_stream_frame, encode_terminal_frame,
    read_stream_frames,
)


def _reader_with(data):
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()
    return reader


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

    def kill(self):
        self.terminated = True


def test_framed_transport_delivers_distinct_channels_without_diagnostic_leak():
    async def check():
        process = _FinishedCommand(
            STREAM_MAGIC + encode_stream_frame("out", b"stdout\n")
            + encode_stream_frame("err", b"stderr\n")
            + encode_terminal_frame({"state": "finished", "return_code": 0}),
            b"Warning: transport diagnostic\n",
        )
        chunks = []
        terminal = await follow_framed_command(process, lambda chunk, kind: chunks.append((kind, chunk)))
        assert chunks == [("out", b"stdout\n"), ("err", b"stderr\n")]
        assert terminal == {"state": "finished", "return_code": 0}
    asyncio.run(check())


def test_framed_transport_preserves_terminal_receipt_when_helper_exits_nonzero():
    async def check():
        process = _FinishedCommand(
            STREAM_MAGIC + encode_terminal_frame({"state": "finished", "return_code": 0}),
            b"remote helper traceback\n", returncode=2,
        )
        terminal = await follow_framed_command(process, lambda *_: None)
        assert terminal == {"state": "finished", "return_code": 0}
    asyncio.run(check())


@pytest.mark.parametrize("payload", [
    b"null", b"[]", b"42", b'{"missing":"state"}', b'{"state":"invalid"}', b"{invalid",
])
def test_terminal_frame_rejects_invalid_outcomes(payload):
    from kkrunner.remote.stream import _HEADER

    async def check():
        source = _reader_with(STREAM_MAGIC + _HEADER.pack(b"T", len(payload)) + payload)
        with pytest.raises(StreamProtocolError, match="terminal frame|status frame JSON"):
            await read_stream_frames(source).__anext__()
    asyncio.run(check())


def test_invalid_terminal_frame_terminates_observer_and_preserves_error():
    from kkrunner.remote.stream import _HEADER

    async def check():
        payload = b"null"
        process = _FinishedCommand(STREAM_MAGIC + _HEADER.pack(b"T", len(payload)) + payload)
        with pytest.raises(StreamProtocolError, match="Invalid terminal frame outcome"):
            await follow_framed_command(process, lambda *_: None)
        assert process.terminated is False or process.returncode == 0
    asyncio.run(check())


@pytest.mark.parametrize("state", tuple(RunState))
def test_terminal_frame_accepts_every_run_state(state):
    from kkrunner.remote.stream import _HEADER

    async def check():
        payload = ('{"state":"' + state.value + '"}').encode()
        source = _reader_with(STREAM_MAGIC + _HEADER.pack(b"T", len(payload)) + payload)
        assert await read_stream_frames(source).__anext__() == ("terminal", {"state": state.value})
    asyncio.run(check())


def test_follow_rejects_clean_exit_without_terminal_receipt():
    async def check():
        process = _FinishedCommand(STREAM_MAGIC + encode_stream_frame("out", b"partial"))
        chunks = []
        with pytest.raises(StreamProtocolError, match="without terminal frame"):
            await follow_framed_command(process, lambda data, kind: chunks.append((kind, data)))
        assert chunks == [("out", b"partial")]
    asyncio.run(check())


def test_follow_rejects_data_after_terminal_receipt():
    async def check():
        process = _FinishedCommand(
            STREAM_MAGIC + encode_terminal_frame({"state": "finished", "return_code": 0})
            + encode_stream_frame("out", b"late"),
        )
        with pytest.raises(StreamProtocolError, match="data after terminal frame"):
            await follow_framed_command(process, lambda *_: None)
    asyncio.run(check())


class _DiagnosticProcess:
    """Subprocess with an independent synthetic SSH diagnostic channel."""

    def __init__(self, process):
        self._process = process
        self.stdin = process.stdin
        self.pid = process.pid
        self.stdout = process.stdout
        self.stderr = _reader_with(b"Warning: Permanently added test host\n")

    @property
    def returncode(self):
        return self._process.returncode

    async def wait(self):
        return await self._process.wait()

    async def communicate(self, input=None):
        stdout, stderr = await self._process.communicate(input)
        return stdout, stderr + await self.stderr.read()

    def terminate(self):
        self._process.terminate()

    def kill(self):
        self._process.kill()


class _DiagnosticTransport(SshTransport):
    def __init__(self):
        super().__init__("loopback", python_command=(sys.executable,))

    def _command(self, argv, cwd=None):
        assert cwd is None
        return tuple(argv)

    async def start(self, argv, *, cwd=None, stdin=None):
        return _DiagnosticProcess(await super().start(argv, cwd=cwd, stdin=stdin))


def test_ssh_diagnostic_is_not_written_into_calculation_output(tmp_path):
    local_root, remote_root = tmp_path / "local", tmp_path / "remote"
    workdir = local_root / "calculation"
    workdir.mkdir(parents=True)
    remote_root.mkdir()
    client = EndpointClient(SupervisedBackend, transport=_DiagnosticTransport())
    spec = ExecutionSpec(
        argv=(sys.executable, "-c", "import sys; print('stdout'); print('stderr', file=sys.stderr)"),
        directory=workdir,
        output_file=workdir / "output.txt",
        error_file=workdir / "output.err",
        staging=StagingSpec(local_root=local_root, remote_root=remote_root),
    )
    assert Run.untracked(client, spec).run().return_code == 0
    assert (workdir / "output.txt").read_bytes() == b"stdout\n"
    assert (workdir / "output.err").read_bytes() == b"stderr\n"
    assert b"Permanently added" not in (workdir / "output.txt").read_bytes()
    assert b"Permanently added" not in (workdir / "output.err").read_bytes()
