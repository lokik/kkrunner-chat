"""Framed FOLLOW transport keeps native output and SSH diagnostics separate."""

import asyncio
import sys

import pytest

from kkrunner import ExecutionSpec, Run
from kkrunner.backends import SupervisedBackend
from kkrunner.client import ExecutionClient, SshTarget, StagingSpec
from kkrunner.targets.implementations.ssh.follow import follow_framed_command
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


class _DiagnosticTransport(SshTarget):
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
    client = ExecutionClient(SupervisedBackend, target=_DiagnosticTransport())
    spec = ExecutionSpec(
        argv=(sys.executable, "-c", "import sys; print('stdout'); print('stderr', file=sys.stderr)"),
        directory=workdir,
        output_file=workdir / "output.txt",
        error_file=workdir / "output.err",
        staging=StagingSpec(local_root=local_root, remote_root=remote_root),
    )
    assert client.create_run(spec, tracked=False).run().return_code == 0
    assert (workdir / "output.txt").read_bytes() == b"stdout\n"
    assert (workdir / "output.err").read_bytes() == b"stderr\n"
    assert b"Permanently added" not in (workdir / "output.txt").read_bytes()
    assert b"Permanently added" not in (workdir / "output.err").read_bytes()


def test_ssh_supervised_observer_attaches_before_live_only_program_launch(tmp_path, monkeypatch):
    monkeypatch.setenv("KKRunner_REMOTE_STATE_DIR", str(tmp_path / "backend-state"))
    local_root, remote_root = tmp_path / "local", tmp_path / "remote"
    workdir = local_root / "calculation"
    workdir.mkdir(parents=True)
    remote_root.mkdir()
    marker = remote_root / "calculation" / "started"

    class NoSecondObserver(_DiagnosticTransport):
        async def follow(self, operation, identity, on_output, **options):
            pytest.fail("New supervised submission must not open another SSH FOLLOW")

    client = ExecutionClient(SupervisedBackend, target=NoSecondObserver())
    spec = ExecutionSpec(
        argv=(sys.executable, "-c",
              "from pathlib import Path; import sys; Path('started').touch(); "
              "print('first'); print('warning', file=sys.stderr)"),
        directory=workdir,
        staging=StagingSpec(local_root=local_root, remote_root=remote_root),
    )
    seen = {"out": [], "err": []}
    run = client.create_run(spec, tracked=False,
                            output_listener=lambda text, kind: seen[kind].append(text))
    assert run.run().return_code == 0
    assert {kind: "".join(chunks) for kind, chunks in seen.items()} == {
        "out": "first\n", "err": "warning\n",
    }
    assert marker.exists()


def test_ssh_supervised_preflight_failure_reports_no_submission(tmp_path, monkeypatch):
    from kkrunner.backends.contracts import SubmissionNotStarted

    monkeypatch.setenv("KKRunner_REMOTE_STATE_DIR", str(tmp_path / "backend-state"))
    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    client = ExecutionClient(SupervisedBackend, target=_DiagnosticTransport())
    run = client.create_run(ExecutionSpec(
        [str(remote / "missing-executable")],
        directory=local, staging=StagingSpec(local_root=local, remote_root=remote),
    ), tracked=False)
    with pytest.raises(SubmissionNotStarted, match="Executable not found"):
        run.submit()
    assert run.backend_id is None


def test_ssh_observer_eof_closes_helper_without_cancelling_detached_job(tmp_path, monkeypatch):
    """EOF alone, without terminating SSH, must release a silent observation."""
    from kkrunner.client.contracts import Operation
    from kkrunner.targets.implementations.ssh.rpc import operation_command
    from kkrunner.targets.rpc import operation_payload

    monkeypatch.setenv("KKRunner_REMOTE_STATE_DIR", str(tmp_path / "backend-state"))
    gate = tmp_path / "release"
    output = tmp_path / "stdout"
    error = tmp_path / "stderr"

    async def exercise():
        request = {
            "argv": [sys.executable, "-c",
                     "import time; from pathlib import Path\n"
                     "while not Path('release').exists(): time.sleep(.01)\n"
                     "print('finished')"],
            "directory": str(tmp_path), "detached": True,
            "output_file": str(output), "error_file": str(error),
        }
        command = operation_command(
            Operation.for_backend(SupervisedBackend, "submit_and_follow"),
            python=(sys.executable,), framed="submit",
        )
        process = await asyncio.create_subprocess_exec(
            *command, operation_payload((request,), {}),
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        remote_id = None
        frames = read_stream_frames(process.stdout)
        try:
            kind, facts = await asyncio.wait_for(anext(frames), 10)
            assert kind == "started"
            remote_id = facts["remote_id"]
            process.stdin.close()
            # No signal to the helper: closing stdin alone must be sufficient.
            assert await asyncio.wait_for(process.wait(), 5) == 0
            assert SupervisedBackend.status(remote_id)["state"] == "running"
            gate.touch()

            async def finished():
                while True:
                    facts = SupervisedBackend.status(remote_id)
                    if RunState(facts["state"]).is_terminal:
                        return facts
                    await asyncio.sleep(.01)

            facts = await asyncio.wait_for(finished(), 5)
            assert facts["return_code"] == 0
            assert output.read_bytes() == b"finished\n"
        finally:
            gate.touch()
            await frames.aclose()
            if process.returncode is None:
                process.kill()
                await process.wait()
            if remote_id is not None:
                SupervisedBackend.cancel(remote_id)

    asyncio.run(exercise())
