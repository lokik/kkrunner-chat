"""End-to-end supervisor lifecycle tests, independent of the SSH transport."""

import asyncio
import os
import socket
import sys

import pytest

from kkrunner.backends.implementations.supervised import worker as remote_process_worker


def _spec(tmp_path, socket_fd, *, detached, argv=None):
    return {
        "argv": argv or [sys.executable, "-c", "import time; time.sleep(20)"],
        "directory": str(tmp_path),
        "output_file": str(tmp_path / "stdout"),
        "error_file": str(tmp_path / "stderr"),
        "receipt_id": "a" * 32,
        "socket_fd": socket_fd,
        "detached": detached,
    }


def _initial_connection(spec):
    """Pass the child endpoint exactly as Popen does in production."""
    client, supervisor = socket.socketpair()
    spec["initial_follower_fd"] = supervisor.detach()
    return client


@pytest.mark.parametrize("output", [False, True])
def test_attached_run_stops_when_follow_connection_drops(tmp_path, monkeypatch, output):

    async def check():
        captured = []
        monkeypatch.setattr(
            remote_process_worker, "publish_outcome", lambda *args, **kwargs: captured.append((args, kwargs))
        )
        rd, wr = os.pipe()
        spec = _spec(tmp_path, wr, detached=False)
        spec["initial_output"] = output
        connection = _initial_connection(spec)
        task = asyncio.create_task(remote_process_worker._run(spec))
        with os.fdopen(rd, "rb") as pipe:
            assert await asyncio.to_thread(pipe.readline)
        reader, writer = await asyncio.open_unix_connection(sock=connection)
        assert await reader.readexactly(8) == b"A2SFOLL1"
        writer.close()
        await writer.wait_closed()
        assert await asyncio.wait_for(task, timeout=5) != 0
        assert captured[0][0][1].value == "cancelled"
        assert captured[0][1] == {"error": None}

    asyncio.run(check())


@pytest.mark.parametrize("live_only", [False, True])
def test_detached_run_survives_follow_disconnect(tmp_path, monkeypatch, live_only):

    async def check():
        captured = []
        monkeypatch.setattr(
            remote_process_worker, "publish_outcome", lambda *args, **kwargs: captured.append((args, kwargs))
        )
        rd, wr = os.pipe()
        spec = _spec(
            tmp_path,
            wr,
            detached=True,
            argv=[
                sys.executable,
                "-c",
                "import time; from pathlib import Path; print('first', flush=True); "
                "time.sleep(.2); print('last', flush=True); Path('completed').touch()",
            ],
        )
        if live_only:
            spec.update(output_file=None, error_file=None)
        connection = _initial_connection(spec)
        task = asyncio.create_task(remote_process_worker._run(spec))
        with os.fdopen(rd, "rb") as pipe:
            assert await asyncio.to_thread(pipe.readline)
        reader, writer = await asyncio.open_unix_connection(sock=connection)
        assert await reader.readexactly(8) == b"A2SFOLL1"
        writer.close()
        await writer.wait_closed()
        assert await asyncio.wait_for(task, timeout=5) == 0
        assert (tmp_path / "completed").exists()
        if live_only:
            assert not (tmp_path / "stdout").exists()
        else:
            assert (tmp_path / "stdout").read_bytes() == b"first\nlast\n"
        assert captured[0][0][1].value == "finished"
        assert captured[0][1] == {"error": None}

    asyncio.run(check())


def test_cancel_is_delivered_to_supervisor_not_just_supervisor_pid(tmp_path, monkeypatch):

    async def check():
        captured = []
        monkeypatch.setattr(remote_process_worker, "publish_outcome", lambda *args, **kwargs: captured.append(args))
        rd, wr = os.pipe()
        task = asyncio.create_task(remote_process_worker._run(_spec(tmp_path, wr, detached=True)))
        with os.fdopen(rd, "rb") as pipe:
            path = (await asyncio.to_thread(pipe.readline)).decode().strip()
        reader, writer = await asyncio.open_unix_connection(path)
        writer.write(b'{"command":"cancel"}\n')
        await writer.drain()
        assert b'"accepted":true' in await reader.readline()
        writer.close()
        await writer.wait_closed()
        await asyncio.wait_for(task, 5)
        assert captured[0][1].value == "cancelled"

    asyncio.run(check())


def test_failed_detached_launch_clears_previous_output(tmp_path, monkeypatch):

    async def check():
        captured = []
        monkeypatch.setattr(remote_process_worker, "publish_outcome", lambda *args, **kwargs: captured.append(args))
        (tmp_path / "stdout").write_bytes(b"previous")
        rd, wr = os.pipe()
        spec = _spec(tmp_path, wr, detached=True, argv=[str(tmp_path / "missing-command")])
        result = await remote_process_worker._run(spec)
        with os.fdopen(rd, "rb") as pipe:
            assert pipe.readline()
        assert result != 0
        assert (tmp_path / "stdout").read_bytes() == b""
        assert captured[0][1].value == "failed"
        assert not list(tmp_path.glob(".*kkrunner-*"))

    asyncio.run(check())


@pytest.mark.parametrize(
    "failure, expected_phase, expected_message",
    [
        pytest.param(
            "publish", "execution", "Failed to write process output: disk full during output", id="spool-write"
        ),
        pytest.param("sync", "artifacts", "Failed to sync process output: cannot fsync output", id="final-sync"),
    ],
)
def test_supervisor_output_failure_is_reported_without_losing_diagnosis(
    tmp_path, monkeypatch, failure, expected_phase, expected_message
):
    from kkrunner.backends.implementations.supervised.streams import SupervisorStreams
    from kkrunner.runs.base import FailurePhase, RunState

    reported = []
    monkeypatch.setattr(
        remote_process_worker, "publish_outcome", lambda *args, **kwargs: reported.append((args, kwargs))
    )

    def raise_failure(*args, **kwargs):
        if failure == "publish":
            raise OSError("disk full during output")
        raise OSError("cannot fsync output")

    monkeypatch.setattr(SupervisorStreams, failure, raise_failure)

    async def check():
        rd, wr = os.pipe()
        spec = _spec(tmp_path, wr, detached=True, argv=[sys.executable, "-c", "print('payload', flush=True)"])
        await asyncio.wait_for(remote_process_worker._run(spec), 5)
        with os.fdopen(rd, "rb") as read_pipe:
            assert read_pipe.readline()

    asyncio.run(check())
    assert len(reported) == 1
    args, kwargs = reported[0]
    assert args[1] == RunState.FAILED
    assert args[3] == FailurePhase(expected_phase)
    if failure == "sync":
        assert args[2] == 0
        assert kwargs["error"] == expected_message
    else:
        assert expected_message in kwargs["error"]


def test_receipt_write_failure_never_emits_durable_terminal_claim(tmp_path, monkeypatch):
    from kkrunner.backends.implementations.supervised.streams import SupervisorStreams

    observed = []
    original_terminal = SupervisorStreams.terminal

    def record_terminal(self, outcome):
        observed.append(outcome)
        original_terminal(self, outcome)

    monkeypatch.setattr(SupervisorStreams, "terminal", record_terminal)

    def fail_receipt(*args, **kwargs):
        raise OSError("mailbox fsync failed")

    monkeypatch.setattr(remote_process_worker, "publish_outcome", fail_receipt)

    async def check():
        rd, wr = os.pipe()
        spec = _spec(tmp_path, wr, detached=True, argv=[sys.executable, "-c", "print('payload', flush=True)"])
        assert await asyncio.wait_for(remote_process_worker._run(spec), 5) == 0
        with os.fdopen(rd, "rb") as read_pipe:
            assert read_pipe.readline()

    asyncio.run(check())
    assert observed == [
        {
            "state": "unknown",
            "return_code": None,
            "failure_phase": None,
            "error": "Unable to persist terminal receipt: mailbox fsync failed",
        }
    ]
    assert (tmp_path / "stdout").read_bytes() == b"payload\n"


def test_supervisor_pumps_both_binary_channels_to_separate_files(tmp_path, monkeypatch):
    reported = []
    monkeypatch.setattr(
        remote_process_worker, "publish_outcome", lambda *args, **kwargs: reported.append((args, kwargs))
    )

    async def run():
        rd, wr = os.pipe()
        spec = _spec(
            tmp_path,
            wr,
            detached=True,
            argv=[
                sys.executable,
                "-c",
                "import sys; sys.stdout.buffer.write(b'A' * 65536 + bytes([0, 255])); sys.stdout.buffer.flush(); sys.stderr.buffer.write(b'B' * 65536 + bytes([1, 254])); sys.stderr.buffer.flush()",
            ],
        )
        return_code = await asyncio.wait_for(remote_process_worker._run(spec), 5)
        with os.fdopen(rd, "rb") as pipe:
            assert pipe.readline()
        return return_code

    assert asyncio.run(run()) == 0
    assert (tmp_path / "stdout").read_bytes() == b"A" * 65536 + bytes([0, 255])
    assert (tmp_path / "stderr").read_bytes() == b"B" * 65536 + bytes([1, 254])
    assert reported[0][0][1].value == "finished"


def test_unexpected_child_spawn_failure_is_classified_as_launch(tmp_path, monkeypatch):
    """Non-OSError failures before child creation still belong to LAUNCH."""
    from kkrunner.runs.base import FailurePhase, RunState

    reported = []

    async def broken_spawn(*_args, **_kwargs):
        raise RuntimeError("failed to prepare child subprocess")

    monkeypatch.setattr(remote_process_worker, "_start_process", broken_spawn)
    monkeypatch.setattr(
        remote_process_worker, "publish_outcome", lambda *args, **kwargs: reported.append((args, kwargs))
    )

    async def check():
        rd, wr = os.pipe()
        result = await asyncio.wait_for(remote_process_worker._run(_spec(tmp_path, wr, detached=True)), 5)
        with os.fdopen(rd, "rb") as pipe:
            assert pipe.readline()
        return result

    assert asyncio.run(check()) == 127
    assert len(reported) == 1
    args, kwargs = reported[0]
    assert args[1] == RunState.FAILED
    assert args[2] == 127
    assert args[3] == FailurePhase.LAUNCH
    assert kwargs["error"] == "failed to prepare child subprocess"


@pytest.mark.parametrize("named_stderr", [False, True])
def test_preconnected_supervisor_delivers_initial_bytes_even_before_client_reads(tmp_path, monkeypatch, named_stderr):
    from kkrunner.backends.implementations.supervised.streams import receive

    async def check():
        monkeypatch.setattr(remote_process_worker, "publish_outcome", lambda *args, **kwargs: None)
        rd, wr = os.pipe()
        marker = tmp_path / "started"
        spec = _spec(tmp_path, wr, detached=True, argv=[
            sys.executable, "-c",
            "from pathlib import Path; import sys; print('first', flush=True); "
            "print('warning', file=sys.stderr, flush=True); Path('started').touch()",
        ])
        spec["output_file"] = None
        if not named_stderr:
            spec["error_file"] = None
        connection = _initial_connection(spec)
        task = asyncio.create_task(remote_process_worker._run(spec))
        try:
            with os.fdopen(rd, "rb") as pipe:
                assert await asyncio.to_thread(pipe.readline)

            async def launched():
                while not marker.exists():
                    await asyncio.sleep(.01)

            # No FOLLOW request, no reading from the socket, and no deadline
            # in the production launch path. The program can already execute.
            await asyncio.wait_for(launched(), 3)
            reader, writer = await asyncio.open_unix_connection(sock=connection)
            try:
                received = {"out": bytearray(), "err": bytearray()}

                async def collect():
                    async for kind, offset, data in receive(reader):
                        if kind == "terminal":
                            return data
                        assert offset == len(received[kind])
                        received[kind].extend(data)

                result = await asyncio.wait_for(collect(), 5)
                assert result["return_code"] == 0
                assert received == {"out": b"first\n", "err": b"warning\n"}
            finally:
                writer.close()
                await writer.wait_closed()
            assert await asyncio.wait_for(task, 5) == 0
        finally:
            connection.close()
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(check())


def test_cancel_attached_job_with_backpressured_live_output(tmp_path, monkeypatch):
    from kkrunner.backends.implementations.supervised import mailbox
    from kkrunner.backends.implementations.supervised.streams import receive

    monkeypatch.setenv("KKRunner_REMOTE_STATE_DIR", str(tmp_path / "state"))

    async def check():
        rd, wr = os.pipe()
        spec = _spec(tmp_path, wr, detached=False, argv=[
            sys.executable, "-c",
            "from pathlib import Path; import os, time; Path('started').touch(); "
            "os.write(1, b'x' * (8 * 1024 * 1024)); time.sleep(20)",
        ])
        spec.update(output_file=None, error_file=None)
        client, supervisor = socket.socketpair()
        supervisor.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        spec["initial_follower_fd"] = supervisor.detach()
        task = asyncio.create_task(remote_process_worker._run(spec))
        try:
            with os.fdopen(rd, "rb") as pipe:
                path = (await asyncio.to_thread(pipe.readline)).decode().strip()

            async def started():
                while not (tmp_path / "started").exists():
                    await asyncio.sleep(.01)

            await asyncio.wait_for(started(), 5)
            await asyncio.sleep(.1)
            assert not task.done()
            reader, writer = await asyncio.open_unix_connection(path)
            try:
                writer.write(b'{"command":"cancel"}\n')
                await writer.drain()
                assert b'"accepted":true' in await asyncio.wait_for(reader.readline(), 5)
            finally:
                writer.close()
                await writer.wait_closed()

            async def completed():
                while mailbox.read_outcome(spec["receipt_id"]) is None:
                    await asyncio.sleep(.01)
                return mailbox.read_outcome(spec["receipt_id"])

            # The durable outcome must exist even though no output was read.
            outcome = await asyncio.wait_for(completed(), 5)
            assert outcome["state"] == "cancelled"
            assert not task.done()

            reader, writer = await asyncio.open_unix_connection(sock=client)
            try:
                async def drain():
                    count = 0
                    async for kind, offset, data in receive(reader):
                        if kind == "terminal":
                            assert data == dict(outcome, output_drained=True)
                            assert count > 0
                            return
                        assert kind == "out" and offset == count
                        assert data == b"x" * len(data)
                        count += len(data)

                await asyncio.wait_for(drain(), 5)
                await asyncio.wait_for(task, 5)
            finally:
                writer.close()
                await writer.wait_closed()
        finally:
            client.close()
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(check())


def test_natural_exit_releases_attached_backpressure(tmp_path, monkeypatch):
    from kkrunner.backends import SupervisedBackend
    from kkrunner.backends.implementations.supervised import mailbox
    from kkrunner.backends.implementations.supervised.streams import receive
    from kkrunner.remote.handles import encode_handle
    from kkrunner.support.process_identity import ProcessIdentity

    monkeypatch.setenv("KKRunner_REMOTE_STATE_DIR", str(tmp_path / "state"))

    async def check():
        # Write enough to cross the follower watermark, but let the process
        # exit with the final chunk still blocked in the socket writer.
        rd, wr = os.pipe()
        spec = _spec(tmp_path, wr, detached=False, argv=[
            sys.executable, "-c",
            "import os; os.write(1, b'x' * (540 * 1024))",
        ])
        spec.update(output_file=None, error_file=None)
        client, supervisor = socket.socketpair()
        supervisor.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        spec["initial_follower_fd"] = supervisor.detach()
        task = asyncio.create_task(remote_process_worker._run(spec))
        try:
            with os.fdopen(rd, "rb") as pipe:
                path = (await asyncio.to_thread(pipe.readline)).decode().strip()
            remote_id = encode_handle({
                "kind": "kkrunner", "socket_path": path,
                "receipt_id": spec["receipt_id"],
                "identity": ProcessIdentity.read(os.getpid()).to_dict(),
            })

            async def completed():
                while mailbox.read_outcome(spec["receipt_id"]) is None:
                    await asyncio.sleep(.01)
                return mailbox.read_outcome(spec["receipt_id"])

            assert (await asyncio.wait_for(completed(), 5))["state"] == "finished"
            assert SupervisedBackend.status([remote_id])[0]["state"] == "finished"
            assert not task.done()
            reader, writer = await asyncio.open_unix_connection(sock=client)
            try:
                async def drain():
                    count = 0
                    async for kind, offset, data in receive(reader):
                        if kind == "terminal":
                            assert data["return_code"] == 0
                            return count
                        assert kind == "out" and offset == count
                        assert data == b"x" * len(data)
                        count += len(data)

                assert await asyncio.wait_for(drain(), 5) == 540 * 1024
                assert await asyncio.wait_for(task, 5) == 0
            finally:
                writer.close()
                await writer.wait_closed()
        finally:
            client.close()
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(check())


def test_spool_failure_releases_other_backpressured_channel(tmp_path, monkeypatch):
    from kkrunner.backends.implementations.supervised import mailbox
    from kkrunner.backends.implementations.supervised.streams import SupervisorStreams

    monkeypatch.setenv("KKRunner_REMOTE_STATE_DIR", str(tmp_path / "state"))
    publish = SupervisorStreams.publish

    async def fail_stdout(self, kind, data):
        if kind == "out":
            raise OSError("disk full")
        await publish(self, kind, data)

    monkeypatch.setattr(SupervisorStreams, "publish", fail_stdout)

    async def check():
        rd, wr = os.pipe()
        spec = _spec(tmp_path, wr, detached=False, argv=[
            sys.executable, "-c",
            "import os, threading, time; "
            "threading.Thread(target=lambda: os.write(2, b'x' * (8 * 1024 * 1024)), daemon=True).start(); "
            "time.sleep(.2); os.write(1, b'fail'); time.sleep(20)",
        ])
        spec["error_file"] = None
        client, supervisor = socket.socketpair()
        supervisor.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        spec["initial_follower_fd"] = supervisor.detach()
        task = asyncio.create_task(remote_process_worker._run(spec))
        try:
            with os.fdopen(rd, "rb") as pipe:
                assert await asyncio.to_thread(pipe.readline)

            async def completed():
                while mailbox.read_outcome(spec["receipt_id"]) is None:
                    await asyncio.sleep(.01)
                return mailbox.read_outcome(spec["receipt_id"])

            outcome = await asyncio.wait_for(completed(), 5)
            assert outcome["state"] == "failed"
            assert "disk full" in outcome["error"]
        finally:
            client.close()
            await asyncio.wait_for(task, 5)

    asyncio.run(check())


def test_exited_process_with_open_output_pipe_reports_finishing(tmp_path, monkeypatch):
    from kkrunner.backends import SupervisedBackend
    from kkrunner.backends.implementations.supervised import mailbox
    from kkrunner.remote.handles import encode_handle
    from kkrunner.support.process_identity import ProcessIdentity

    monkeypatch.setenv("KKRunner_REMOTE_STATE_DIR", str(tmp_path / "state"))

    async def check():
        # A descendant temporarily keeps the stdout pipe open after its
        # parent exits. No slow reader or mocked lifecycle is necessary.
        release = tmp_path / "release"
        child = (
            "from pathlib import Path; import time; "
            "deadline = time.monotonic() + 8\n"
            "while not Path('release').exists() and time.monotonic() < deadline: "
            "time.sleep(.01)"
        )
        rd, wr = os.pipe()
        spec = _spec(tmp_path, wr, detached=True, argv=[
            sys.executable, "-c",
            "import subprocess, sys; subprocess.Popen([sys.executable, '-c', " + repr(child) + "])",
        ])
        task = asyncio.create_task(remote_process_worker._run(spec))
        try:
            with os.fdopen(rd, "rb") as pipe:
                path = (await asyncio.to_thread(pipe.readline)).decode().strip()
            remote_id = encode_handle({
                "kind": "kkrunner", "socket_path": path,
                "receipt_id": spec["receipt_id"],
                "identity": ProcessIdentity.read(os.getpid()).to_dict(),
            })

            async def finishing():
                while SupervisedBackend.status([remote_id])[0]["state"] != "finishing":
                    await asyncio.sleep(.01)

            await asyncio.wait_for(finishing(), 5)
            assert not task.done()
            assert mailbox.read_outcome(spec["receipt_id"]) is None
            release.touch()
            assert await asyncio.wait_for(task, 5) == 0
            assert SupervisedBackend.status([remote_id])[0]["state"] == "finished"
        finally:
            release.touch()
            await asyncio.wait_for(task, 5)

    asyncio.run(check())


def test_draining_supervisor_serves_output_after_acknowledgement(tmp_path, monkeypatch):
    from kkrunner.backends import SupervisedBackend
    from kkrunner.backends.implementations.supervised import mailbox
    from kkrunner.backends.implementations.supervised.streams import receive
    from kkrunner.remote.handles import encode_handle
    from kkrunner.support.process_identity import ProcessIdentity

    monkeypatch.setenv("KKRunner_REMOTE_STATE_DIR", str(tmp_path / "state"))

    async def check():
        rd, wr = os.pipe()
        spec = _spec(tmp_path, wr, detached=True, argv=[
            sys.executable, "-c", "import sys; sys.stdout.buffer.write(b'x' * (4 * 1024 * 1024))",
        ])
        client, supervisor = socket.socketpair()
        supervisor.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        spec["initial_follower_fd"] = supervisor.detach()
        task = asyncio.create_task(remote_process_worker._run(spec))
        try:
            with os.fdopen(rd, "rb") as pipe:
                path = (await asyncio.to_thread(pipe.readline)).decode().strip()
            remote_id = encode_handle({
                "kind": "kkrunner", "socket_path": path,
                "receipt_id": spec["receipt_id"],
                "identity": ProcessIdentity.read(os.getpid()).to_dict(),
            })

            async def completed():
                while mailbox.read_outcome(spec["receipt_id"]) is None:
                    await asyncio.sleep(.01)

            await asyncio.wait_for(completed(), 5)
            assert not task.done(), "The supervisor must still be serving the slow observer"
            assert SupervisedBackend.status([remote_id])[0]["state"] == "finished"
            assert SupervisedBackend.acknowledge(remote_id)["acknowledged"]
            assert SupervisedBackend.status([remote_id])[0]["state"] == "unknown"
            reader, writer = await asyncio.open_unix_connection(sock=client)
            try:
                async def drain():
                    count = 0
                    async for kind, offset, data in receive(reader):
                        if kind == "terminal":
                            assert data["return_code"] == 0
                            return count
                        assert kind == "out" and offset == count
                        assert data == b"x" * len(data)
                        count += len(data)

                assert await asyncio.wait_for(drain(), 5) == 4 * 1024 * 1024
                assert await asyncio.wait_for(task, 5) == 0
            finally:
                writer.close()
                await writer.wait_closed()
        finally:
            client.close()
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(check())


@pytest.mark.parametrize("detached", [False, True])
def test_initial_observer_lost_before_supervisor_starts(tmp_path, monkeypatch, detached):
    async def check():
        captured = []
        monkeypatch.setattr(remote_process_worker, "publish_outcome", lambda *args, **kwargs: captured.append(args))
        rd, wr = os.pipe()
        spec = _spec(tmp_path, wr, detached=detached, argv=[
            sys.executable, "-c",
            "import time; from pathlib import Path; time.sleep(.2); Path('completed').touch()",
        ])
        spec.update(output_file=None, error_file=None)
        connection = _initial_connection(spec)
        connection.close()
        task = asyncio.create_task(remote_process_worker._run(spec))
        try:
            with os.fdopen(rd, "rb") as pipe:
                assert await asyncio.to_thread(pipe.readline)
            await asyncio.wait_for(task, 5)
            assert (tmp_path / "completed").exists() is detached
            assert captured[0][1].value == ("finished" if detached else "cancelled")
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(check())


@pytest.mark.parametrize("detached", [False, True])
def test_supervisor_wait_only_connection_receives_no_output(tmp_path, monkeypatch, detached):
    from kkrunner.backends.implementations.supervised.streams import receive

    async def check():
        monkeypatch.setattr(remote_process_worker, "publish_outcome", lambda *args, **kwargs: None)
        rd, wr = os.pipe()
        spec = _spec(tmp_path, wr, detached=detached, argv=[
            sys.executable, "-c",
            "import sys; sys.stdout.buffer.write(b'x' * 1048576); print('err', file=sys.stderr)",
        ])
        spec["initial_output"] = False
        connection = _initial_connection(spec)
        task = asyncio.create_task(remote_process_worker._run(spec))
        try:
            with os.fdopen(rd, "rb") as pipe:
                assert await asyncio.to_thread(pipe.readline)
            reader, writer = await asyncio.open_unix_connection(sock=connection)
            try:
                async def collect():
                    return [kind async for kind, _, _ in receive(reader)]
                assert await asyncio.wait_for(collect(), 5) == ["terminal"]
                assert await asyncio.wait_for(task, 5) == 0
            finally:
                writer.close()
                await writer.wait_closed()
        finally:
            connection.close()
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        assert (tmp_path / "stdout").read_bytes() == b"x" * 1048576
        assert (tmp_path / "stderr").read_bytes() == b"err\n"

    asyncio.run(check())
