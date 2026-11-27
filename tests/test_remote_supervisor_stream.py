"""Regression tests for the transient supervisor FOLLOW service."""

import asyncio

import pytest

from kkrunner.backends.implementations.supervised.streams import _HIGH, SupervisorStreams, _Follower, follow_socket, frame


def test_supervisor_live_replay_and_terminal(tmp_path):

    async def run():
        output = tmp_path / "out"
        error = tmp_path / "err"
        streams = SupervisorStreams(output, error)
        await streams.start()
        try:
            await streams.publish("out", b"before\n")
            await streams.publish("err", b"warning\n")
            received = []

            async def follow():
                return await follow_socket(
                    streams.socket_path,
                    {"out": 0, "err": 0},
                    lambda kind, offset, data: received.append((kind, offset, data)),
                )

            follower = asyncio.create_task(follow())
            await asyncio.sleep(0.05)
            await streams.publish("out", b"after\n")
            streams.terminal({"state": "finished", "return_code": 0})
            outcome = await asyncio.wait_for(follower, 2)
            assert outcome == {"state": "finished", "return_code": 0}
            assert received == [("out", 0, b"before\n"), ("err", 0, b"warning\n"), ("out", 7, b"after\n")]
            assert output.read_bytes() == b"before\nafter\n"
            assert error.read_bytes() == b"warning\n"
        finally:
            await streams.finish()

    asyncio.run(run())


def test_supervisor_follow_resumes_from_offset(tmp_path):

    async def run():
        streams = SupervisorStreams(tmp_path / "out", tmp_path / "err")
        await streams.start()
        try:
            await streams.publish("out", b"abcdef")
            await streams.publish("err", b"12345")
            got = []
            follower = asyncio.create_task(
                follow_socket(streams.socket_path, {"out": 3, "err": 2}, lambda *args: got.append(args))
            )
            await asyncio.sleep(0.05)
            streams.terminal({"state": "failed", "return_code": 7})
            result = await asyncio.wait_for(follower, 2)
            assert got == [("out", 3, b"def"), ("err", 2, b"345")]
            assert result["return_code"] == 7
        finally:
            await streams.finish()

    asyncio.run(run())


def test_supervisor_merged_output_uses_one_cursor(tmp_path):

    async def run():
        target = tmp_path / "merged"
        streams = SupervisorStreams(target, target)
        await streams.start()
        try:
            await streams.publish("out", b"stdout")
            await streams.publish("err", b"stderr")
            got = []
            follower = asyncio.create_task(
                follow_socket(streams.socket_path, {"out": 0}, lambda *args: got.append(args))
            )
            await asyncio.sleep(0.05)
            streams.terminal({"state": "finished", "return_code": 0})
            await asyncio.wait_for(follower, 2)
            assert got == [("out", 0, b"stdoutstderr")]
            assert target.read_bytes() == b"stdoutstderr"
        finally:
            await streams.finish()

    asyncio.run(run())


def test_supervisor_rejects_offsets_outside_spool(tmp_path):

    async def run():
        streams = SupervisorStreams(tmp_path / "out", tmp_path / "err")
        await streams.start()
        try:
            await streams.publish("out", b"a")
            with pytest.raises((asyncio.IncompleteReadError, ConnectionError)):
                await follow_socket(streams.socket_path, {"out": 2, "err": 0}, lambda *_: None)
        finally:
            await streams.finish()

    asyncio.run(run())


def test_attached_supervisor_detects_session_disconnect(tmp_path):

    async def run():
        streams = SupervisorStreams(tmp_path / "out", tmp_path / "err", detached=False)
        await streams.start()
        reader = writer = None
        try:
            reader, writer = await asyncio.open_unix_connection(streams.socket_path)
            writer.write(b'{"command":"follow","offsets":{"out":0,"err":0}}\n')
            await writer.drain()
            assert await asyncio.wait_for(reader.readexactly(8), 2) == b"A2SFOLL1"
            writer.close()
            await writer.wait_closed()
            await asyncio.wait_for(streams.disconnected.wait(), 2)
        finally:
            if writer is not None:
                writer.close()
            await streams.finish()

    asyncio.run(run())


@pytest.mark.parametrize("persist_streams", (False, True))
def test_detached_supervisor_releases_silent_disconnected_followers(tmp_path, persist_streams):

    async def run():
        out = tmp_path / "out" if persist_streams else None
        err = tmp_path / "err" if persist_streams else None
        streams = SupervisorStreams(out, err, detached=True)
        await streams.start()
        try:
            for _ in range(3):
                reader, writer = await asyncio.open_unix_connection(streams.socket_path)
                writer.write(b'{"command":"follow","offsets":{"out":0,"err":0}}\n')
                await writer.drain()
                assert await reader.readexactly(8) == b"A2SFOLL1"
                assert len(streams._followers) == 1
                follower = next(iter(streams._followers))
                writer.close()
                await writer.wait_closed()
                await asyncio.wait_for(follower.done.wait(), 2)
                assert not streams._followers
                assert not streams.disconnected.is_set()
            streams.terminal({"state": "finished", "return_code": 0})
        finally:
            await streams.finish()

    asyncio.run(run())


def test_none_streams_live_only_without_spool_files(tmp_path):

    async def run():
        streams = SupervisorStreams(None, None)
        await streams.start()
        try:
            received = []
            task = asyncio.create_task(
                follow_socket(streams.socket_path, {"out": 0, "err": 0}, lambda *args: received.append(args))
            )
            await asyncio.sleep(0.05)
            await streams.publish("out", b"observed")
            await streams.publish("err", b"warning")
            streams.sync()
            streams.terminal({"state": "finished", "return_code": 0})
            assert (await asyncio.wait_for(task, 2))["return_code"] == 0
            assert [item[2] for item in received] == [b"observed", b"warning"]
            assert list(tmp_path.iterdir()) == []
            assert streams._spools == {}
        finally:
            await streams.finish()

    asyncio.run(run())


def test_live_only_stream_does_not_replay_earlier_bytes(tmp_path):

    async def run():
        streams = SupervisorStreams(None, None)
        await streams.start()
        try:
            await streams.publish("out", b"old")
            seen = []
            follower = asyncio.create_task(
                follow_socket(streams.socket_path, {"out": None, "err": None}, lambda *args: seen.append(args))
            )
            await asyncio.sleep(0.05)
            await streams.publish("out", b"new")
            streams.terminal({"state": "finished", "return_code": 0})
            await asyncio.wait_for(follower, 2)
            assert seen == [("out", 3, b"new")]
        finally:
            await streams.finish()

    asyncio.run(run())


def test_supervisor_writes_declared_output_during_execution(tmp_path):

    async def run():
        output, error = (tmp_path / "out", tmp_path / "err")
        output.write_bytes(b"old")
        streams = SupervisorStreams(output, error)
        await streams.start()
        try:
            assert output.read_bytes() == b""
            await streams.publish("out", b"partial")
            await streams.publish("err", b"diagnostic")
            assert output.read_bytes() == b"partial"
            assert error.read_bytes() == b"diagnostic"
            streams.terminal({"state": "cancelled", "return_code": -15})
        finally:
            await streams.finish()
        assert output.read_bytes() == b"partial"
        assert error.read_bytes() == b"diagnostic"

    asyncio.run(run())


def test_terminal_drains_slow_client_and_closes_connection(tmp_path):
    from kkrunner.backends.implementations.supervised.streams import receive

    async def run():
        output = tmp_path / "out"
        streams = SupervisorStreams(output, None)
        await streams.start()
        reader, writer = await asyncio.open_unix_connection(streams.socket_path)
        writer.write(b'{"command":"follow","offsets":{"out":0,"err":0}}\n')
        await writer.drain()
        # Wait until the observer is registered, without consuming its greeting.
        while not streams._followers:
            await asyncio.sleep(.001)
        try:
            for _ in range(1024):
                await streams.publish("out", b"x" * 8192)
            streams.terminal({"state": "finished", "return_code": 0})
            finishing = asyncio.create_task(streams.finish())
            await asyncio.sleep(.1)
            assert not finishing.done()
            count = 0
            async for kind, offset, data in receive(reader):
                if kind == "terminal":
                    assert data["return_code"] == 0
                else:
                    assert offset == count
                    count += len(data)
            assert count == output.stat().st_size == 1024 * 8192
            # The server closes its end without waiting for client close.
            assert await reader.read() == b""
            await asyncio.wait_for(finishing, 5)
        finally:
            writer.close()
            await writer.wait_closed()

    asyncio.run(run())


class _FakeFollowerTransport:
    def __init__(self, buffered=0):
        self.buffered = buffered
        self.aborted = False

    def get_write_buffer_size(self):
        return self.buffered

    def abort(self):
        self.aborted = True


class _FakeFollowerWriter:
    def __init__(self, buffered=0):
        self.transport = _FakeFollowerTransport(buffered)
        self.frames = []

    def write(self, data):
        self.frames.append(data)
        self.transport.buffered += len(data)


def test_live_only_channel_is_delivered_during_other_channel_replay():

    async def run():
        streams = SupervisorStreams("saved-stdout", None)
        await streams.publish("out", b"previous stdout")
        writer = _FakeFollowerWriter()
        follower = _Follower(writer, {"out": 0, "err": 0}, replay=True)
        streams._followers.add(follower)
        await streams.publish("err", b"live stderr")
        assert follower.replay
        assert writer.frames == [frame("err", 0, b"live stderr")]
        assert follower.offsets == {"out": 0, "err": len(b"live stderr")}
        assert not writer.transport.aborted

    asyncio.run(run())


@pytest.mark.parametrize("spooled, replaying", [(False, False), (True, False), (False, True)])
@pytest.mark.parametrize("already_full", (False, True))
def test_publish_backpressure_before_and_after_write(spooled, replaying, already_full):

    async def check():
        streams = SupervisorStreams("saved-stdout" if spooled or replaying else None, None)
        kind = "err" if replaying else "out"
        data = b"payload"
        wire = frame(kind, 0, data)
        buffered = _HIGH if already_full else _HIGH - len(wire) + 1
        writer = _FakeFollowerWriter(buffered=buffered)
        follower = _Follower(writer, {"out": 0, "err": 0}, replay=replaying)
        streams._followers.add(follower)
        await streams.publish(kind, data)
        assert writer.frames == ([] if already_full else [wire])
        assert follower.offsets[kind] == (0 if already_full else len(data))
        if spooled:
            assert follower.replay
            assert follower.wake.is_set()
            assert not writer.transport.aborted
        else:
            assert writer.transport.aborted

    asyncio.run(check())


def test_replaying_saved_channel_does_not_send_live_duplicates():

    async def check():
        streams = SupervisorStreams("saved-stdout", None)
        writer = _FakeFollowerWriter(buffered=_HIGH)
        follower = _Follower(writer, {"out": 0, "err": 0}, replay=True)
        streams._followers.add(follower)
        await streams.publish("out", b"to be replayed")
        assert writer.frames == []
        assert follower.offsets["out"] == 0
        assert follower.wake.is_set()
        assert not writer.transport.aborted

    asyncio.run(check())


@pytest.mark.parametrize("disconnect", [False, True])
def test_attached_live_only_backpressure(tmp_path, disconnect):
    import socket
    from kkrunner.backends.implementations.supervised.streams import receive

    async def run():
        streams = SupervisorStreams(detached=False)
        await streams.start()
        client, supervisor = socket.socketpair()
        supervisor.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        await streams.attach_initial(supervisor)
        reader, writer = await asyncio.open_unix_connection(sock=client)
        payload = b"x" * 65536

        async def produce():
            for _ in range(64):
                await streams.publish("out", payload)
            streams.terminal({"state": "finished", "return_code": 0})
            await streams.finish()

        producer = asyncio.create_task(produce())
        try:
            await asyncio.sleep(.1)
            assert not producer.done()
            if disconnect:
                writer.close()
                await writer.wait_closed()
                await asyncio.wait_for(producer, 5)
                assert streams.disconnected.is_set()
            else:
                size = 0
                async for kind, offset, data in receive(reader):
                    if kind == "terminal":
                        assert data["return_code"] == 0
                    else:
                        assert offset == size
                        size += len(data)
                assert size == 64 * len(payload)
                await asyncio.wait_for(producer, 5)
        finally:
            writer.close()
            await writer.wait_closed()

    asyncio.run(run())


def test_supervisor_start_closes_first_stream_if_second_cannot_open(tmp_path):

    async def check():
        out = tmp_path / "out"
        err = tmp_path / "stderr-is-a-directory"
        err.mkdir()
        streams = SupervisorStreams(out, err)
        with pytest.raises(IsADirectoryError):
            await streams.start()
        assert streams._spools["out"].closed
        assert streams._server is None
        assert streams.socket_path is None

    asyncio.run(check())


def test_supervisor_start_closes_streams_and_directory_on_socket_bind_error(tmp_path, monkeypatch):

    async def fail_bind(*args, **kwargs):
        raise OSError("cannot bind supervisor socket")

    monkeypatch.setattr(asyncio, "start_unix_server", fail_bind)

    async def check():
        streams = SupervisorStreams(tmp_path / "out", tmp_path / "err")
        with pytest.raises(OSError, match="cannot bind supervisor socket"):
            await streams.start()
        assert all((handle.closed for handle in streams._spools.values()))
        assert streams._server is None
        assert not (tmp_path / "out").is_dir()
        assert not __import__("os").path.exists(streams._socket_dir)

    asyncio.run(check())


def test_supervisor_start_closes_bound_server_if_chmod_fails(tmp_path, monkeypatch):
    from kkrunner.backends.implementations.supervised import streams as module

    def fail_chmod(path, mode):
        raise OSError("socket permissions unavailable")

    monkeypatch.setattr(module.os, "chmod", fail_chmod)

    async def check():
        streams = SupervisorStreams(tmp_path / "out", None)
        with pytest.raises(OSError, match="socket permissions unavailable"):
            await streams.start()
        assert streams._spools["out"].closed
        assert streams._server is not None
        assert not streams._server.is_serving()
        assert not __import__("os").path.exists(streams._socket_dir)

    asyncio.run(check())


def test_supervisor_start_preserves_failure_when_cleanup_also_fails(tmp_path, monkeypatch):
    """A second failure from finish() must not mask the original error."""
    import os

    startup_error = OSError("socket bind failed")
    cleanup_error = OSError("cleanup failed")
    original_finish = SupervisorStreams.finish

    async def fail_bind(*args, **kwargs):
        raise startup_error

    async def cleanup_then_fail(self):
        await original_finish(self)
        raise cleanup_error

    monkeypatch.setattr(asyncio, "start_unix_server", fail_bind)
    monkeypatch.setattr(SupervisorStreams, "finish", cleanup_then_fail)

    async def check():
        streams = SupervisorStreams(tmp_path / "out", None)
        with pytest.raises(OSError) as raised:
            await streams.start()
        assert raised.value is startup_error
        assert raised.value.__cause__ is cleanup_error
        assert streams._spools["out"].closed
        assert not os.path.exists(streams._socket_dir)

    asyncio.run(check())


def test_live_only_numeric_offset_cannot_silently_skip_previous_bytes():
    async def run():
        streams = SupervisorStreams()
        await streams.start()
        try:
            await streams.publish("out", b"already gone")
            with pytest.raises((asyncio.IncompleteReadError, ConnectionError)):
                await follow_socket(streams.socket_path, {"out": 0, "err": 0}, lambda *_: None)
        finally:
            await streams.finish()

    asyncio.run(run())


@pytest.mark.parametrize("initial", [False, True])
def test_follow_supports_async_output_sinks_on_both_connections(tmp_path, initial):
    import io
    import socket
    from kkrunner.backends.implementations.supervised.streams import output_callback

    class AsyncSink(io.BytesIO):
        async def write(self, data):
            await asyncio.sleep(0)
            return super().write(data)

    async def exercise():
        streams = SupervisorStreams(tmp_path / "stdout", tmp_path / "stderr")
        await streams.start()
        connection = None
        try:
            if initial:
                connection, supervisor = socket.socketpair()
                await streams.attach_initial(supervisor)
            await streams.publish("out", b"first")
            await streams.publish("err", b"warning")
            streams.terminal({"state": "finished", "return_code": 0})
            offsets = {"out": 0, "err": 0}
            out, err = AsyncSink(), AsyncSink()
            terminal = await asyncio.wait_for(follow_socket(
                None if initial else streams.socket_path, dict(offsets),
                output_callback(out, err, offsets), connection=connection,
            ), 5)
            assert terminal["return_code"] == 0
            assert out.getvalue() == b"first"
            assert err.getvalue() == b"warning"
            assert offsets == {"out": 5, "err": 7}
        finally:
            if connection is not None:
                connection.close()
            await streams.finish()

    asyncio.run(exercise())
