"""Tests for the shared growing-file observation contract."""

import asyncio
import os
from threading import Event

import pytest

from kkrunner.support.growing_file_tail import GrowingFileTail, follow_growing_files


async def _follow_with_mock_scheduler(paths, on_chunk, wait, *, terminal=None, poll_interval=1.0):
    """Exercise file observation against an independent native scheduler wait."""
    finished = asyncio.create_task(asyncio.to_thread(wait))

    async def terminal_result():
        if not finished.done():
            return None
        native_status = finished.result()
        return await terminal(native_status) if terminal is not None else native_status

    result, _ = await follow_growing_files(
        paths, on_chunk, terminal_result, poll_interval=poll_interval, wakeup=finished,
    )
    return result


def test_append_and_eof(tmp_path):

    async def check():
        path = tmp_path / "out"
        path.write_bytes(b"first")
        async with GrowingFileTail({"out": path}) as tail:
            assert list(tail.read_available()) == [("out", b"first")]
            assert list(tail.read_available()) == []
            path.open("ab").write(b"second")
            await tail.wait_for_change()
            assert list(tail.read_available()) == [("out", b"second")]
            assert list(tail.read_available()) == []

    asyncio.run(check())


def test_missing_then_created(tmp_path):

    async def check():
        path = tmp_path / "new"
        async with GrowingFileTail({"out": path}) as tail:
            assert list(tail.read_available()) == []
            path.write_bytes(b"ready")
            await tail.wait_for_change()
            assert list(tail.read_available()) == [("out", b"ready")]

    asyncio.run(check())


def test_rename_keeps_offset(tmp_path):

    async def check():
        temporary = tmp_path / "launching"
        final = tmp_path / "out"
        temporary.write_bytes(b"abc")
        async with GrowingFileTail({"out": temporary}, watch_paths=(final,)) as tail:
            assert list(tail.read_available()) == [("out", b"abc")]
            os.replace(temporary, final)
            with final.open("ab") as destination:
                destination.write(b"def")
            assert list(tail.read_available({"out": final})) == [("out", b"def")]

    asyncio.run(check())


def test_live_only_starts_at_eof(tmp_path):

    async def check():
        path = tmp_path / "out"
        path.write_bytes(b"previous")
        async with GrowingFileTail({"out": path}, offsets={"out": None}) as tail:
            assert list(tail.read_available()) == []
            with path.open("ab") as destination:
                destination.write(b"new")
            assert list(tail.read_available()) == [("out", b"new")]

    asyncio.run(check())


def test_bounded_chunks(tmp_path):

    async def check():
        path = tmp_path / "out"
        path.write_bytes(b"123456")
        async with GrowingFileTail({"out": path}, chunk_size=2) as tail:
            assert list(tail.read_available()) == [("out", b"12")]
            assert list(tail.read_available()) == [("out", b"34")]
            assert list(tail.read_available()) == [("out", b"56")]
            assert list(tail.read_available()) == []

    asyncio.run(check())


def test_replaced_path_keeps_original_inode(tmp_path):

    async def check():
        path = tmp_path / "out"
        path.write_bytes(b"old")
        async with GrowingFileTail({"out": path}) as tail:
            with path.open("ab") as original:
                assert list(tail.read_available()) == [("out", b"old")]
                replacement = tmp_path / "replacement"
                replacement.write_bytes(b"new content")
                os.replace(replacement, path)
                original.write(b" remainder")
                original.flush()
                assert list(tail.read_available()) == [("out", b" remainder")]
                assert list(tail.read_available()) == []
                assert tail.snapshot_sizes() == {"out": len(b"old remainder")}

    asyncio.run(check())


def test_invalid_fallback_interval():
    try:
        GrowingFileTail({"out": "/tmp/example"}, poll_interval=0)
    except ValueError:
        pass
    else:
        raise AssertionError("Nonpositive poll intervals must be rejected")


def test_follow_growing_files_drains_both_channels_before_terminal(tmp_path):

    async def check():
        out, err = (tmp_path / "out", tmp_path / "err")
        out.write_bytes(b"123456")
        err.write_bytes(b"xyz")
        observed = []
        checks = []

        async def terminal():
            checks.append(True)
            return "done"

        result, drained = await follow_growing_files(
            {"out": out, "err": err}, lambda kind, data: observed.append((kind, data)), terminal
        )
        assert result == "done" and drained
        assert observed == [("out", b"123456"), ("err", b"xyz")]
        assert len(checks) == 1

    asyncio.run(check())


def test_follow_growing_files_skips_undefined_channels(tmp_path):

    async def check():
        seen = []

        async def terminal():
            return "done"

        result, drained = await follow_growing_files(
            {"out": None, "err": None}, lambda kind, data: seen.append((kind, data)), terminal
        )
        assert result == "done" and drained
        assert seen == []
        assert list(tmp_path.iterdir()) == []

    asyncio.run(check())


def test_follow_job_files_drains_output_created_at_completion(tmp_path):

    async def check():
        path = tmp_path / "later.out"
        entered, release = (Event(), Event())
        seen = []
        calls = []

        def wait():
            calls.append("wait")
            entered.set()
            assert release.wait(2)
            path.write_bytes(b"final bytes")
            return "done"

        following = asyncio.create_task(
            _follow_with_mock_scheduler({"out": path}, lambda kind, data: seen.append((kind, data)), wait, poll_interval=0.01)
        )
        try:
            assert await asyncio.to_thread(entered.wait, 2)
        finally:
            release.set()
        assert await asyncio.wait_for(following, 3) == "done"
        assert calls == ["wait"]
        assert seen == [("out", b"final bytes")]

    asyncio.run(check())


def test_follow_job_files_maps_terminal_status(tmp_path):

    async def check():
        observed = []

        async def terminal(native_status):
            observed.append(native_status)
            return {"state": "finished", "return_code": native_status}

        return_value = await _follow_with_mock_scheduler(
            {"out": tmp_path / "missing"}, lambda *_: None, lambda: 42, terminal=terminal, poll_interval=0.01
        )
        assert return_value == {"state": "finished", "return_code": 42}
        assert observed == [42]

    asyncio.run(check())


def test_follow_job_files_propagates_wait_errors(tmp_path):

    def wait():
        raise RuntimeError("scheduler wait failed")

    async def check():
        with pytest.raises(RuntimeError, match="scheduler wait failed"):
            await asyncio.wait_for(
                _follow_with_mock_scheduler({"out": tmp_path / "missing"}, lambda *_: None, wait, poll_interval=0.01), 3
            )

    asyncio.run(check())


def test_follow_job_files_cancelled_observation_does_not_cancel_job(tmp_path):

    async def check():
        entered, release, exited = (Event(), Event(), Event())

        def wait():
            entered.set()
            assert release.wait(2)
            exited.set()
            return "finished"

        follower = asyncio.create_task(
            _follow_with_mock_scheduler({"out": tmp_path / "missing"}, lambda *_: None, wait, poll_interval=0.01)
        )
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            follower.cancel()
            with pytest.raises(asyncio.CancelledError):
                await follower
            assert not exited.is_set()
        finally:
            release.set()
        assert await asyncio.to_thread(exited.wait, 2)

    asyncio.run(check())


def test_terminal_output_drain_is_bounded_when_producer_keeps_writing(tmp_path):

    async def check():
        path = tmp_path / "stream"
        path.write_bytes(b"A" * (65536 * 2))
        received = []
        checks = []

        def on_chunk(kind, data):
            received.append(data)
            with path.open("ab") as sink:
                sink.write(b"more data")

        async def terminal():
            checks.append(True)
            return "unknown"

        result, drained = await asyncio.wait_for(follow_growing_files({"out": path}, on_chunk, terminal, poll_interval=0.01), 3)
        assert result == "unknown" and drained
        assert 2 <= len(received) <= 3
        assert len(checks) == 1

    asyncio.run(check())


def test_read_error_does_not_wait_for_scheduler_job_to_end(tmp_path, monkeypatch):
    from threading import Event

    entered, release = (Event(), Event())

    def wait():
        entered.set()
        release.wait(10)
        return "finished"

    def unreadable(self, *args, **kwargs):
        raise PermissionError("output inaccessible")

    monkeypatch.setattr(GrowingFileTail, "read_available", unreadable)

    async def check():
        try:
            with pytest.raises(PermissionError, match="inaccessible"):
                await asyncio.wait_for(
                    _follow_with_mock_scheduler({"out": tmp_path / "job.out"}, lambda *_: None, wait, poll_interval=0.01), 2
                )
            assert await asyncio.to_thread(entered.wait, 1)
            assert not release.is_set()
        finally:
            # asyncio.run() waits for executor threads to finish. Unblock the
            # simulated scheduler *before* that shutdown, not afterwards.
            release.set()

    asyncio.run(check())


def test_terminal_creates_output_after_empty_read(tmp_path):

    async def check():
        path = tmp_path / "late.out"
        seen = []

        async def terminal():
            path.write_bytes(b"last bytes")
            return "finished"

        result, drained = await asyncio.wait_for(
            follow_growing_files(
                {"out": path}, lambda kind, data: seen.append((kind, data)), terminal, poll_interval=0.01
            ),
            2,
        )
        assert result == "finished" and drained
        assert seen == [("out", b"last bytes")]

    asyncio.run(check())


@pytest.mark.parametrize("change", ("remove", "truncate"))
def test_terminal_drain_when_path_is_removed_or_inode_truncated(tmp_path, monkeypatch, change):
    """Removing a pathname retains its inode; truncating that inode is an error."""

    async def check():
        path = tmp_path / "out"
        path.write_bytes(b"X" * (65536 * 3))
        original_snapshot = GrowingFileTail.snapshot_sizes
        snapshots = []

        def snapshot(tail):
            sizes = original_snapshot(tail)
            snapshots.append(sizes)
            if len(snapshots) == 1:
                if change == "remove":
                    path.unlink()
                else:
                    path.write_bytes(b"")
            return sizes

        monkeypatch.setattr(GrowingFileTail, "snapshot_sizes", snapshot)

        async def terminal():
            return "finished"

        seen = []
        following = follow_growing_files(
            {"out": path}, lambda kind, data: seen.append(data), terminal, poll_interval=0.01,
        )
        if change == "remove":
            assert await asyncio.wait_for(following, 1) == ("finished", True)
            assert b"".join(seen) == b"X" * (65536 * 3)
        else:
            with pytest.raises(ValueError, match="Offset is beyond the spool"):
                await asyncio.wait_for(following, 1)
            assert seen == [b"X" * 65536]
        assert snapshots[0] == {"out": 65536 * 3}

    asyncio.run(check())


def test_continuous_output_yields_to_scheduler_completion(tmp_path):

    async def check():
        path = tmp_path / "busy"
        path.write_bytes(b"X" * (65536 * 10))
        completed = asyncio.Event()
        received = []

        def on_chunk(kind, data):
            received.append(data)
            with path.open("ab") as stream:
                stream.write(data)

        async def mark_terminal():
            await asyncio.sleep(0)
            completed.set()

        asyncio.create_task(mark_terminal())

        async def terminal():
            return "finished" if completed.is_set() else None

        result, drained = await asyncio.wait_for(follow_growing_files({"out": path}, on_chunk, terminal, poll_interval=0.01), 1)
        assert result == "finished" and drained
        assert len(received) < 100

    asyncio.run(check())


def test_completed_wakeup_does_not_spin_while_confirmation_lags(tmp_path):
    """A one-shot notification must not trigger a busy loop."""
    import time

    async def check():
        wakeup = asyncio.get_running_loop().create_future()
        wakeup.set_result(True)
        queries = []

        async def terminal():
            queries.append(time.monotonic())
            return "finished" if len(queries) == 2 else None

        result, drained = await asyncio.wait_for(
            follow_growing_files(
                {"out": tmp_path / "missing"},
                lambda *_: None,
                terminal,
                poll_interval=0.03,
                wakeup=wakeup,
            ),
            2,
        )
        assert result == "finished" and drained
        assert len(queries) == 2
        assert queries[1] - queries[0] >= 0.01

    asyncio.run(check())


def test_callback_wakes_silent_file_follower_without_poll_timeout(tmp_path):

    async def check():
        loop = asyncio.get_running_loop()
        completed = loop.create_future()
        entered = asyncio.Event()
        seen = []

        async def terminal():
            entered.set()
            return "finished" if completed.done() else None

        task = asyncio.create_task(
            follow_growing_files(
                {"out": tmp_path / "does-not-exist"},
                lambda *args: seen.append(args),
                terminal,
                poll_interval=30,
                wakeup=completed,
            )
        )
        await asyncio.wait_for(entered.wait(), 1)
        completed.set_result(True)
        assert await asyncio.wait_for(task, 2) == ("finished", True)
        assert seen == []

    asyncio.run(check())



def test_completed_named_replay_stops_on_unsubscribe(tmp_path):
    from kkrunner.support.growing_file_tail import controlled_named_files

    async def check():
        path = tmp_path / "out"
        path.write_bytes(b"x" * (65536 * 8))
        control = asyncio.StreamReader()
        received = bytearray()

        class Sink:
            async def write(self, data):
                received.extend(data)
                control.feed_data(b"unfollow\n")
                await asyncio.sleep(0)

            def flush(self):
                pass

        facts = await asyncio.wait_for(controlled_named_files(
            {"out": path, "err": None}, {"out": Sink(), "err": Sink()},
            {"out": 0, "err": 0}, lambda: {"state": "finished"},
            .1, output=True, control=control,
        ), 2)
        assert facts["state"] == "finished"
        assert facts["output_drained"] is False
        assert 0 < len(received) < path.stat().st_size

    asyncio.run(check())



def test_output_cursor_advances_only_after_delivery(tmp_path):
    async def check():
        path = tmp_path / "out"
        path.write_bytes(b"output")
        offsets = {"out": 0}

        async def terminal():
            return "finished"

        def fail(kind, data):
            raise OSError("receiver failed")

        with pytest.raises(OSError, match="receiver failed"):
            await follow_growing_files({"out": path}, fail, terminal, offsets=offsets)
        assert offsets == {"out": 0}
        received = []
        result, drained = await follow_growing_files(
            {"out": path}, lambda kind, data: received.append(data), terminal, offsets=offsets,
        )
        assert (result, drained) == ("finished", True)
        assert received == [b"output"]

    asyncio.run(check())


@pytest.mark.parametrize("installed_reader", [False, True])
def test_file_subscription_does_not_read_without_subscriber(tmp_path, installed_reader):
    import io
    from kkrunner.support.growing_file_tail import follow_polled_files, observe_file_subscription

    async def check():
        path = tmp_path / "out"
        path.write_bytes(b"first")
        control = asyncio.StreamReader()
        completed = asyncio.Event()
        delivered = asyncio.Event()
        stopped = asyncio.Event()
        starts = []

        async def completion():
            await completed.wait()
            return 0

        selected = follow_growing_files if installed_reader else follow_polled_files

        async def reader(*args, **kwargs):
            starts.append(True)
            try:
                return await selected(*args, **kwargs)
            finally:
                stopped.set()

        class Sink(io.BytesIO):
            def write(self, data):
                count = super().write(data)
                delivered.set()
                return count

        sink = Sink()
        task = asyncio.create_task(observe_file_subscription(
            completion(), {"out": path}, {"out": sink}, {"out": 0}, control,
            output=False, poll_interval=.02, reader=reader,
        ))
        try:
            await asyncio.sleep(.05)
            assert not starts
            control.feed_data(b"follow\n")
            await asyncio.wait_for(delivered.wait(), 2)
            assert sink.getvalue() == b"first"
            control.feed_data(b"unfollow\n")
            await asyncio.wait_for(stopped.wait(), 2)
            with path.open("ab") as file:
                file.write(b"second")
            await asyncio.sleep(.05)
            assert sink.getvalue() == b"first"
            delivered.clear()
            control.feed_data(b"follow\n")
            await asyncio.wait_for(delivered.wait(), 2)
            completed.set()
            assert await asyncio.wait_for(task, 2) == (0, True, None)
            assert starts == [True, True]
            assert sink.getvalue() == b"firstsecond"
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(check())


@pytest.mark.parametrize("installed_reader", [False, True])
@pytest.mark.parametrize("pause", [False, True])
def test_subscription_drains_original_inode_after_path_reuse(tmp_path, installed_reader, pause):
    import io
    from kkrunner.support.growing_file_tail import follow_polled_files, observe_file_subscription

    async def check():
        path = tmp_path / "out"
        original_bytes = b"A" * (65536 * 3)
        path.write_bytes(original_bytes)
        control = asyncio.StreamReader()
        completed = asyncio.Event()
        first_chunk = asyncio.Event()
        release = asyncio.Event()
        stopped = asyncio.Event()
        selected = follow_growing_files if installed_reader else follow_polled_files
        async def completion():
            await completed.wait()
            return 0

        async def reader(*args, **kwargs):
            try:
                return await selected(*args, **kwargs)
            finally:
                stopped.set()

        class Sink(io.BytesIO):
            async def write(self, data):
                count = super().write(data)
                if not first_chunk.is_set():
                    first_chunk.set()
                    await release.wait()
                return count

        sink = Sink()
        task = asyncio.create_task(observe_file_subscription(
            completion(), {"out": path}, {"out": sink}, {"out": 0}, control,
            output=True, poll_interval=.02, reader=reader,
        ))
        try:
            await asyncio.wait_for(first_chunk.wait(), 2)
            if pause:
                control.feed_data(b"unfollow\n")
                release.set()
                await asyncio.wait_for(stopped.wait(), 2)
            replacement = tmp_path / "next"
            replacement.write_bytes(b"B" * (65536 * 4))
            os.replace(replacement, path)
            if pause:
                control.feed_data(b"follow\n")
                # Let the new subscription start before reporting completion.
                await asyncio.sleep(.05)
            completed.set()
            release.set()
            assert await asyncio.wait_for(task, 2) == (0, True, None)
            assert sink.getvalue() == original_bytes
            assert path.read_bytes() == b"B" * (65536 * 4)
        finally:
            release.set()
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(check())
