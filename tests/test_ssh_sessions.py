"""SSH/Python session reuse without an actual SSH server."""

import asyncio
import os
import sys
import time

import pytest

from kkrunner.client.contracts import Operation
from kkrunner.targets.implementations.ssh.source import portable_bundle
from kkrunner.targets import SshTarget
from kkrunner.targets.implementations.ssh import sessions
from kkrunner.support.decorators import depends_on, portable


@portable
def _double(value):
    return value * 2


@portable
@depends_on(_double)
def _add_one(value):
    return _double(value) + 1


@portable
def _remote_pid(delay=0):
    import os
    import time
    if delay:
        time.sleep(delay)
    return os.getpid()


@portable
def _remote_error():
    raise ValueError("application error")


@portable
async def _remote_async(value):
    import asyncio
    await asyncio.sleep(0)
    return value


class LoopbackTransport:
    """Spawn the actual session worker locally, without an SSH installation."""

    python_command = (sys.executable,)
    pool_size = 2
    pool_idle_seconds = 2

    @staticmethod
    def _command(argv, cwd=None):
        assert cwd is None
        return tuple(argv)


def test_portable_identity_depends_on_module_and_qualname_only():
    symbol, definitions = portable_bundle(_add_one)
    assert symbol.startswith("_kkrunner_")
    assert len(definitions) == 2
    assert "_double = _kkrunner_" in definitions[-1][1]
    scope = {}
    for _name, code in definitions:
        exec(code, scope)
    assert scope[symbol](4) == 9

    # Changing a function's source does not alter its stable identity.
    @portable
    def other():
        return 99

    other.__module__ = _add_one.__module__
    other.__qualname__ = _add_one.__qualname__
    assert portable_bundle(other)[0] == symbol


@pytest.fixture
def loopback(monkeypatch):
    monkeypatch.setattr(SshTarget, "_command", staticmethod(LoopbackTransport._command))
    return SshTarget("loopback", python_command=(sys.executable,),
                     pool_size=2, pool_idle_seconds=2)


def test_reuse_install_once_and_isolate_application_errors(loopback):
    pool = loopback

    async def run():
        first = await pool.call(Operation.standalone(_remote_pid))
        assert first != os.getpid()
        assert await pool.call(Operation.standalone(_remote_pid)) == first
        assert await pool.call(Operation.standalone(_add_one), 12) == 25
        assert await pool.call(Operation.standalone(_add_one), 13) == 27
        assert await pool.call(Operation.standalone(_remote_async), 10) == 10
        with pytest.raises(ValueError, match="application error"):
            await pool.call(Operation.standalone(_remote_error))
        assert await pool.call(Operation.standalone(_remote_pid)) == first

    try:
        asyncio.run(run())
        with sessions._CHANGED:
            idle = list(sessions._POOLS[pool]._idle)
        assert len(idle) == 1
        assert len(idle[0][1].loaded) == 5
    finally:
        pool.close()


def test_pool_scales_to_simultaneous_requests(loopback):
    pool = loopback

    async def run():
        pids = await asyncio.gather(
            pool.call(Operation.standalone(_remote_pid), 1.0),
            pool.call(Operation.standalone(_remote_pid), 1.0),
        )
        assert len(set(pids)) == 2

    try:
        asyncio.run(run())
        with sessions._CHANGED:
            assert len(sessions._POOLS[pool]._idle) == 2
    finally:
        pool.close()


def test_worker_failure_is_not_automatically_retried(loopback):
    pool = loopback

    async def run():
        first = await pool.call(Operation.standalone(_remote_pid))
        with sessions._CHANGED:
            _, session = sessions._POOLS[pool]._idle[0]
        session.process.kill()
        session.process.wait()
        with pytest.raises(ConnectionError):
            await pool.call(Operation.standalone(_remote_pid))
        assert await pool.call(Operation.standalone(_remote_pid)) != first

    try:
        asyncio.run(run())
    finally:
        pool.close()


@portable
def _slow_operation():
    import time
    time.sleep(30)
    return 1


def test_cancelled_call_retires_worker_without_retry(loopback):
    pool = loopback

    async def run():
        task = asyncio.create_task(pool.call(Operation.standalone(_slow_operation)))
        for _ in range(100):
            with sessions._CHANGED:
                current = sessions._POOLS.get(pool)
                started = current is not None and any(current._pending.values())
            if started:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("Remote worker did not start")
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        # The interrupted session is never returned to the reusable pool.
        for _ in range(100):
            with sessions._CHANGED:
                finished = pool not in sessions._POOLS
            if finished:
                break
            await asyncio.sleep(0.02)
        assert finished
        assert isinstance(await pool.call(Operation.standalone(_remote_pid)), int)

    try:
        asyncio.run(run())
    finally:
        pool.close()


async def _until(predicate, timeout=5):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError("Timed out waiting for SSH pool state")
        await asyncio.sleep(.02)


def _registered(target):
    with sessions._CHANGED:
        return sessions._POOLS.get(target)


def test_idle_workers_and_empty_pool_exit(loopback):
    from dataclasses import replace
    target = replace(loopback, pool_idle_seconds=1)

    async def run():
        first = await target.call(Operation.standalone(_remote_pid))
        old = _registered(target)
        await _until(lambda: _registered(target) is None)
        assert old is not None
        assert await target.call(Operation.standalone(_remote_pid)) != first
        assert _registered(target) is not old

    asyncio.run(run())


def test_individual_session_expires_while_other_request_is_active(loopback, tmp_path):
    from dataclasses import replace
    target = replace(loopback, pool_idle_seconds=1)
    operation = Operation.standalone(_wait_for_file)
    started, release = tmp_path / "started", tmp_path / "release"

    async def run():
        task = asyncio.create_task(target.call(operation, str(started), str(release)))
        try:
            await _until(started.exists)
            idle_pid = await target.call(Operation.standalone(_remote_pid))
            original = _registered(target)
            with sessions._CHANGED:
                idle_session = original._idle[0][1]
            await _until(lambda: idle_session.process.poll() is not None)
            assert not task.done()
            assert _registered(target) is original
            assert await target.call(Operation.standalone(_remote_pid)) != idle_pid
        finally:
            release.touch()
            assert await asyncio.wait_for(task, 5) == 42

    asyncio.run(run())


@portable
def _wait_for_file(started, release):
    import time
    from pathlib import Path
    Path(started).touch()
    deadline = time.monotonic() + 15
    while not Path(release).exists():
        if time.monotonic() > deadline:
            raise TimeoutError("Test did not release remote operation")
        time.sleep(.02)
    return 42


def test_queued_requests_keep_pool_alive(loopback, tmp_path):
    from dataclasses import replace
    target = replace(loopback, pool_size=1, pool_idle_seconds=1)
    started, release = tmp_path / "started", tmp_path / "release"

    async def run():
        first = asyncio.create_task(target.call(
            Operation.standalone(_wait_for_file), str(started), str(release),
        ))
        await _until(started.exists)
        old = _registered(target)
        queued = asyncio.create_task(target.call(Operation.standalone(_remote_pid)))
        try:
            await asyncio.sleep(1.2)
            assert _registered(target) is old
            assert not queued.done()
        finally:
            release.touch()
            assert await asyncio.wait_for(first, 5) == 42
            assert isinstance(await asyncio.wait_for(queued, 5), int)
        await _until(lambda: _registered(target) is None)

    asyncio.run(run())


def test_pool_removed_after_failed_session_start(loopback, monkeypatch):
    def fail(*args, **kwargs):
        raise OSError("SSH unavailable")

    monkeypatch.setattr(sessions.subprocess, "Popen", fail)

    async def run():
        with pytest.raises(OSError, match="SSH unavailable"):
            await loopback.call(Operation.standalone(_remote_pid))
        await _until(lambda: _registered(loopback) is None)

    asyncio.run(run())


def test_evicting_client_does_not_close_shared_target_sessions(monkeypatch):
    from kkrunner.backends.config import BackendConfig
    from kkrunner.client.pool import ClientPool
    from kkrunner.targets import SshTarget

    monkeypatch.setattr(SshTarget, "_command", staticmethod(LoopbackTransport._command))
    pool = ClientPool()
    target = SshTarget("loopback-client-cache", python_command=(sys.executable,))
    equivalent = SshTarget("loopback-client-cache", python_command=(sys.executable,))
    first = pool.for_config(target, BackendConfig("direct"))
    second = pool.for_config(equivalent, BackendConfig("kkrunner"))

    async def pid(client):
        return await client.target.call(Operation.standalone(_remote_pid))

    try:
        initial = asyncio.run(pid(first))
        assert asyncio.run(pid(second)) == initial
        assert pool.discard((target, first.backend_config)) is first
        assert asyncio.run(pid(second)) == initial
        pool.clear()
        assert asyncio.run(pid(second)) == initial
        target.close()
        # Explicit target shutdown, unlike cache eviction, retires the worker.
        assert asyncio.run(pid(second)) != initial
    finally:
        equivalent.close()


def test_expiring_pool_does_not_block_new_request(loopback, monkeypatch):
    from dataclasses import replace
    from threading import Event

    target = replace(loopback, pool_idle_seconds=1)
    entered, release = Event(), Event()
    original_close = sessions._Session.close

    async def run():
        first_pid = await target.call(Operation.standalone(_remote_pid))
        with sessions._CHANGED:
            old_pool = sessions._POOLS[target]
            old_session = old_pool._idle[0][1]

        def close(session):
            if session is old_session:
                entered.set()
                if not release.wait(5):
                    raise AssertionError("Test did not release expired session")
            original_close(session)

        monkeypatch.setattr(sessions._Session, "close", close)
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            # The registry has already dropped the old pool, even though
            # closing its interpreter is still in progress.
            assert await asyncio.wait_for(
                target.call(Operation.standalone(_remote_pid)), 3,
            ) != first_pid
            assert _registered(target) is not old_pool
        finally:
            release.set()
        await _until(lambda: old_session.process.poll() is not None)

    asyncio.run(run())


def test_close_target_cancels_pending_calls_without_closing_other_target(loopback, tmp_path):
    from dataclasses import replace
    target = replace(loopback, pool_size=1)
    other = replace(loopback, host="other-loopback", pool_idle_seconds=30)
    started, release = tmp_path / "started", tmp_path / "release"

    async def run():
        other_pid = await other.call(Operation.standalone(_remote_pid))
        first = asyncio.create_task(target.call(
            Operation.standalone(_wait_for_file), str(started), str(release),
        ))
        queued = None
        try:
            await _until(started.exists)
            old_pool = _registered(target)
            queued = asyncio.create_task(target.call(Operation.standalone(_remote_pid)))

            def enqueued():
                with sessions._CHANGED:
                    return len(old_pool._pending) == 2

            await _until(enqueued)
            target.close()
            results = await asyncio.wait_for(
                asyncio.gather(first, queued, return_exceptions=True), 5,
            )
            assert isinstance(results[0], ConnectionError)
            assert isinstance(results[1], asyncio.CancelledError)
            assert await other.call(Operation.standalone(_remote_pid)) == other_pid
            assert isinstance(await target.call(Operation.standalone(_remote_pid)), int)
            assert _registered(target) is not old_pool
        finally:
            release.touch()
            await asyncio.gather(
                first, *(task for task in (queued,) if task is not None),
                return_exceptions=True,
            )

    asyncio.run(run())
