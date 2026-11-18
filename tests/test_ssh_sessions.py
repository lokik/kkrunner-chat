"""SSH/Python session reuse without an actual SSH server."""

import asyncio
import os
import sys
import time

import pytest

from kkrunner.client.contracts import Operation
from kkrunner.transports.implementations.ssh.source import portable_bundle
from kkrunner.transports.implementations.ssh.sessions import SshSessionPool
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


def test_reuse_install_once_and_isolate_application_errors():
    pool = SshSessionPool(LoopbackTransport())

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
        with pool._lock:
            sessions = list(pool._idle)
        assert len(sessions) == 1
        assert len(sessions[0].loaded) == 5
    finally:
        pool.close()


def test_pool_scales_to_simultaneous_requests():
    pool = SshSessionPool(LoopbackTransport())

    async def run():
        pids = await asyncio.gather(
            pool.call(Operation.standalone(_remote_pid), 1.0),
            pool.call(Operation.standalone(_remote_pid), 1.0),
        )
        assert len(set(pids)) == 2

    try:
        asyncio.run(run())
        with pool._lock:
            assert len(pool._sessions) == 2
    finally:
        pool.close()


def test_worker_failure_is_not_automatically_retried():
    pool = SshSessionPool(LoopbackTransport())

    async def run():
        first = await pool.call(Operation.standalone(_remote_pid))
        with pool._lock:
            session = pool._idle[0]
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


def test_cancelled_call_retires_worker_without_retry():
    pool = SshSessionPool(LoopbackTransport())

    async def run():
        task = asyncio.create_task(pool.call(Operation.standalone(_slow_operation)))
        for _ in range(100):
            with pool._lock:
                started = bool(pool._inflight)
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
            with pool._lock:
                finished = not pool._inflight and not pool._sessions
            if finished:
                break
            await asyncio.sleep(0.02)
        assert finished
        assert isinstance(await pool.call(Operation.standalone(_remote_pid)), int)

    try:
        asyncio.run(run())
    finally:
        pool.close()


def test_idle_workers_exit():
    transport = LoopbackTransport()
    transport.pool_idle_seconds = 1
    pool = SshSessionPool(transport)
    try:
        asyncio.run(pool.call(Operation.standalone(_remote_pid)))
        deadline = time.monotonic() + 4
        while time.monotonic() < deadline:
            with pool._lock:
                if not pool._sessions:
                    break
            time.sleep(0.1)
        with pool._lock:
            assert not pool._sessions
    finally:
        pool.close()
