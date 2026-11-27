"""Reusable SSH + Python interpreters shared across client event loops.

One session is one SSH subprocess with one remote Python interpreter. The
client alone tracks delivered portable definitions for each live session.
The remote process only executes source into its normal Python globals.
"""

from __future__ import annotations

import asyncio
import atexit
import json
import subprocess
import threading
import time
from concurrent.futures import CancelledError, ThreadPoolExecutor

from kkrunner.targets.implementations.ssh.worker import WORKER_SOURCE

from ...rpc import operation_result
from .source import portable_bundle
from .helpers import ssh_function


class _Session:
    def __init__(self, transport, cancelled=None):
        self.loaded = set()
        self.diagnostics = bytearray()
        self.process = subprocess.Popen(
            transport._command((*transport.python_command, "-u", "-c", WORKER_SOURCE)),
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            bufsize=-1,
        )
        self._stderr_reader = threading.Thread(target=self._drain_stderr, daemon=True)
        self._stderr_reader.start()
        ready = threading.Event()
        handshake = []

        def read_handshake():
            try:
                handshake.append(self.process.stdout.readline())
            except BaseException as exc:
                handshake.append(exc)
            finally:
                ready.set()

        threading.Thread(target=read_handshake, daemon=True).start()
        try:
            deadline = time.monotonic() + 60
            while not ready.wait(0.1):
                if cancelled is not None and cancelled.is_set():
                    raise CancelledError()
                if time.monotonic() >= deadline:
                    raise TimeoutError("Timed out starting remote Python session")
            if handshake != [b"KKRUNNER_SESSION_1\n"]:
                raise ConnectionError("Remote Python session did not start: " + self.error_text())
        except BaseException:
            self.close()
            raise

    def _drain_stderr(self):
        try:
            while chunk := self.process.stderr.read1(16384):
                self.diagnostics.extend(chunk)
                if len(self.diagnostics) > 65536:
                    del self.diagnostics[:-65536]
        except (OSError, ValueError):
            pass

    def error_text(self):
        return self.diagnostics.decode("utf8", errors="replace").strip()

    def invoke(self, symbol, definitions, args, kwargs):
        missing = [(name, source) for name, source in definitions if name not in self.loaded]
        request = {"definitions": missing, "function": symbol,
                   "args": args, "kwargs": kwargs}
        message = json.dumps(request, allow_nan=False, separators=(",", ":")).encode("utf8") + b"\n"
        try:
            self.process.stdin.write(message)
            self.process.stdin.flush()
            line = self.process.stdout.readline()
        except OSError as exc:
            raise ConnectionError("Remote Python session lost: " + self.error_text()) from exc
        if not line:
            raise ConnectionError("Remote Python session closed: " + self.error_text())
        try:
            reply = json.loads(line)
        except (ValueError, UnicodeError) as exc:
            raise ConnectionError("Invalid remote Python session response") from exc
        if not isinstance(reply, dict) or reply.get("version") != 1 or reply.get("loaded") is not True:
            raise ConnectionError("Remote Python session could not install function definitions: "
                                  + str(reply.get("error") if isinstance(reply, dict) else reply))
        self.loaded.update(name for name, _ in missing)
        return reply

    def abort(self):
        """Interrupt a blocked request; never reuse this interpreter."""
        try:
            self.process.terminate()
        except ProcessLookupError:
            pass

    def close(self):
        process = self.process
        if process.poll() is None:
            try:
                process.stdin.close()
            except (OSError, ValueError):
                pass
            try:
                process.wait(timeout=0.5)
            except subprocess.TimeoutExpired:
                self.abort()
                try:
                    process.wait(timeout=0.5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        else:
            process.wait()
        for stream in (process.stdin, process.stdout, process.stderr):
            if stream is not None:
                try:
                    stream.close()
                except (OSError, ValueError):
                    pass

class SshSessionPool:
    """Bounded SSH/Python workers for one target configuration.

    All pool bookkeeping uses the registry condition. Blocking pipe I/O,
    process startup and process cleanup never hold that lock.
    """

    def __init__(self, transport):
        self.target = transport
        self.idle_timeout = transport.pool_idle_seconds
        self._idle = []
        # A token exists from submission through Future completion (including
        # queued calls and interpreter startup). Its value is the leased session.
        self._pending = {}
        self._executor = ThreadPoolExecutor(
            max_workers=transport.pool_size, thread_name_prefix="kkrunner-ssh",
        )

    def _take(self, cancelled):
        with _CHANGED:
            if cancelled.is_set():
                raise CancelledError()
            if self._idle:
                _, session = self._idle.pop()
                self._pending[cancelled] = session
                _CHANGED.notify()
                return session

        session = _Session(self.target, cancelled)
        with _CHANGED:
            if not cancelled.is_set():
                self._pending[cancelled] = session
                return session
        session.close()
        raise CancelledError()

    def _invoke(self, operation, args, kwargs, cancelled):
        function = ssh_function(operation)
        symbol, definitions = portable_bundle(function)
        session = self._take(cancelled)
        reusable = False
        try:
            if cancelled.is_set():
                raise CancelledError()
            reply = session.invoke(symbol, definitions, args, kwargs)
            reusable = True
        finally:
            with _CHANGED:
                # Release the lease atomically with returning the interpreter
                # to the pool. Cancellation must never abort its next user.
                self._pending[cancelled] = None
                if reusable and not cancelled.is_set():
                    self._idle.append((time.monotonic() + self.idle_timeout, session))
                    session = None
            if session is not None:
                session.close()
        # Application errors do not poison a healthy interpreter.
        return operation_result(json.dumps(reply).encode("utf8"))

    def _submit(self, operation, args, kwargs):
        """Called under the registry lock, before an idle pool can expire."""
        cancelled = threading.Event()
        self._pending[cancelled] = None
        try:
            future = self._executor.submit(self._invoke, operation, args, kwargs, cancelled)
        except BaseException:
            del self._pending[cancelled]
            _CHANGED.notify()
            raise

        def done(_):
            with _CHANGED:
                del self._pending[cancelled]
                _CHANGED.notify()

        future.add_done_callback(done)
        return future, cancelled

    def _cancel(self, token):
        """Cancel one request, without releasing its lease prematurely."""
        with _CHANGED:
            token.set()
            session = self._pending.get(token)
            if session is not None:
                session.abort()

    def _detach(self):
        """Remove idle workers and signal outstanding requests under the lock."""
        for token, session in self._pending.items():
            token.set()
            if session is not None:
                session.abort()
        idle = [session for _, session in self._idle]
        self._idle.clear()
        return idle

    def _shutdown(self):
        self._executor.shutdown(wait=False, cancel_futures=True)


_POOLS = {}
_CHANGED = threading.Condition()
_EXPIRY_THREAD = None


async def call_pooled(transport, operation, *args, **kwargs):
    """Acquire a pool and enqueue a request as one registry operation."""
    global _EXPIRY_THREAD
    with _CHANGED:
        if _EXPIRY_THREAD is None:
            _EXPIRY_THREAD = threading.Thread(
                target=_expire_idle, name="kkrunner-ssh-expiry", daemon=True,
            )
            _EXPIRY_THREAD.start()
        pool = _POOLS.get(transport)
        if pool is None:
            pool = _POOLS[transport] = SshSessionPool(transport)
        future, token = pool._submit(operation, args, kwargs)
    try:
        return await asyncio.wrap_future(future)
    except asyncio.CancelledError:
        pool._cancel(token)
        raise


def _expire_idle():
    """Sleep until the earliest idle deadline or a change to the registry."""
    while True:
        expired = []
        empty = []
        with _CHANGED:
            now = time.monotonic()
            deadline = None
            for target, pool in tuple(_POOLS.items()):
                idle = []
                for expires, session in pool._idle:
                    if expires <= now:
                        expired.append(session)
                    else:
                        idle.append((expires, session))
                        deadline = expires if deadline is None else min(deadline, expires)
                pool._idle = idle
                if not pool._idle and not pool._pending:
                    del _POOLS[target]
                    empty.append(pool)
            if not expired and not empty:
                _CHANGED.wait(None if deadline is None else max(0, deadline - time.monotonic()))
                continue
        # An expired session or pool is detached before releasing the lock.
        # Neither slow shutdown nor remote I/O blocks requests on other targets.
        for pool in empty:
            pool._shutdown()
        for session in expired:
            session.close()


def close_pool(transport):
    with _CHANGED:
        pool = _POOLS.pop(transport, None)
        if pool is None:
            return
        idle = pool._detach()
        _CHANGED.notify()
    pool._shutdown()
    for session in idle:
        session.close()


def _close_all():
    with _CHANGED:
        pools = tuple(_POOLS.values())
        _POOLS.clear()
        idle = [session for pool in pools for session in pool._detach()]
        _CHANGED.notify()
    for pool in pools:
        pool._shutdown()
    for session in idle:
        session.close()


atexit.register(_close_all)
