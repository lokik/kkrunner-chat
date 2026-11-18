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

from .worker import WORKER_SOURCE

from ...rpc import operation_result
from .source import portable_bundle


class _Session:
    def __init__(self, transport, cancelled=None):
        self.loaded = set()
        self.last_used = time.monotonic()
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
        self.last_used = time.monotonic()
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
    """Bounded pool of reusable SSH/Python subprocess pairs.

    A dedicated thread executor handles blocking pipes, so sessions do not
    belong to the short-lived event loops created by run_coro_sync and Job.
    """

    def __init__(self, transport):
        self.transport = transport
        self.idle_timeout = transport.pool_idle_seconds
        self._lock = threading.Lock()
        self._idle = []
        self._sessions = set()
        self._inflight = {}
        self._closed = threading.Event()
        self._executor = ThreadPoolExecutor(
            max_workers=transport.pool_size, thread_name_prefix="kkrunner-ssh",
        )
        self._reaper = threading.Thread(target=self._reap, daemon=True)
        self._reaper.start()

    def _take(self, cancelled):
        with self._lock:
            if self._closed.is_set():
                raise RuntimeError("SSH session pool has been closed")
            if self._idle:
                return self._idle.pop()
        # ThreadPoolExecutor bounds the total number of simultaneous calls;
        # each call either owns one session or starts exactly one new session.
        session = _Session(self.transport, cancelled)
        with self._lock:
            if self._closed.is_set():
                session.close()
                raise RuntimeError("SSH session pool has been closed")
            self._sessions.add(session)
        return session

    def _retire(self, session):
        with self._lock:
            self._sessions.discard(session)
            if session in self._idle:
                self._idle.remove(session)
        session.close()

    def _invoke(self, operation, args, kwargs, cancelled):
        function = (
            operation.function if operation.function is not None
            else getattr(operation.backend.SshHelper, operation.method)
        )
        symbol, definitions = portable_bundle(function)
        session = self._take(cancelled)
        with self._lock:
            self._inflight[cancelled] = session
        try:
            if cancelled.is_set():
                raise CancelledError()
            reply = session.invoke(symbol, definitions, args, kwargs)
        except BaseException:
            # Never retry a possibly submitted operation on another connection.
            self._retire(session)
            raise
        finally:
            with self._lock:
                self._inflight.pop(cancelled, None)
        with self._lock:
            if not self._closed.is_set() and not cancelled.is_set():
                self._idle.append(session)
                session = None
        if session is not None:
            session.close()
        # Application exceptions are not connection failures: keep this
        # interpreter alive and decode the ordinary remote error contract.
        return operation_result(json.dumps(reply).encode("utf8"))

    async def call(self, operation, *args, **kwargs):
        cancelled = threading.Event()
        future = self._executor.submit(self._invoke, operation, args, kwargs, cancelled)
        try:
            return await asyncio.wrap_future(future)
        except asyncio.CancelledError:
            # Abort the worker rather than allowing an unobserved submission
            # to continue using this transport. Never replay the request.
            cancelled.set()
            with self._lock:
                session = self._inflight.get(cancelled)
            if session is not None:
                session.abort()
            raise

    def _reap(self):
        while not self._closed.wait(min(30.0, max(1.0, self.idle_timeout / 2))):
            now = time.monotonic()
            with self._lock:
                expired = [s for s in self._idle if now - s.last_used >= self.idle_timeout]
                for session in expired:
                    self._idle.remove(session)
                    self._sessions.remove(session)
            for session in expired:
                session.close()

    def close(self):
        self._closed.set()
        self._executor.shutdown(wait=False, cancel_futures=True)
        with self._lock:
            sessions = tuple(self._sessions)
            self._sessions.clear()
            self._idle.clear()
            inflight = tuple(self._inflight)
        for token in inflight:
            token.set()
        for session in sessions:
            session.close()


_POOLS = {}
_POOLS_LOCK = threading.Lock()


def pool_for(transport):
    with _POOLS_LOCK:
        pool = _POOLS.get(transport)
        if pool is None or pool._closed.is_set():
            pool = SshSessionPool(transport)
            _POOLS[transport] = pool
        return pool


def close_pool(transport):
    with _POOLS_LOCK:
        pool = _POOLS.pop(transport, None)
    if pool is not None:
        pool.close()


def _close_all():
    with _POOLS_LOCK:
        pools = tuple(_POOLS.values())
        _POOLS.clear()
    for pool in pools:
        pool.close()


atexit.register(_close_all)
