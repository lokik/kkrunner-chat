"""Framed FOLLOW streams and transient sockets."""

import asyncio
import inspect
import json
import os
import shutil
import struct
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from kkrunner.support.durability import fsync_directory

MAGIC = b"A2SFOLL1"
_HEADER = struct.Struct("!cQQ")
_CODES = {"out": b"O", "err": b"E"}
_BACK = {code: kind for kind, code in _CODES.items()}
_CHUNK = 65536
_HIGH = 512 * 1024
_LOW = 128 * 1024
_MAX_REQUEST = 4096


def frame(kind, offset, data):
    return _HEADER.pack(_CODES[kind], offset, len(data)) + data


class SupervisorClosedBeforeFollow(asyncio.IncompleteReadError):
    """The supervisor closed before sending any protocol bytes."""


class SupervisorConnectionError(ConnectionError):
    """Socket I/O failed, independently of the client's output consumer."""


async def receive(reader):
    """Yield offset-bearing stream frames followed by terminal facts."""
    try:
        try:
            magic = await reader.readexactly(len(MAGIC))
        except asyncio.IncompleteReadError as exc:
            if not exc.partial:
                raise SupervisorClosedBeforeFollow(exc.partial, exc.expected) from exc
            raise
        if magic != MAGIC:
            raise ValueError("Unknown supervisor stream protocol")
        while True:
            code, offset, length = _HEADER.unpack(await reader.readexactly(_HEADER.size))
            if length > (_MAX_REQUEST if code == b"T" else _CHUNK):
                raise ValueError("Oversized supervisor frame")
            payload = await reader.readexactly(length)
            if code == b"T":
                yield "terminal", 0, json.loads(payload)
                return
            if code not in _BACK:
                raise ValueError("Invalid supervisor channel")
            yield _BACK[code], offset, payload
    except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError) as exc:
        raise SupervisorConnectionError(str(exc)) from exc


@dataclass(eq=False)
class _Follower:
    writer: asyncio.StreamWriter
    offsets: dict
    replay: bool = True
    wake: asyncio.Event = field(default_factory=asyncio.Event)
    done: asyncio.Event = field(default_factory=asyncio.Event)


class SupervisorStreams:
    """One event loop writes the spools and fans out output to followers."""

    def __init__(self, stdout_path=None, stderr_path=None, *, detached=True, socket_dir=None):
        self.paths = {
            "out": None if stdout_path is None else os.fspath(stdout_path),
            "err": None if stderr_path is None else os.fspath(stderr_path),
        }
        self.merged = (
            stdout_path is not None
            and stderr_path is not None
            and os.path.abspath(stdout_path) == os.path.abspath(stderr_path)
        )
        if self.merged:
            del self.paths["err"]
        self.detached = detached
        self.disconnected = asyncio.Event()
        self.cancelled = asyncio.Event()
        self._release_backpressure = asyncio.Event()
        self._session_follower = None
        self._initial_task = None
        self._spools = {}
        self._live_spools = {}
        self._sizes = {kind: 0 for kind in self.paths}
        self._followers = set()
        self._connections = set()
        self._server = None
        self._terminal = None
        self._socket_dir = socket_dir
        self.socket_path = None

    async def start(self):
        try:
            for kind, path in self.paths.items():
                if path is not None:
                    Path(path).parent.mkdir(parents=True, exist_ok=True)
                    self._spools[kind] = open(path, "w+b", buffering=0)
            if self._socket_dir is None:
                self._socket_dir = tempfile.mkdtemp(prefix="kkrunner-follow-")
            self.socket_path = os.path.join(self._socket_dir, "socket")
            self._server = await asyncio.start_unix_server(self._connect, self.socket_path)
            os.chmod(self.socket_path, 0o600)
            return self.socket_path
        except BaseException as startup_error:
            # Keep the original startup failure as the raised exception,
            # while retaining the cleanup failure as its explicit cause.
            try:
                await self.finish()
            except BaseException as cleanup_error:
                raise startup_error from cleanup_error
            raise

    def release_backpressure(self):
        """Execution no longer depends on the speed of its attached observer."""
        self._release_backpressure.set()

    async def publish(self, kind, data):
        if self._terminal is not None:
            raise RuntimeError("Output published after termination")
        if self.merged and kind == "err":
            kind = "out"
        if not data:
            return
        data = bytes(data)
        offset = self._sizes[kind]
        spool = self._spools.get(kind)
        if spool is not None:
            if spool.write(data) != len(data):
                raise OSError("Incomplete process output write")
            spool.flush()
        self._sizes[kind] += len(data)
        wire = frame(kind, offset, data)
        for follower in tuple(self._followers):
            if follower.writer.is_closing():
                continue
            # A spooled channel can catch up from disk. A live-only channel
            # cannot: forward it even while another channel is replaying.
            if follower.replay and self.paths[kind] is not None:
                follower.wake.set()
                continue
            if not self.detached and self.paths[kind] is None:
                # Once execution is ending, retain pending live-only bytes
                # independently of the observer's reading speed.
                writer = follower.writer
                if kind in self._live_spools or (
                    self._release_backpressure.is_set() and writer.transport.get_write_buffer_size() >= _HIGH
                ):
                    try:
                        if kind not in self._live_spools:
                            self._live_spools[kind] = (tempfile.TemporaryFile(buffering=0), offset)
                        if self._live_spools[kind][0].write(data) != len(data):
                            raise OSError("Incomplete observation spool write")
                    except OSError:
                        # Failure to retain optional observation data cannot
                        # change the native execution result.
                        writer.transport.abort()
                        continue
                    follower.replay = True
                    follower.wake.set()
                    continue
                try:
                    writer.write(wire)
                    follower.offsets[kind] += len(data)
                    if writer.transport.get_write_buffer_size() >= _HIGH and not self._release_backpressure.is_set():
                        draining = asyncio.create_task(writer.drain())
                        released = asyncio.create_task(self._release_backpressure.wait())
                        try:
                            done, _ = await asyncio.wait(
                                (draining, released), return_when=asyncio.FIRST_COMPLETED,
                            )
                            if draining in done:
                                draining.result()
                        finally:
                            for task in (draining, released):
                                task.cancel()
                            await asyncio.gather(draining, released, return_exceptions=True)
                except (ConnectionError, OSError):
                    self.disconnected.set()
                    writer.transport.abort()
                continue
            # A full buffer skips the write; a write that fills the buffer
            # triggers exactly the same backpressure handling below.
            if follower.writer.transport.get_write_buffer_size() < _HIGH:
                follower.writer.write(wire)
                follower.offsets[kind] += len(data)
            if follower.writer.transport.get_write_buffer_size() >= _HIGH:
                if self.paths[kind] is None:
                    # Live-only bytes cannot be recovered after disconnect.
                    follower.writer.transport.abort()
                else:
                    follower.replay = True
                    follower.wake.set()

    def sync(self):
        """Durability barrier before publishing successful terminal outcome."""
        for kind, spool in self._spools.items():
            spool.flush()
            os.fsync(spool.fileno())
            fsync_directory(Path(self.paths[kind]).parent)

    def terminal(self, outcome):
        self._terminal = dict(outcome)
        for follower in tuple(self._followers):
            follower.wake.set()

    def _register_follower(self, writer, offsets):
        if not isinstance(offsets, dict) or set(offsets) != set(self.paths):
            raise ValueError("Invalid stream offsets")
        for kind, offset in offsets.items():
            if offset is None:
                # from_bytes=None means observe only newly arriving bytes.
                offsets[kind] = self._sizes[kind]
                continue
            if type(offset) is not int or offset < 0:
                raise ValueError("Invalid stream offset")
            if self.paths[kind] is not None and offset > self._sizes[kind]:
                raise ValueError("Offset is beyond the spool")
            if self.paths[kind] is None and offset != self._sizes[kind]:
                raise ValueError("Requested live-only output is no longer available")
        writer.transport.set_write_buffer_limits(high=_HIGH, low=_LOW)
        follower = _Follower(writer, offsets.copy())
        if not self.detached:
            if self._session_follower is not None:
                raise ValueError("Attached execution already has a client")
            self._session_follower = follower
        writer.write(MAGIC)
        self._followers.add(follower)
        return follower

    async def attach_initial(self, connection):
        """Install the inherited connection before the calculation can launch."""
        reader, writer = await asyncio.open_unix_connection(sock=connection)
        try:
            follower = self._register_follower(writer, dict.fromkeys(self.paths, 0))
        except BaseException:
            writer.close()
            raise
        if not self.detached and (reader.at_eof() or writer.is_closing()):
            self.disconnected.set()
        self._initial_task = asyncio.create_task(self._connect(reader, writer, follower=follower))

    async def _connect(self, reader, writer, *, follower=None):
        self._connections.add(writer)
        disconnect_watch = None
        try:
            if follower is None:
                if not self._server.is_serving():
                    return
                raw = await reader.readline()
                if not raw or len(raw) > _MAX_REQUEST:
                    raise ValueError("Malformed supervisor request")
                request = json.loads(raw)
                if request.get("command") == "cancel":
                    self.cancelled.set()
                    writer.write(b'{"accepted":true}\n')
                    await writer.drain()
                    return
                if request.get("command") != "follow":
                    raise ValueError("Unknown supervisor command")
                follower = self._register_follower(writer, request.get("offsets"))
            # Detached clients can disconnect while a calculation is silent.
            # Observe EOF for *every* follower so an idle socket does not
            # remain in _followers until the next output or terminal event.
            disconnect_watch = asyncio.create_task(reader.read(1))

            def on_disconnect(task):
                if task.cancelled():
                    return
                if not self.detached:
                    self.disconnected.set()
                # EOF also releases a writer blocked in drain().
                writer.transport.abort()
                follower.wake.set()

            disconnect_watch.add_done_callback(on_disconnect)
            while True:
                if disconnect_watch is not None and disconnect_watch.done():
                    return
                if follower.replay:
                    await writer.drain()
                    for kind in self.paths:
                        offset = follower.offsets[kind]
                        if offset < self._sizes[kind]:
                            if kind in self._live_spools:
                                spool, first = self._live_spools[kind]
                            elif kind in self._spools:
                                spool, first = self._spools[kind], 0
                            else:
                                raise ConnectionError("Follower output is no longer available")
                            data = os.pread(spool.fileno(), min(_CHUNK, self._sizes[kind] - offset), offset - first)
                            if not data:
                                raise RuntimeError("Supervisor spool truncated")
                            writer.write(frame(kind, offset, data))
                            follower.offsets[kind] += len(data)
                            await writer.drain()
                            break
                    else:
                        # No suspension between checking EOF and enabling LIVE.
                        follower.replay = False
                if self._terminal is not None and all(
                    follower.offsets[kind] == self._sizes[kind] for kind in self.paths
                ):
                    payload = json.dumps(self._terminal, separators=(",", ":")).encode()
                    writer.write(_HEADER.pack(b"T", 0, len(payload)) + payload)
                    await writer.drain()
                    return
                if not follower.replay:
                    await follower.wake.wait()
                    follower.wake.clear()
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, asyncio.IncompleteReadError):
            pass
        finally:
            if follower is self._session_follower and follower is not None and self._terminal is None:
                self.disconnected.set()
            if disconnect_watch is not None:
                disconnect_watch.cancel()
                await asyncio.gather(disconnect_watch, return_exceptions=True)
            writer.close()
            try:
                await writer.wait_closed()
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                pass
            finally:
                self._connections.discard(writer)
                if follower is not None:
                    self._followers.discard(follower)
                    follower.done.set()

    def stop_accepting(self):
        """Retire the execution endpoint without closing existing followers."""
        if self._server is not None:
            self._server.close()
            Path(self.socket_path).unlink(missing_ok=True)

    async def finish(self):
        """Let active clients drain their replay and close their connections."""
        self.stop_accepting()
        # Unregistered connections have no output to drain.
        draining = {f.writer for f in self._followers} if self._terminal is not None else set()
        for writer in self._connections - draining:
            writer.transport.abort()
        if self._followers:
            await asyncio.gather(*(f.done.wait() for f in tuple(self._followers)))
        if self._server is not None:
            await self._server.wait_closed()
        if self._initial_task is not None:
            if not self._initial_task.done():
                self._initial_task.cancel()
            await asyncio.gather(self._initial_task, return_exceptions=True)
        for spool in self._spools.values():
            spool.close()
        for spool, _ in self._live_spools.values():
            spool.close()
        if self._socket_dir is not None:
            shutil.rmtree(self._socket_dir, ignore_errors=True)


def output_callback(stdout, stderr, offsets):
    """Validate byte positions and deliver to synchronous or asynchronous sinks."""
    streams = {"out": stdout, "err": stderr}

    async def on_chunk(kind, offset, data):
        previous = offsets[kind]
        if previous is not None and offset != previous:
            raise ValueError("Supervisor FOLLOW offset mismatch")
        offsets[kind] = offset + len(data)
        sink = streams[kind]
        result = sink.write(data)
        if inspect.isawaitable(result):
            await result
        sink.flush()

    return on_chunk


async def follow_socket(path, offsets, on_chunk, *, connection=None):
    """Read an initial inherited connection or request FOLLOW on a named socket."""
    try:
        reader, writer = await asyncio.open_unix_connection(path, sock=connection)
    except (FileNotFoundError, ConnectionRefusedError, ConnectionResetError,
            ConnectionAbortedError, BrokenPipeError) as exc:
        raise SupervisorConnectionError(str(exc)) from exc
    try:
        if connection is None:
            try:
                writer.write(json.dumps({"command": "follow", "offsets": offsets}).encode() + b"\n")
                await writer.drain()
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError) as exc:
                raise SupervisorConnectionError(str(exc)) from exc
        async for kind, offset, data in receive(reader):
            if kind == "terminal":
                return data
            result = on_chunk(kind, offset, data)
            if inspect.isawaitable(result):
                await result
        raise ConnectionError("Supervisor ended without terminal outcome")
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            # Cleanup must preserve a received terminal result or the original
            # protocol/callback failure when the supervisor closes its socket.
            pass
