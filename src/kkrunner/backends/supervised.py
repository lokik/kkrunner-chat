"""Supervised functional backend with durable receipts, socket streams and target worker."""
from __future__ import annotations

from kkrunner.backends.contracts import RecoverableBackend
from kkrunner.runs.base import ExecutionSpec

import hashlib
import json
import os
import re
from pathlib import Path
from uuid import uuid4

from kkrunner.runs.base import FailurePhase, RunState
from kkrunner.support.durability import durable_mkdir, durable_write_json, fsync_directory

_RECEIPT = re.compile(r"[0-9a-f]{32}\Z")


def _state_root():
    """Common state directory for short-lived submissions and outcomes."""
    root = os.environ.get("KKRunner_REMOTE_STATE_DIR")
    if root:
        return Path(root).expanduser()
    return Path(os.environ.get("XDG_STATE_HOME", "~/.local/state")).expanduser() / "kkrunner"


def _path(receipt_id):
    if not isinstance(receipt_id, str) or not _RECEIPT.fullmatch(receipt_id):
        raise ValueError("Invalid remote receipt ID")
    return _state_root() / "remote-outcomes" / (receipt_id + ".json")


def _submission_ticket(run_id):
    """A stable, filename-safe ticket for any valid client Run identity.

    RunRecord permits more than UUID-hex IDs. Hashing the whole identity keeps
    the receipt namespace independent of path separators and user-facing IDs.
    """
    if (
        not isinstance(run_id, str) or not run_id or run_id in (".", "..")
        or any(char in run_id for char in ("/", "\\", "\x00"))
    ):
        raise ValueError("Invalid persistent run id")
    name = hashlib.sha256(run_id.encode("utf8")).hexdigest()
    return _state_root() / "remote-submissions" / (name + ".json")


def _mailbox_publish_submission(run_id, reply):
    """Temporary identity handoff, never a target-side run lifecycle record."""
    path = _submission_ticket(run_id)
    durable_mkdir(path.parent, parents=True, exist_ok=True, mode=0o700)
    durable_write_json(path, reply, mode=0o600)


def _mailbox_recover_submission(run_id):
    try:
        return json.loads(_submission_ticket(run_id).read_text())
    except FileNotFoundError:
        return None


def _mailbox_acknowledge_submission(run_id):
    path = _submission_ticket(run_id)
    try:
        path.unlink()
    except FileNotFoundError:
        return False
    fsync_directory(path.parent)
    return True


def _mailbox_publish_outcome(receipt_id, state, return_code, failure_phase=None, *, error=None):
    """Publish once: cancellation must not overwrite natural completion."""
    state = RunState(state)
    if not state.is_terminal:
        raise ValueError("Backend receipt requires a terminal state")
    path = _path(receipt_id)
    durable_mkdir(path.parent, parents=True, exist_ok=True, mode=0o700)
    temporary = path.parent / ("." + receipt_id + "." + uuid4().hex + ".prepared")
    try:
        durable_write_json(
            temporary,
            {
                "state": state.value,
                "return_code": None if return_code is None else int(return_code),
                "failure_phase": None if failure_phase is None else FailurePhase(failure_phase).value,
                "error": error,
            },
            separators=(",", ":"),
            mode=0o600,
        )
        try:
            os.link(temporary, path)
        except FileExistsError:
            return False
        fsync_directory(path.parent)
        return True
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        fsync_directory(path.parent)


def _mailbox_read_outcome(receipt_id):
    """Read non-destructively so interrupted clients can retry."""
    try:
        result = json.loads(_path(receipt_id).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    try:
        if not RunState(result["state"]).is_terminal:
            raise ValueError("Receipt is not terminal")
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("Invalid terminal receipt") from exc
    return result


def _mailbox_acknowledge_outcome(receipt_id):
    """Delete only after the client has durably committed its own run state."""
    path = _path(receipt_id)
    try:
        path.unlink()
    except FileNotFoundError:
        return False
    fsync_directory(path.parent)
    return True


import asyncio
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


async def receive(reader):
    """Yield offset-bearing stream frames followed by terminal facts."""
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
        self.connected = asyncio.Event()
        self.disconnected = asyncio.Event()
        self.cancelled = asyncio.Event()
        self._session_follower = None
        self._spools = {}
        self._sizes = {kind: 0 for kind in self.paths}
        self._followers = set()
        self._server = None
        self._terminal = None
        self._socket_dir = socket_dir
        self.socket_path = None

    async def start(self):
        try:
            for kind, path in self.paths.items():
                if path is not None:
                    Path(path).parent.mkdir(parents=True, exist_ok=True)
                    # Truncate the declared stream at launch. Keep the actual file
                    # available during execution and after failure or cancellation.
                    self._spools[kind] = open(path, "wb", buffering=0)
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

    def publish(self, kind, data):
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
            spool.write(data)
            spool.flush()
        self._sizes[kind] += len(data)
        wire = frame(kind, offset, data)
        for follower in tuple(self._followers):
            # A spooled channel can catch up from disk. A live-only channel
            # cannot: forward it even while another channel is replaying.
            if follower.replay and self.paths[kind] is not None:
                follower.wake.set()
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
            # A client that stopped reading must not prevent the detached
            # supervisor from exiting. Already published bytes remain in the
            # named spool and can be recovered on a later attach.
            if follower.writer.transport.get_write_buffer_size() >= _HIGH:
                follower.writer.transport.abort()
            follower.wake.set()

    async def _connect(self, reader, writer):
        follower = None
        disconnect_watch = None
        try:
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
            offsets = request.get("offsets")
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
                if self.paths[kind] is None:
                    if offset != 0:
                        raise ValueError("Live-only output cannot be replayed")
                    offsets[kind] = self._sizes[kind]
            writer.transport.set_write_buffer_limits(high=_HIGH, low=_LOW)
            follower = _Follower(writer, offsets.copy())
            if not self.detached:
                if self._session_follower is not None:
                    raise ValueError("Attached execution already has a client")
                self._session_follower = follower
                self.connected.set()
            self._followers.add(follower)
            # Detached clients can disconnect while a calculation is silent.
            # Observe EOF for *every* follower so an idle socket does not
            # remain in _followers until the next output or terminal event.
            disconnect_watch = asyncio.create_task(reader.read(1))

            def on_disconnect(task):
                if task.cancelled():
                    return
                if not self.detached:
                    self.disconnected.set()
                follower.wake.set()

            disconnect_watch.add_done_callback(on_disconnect)
            writer.write(MAGIC)
            while True:
                if disconnect_watch is not None and disconnect_watch.done():
                    return
                if follower.replay:
                    await writer.drain()
                    for kind, path in self.paths.items():
                        if path is None:
                            if follower.offsets[kind] != self._sizes[kind]:
                                raise ConnectionError("Live-only follower missed output")
                            continue
                        offset = follower.offsets[kind]
                        if offset < self._sizes[kind]:
                            with open(path, "rb", buffering=0) as spool:
                                spool.seek(offset)
                                data = spool.read(min(_CHUNK, self._sizes[kind] - offset))
                            if not data:
                                raise RuntimeError("Supervisor spool truncated")
                            writer.write(frame(kind, offset, data))
                            follower.offsets[kind] += len(data)
                            if self._terminal is not None and writer.transport.get_write_buffer_size() >= _HIGH:
                                writer.transport.abort()
                                return
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
            if follower is not None:
                self._followers.discard(follower)
                if follower is self._session_follower and self._terminal is None:
                    self.disconnected.set()
                follower.done.set()
            if disconnect_watch is not None:
                disconnect_watch.cancel()
            writer.close()
            try:
                await writer.wait_closed()
            except (BrokenPipeError, ConnectionResetError):
                pass

    async def finish(self):
        """Let active clients drain the replay before removing the socket."""
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        if self._followers:
            # Terminal publication must not be held hostage indefinitely by a
            # connected client which reads too slowly to complete its replay.
            pending = tuple(self._followers)
            finished, stalled = await asyncio.wait(
                [asyncio.create_task(f.done.wait()) for f in pending],
                timeout=2.0,
            )
            if stalled:
                for follower in pending:
                    if not follower.done.is_set():
                        follower.writer.transport.abort()
                await asyncio.wait(stalled, timeout=1.0)
            for task in stalled:
                task.cancel()
        for spool in self._spools.values():
            spool.close()
        if self._socket_dir is not None:
            shutil.rmtree(self._socket_dir, ignore_errors=True)


async def follow_socket(path, offsets, on_chunk):
    reader, writer = await asyncio.open_unix_connection(path)
    try:
        writer.write(json.dumps({"command": "follow", "offsets": offsets}).encode() + b"\n")
        await writer.drain()
        async for kind, offset, data in receive(reader):
            if kind == "terminal":
                return data
            on_chunk(kind, offset, data)
        raise ConnectionError("Supervisor ended without terminal outcome")
    finally:
        writer.close()
        await writer.wait_closed()


import asyncio
import json
import os
import signal
import subprocess
import sys

from kkrunner.backends.support import pump_stream, stop_local_process
from kkrunner.runs.base import FailurePhase, RunState

_SPEC_ENV = "KKRunner_REMOTE_PROCESS_SPEC"
_START_FD_ENV = "KKRunner_REMOTE_PROCESS_START_FD"
_LAUNCH_FAILURE_RETURN_CODE = 127


def _exception_message(exc):
    """Include explicitly chained failures in the persisted run outcome."""
    message = str(exc)
    if exc.__cause__ is not None:
        message += f"; secondary error: {exc.__cause__}"
    return message


def _wait_for_parent_release():
    value = os.environ.pop(_START_FD_ENV, None)
    if value is None:
        return
    fd = int(value)
    try:
        if os.read(fd, 1) != b"1":
            raise RuntimeError("Remote supervisor launch was not released by parent")
    finally:
        os.close(fd)


async def _run(spec):
    streams = SupervisorStreams(
        spec.get("output_file"),
        spec.get("error_file"),
        detached=spec.get("detached", True),
        socket_dir=spec.get("socket_dir"),
    )
    state, phase, return_code, message = RunState.FAILED, FailurePhase.LAUNCH, 127, None
    opened = []
    process = None
    streams_started = False
    try:
        try:
            await streams.start()
            streams_started = True
            os.write(int(spec["socket_fd"]), (streams.socket_path + "\n").encode())
        finally:
            # Closing the handshake FD also wakes a waiting parent when the
            # supervisor cannot create its FOLLOW socket.
            os.close(int(spec["socket_fd"]))
        if not streams.detached:
            try:
                await asyncio.wait_for(streams.connected.wait(), timeout=30)
            except asyncio.TimeoutError:
                message = "Client never connected to attached supervisor"
                return return_code
        if streams.cancelled.is_set() or (not streams.detached and streams.disconnected.is_set()):
            state, phase, return_code = RunState.CANCELLED, None, -signal.SIGTERM
            return return_code
        stdin = subprocess.DEVNULL
        if spec.get("stdin"):
            stdin = open(spec["stdin"], "rb")
            opened.append(stdin)
        process = await asyncio.create_subprocess_exec(
            *spec["argv"],
            cwd=spec["directory"],
            stdin=stdin,
            stdout=asyncio.subprocess.PIPE,
            stderr=(asyncio.subprocess.STDOUT if streams.merged else asyncio.subprocess.PIPE),
            start_new_session=True,
        )
        state, phase, return_code = RunState.RUNNING, None, None
        waiting = asyncio.create_task(process.wait())
        pumps = [asyncio.create_task(pump_stream(process.stdout, lambda data: streams.publish("out", data)))]
        if not streams.merged:
            pumps.append(asyncio.create_task(pump_stream(process.stderr, lambda data: streams.publish("err", data))))
        # A failing spool must terminate the process rather than deadlock a pipe.
        stop = asyncio.create_task(streams.cancelled.wait())
        lost = asyncio.create_task(streams.disconnected.wait()) if not streams.detached else None
        try:
            while True:
                watches = [waiting, stop, *(p for p in pumps if not p.done())]
                if lost is not None:
                    watches.append(lost)
                done, _ = await asyncio.wait(watches, return_when=asyncio.FIRST_COMPLETED)
                failed = next(
                    (t.exception() for t in pumps if t.done() and not t.cancelled() and t.exception() is not None), None
                )
                if failed is not None:
                    message = f"Failed to write process output: {failed}"
                    state, phase = RunState.FAILED, FailurePhase.EXECUTION
                    await stop_local_process(process, process_group=True, timeout=1.0, waiting=waiting)
                    break
                if waiting.done():
                    break
                if stop in done or (lost is not None and lost in done):
                    state, phase = RunState.CANCELLED, None
                    await stop_local_process(process, process_group=True, timeout=1.0, waiting=waiting)
                    break
            return_code = await waiting
            # Inspect all pump outcomes without allowing their exceptions to
            # replace the diagnosed output-write failure and its context.
            pump_results = await asyncio.gather(*pumps, return_exceptions=True)
            pump_error = next(
                (value for value in pump_results if isinstance(value, BaseException)),
                None,
            )
            if pump_error is not None:
                state, phase = RunState.FAILED, FailurePhase.EXECUTION
                message = f"Failed to write process output: {pump_error}"
            elif state == RunState.RUNNING:
                state = RunState.FINISHED if return_code == 0 else RunState.FAILED
                phase = None if return_code == 0 else FailurePhase.EXECUTION
        finally:
            stop.cancel()
            if lost is not None:
                lost.cancel()
    except OSError as exc:
        message = _exception_message(exc)
        state, phase = RunState.FAILED, (FailurePhase.LAUNCH if process is None else FailurePhase.EXECUTION)
    except Exception as exc:
        message = _exception_message(exc)
        state, phase = RunState.FAILED, (FailurePhase.LAUNCH if process is None else FailurePhase.EXECUTION)
        if process is not None and process.returncode is None:
            await stop_local_process(process, process_group=True, timeout=1.0)
        return_code = process.returncode if process is not None else 127
    finally:
        for f in opened:
            try:
                f.close()
            except OSError as exc:
                # Failure to close stdin must not suppress the primary launch
                # error or prevent the durable terminal outcome from being
                # published. On an otherwise successful run it is a failure.
                if state == RunState.FINISHED:
                    state, phase = RunState.FAILED, FailurePhase.EXECUTION
                    message = f"Failed to close process input: {exc}"
                elif message is None:
                    message = f"Failed to close process input: {exc}"
        if streams_started:
            try:
                streams.sync()
            except OSError as exc:
                state, phase = RunState.FAILED, FailurePhase.ARTIFACTS
                message = f"Failed to sync process output: {exc}"
        outcome = {
            "state": state.value,
            "return_code": return_code,
            "failure_phase": None if phase is None else phase.value,
            "error": message,
        }
        try:
            _mailbox_publish_outcome(
                spec["receipt_id"],
                state,
                return_code,
                phase,
                error=message,
            )
        except OSError as exc:
            # No durable terminal receipt exists. The client must preserve its
            # active run and retry recovery, never commit FINISHED from a socket.
            outcome = {
                "state": RunState.UNKNOWN.value,
                "return_code": None,
                "failure_phase": None,
                "error": f"Unable to persist terminal receipt: {exc}",
            }
        finally:
            # start() already attempted cleanup if startup failed. There is
            # no server to notify in that case; avoid a second failing cleanup.
            if streams_started:
                streams.terminal(outcome)
                await streams.finish()
    return return_code if return_code is not None else 127


def main():
    raw = os.environ.pop(_SPEC_ENV, None)
    if raw is None:
        raise RuntimeError(f"Missing {_SPEC_ENV}")
    spec = json.loads(raw)
    _wait_for_parent_release()
    return asyncio.run(_run(spec))




import asyncio
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
from pathlib import Path
from uuid import uuid4

from kkrunner.remote.handles import decode_handle, encode_handle
from kkrunner.remote.messages import request_to_spec, status_reply
from kkrunner.files.availability import prepare_target_outputs
from kkrunner.runs.base import FailurePhase, RunState, SubmissionNotStarted
from kkrunner.support.process_identity import ProcessIdentity

def _acknowledge_submission_impl(run_id):
    return {"acknowledged": _mailbox_acknowledge_submission(run_id)}


def _absolute(directory, value):
    if value is None:
        return None
    path = Path(value)
    if not path.is_absolute():
        path = Path(directory) / path
    return os.fspath(path)

def _clean_request_outputs(spec, directory, output_path=None, error_path=None):
    """Adapt supervised process declarations to the shared target-side rules."""
    def paths(values):
        return [_absolute(directory, value) for value in values if value is not None]

    prepare_target_outputs({
        "input_file": _absolute(directory, spec.input_file),
        "stdin": _absolute(directory, spec.stdin),
        "input_files": paths(spec.input_files),
        "output_file": output_path,
        "error_file": error_path,
        "client_sync": {
            "output_files": paths(spec.output_files),
            "cache_files": [{"path": path} for path in paths(spec.cache_files)],
        },
    })

def _local_submit(request):
    if os.name != "posix":
        raise SubmissionNotStarted("Managed local execution requires POSIX process groups and Unix sockets")
    from kkrunner.backends.support import materialize_local_spec

    # Everything here happens before launching a supervisor. A failed local
    # filesystem setup proves that submission never started, unlike a lost
    # reply after Popen.
    try:
        directory = Path(request["directory"]).expanduser().resolve()
        directory.mkdir(parents=True, exist_ok=True)
        original_spec = request_to_spec(dict(request, directory=os.fspath(directory)))
        spec = materialize_local_spec(original_spec)

        output_path = _absolute(directory, spec.output_file)
        error_path = _absolute(directory, spec.error_file)
        for path in (output_path, error_path):
            if path is not None:
                Path(path).parent.mkdir(parents=True, exist_ok=True)
        _clean_request_outputs(original_spec, directory, output_path, error_path)
    except (OSError, ValueError) as exc:
        # Pre-launch validation and filesystem failures are known non-submissions.
        raise SubmissionNotStarted(str(exc)) from exc

    run_id = str(request.get("id") or uuid4().hex)
    receipt_id = uuid4().hex
    socket_dir = None
    process = None
    published = False
    start_fd = release_fd = socket_read_fd = socket_write_fd = None
    try:
        # IPC creation belongs inside the launch cleanup boundary.
        socket_dir = tempfile.mkdtemp(prefix="kkrunner-follow-")
        socket_path = os.path.join(socket_dir, "socket")
        worker_spec = {
            "argv": list(spec.argv),
            "directory": os.fspath(directory),
            "stdin": _absolute(directory, spec.stdin),
            "output_file": output_path,
            "error_file": error_path,
            "receipt_id": receipt_id,
            "socket_dir": socket_dir,
            "detached": bool(request.get("detached", True)),
        }
        env = os.environ.copy()
        env["KKRunner_RUN_ID"] = run_id

        start_fd, release_fd = os.pipe()
        socket_read_fd, socket_write_fd = os.pipe()
        worker_spec["socket_fd"] = socket_write_fd
        env[_SPEC_ENV] = json.dumps(worker_spec, separators=(",", ":"))
        env[_START_FD_ENV] = str(start_fd)
        try:
            process = subprocess.Popen(
                [sys.executable, "-m", "kkrunner.backends.supervised"],
                cwd=directory,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                env=env,
                start_new_session=True,
                close_fds=True,
                pass_fds=(start_fd, socket_write_fd),
            )
        finally:
            # Close write ends before awaiting the supervisor handshake.
            if start_fd is not None:
                fd, start_fd = start_fd, None
                os.close(fd)
            if socket_write_fd is not None:
                fd, socket_write_fd = socket_write_fd, None
                os.close(fd)

        identity = ProcessIdentity.read(process.pid)
        if identity is None:
            raise SubmissionNotStarted("Unable to verify the started remote process identity")
        remote_id = encode_handle(
            {
                "kind": "process",
                "identity": identity.to_dict(),
                "receipt_id": receipt_id,
                "socket_path": socket_path,
            }
        )

        # Durable identity precedes releasing the supervisor. Until this
        # receipt is safely stored, the child cannot launch a calculation.
        if request.get("detached"):
            _mailbox_publish_submission(run_id, {
                "remote_id": remote_id,
                "directory": os.fspath(directory),
                "output_file": output_path,
                "error_file": error_path,
            })
            published = True

        os.write(release_fd, b"1")
        with os.fdopen(socket_read_fd, "rb", closefd=True) as ready:
            socket_read_fd = None
            observed_path = ready.readline().decode("utf8").rstrip("\n")
        if observed_path != socket_path:
            raise RuntimeError("Remote supervisor did not create its follow socket")
    except BaseException as exc:
        if not published and process is not None:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        if not published and socket_dir is not None:
            shutil.rmtree(socket_dir, ignore_errors=True)
        if isinstance(exc, OSError) and not published:
            raise SubmissionNotStarted(str(exc)) from exc
        raise
    finally:
        for fd in (start_fd, release_fd, socket_read_fd, socket_write_fd):
            if fd is not None:
                os.close(fd)

    reply = {
        "remote_id": remote_id,
        "state": RunState.RUNNING.value,
        "directory": os.fspath(directory),
        "input_file": _absolute(directory, spec.input_file),
        "output_file": output_path,
        "error_file": error_path,
        "return_code": None,
        "failure_phase": None,
    }
    return reply

def _process_outcome(receipt_id):
    data = _mailbox_read_outcome(receipt_id)
    if data is None:
        return None
    try:
        state = RunState(data["state"])
        return_code = data.get("return_code")
        raw_phase = data.get("failure_phase")
        failure_phase = None if raw_phase is None else FailurePhase(raw_phase)
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError("Invalid remote process outcome artifact") from exc
    return state, return_code, failure_phase, data.get("error")

def _submit_impl(request):
    """Submit one process job, preserving the atomic release barrier."""
    return _local_submit(request)


def _status_impl(remote_id):
    handle = decode_handle(remote_id)
    if handle["kind"] != "process":
        raise ValueError("Not a process job reference")
    outcome = _process_outcome(handle["receipt_id"])
    if outcome is not None:
        state, return_code, failure_phase, error = outcome
    else:
        state = RunState.RUNNING if ProcessIdentity.from_dict(handle["identity"]).is_running() else RunState.UNKNOWN
        return_code = None
        failure_phase = None
        error = None
    return status_reply(state, return_code, failure_phase=failure_phase, error=error)


def _cancel_impl(remote_id, timeout=1.0):
    handle = decode_handle(remote_id)
    if handle["kind"] != "process":
        raise ValueError("Not a process job reference")
    outcome = _process_outcome(handle["receipt_id"])
    if outcome is not None:
        return status(remote_id)
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
            conn.settimeout(timeout)
            conn.connect(handle["socket_path"])
            conn.sendall(b'{"command":"cancel"}\n')
            reply = conn.recv(256)
            if b'"accepted":true' not in reply:
                raise RuntimeError("Supervisor rejected cancellation")
    except (FileNotFoundError, ConnectionRefusedError):
        return status(remote_id)
    return status(remote_id)


def _acknowledge_impl(remote_id):
    handle = decode_handle(remote_id)
    if handle["kind"] != "process":
        raise ValueError("Not a process job reference")
    return {"acknowledged": _mailbox_acknowledge_outcome(handle["receipt_id"])}


def _recover_impl(run_id, request, *, submitted_at=None):
    reply = _mailbox_recover_submission(run_id)
    if reply is None:
        return None
    return dict(reply, directory=request["directory"], **status(reply["remote_id"]))


def _follow_impl(
    remote_id,
    stdout,
    stderr,
    *,
    stdout_offset=0,
    stderr_offset=0,
    poll_interval=0.1,
    stdout_path=None,
    stderr_path=None,
    observation_started=None,
):
    """Follow direct supervisors or scheduler-owned streams until terminal state."""
    handle = decode_handle(remote_id)
    offsets = {"out": stdout_offset, "err": stderr_offset}
    streams = {"out": stdout, "err": stderr}
    paths = {
        "out": stdout_path,
        "err": stderr_path,
    }

    if handle["kind"] != "process":
        raise ValueError("Unknown execution backend")
    
    path = handle.get("socket_path")
    if path is None:
        raise RuntimeError("Backend handle contains no follow socket")

    # Use the same frame decoder for the local socket and the remote CLI.
    # A merged stream has only one logical channel.
    def on_chunk(kind, offset, data):
        previous = offsets[kind]
        if previous is not None and offset != previous and paths[kind] is not None:
            raise ValueError("Supervisor FOLLOW offset mismatch")
        offsets[kind] = offset + len(data)
        streams[kind].write(data)
        streams[kind].flush()

    async def drain_from_socket():
        if observation_started is not None:
            observation_started(asyncio.get_running_loop(), asyncio.current_task())
        requested = dict(offsets)
        if stdout_path is not None and stdout_path == stderr_path:
            requested.pop("err")
        return await follow_socket(path, requested, on_chunk)

    try:
        return asyncio.run(drain_from_socket())
    except SupervisorClosedBeforeFollow:
        # A failed launch can finish after connect() but before the server
        # handles FOLLOW. No protocol bytes or process output were received.
        # Only its durable launch-failure receipt proves this harmless race;
        # partial magic, truncated frames and other states remain errors.
        if any(offset is not None and (type(offset) is not int or offset != 0) for offset in offsets.values()):
            raise
        status = status(remote_id)
        if status["state"] != RunState.FAILED.value or status.get("failure_phase") != FailurePhase.LAUNCH.value:
            raise
        return status
    except (FileNotFoundError, ConnectionRefusedError):
        # A completed supervisor has removed its transient socket. Replay
        # declared spools once, without the historical polling lifecycle.
        status = status(remote_id)
        if not RunState(status["state"]).is_terminal:
            raise
        if not any(path is not None for path in paths.values()):
            raise ValueError("Output was live-only and cannot be reattached")
        seen = set()
        for kind in ("out", "err"):
            path = paths[kind]
            if path is None or path in seen:
                continue
            seen.add(path)
            with open(path, "rb") as source:
                if offsets[kind] is None:
                    source.seek(0, os.SEEK_END)
                else:
                    source.seek(offsets[kind])
                while True:
                    data = source.read(65536)
                    if not data:
                        break
                    streams[kind].write(data)
                    streams[kind].flush()
        return status


class SupervisedProcessBackend(RecoverableBackend):
    """Create recoverable supervisor-owned Jobs; not a direct-process subclass."""

    name = "process"
    supports_result_file_retrieval = False
    supports_tracked_runs = True
    requires_backend_identity = True
    default_persistent = True
    default_detached = True

    def create_job(self, spec: ExecutionSpec, *, submission_key=None, callback=None):
        spec.require_complete()
        from uuid import uuid4
        from kkrunner.client.local_managed import local_managed_job
        return local_managed_job(
            spec, backend="process", submission_key=submission_key or uuid4().hex,
            callback=callback,
        )

    def recover_submission(self, run_id, submission, *, submitted_at=None):
        recover_remote = recover
        reply = recover_remote(
            run_id,
            {"executor": "process", "directory": submission.execution.directory.local},
        )
        from kkrunner.backends.contracts import JobRef, RecoveryResult
        if reply is None:
            return RecoveryResult.indeterminate("No submission receipt was found")
        return RecoveryResult.found(
            JobRef("process", reply["remote_id"]),
            directory=reply.get("directory"),
            output_file=reply.get("output_file"),
            error_file=reply.get("error_file"),
        )

    def attach_job(self, context, *, callback=None, output_listener=None, start=False):
        from kkrunner.client.local_supervised import attach_local
        return attach_local(
            self, context, callback=callback, output_listener=output_listener, start=start,
        )

    def acknowledge_submission(self, run):
        from kkrunner.backends.supervised import acknowledge_submission
        acknowledge_submission(run.execution.submission_key or run.id)

    def acknowledge(self, run):
        acknowledge_remote = acknowledge
        acknowledge_remote(run.execution.backend_id)

    def status(self, *runs):
        from kkrunner.client.local_supervised import local_status
        statuses = []
        for run in runs:
            if run.execution.endpoint.executor != "local-supervised":
                raise ValueError("Run does not belong to the supervised local backend")
            statuses.append(local_status(run.execution.backend_id))
        return tuple(statuses)


def stream(job_ref, **options):
    import sys
    from kkrunner.backends.supervised import follow
    return follow(job_ref, sys.stdout.buffer, sys.stderr.buffer, **options)



# Public functional operations deliberately import their implementation inside
# the call. They carry no backend object, closure, or module-global mutable state.
def submit(request):
    from kkrunner.backends.supervised import _submit_impl
    return _submit_impl(request)


def status(job_ref):
    from kkrunner.backends.supervised import _status_impl
    return _status_impl(job_ref)


def cancel(job_ref):
    from kkrunner.backends.supervised import _cancel_impl
    return _cancel_impl(job_ref)


def recover(submission_key, request, *, submitted_at=None):
    from kkrunner.backends.supervised import _recover_impl
    return _recover_impl(submission_key, request, submitted_at=submitted_at)


def acknowledge(job_ref):
    from kkrunner.backends.supervised import _acknowledge_impl
    return _acknowledge_impl(job_ref)


def acknowledge_submission(submission_key):
    from kkrunner.backends.supervised import _acknowledge_submission_impl
    return _acknowledge_submission_impl(submission_key)


def follow(job_ref, stdout, stderr, **options):
    from kkrunner.backends.supervised import _follow_impl
    return _follow_impl(job_ref, stdout, stderr, **options)



if __name__ == "__main__":
    sys.exit(main())
