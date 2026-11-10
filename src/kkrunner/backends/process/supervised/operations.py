"""Process backend: supervised submission, job identity and observation.

The submission receipt and supervisor are backend-owned.  Local managed
execution calls these operations directly; the remote agent only dispatches.
"""

from __future__ import annotations

import asyncio
import hashlib
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

from kkrunner.backends.handles import _decode_handle, _encode_handle
from kkrunner.backends.replies import _status_reply
from kkrunner.backends.specs import _request_spec
from kkrunner.backends.process.supervised.mailbox import _state_root, acknowledge_outcome, read_outcome
from kkrunner.backends.process.supervised.worker import _SPEC_ENV, _START_FD_ENV
from kkrunner.files.availability import clear_declared_outputs, validate_output_collisions
from kkrunner.runs.base import FailurePhase, RunState, SubmissionNotStarted
from kkrunner.support.durability import durable_mkdir, durable_write_json, fsync_directory
from kkrunner.support.process_identity import ProcessIdentity

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

def _publish_submission(run_id, reply):
    """Temporary identity handoff, never a target-side run lifecycle record."""
    path = _submission_ticket(run_id)
    durable_mkdir(path.parent, parents=True, exist_ok=True, mode=0o700)
    durable_write_json(path, reply, mode=0o600)

def _recover_submission(run_id):
    try:
        return json.loads(_submission_ticket(run_id).read_text())
    except FileNotFoundError:
        return None

def acknowledge_submission(run_id):
    path = _submission_ticket(run_id)
    try:
        path.unlink()
    except FileNotFoundError:
        return {"acknowledged": False}
    fsync_directory(path.parent)
    return {"acknowledged": True}

def _absolute(directory, value):
    if value is None:
        return None
    path = Path(value)
    if not path.is_absolute():
        path = Path(directory) / path
    return os.fspath(path)

def _clean_request_outputs(spec, directory, output_path=None, error_path=None):
    """Clear prior outputs while protecting inputs and caches in the parsed spec."""
    declared = tuple(
        _absolute(directory, path) for path in (*spec.output_files, output_path, error_path) if path is not None
    )
    protected = tuple(
        _absolute(directory, path)
        for path in (spec.input_file, spec.stdin, *spec.input_files, *spec.cache_files)
        if path is not None
    )
    validate_output_collisions(declared, protected=protected)
    clear_declared_outputs(declared, protected=protected)

def _local_submit(request):
    if os.name != "posix":
        raise SubmissionNotStarted("Managed local execution requires POSIX process groups and Unix sockets")
    from kkrunner.backends.process.primitives import materialize_local_spec

    # Everything here happens before launching a supervisor. A failed local
    # filesystem setup proves that submission never started, unlike a lost
    # reply after Popen.
    try:
        directory = Path(request["directory"]).expanduser().resolve()
        directory.mkdir(parents=True, exist_ok=True)
        original_spec = _request_spec(dict(request, directory=os.fspath(directory)))
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
                [sys.executable, "-m", "kkrunner.backends.process.supervised.worker"],
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
        remote_id = _encode_handle(
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
            _publish_submission(run_id, {
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
    data = read_outcome(receipt_id)
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

def submit_remote(request):
    """Submit one process job, preserving the atomic release barrier."""
    return _local_submit(request)


def status_remote(remote_id):
    handle = _decode_handle(remote_id)
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
    return _status_reply(state, return_code, failure_phase=failure_phase, error=error)


def cancel_remote(remote_id, timeout=1.0):
    handle = _decode_handle(remote_id)
    if handle["kind"] != "process":
        raise ValueError("Not a process job reference")
    outcome = _process_outcome(handle["receipt_id"])
    if outcome is not None:
        return status_remote(remote_id)
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
            conn.settimeout(timeout)
            conn.connect(handle["socket_path"])
            conn.sendall(b'{"command":"cancel"}\n')
            reply = conn.recv(256)
            if b'"accepted":true' not in reply:
                raise RuntimeError("Supervisor rejected cancellation")
    except (FileNotFoundError, ConnectionRefusedError):
        return status_remote(remote_id)
    return status_remote(remote_id)


def acknowledge_remote(remote_id):
    handle = _decode_handle(remote_id)
    if handle["kind"] != "process":
        raise ValueError("Not a process job reference")
    return {"acknowledged": acknowledge_outcome(handle["receipt_id"])}


def recover_remote(run_id, request, *, submitted_at=None):
    reply = _recover_submission(run_id)
    if reply is None:
        return None
    return dict(reply, directory=request["directory"], **status_remote(reply["remote_id"]))


def follow_remote(
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
    handle = _decode_handle(remote_id)
    offsets = {"out": stdout_offset, "err": stderr_offset}
    streams = {"out": stdout, "err": stderr}
    paths = {
        "out": stdout_path,
        "err": stderr_path,
    }

    if handle["kind"] != "process":
        raise ValueError("Unknown execution backend")
    from kkrunner.backends.process.supervised.streams import SupervisorClosedBeforeFollow, follow_socket

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
        status = status_remote(remote_id)
        if status["state"] != RunState.FAILED.value or status.get("failure_phase") != FailurePhase.LAUNCH.value:
            raise
        return status
    except (FileNotFoundError, ConnectionRefusedError):
        # A completed supervisor has removed its transient socket. Replay
        # declared spools once, without the historical polling lifecycle.
        status = status_remote(remote_id)
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

