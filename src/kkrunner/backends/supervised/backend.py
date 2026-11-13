"""Installed supervisor target operations."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import warnings
from pathlib import Path
from uuid import uuid4

from kkrunner.backends.contracts import (
    Backend,
    BackendCapability,
    ManagedOperations,
    SubmissionNotStarted,
)
from kkrunner.backends.supervised import mailbox
from kkrunner.backends.supervised.streams import (
    SupervisorClosedBeforeFollow,
    SupervisorConnectionError,
    follow_socket,
)
from kkrunner.backends.supervised.worker import _SPEC_ENV, _START_FD_ENV
from kkrunner.backends.support.class_helper import imported_ssh_helper
from kkrunner.backends.support.messages import (
    AcknowledgeReply,
    ExecutionRequest,
    RecoveryReply,
    RecoveryRequest,
    StatusReply,
    SubmissionReply,
    normalize_request,
    status_reply,
)
from kkrunner.execution import FailurePhase, MpiNotFoundWarning, RunState
from kkrunner.files.availability import prepare_target_outputs
from kkrunner.remote.handles import decode_handle, encode_handle
from kkrunner.support.process_identity import ProcessIdentity

from .support import _process_outcome


class SupervisedBackend(Backend, ManagedOperations):
    """Native target operations, independent of endpoints and transport."""

    name = "process"
    resolves_mpi_launcher = True
    SshHelper = imported_ssh_helper
    execution_mode = "managed"
    default_detached = True
    def __init__(self, **options):
        super().__init__(**options)
        self.supports = frozenset({BackendCapability.REATTACH, BackendCapability.STATUS, BackendCapability.RECOVER, BackendCapability.CANCEL})

    @staticmethod
    def acknowledge_submission(run_id: str) -> AcknowledgeReply:
        return {"acknowledged": mailbox.acknowledge_submission(run_id)}

    @staticmethod
    def submit(request: ExecutionRequest) -> SubmissionReply:
        """Submit one process job, preserving the atomic release barrier."""
        if os.name != "posix":
            raise SubmissionNotStarted("Managed local execution requires POSIX process groups and Unix sockets")
        from kkrunner.backends.support.process import prepare_mpi

        # Everything here happens before launching a supervisor. A failed local
        # filesystem setup proves that submission never started, unlike a lost
        # reply after Popen.
        try:
            SupervisedBackend(**dict(request.get("backend_options") or {}))
            request = normalize_request(request)
            prepared = prepare_mpi(request)
            if prepared["warning"] is not None:
                warnings.warn(prepared["warning"], MpiNotFoundWarning, stacklevel=2)
            execution = prepared["request"]
            directory = Path(request["directory"])
            directory.mkdir(parents=True, exist_ok=True)

            output_path = request["output_file"]
            error_path = request["error_file"]
            for path in (output_path, error_path):
                if path is not None:
                    Path(path).parent.mkdir(parents=True, exist_ok=True)
            prepare_target_outputs(request)
        except Exception as exc:
            # Pre-launch validation and filesystem failures are known non-submissions.
            raise SubmissionNotStarted(str(exc)) from exc

        run_id = str(request.get("submission_key") or uuid4().hex)
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
                "argv": execution["argv"],
                "directory": os.fspath(directory),
                "stdin": execution["stdin"],
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
                    [sys.executable, "-m", "kkrunner.backends.supervised.worker"],
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
                mailbox.publish_submission(run_id, {
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
            for cleanup_fd in (start_fd, release_fd, socket_read_fd, socket_write_fd):
                if cleanup_fd is not None:
                    os.close(cleanup_fd)

        reply: SubmissionReply = {
            **status_reply(RunState.RUNNING),
            "remote_id": remote_id,
            "directory": os.fspath(directory),
            "input_file": request["input_file"],
            "output_file": output_path,
            "error_file": error_path,
        }
        return reply

    @staticmethod
    def status(remote_id: str) -> StatusReply:
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

    @staticmethod
    def cancel(remote_id: str, timeout: float = 1.0) -> StatusReply:
        handle = decode_handle(remote_id)
        if handle["kind"] != "process":
            raise ValueError("Not a process job reference")
        outcome = _process_outcome(handle["receipt_id"])
        if outcome is not None:
            return SupervisedBackend.status(remote_id)
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
                conn.settimeout(timeout)
                conn.connect(handle["socket_path"])
                conn.sendall(b'{"command":"cancel"}\n')
                reply = conn.recv(256)
                if b'"accepted":true' not in reply:
                    raise RuntimeError("Supervisor rejected cancellation")
        except (FileNotFoundError, ConnectionRefusedError):
            return SupervisedBackend.status(remote_id)
        return SupervisedBackend.status(remote_id)

    @staticmethod
    def acknowledge(remote_id: str) -> AcknowledgeReply:
        handle = decode_handle(remote_id)
        if handle["kind"] != "process":
            raise ValueError("Not a process job reference")
        return {"acknowledged": mailbox.acknowledge_outcome(handle["receipt_id"])}

    @staticmethod
    def recover(
        run_id: str, request: RecoveryRequest, *, submitted_at: str | None = None,
    ) -> RecoveryReply:
        reply = mailbox.recover_submission(run_id)
        if reply is None:
            return None
        return {
            **SupervisedBackend.status(reply["remote_id"]),
            "remote_id": reply["remote_id"], "directory": request["directory"],
            "output_file": reply.get("output_file"), "error_file": reply.get("error_file"),
        }

    @staticmethod
    async def follow(
        remote_id,
        stdout,
        stderr,
        *,
        stdout_offset=0,
        stderr_offset=0,
        poll_interval=0.1,
        stdout_path=None,
        stderr_path=None,
    ):
        """Observe one supervisor; cancellation closes only the follower socket."""
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

        requested = dict(offsets)
        if stdout_path is not None and stdout_path == stderr_path:
            requested.pop("err")
        try:
            return await follow_socket(path, requested, on_chunk)
        except SupervisorClosedBeforeFollow:
            # A failed launch can finish after connect() but before the server
            # handles FOLLOW. No protocol bytes or process output were received.
            # Only its durable launch-failure receipt proves this harmless race;
            # partial magic, truncated frames and other states remain errors.
            if any(offset is not None and (type(offset) is not int or offset != 0) for offset in offsets.values()):
                raise
            facts = SupervisedBackend.status(remote_id)
            if facts["state"] != RunState.FAILED.value or facts.get("failure_phase") != FailurePhase.LAUNCH.value:
                raise
            return facts
        except SupervisorConnectionError:
            # A completed supervisor has removed its transient socket. Replay
            # declared spools once, without the historical polling lifecycle.
            facts = SupervisedBackend.status(remote_id)
            if not RunState(facts["state"]).is_terminal:
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
                        await asyncio.sleep(0)
            return facts

    @staticmethod
    async def stream(job_ref: str, **options):
        import sys
        return await SupervisedBackend.follow(job_ref, sys.stdout.buffer, sys.stderr.buffer, **options)

    @staticmethod
    async def follow_framed(job_ref: str, **options):
        from kkrunner.remote.stream import follow_framed
        return await follow_framed(SupervisedBackend.follow, job_ref, **options)
