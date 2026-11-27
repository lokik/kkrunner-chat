"""Installed supervisor target operations."""

from __future__ import annotations

import asyncio
import inspect
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
from kkrunner.backends.implementations.supervised import mailbox
from kkrunner.backends.implementations.supervised.streams import (
    SupervisorClosedBeforeFollow,
    SupervisorConnectionError,
    follow_socket,
    output_callback,
)
from kkrunner.backends.implementations.supervised.worker import _SPEC_ENV, _START_FD_ENV
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

    name = "kkrunner"
    resolves_mpi_launcher = True
    execution_mode = "managed"
    default_detached = True
    _supports = frozenset({
        BackendCapability.REATTACH, BackendCapability.STATUS, BackendCapability.RECOVER,
        BackendCapability.INDEPENDENT_OUTCOME,
        BackendCapability.CANCEL, BackendCapability.SUBMIT_AND_FOLLOW, BackendCapability.ACKNOWLEDGE,
        BackendCapability.FLEXIBLE_DETACHMENT,
    })

    @staticmethod
    def acknowledge_submission(run_id: str) -> AcknowledgeReply:
        return {"acknowledged": mailbox.acknowledge_submission(run_id)}

    @staticmethod
    def submit(request: ExecutionRequest) -> SubmissionReply:
        """Launch a detached calculation without an initial observer."""
        return _submit(request)

    @staticmethod
    async def submit_and_follow(request, *, stdout, stderr, started, output=True, control=None):
        """Launch with an inherited FOLLOW connection; no later attach is needed."""
        try:
            client, supervisor = socket.socketpair()
        except OSError as exc:
            return dict(status_reply(RunState.FAILED, failure_phase=FailurePhase.SUBMISSION,
                                     error=str(exc)), remote_id=None)
        with client, supervisor:
            try:
                # The launch handshake is bounded by the lifetime of the child
                # pipe, not by a deadline. The supervisor already owns a socket
                # endpoint before the program can begin.
                reply = _submit(request, initial_follower=supervisor, initial_output=output)
            except SubmissionNotStarted as exc:
                return dict(status_reply(RunState.FAILED, failure_phase=FailurePhase.SUBMISSION,
                                         error=str(exc)), remote_id=None)
            notification = started(reply)
            if inspect.isawaitable(notification):
                await notification
            offsets = {"out": 0, "err": 0}
            live_only = tuple(kind for kind, key in (("out", "output_file"), ("err", "error_file"))
                              if reply.get(key) is None) if control is not None else ()
            return await follow_socket(
                None, offsets, output_callback(stdout, stderr, offsets, live_only=live_only),
                connection=client, output=output, control=control,
            )

    @staticmethod
    def status(remote_id: str) -> StatusReply:
        handle = decode_handle(remote_id)
        if handle["kind"] != "kkrunner":
            raise ValueError("Not a KKRunner supervisor job reference")
        outcome = _process_outcome(handle["receipt_id"])
        if outcome is None:
            active = Path(handle["socket_path"]).with_name("active")
            try:
                state = RunState(active.read_text())
            except FileNotFoundError:
                state = None
            if state is not None and ProcessIdentity.from_dict(handle["identity"]).is_running():
                return status_reply(state)
            # Completion may have been published while checking liveness.
            outcome = _process_outcome(handle["receipt_id"])
            if outcome is None:
                return status_reply(RunState.UNKNOWN)
        state, return_code, failure_phase, error = outcome
        return status_reply(state, return_code, failure_phase=failure_phase, error=error)

    @staticmethod
    def cancel(remote_id: str, timeout: float = 1.0) -> StatusReply:
        handle = decode_handle(remote_id)
        if handle["kind"] != "kkrunner":
            raise ValueError("Not a KKRunner supervisor job reference")
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
        if handle["kind"] != "kkrunner":
            raise ValueError("Not a KKRunner supervisor job reference")
        # Acknowledgement can race with the worker's retirement after publication.
        if _process_outcome(handle["receipt_id"]) is None:
            return {"acknowledged": False}
        Path(handle["socket_path"]).with_name("active").unlink(missing_ok=True)
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
        output=True,
        control=None,
    ):
        """Wait for a supervisor; request output only when explicitly selected."""
        handle = decode_handle(remote_id)
        offsets = {"out": stdout_offset, "err": stderr_offset}
        paths = {
            "out": stdout_path,
            "err": stderr_path,
        }

        if handle["kind"] != "kkrunner":
            raise ValueError("Unknown execution backend")

        path = handle.get("socket_path")
        if path is None:
            raise RuntimeError("Backend handle contains no follow socket")

        on_chunk = output_callback(
            stdout, stderr, offsets,
            live_only=tuple(kind for kind, path in paths.items() if path is None) if control is not None else (),
        )

        requested = dict(offsets)
        if stdout_path is not None and stdout_path == stderr_path:
            requested.pop("err")
        try:
            return await follow_socket(path, requested, on_chunk, output=output, control=control)
        except (SupervisorClosedBeforeFollow, SupervisorConnectionError) as exc:
            # A completed supervisor has removed its transient socket. Replay
            # declared spools once, without the historical polling lifecycle.
            facts = SupervisedBackend.status(remote_id)
            if not RunState(facts["state"]).is_terminal:
                raise
            if not output:
                return facts
            # A launch failure has no calculation output to recover.
            if (facts["state"] == RunState.FAILED.value
                    and facts.get("failure_phase") == FailurePhase.LAUNCH.value
                    and all(offset is None or (type(offset) is int and offset == 0)
                            for offset in offsets.values())):
                return facts
            # An empty greeting can also mean the server rejected the request.
            # Only recover a valid saved-stream cursor in that case.
            if isinstance(exc, SupervisorClosedBeforeFollow):
                if not any(path is not None for path in paths.values()):
                    raise
                if any(offset is not None and (type(offset) is not int or offset < 0)
                       for offset in offsets.values()):
                    raise
            # A numeric cursor requests exact delivery. A disconnected live-only
            # stream cannot be reconstructed, even if the other channel has a spool.
            if any(paths[kind] is None and offset is not None for kind, offset in requested.items()):
                raise ValueError("Live-only output cannot be recovered after observation was lost")
            if not any(path is not None for path in paths.values()):
                raise ValueError("Output was live-only and cannot be reattached")
            from kkrunner.support.growing_file_tail import controlled_named_files

            # A completed named spool must exist; unlike a growing file it
            # cannot still be awaiting creation by the calculation.
            for path in set(paths.values()) - {None}:
                os.stat(path)
            return await controlled_named_files(
                paths, {"out": stdout, "err": stderr}, offsets, lambda: facts,
                poll_interval, output=output, control=control,
            )

    @staticmethod
    async def stream(job_ref: str, **options):
        import sys
        return await SupervisedBackend.follow(job_ref, sys.stdout.buffer, sys.stderr.buffer, **options)

def _submit(request: ExecutionRequest, *, initial_follower=None, initial_output=True) -> SubmissionReply:
    """Submit one process job, preserving the atomic release barrier."""
    if os.name != "posix":
        raise SubmissionNotStarted("Managed local execution requires POSIX process groups and Unix sockets")
    from kkrunner.backends.support.process import prepare_mpi

    # Everything here happens before launching a supervisor. A failed local
    # filesystem setup proves that submission never started, unlike a lost
    # reply after Popen.
    try:
        SupervisedBackend.validate_options(**dict(request.get("backend_options") or {}))
        request = normalize_request(request)
        if not request["detached"] and initial_follower is None:
            raise ValueError("Attached supervisor requires an initial observer")
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

    submission_key = request.get("submission_key")
    run_id = str(submission_key or uuid4().hex)
    receipt_id = uuid4().hex
    socket_dir = None
    process = None
    committed = False
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
            "detached": request["detached"],
        }
        if initial_follower is not None:
            worker_spec["initial_follower_fd"] = initial_follower.fileno()
            worker_spec["initial_output"] = initial_output
        env = os.environ.copy()
        env["KKRunner_RUN_ID"] = run_id

        start_fd, release_fd = os.pipe()
        socket_read_fd, socket_write_fd = os.pipe()
        worker_spec["socket_fd"] = socket_write_fd
        env[_SPEC_ENV] = json.dumps(worker_spec, separators=(",", ":"))
        env[_START_FD_ENV] = str(start_fd)
        try:
            process = subprocess.Popen(
                [sys.executable, "-m", "kkrunner.backends.implementations.supervised.worker"],
                cwd=directory,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                env=env,
                start_new_session=True,
                close_fds=True,
                pass_fds=(start_fd, socket_write_fd) + (() if initial_follower is None else (initial_follower.fileno(),)),
            )
        finally:
            # Close the inherited descriptors in the launching process.
            if initial_follower is not None:
                initial_follower.close()
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
                "kind": "kkrunner",
                "identity": identity.to_dict(),
                "receipt_id": receipt_id,
                "socket_path": socket_path,
            }
        )

        # Only a caller with a submission key can recover a lost acceptance
        # reply. The backend handle and outcome receipt do not depend on it.
        if request.get("detached") and submission_key is not None:
            mailbox.publish_submission(run_id, {
                "remote_id": remote_id,
                "directory": os.fspath(directory),
                "output_file": output_path,
                "error_file": error_path,
            })
            committed = True

        os.write(release_fd, b"1")
        committed = True
        with os.fdopen(socket_read_fd, "rb", closefd=True) as ready:
            socket_read_fd = None
            observed_path = ready.readline().decode("utf8").rstrip("\n")
        if observed_path != socket_path:
            raise RuntimeError("Remote supervisor did not create its follow socket")
    except BaseException as exc:
        if not committed and process is not None:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        if not committed and socket_dir is not None:
            shutil.rmtree(socket_dir, ignore_errors=True)
        if isinstance(exc, OSError) and not committed:
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
