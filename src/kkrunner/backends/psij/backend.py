"""Installed PSI/J operations, independent of client FileSession."""

from __future__ import annotations

from uuid import uuid4

from kkrunner.backends.contracts import (
    Backend,
    BackendCapability,
    JobFailurePhase,
    ManagedOperations,
    SubmissionNotStarted,
)
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
from kkrunner.execution import RunState
from kkrunner.remote.handles import decode_handle, encode_handle

from . import slurm
from .submission import prepare_job, prepare_outputs
from .support import (
    _attach_job,
    _create_executor,
    _follow_scheduler,
    job_status,
    psij_state,
    supports_recovery,
)


class PsijBackend(Backend, ManagedOperations):
    """Native target operations, independent of endpoints and transport."""

    name = "psij"
    default_options = (("executor_name", "slurm"), ("url", None))
    SshHelper = imported_ssh_helper
    execution_mode = "managed"
    default_detached = True
    def __init__(self, **options):
        super().__init__(**options)
        executor_name = self.options["executor_name"]
        url = self.options["url"]
        if not isinstance(executor_name, str) or not executor_name.strip():
            raise ValueError("PSI/J executor_name must be a nonempty string")
        if url is not None and (not isinstance(url, str) or not url):
            raise ValueError("PSI/J url must be a nonempty string or None")
        supports = {BackendCapability.REATTACH, BackendCapability.STATUS}
        if supports_recovery(self.options):
            supports.add(BackendCapability.RECOVER)
        self.supports = frozenset(supports)

    @staticmethod
    def submit(request: ExecutionRequest) -> SubmissionReply:
        try:
            options = PsijBackend(**dict(request.get("backend_options") or {})).options
            request = normalize_request(request)
            psij, executor = _create_executor(options)
            job = prepare_job(psij, request, executor_name=options["executor_name"],
                              submission_key=request.get("submission_key") or uuid4().hex)
            prepare_outputs(request)
        except Exception as exc:
            raise SubmissionNotStarted(str(exc)) from exc
        # Crossing this boundary can submit a job even when the call raises.
        # Such errors must never be reported as SubmissionNotStarted.
        executor.submit(job)
        native_id = job.native_id
        if native_id is None:
            raise RuntimeError("PSI/J submission did not return a native job id")
        remote_id = encode_handle(
            {
                "kind": "psij",
                "executor_name": options["executor_name"],
                "url": options["url"],
                "native_id": str(native_id),
            }
        )
        native_status = job.status
        state = psij_state(native_status)
        return {
            "remote_id": remote_id,
            **status_reply(
                state, getattr(native_status, "exit_code", None),
                failure_phase=JobFailurePhase.EXECUTION if state == RunState.FAILED else None,
            ),
            "directory": request["directory"],
            "input_file": request.get("input_file"),
            "output_file": request.get("output_file"),
            "error_file": request.get("error_file"),
        }

    @staticmethod
    def status(remote_id: str) -> StatusReply:
        handle = decode_handle(remote_id)
        if handle["kind"] != "psij":
            raise ValueError("Not a PSI/J job reference")
        facts = job_status(handle, attached=True)
        return status_reply(
            facts.state, facts.return_code,
            failure_phase=facts.failure_phase, error=facts.message,
        )

    @staticmethod
    def cancel(remote_id: str) -> StatusReply:
        handle = decode_handle(remote_id)
        if handle["kind"] != "psij":
            raise ValueError("Not a PSI/J job reference")
        job = _attach_job(handle)
        job.cancel()
        return PsijBackend.status(remote_id)

    @staticmethod
    def acknowledge(remote_id: str) -> AcknowledgeReply:
        return {"acknowledged": False}

    @staticmethod
    def acknowledge_submission(run_id: str) -> AcknowledgeReply:
        return {"acknowledged": False}

    @staticmethod
    def recover(
        run_id: str, request: RecoveryRequest, *, submitted_at: str | None = None,
    ) -> RecoveryReply:
        try:
            options = PsijBackend(**dict(request.get("backend_options") or {})).options
            if not supports_recovery(options):
                raise NotImplementedError("Submission recovery requires a local Slurm executor")
            facts = slurm.recover(run_id, request["directory"], submitted_at=submitted_at)
        except NotImplementedError as exc:
            return {"unsupported": True, "error": str(exc)}
        if facts is None:
            return None
        native_id = facts["native_id"]
        remote_id = encode_handle(
            {
                "kind": "psij",
                "executor_name": options["executor_name"],
                "url": options["url"],
                "native_id": native_id,
            }
        )
        state = facts["state"]
        return {
            "remote_id": remote_id,
            **status_reply(state, facts["return_code"],
                           failure_phase=JobFailurePhase.EXECUTION if state == RunState.FAILED else None),
            "directory": facts["directory"],
            "output_file": facts["stdout"],
            "error_file": facts["stderr"],
        }

    @staticmethod
    async def follow(
        remote_id, stdout, stderr, *, stdout_offset=0, stderr_offset=0,
        poll_interval=0.1, stdout_path=None, stderr_path=None,
    ):
        handle = decode_handle(remote_id)
        if handle["kind"] != "psij":
            raise ValueError("Not a PSI/J job reference")
        offsets = {"out": stdout_offset, "err": stderr_offset}
        paths = {"out": stdout_path, "err": stderr_path}
        streams = {"out": stdout, "err": stderr}
        return await _follow_scheduler(remote_id, streams, paths, offsets, poll_interval)

    @staticmethod
    async def stream(job_ref: str, **options):
        import sys
        return await PsijBackend.follow(job_ref, sys.stdout.buffer, sys.stderr.buffer, **options)

    @staticmethod
    async def follow_framed(job_ref: str, **options):
        from kkrunner.remote.stream import follow_framed
        return await follow_framed(PsijBackend.follow, job_ref, **options)
