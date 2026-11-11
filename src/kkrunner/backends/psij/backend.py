"""Installed PSI/J operations, independent of client FileSession."""

from __future__ import annotations

import asyncio
import logging

from kkrunner.remote.handles import decode_handle, encode_handle
from kkrunner.backends.support.messages import (
    AcknowledgeReply, ExecutionRequest, RecoveryReply, RecoveryRequest, StatusReply, SubmissionReply,
    request_to_spec, status_reply,
)
from kkrunner.backends.contracts import JobFailurePhase
from kkrunner.runs.states import RunState
from kkrunner.support.growing_file_tail import follow_growing_files
from .executor import PsijExecutor
from .submission import psij_state
from . import slurm


from kkrunner.backends.contracts import Backend, ManagedOperations, BackendCapability
from kkrunner.backends.support.class_helper import imported_ssh_helper
from .support import _create_executor, _follow_scheduler


class PsijBackend(Backend, ManagedOperations):
    """Native target operations, independent of endpoints and transport."""

    name = "psij"
    default_options = (("executor_name", "slurm"), ("url", None))
    SshHelper = imported_ssh_helper
    session_bound = False
    default_persistent = True
    default_detached = True
    supports = frozenset({BackendCapability.REATTACH, BackendCapability.RECOVER})

    @staticmethod
    def submit(request: ExecutionRequest) -> SubmissionReply:
        executor = _create_executor(request.get("backend_options"))
        spec = request_to_spec(request)
        submission = executor.create_submission(spec, submission_key=request.get("id"))
        # PSI/J's _prepare_io() owns cleanup of its complete declared manifest.
        # Running a second cleanup here would duplicate it and risk wildcard caches.
        submission.submit()
        native_id = submission.native_id
        if native_id is None:
            raise RuntimeError("PSI/J submission did not return a native job id")
        remote_id = encode_handle(
            {
                "kind": "psij",
                "executor_name": executor.executor_name,
                "url": executor.url,
                "native_id": native_id,
            }
        )
        return {
            "remote_id": remote_id,
            **status_reply(submission.state, submission.return_code, failure_phase=submission.failure_phase),
            "directory": request["directory"],
            "input_file": request.get("input_file"),
            "output_file": submission.result.output_file,
            "error_file": submission.result.error_file,
        }

    @staticmethod
    def status(remote_id: str) -> StatusReply:
        handle = decode_handle(remote_id)
        if handle["kind"] != "psij":
            raise ValueError("Not a PSI/J job reference")
        if str(handle["executor_name"]).lower() == "slurm" and handle.get("url") is None:
            facts = slurm.status(handle["native_id"])
        else:
            executor = _create_executor(handle)
            facts = executor.status(handle["native_id"])
        return status_reply(
            facts.state, facts.return_code,
            failure_phase=facts.failure_phase, error=facts.message,
        )

    @staticmethod
    def cancel(remote_id: str) -> StatusReply:
        handle = decode_handle(remote_id)
        if handle["kind"] != "psij":
            raise ValueError("Not a PSI/J job reference")
        executor = _create_executor(handle)
        job = executor.attach_job(handle["native_id"])
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
            executor = _create_executor(request.get("backend_options"))
            facts = executor.recover(
                run_id, request["directory"], submitted_at=submitted_at,
            )
        except NotImplementedError as exc:
            return {"unsupported": True, "error": str(exc)}
        if facts is None:
            return None
        native_id = facts["native_id"]
        remote_id = encode_handle(
            {
                "kind": "psij",
                "executor_name": executor.executor_name,
                "url": executor.url,
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
