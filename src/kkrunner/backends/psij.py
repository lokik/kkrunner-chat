"""Installed PSI/J operations; selecting this namespace imports no PSI/J runtime."""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from kkrunner.backends.contracts import Backend, ManagedOperations

if TYPE_CHECKING:
    from typing_extensions import Unpack
    from kkrunner.backends.contracts import ByteSink
    from kkrunner.backends.messages import (
        AcknowledgeReply, ExecutionRequest, FollowOptions, RecoveryReply, RecoveryRequest,
        StatusReply, SubmissionReply,
    )
from kkrunner.backends.class_helper import imported_ssh_helper


class PsijBackend(Backend, ManagedOperations):
    """Installed scheduler target operations; no executor instance crosses SSH."""

    name = "psij"
    default_options = (("executor_name", "slurm"), ("url", None))
    SshHelper = imported_ssh_helper
    session_bound = False
    default_persistent = True
    default_detached = True

    @staticmethod
    def submit(request: ExecutionRequest) -> SubmissionReply:
        from kkrunner.native.psij.operations import submit
        return submit(request)

    @staticmethod
    def status(job_ref: str) -> StatusReply:
        from kkrunner.native.psij.operations import status
        return status(job_ref)

    @staticmethod
    def cancel(job_ref: str) -> StatusReply:
        from kkrunner.native.psij.operations import cancel
        return cancel(job_ref)

    @staticmethod
    def recover(
        submission_key: str, request: RecoveryRequest, *, submitted_at: Optional[str] = None,
    ) -> RecoveryReply:
        from kkrunner.native.psij.operations import recover
        try:
            return recover(submission_key, request, submitted_at=submitted_at)
        except NotImplementedError as exc:
            return {"unsupported": True, "error": str(exc)}

    @staticmethod
    def acknowledge(job_ref: str) -> AcknowledgeReply:
        from kkrunner.native.psij.operations import acknowledge
        return acknowledge(job_ref)

    @staticmethod
    def acknowledge_submission(submission_key: str) -> AcknowledgeReply:
        from kkrunner.native.psij.operations import acknowledge_submission
        return acknowledge_submission(submission_key)

    @staticmethod
    async def follow(
        job_ref: str, stdout: ByteSink, stderr: ByteSink, **options: Unpack[FollowOptions],
    ) -> StatusReply:
        from kkrunner.native.psij.operations import follow
        return await follow(job_ref, stdout, stderr, **options)

    @staticmethod
    async def stream(job_ref: str, **options: Unpack[FollowOptions]) -> StatusReply:
        import sys
        return await PsijBackend.follow(job_ref, sys.stdout.buffer, sys.stderr.buffer, **options)

    @staticmethod
    async def follow_framed(job_ref: str, **options: Unpack[FollowOptions]) -> None:
        from kkrunner.remote.stream import follow_framed
        return await follow_framed(PsijBackend.follow, job_ref, **options)
