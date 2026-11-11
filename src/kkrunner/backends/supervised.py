"""Installed supervisor operations; client attachment belongs to endpoints."""

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


class SupervisedBackend(Backend, ManagedOperations):
    """Installed target backend. All operations are stateless static methods."""

    name = "process"
    resolves_mpi_launcher = True
    SshHelper = imported_ssh_helper
    session_bound = False
    default_persistent = True
    default_detached = True

    @staticmethod
    def submit(request: ExecutionRequest) -> SubmissionReply:
        from kkrunner.native.supervised.operations import _submit_impl
        return _submit_impl(request)

    @staticmethod
    def status(job_ref: str) -> StatusReply:
        from kkrunner.native.supervised.operations import _status_impl
        return _status_impl(job_ref)

    @staticmethod
    def cancel(job_ref: str) -> StatusReply:
        from kkrunner.native.supervised.operations import _cancel_impl
        return _cancel_impl(job_ref)

    @staticmethod
    def recover(
        submission_key: str, request: RecoveryRequest, *, submitted_at: Optional[str] = None,
    ) -> RecoveryReply:
        from kkrunner.native.supervised.operations import _recover_impl
        return _recover_impl(submission_key, request, submitted_at=submitted_at)

    @staticmethod
    def acknowledge(job_ref: str) -> AcknowledgeReply:
        from kkrunner.native.supervised.operations import _acknowledge_impl
        return _acknowledge_impl(job_ref)

    @staticmethod
    def acknowledge_submission(submission_key: str) -> AcknowledgeReply:
        from kkrunner.native.supervised.operations import _acknowledge_submission_impl
        return _acknowledge_submission_impl(submission_key)

    @staticmethod
    async def follow(
        job_ref: str, stdout: ByteSink, stderr: ByteSink, **options: Unpack[FollowOptions],
    ) -> StatusReply:
        from kkrunner.native.supervised.operations import follow
        return await follow(job_ref, stdout, stderr, **options)

    @staticmethod
    async def stream(job_ref: str, **options: Unpack[FollowOptions]) -> StatusReply:
        import sys
        return await SupervisedBackend.follow(job_ref, sys.stdout.buffer, sys.stderr.buffer, **options)

    @staticmethod
    async def follow_framed(job_ref: str, **options: Unpack[FollowOptions]) -> None:
        from kkrunner.remote.stream import follow_framed
        return await follow_framed(SupervisedBackend.follow, job_ref, **options)
