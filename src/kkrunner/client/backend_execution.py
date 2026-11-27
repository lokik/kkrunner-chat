"""Bind native backend operations to a transport for one client execution.

Job uses this single interface. Session-bound execution retains a live call;
managed execution owns an opaque reference and may observe its initial
submission through a live call. Neither implementation owns Run persistence
or file synchronization.
"""

from __future__ import annotations

import warnings
from functools import partial

from kkrunner.backends.contracts import (
    BackendCapability, JobFailurePhase, JobRef, JobStatus, ObservationUnavailable,
    SubmissionNotStarted, decode_job_status,
)
from kkrunner.client.contracts import Operation
from kkrunner.execution import MpiNotFoundWarning, RunState


class _Execution:
    def __init__(self, backend, transport, *, ref=None, status=None):
        self.backend = backend
        self.target = transport
        self.ref = ref
        self.snapshot = JobStatus(RunState.NEW)
        if status is not None:
            self._record(status)
        self._follow_options = {}
        self._call = None

    def _operation(self, name):
        return Operation.for_backend(type(self.backend), name)

    def _record(self, facts):
        token = facts.get("remote_id")
        if token:
            self.ref = JobRef(self.backend.name, str(token))
        self.snapshot = decode_job_status(facts)
        return self.snapshot

    def configure_detached(self, detached):
        if detached != self.backend.default_detached:
            raise ValueError("Detached policy is fixed by the selected backend")

    def configure_observation(self, *, stream_offsets, stream_paths):
        self._follow_options = {
            "stdout_offset": stream_offsets.get("out"), "stderr_offset": stream_offsets.get("err"),
            "stdout_path": stream_paths[0], "stderr_path": stream_paths[1],
        }

    async def _open_call(self, operation, request, on_output, **options):
        self._call = await self.target.open_call(
            self._operation(operation), request, on_output, **options,
        )
        return await self._call.submission()

    async def observe(self, on_output):
        if self._call is None:
            return self._record(await self.target.follow(
                self._operation("follow"), self.ref.token, on_output, **self._follow_options,
            ))
        try:
            return self._record(await self._call.wait())
        except BaseException as exc:
            self._record({"state": "unknown", "error": str(exc)})
            raise

    async def close(self):
        if self._call is not None:
            await self._call.close()


class DirectExecution(_Execution):
    """Backend binding for session-owned execution and its native status messages."""

    def __init__(self, backend, transport, *, local_io=None):
        super().__init__(backend, transport)
        self._local_io = local_io
        self._detached = backend.default_detached

    def configure_detached(self, detached):
        self._detached = detached

    def configure_observation(self, *, stream_offsets, stream_paths):
        if any(offset not in (None, 0) for offset in stream_offsets.values()):
            raise ValueError("Session-bound execution requires empty output streams")

    async def submit(self, request, on_output):
        request = dict(request, detached=self._detached)
        try:
            prepared = await self.target.call(self._operation("prepare_mpi"), request)
            if prepared["warning"] is not None:
                warnings.warn(prepared["warning"], MpiNotFoundWarning, stacklevel=2)
            request = prepared["request"]
            await self.target.call(self._operation("prepare_target_outputs"), request)
            tail_paths = {
                kind: request[key] for kind, key in (("out", "output_file"), ("err", "error_file"))
                if request.get(key) is not None
            } if self._detached else None
            facts = await self._open_call(
                "stream", request, on_output, tail_paths=tail_paths,
                local_io=None if self._local_io is None else partial(self._local_io, self._detached),
                use_stdin=prepared["use_stdin"],
            )
        except BaseException as exc:
            if self._call is None and isinstance(exc, Exception):
                self._record({"state": "failed", "failure_phase": "launch", "error": str(exc)})
                raise SubmissionNotStarted(str(exc)) from exc
            self._record({"state": "unknown", "error": str(exc)})
            raise
        status = self._record(facts)
        if self.ref is None:
            if status.state == RunState.FAILED:
                raise SubmissionNotStarted(status.message or "Backend rejected execution")
            raise ObservationUnavailable("Execution call ended without acceptance confirmation")
        return status

    @property
    def can_stop_observation(self):
        # Attached calls must keep draining their pipes. Detached processes
        # own their output files and survive closing the observation.
        return self._detached

    async def status(self):
        # Preserve confirmed live-call facts. Only a lost observer requires
        # querying the original process by its durable identity.
        if self.ref is None or self.snapshot.state != RunState.UNKNOWN:
            return self.snapshot
        return self._record(
            await self.target.call(self._operation("status"), self.ref.token)
        )

    async def cancel(self):
        await self._call.cancel()


class ManagedExecution(_Execution):
    """Backend binding for independently observable supervisor/scheduler jobs."""

    can_stop_observation = True

    async def submit(self, request, on_output):
        try:
            if BackendCapability.SUBMIT_AND_FOLLOW in self.backend.supports:
                facts = await self._open_call(
                    "submit_and_follow", request, on_output, observe_only=True,
                )
                if (facts.get("remote_id") is None
                        and facts.get("failure_phase") == JobFailurePhase.SUBMISSION.value
                        and facts.get("state") == RunState.FAILED.value):
                    raise SubmissionNotStarted(facts.get("error") or "Backend rejected submission")
            else:
                facts = await self.target.call(self._operation("submit"), request)
        except SubmissionNotStarted as exc:
            self._record({"state": "failed", "failure_phase": "submission", "error": str(exc)})
            raise
        except BaseException as exc:
            self._record({"state": "unknown", "error": str(exc)})
            raise
        status = self._record(facts)
        if self.ref is None:
            self._record({"state": "unknown", "error": "Backend accepted no execution identity"})
            raise ObservationUnavailable(self.snapshot.message)
        return status

    async def status(self):
        if self.ref is None:
            return self.snapshot
        return self._record(await self.target.call(self._operation("status"), self.ref.token))

    async def cancel(self):
        return self._record(await self.target.call(self._operation("cancel"), self.ref.token))


def bind_execution(backend, transport, *, local_io=None, ref=None, status=None):
    """Choose backend semantics once, outside the generic client Job."""
    if backend.execution_mode == "direct":
        if ref is not None:
            # A direct attachment observes saved files; it never submits a job.
            return ManagedExecution(backend, transport, ref=ref, status=status)
        return DirectExecution(backend, transport, local_io=local_io)
    if backend.execution_mode == "managed":
        return ManagedExecution(backend, transport, ref=ref, status=status)
    raise ValueError(f"Unknown backend execution mode: {backend.execution_mode!r}")
