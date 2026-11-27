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
        # An observer of an existing job does not own its lifetime.
        self._detached = ref is not None or backend.default_detached
        self.snapshot = JobStatus(RunState.NEW)
        self.output_error = None
        if status is not None:
            self._record(status)
        self._follow_options = {}
        self._call = None
        self._initial_output = True
        self._output_offsets = {"out": 0, "err": 0}

    def _operation(self, name):
        return Operation.for_backend(type(self.backend), name)

    def _record(self, facts):
        token = facts.get("remote_id")
        if token:
            self.ref = JobRef(self.backend.name, str(token))
        self.snapshot = decode_job_status(facts)
        return self.snapshot

    def configure_detached(self, detached):
        if (detached != self.backend.default_detached
                and BackendCapability.FLEXIBLE_DETACHMENT not in self.backend.supports):
            raise ValueError("Detached policy is fixed by the selected backend")
        self._detached = detached

    @property
    def can_stop_observation(self):
        # Attached calls must continue draining their output.
        return self._detached

    def configure_observation(self, *, stream_offsets, stream_paths):
        self._follow_options = {
            "stdout_offset": stream_offsets.get("out"), "stderr_offset": stream_offsets.get("err"),
            "stdout_path": stream_paths[0], "stderr_path": stream_paths[1],
        }
        self._output_offsets = {
            "out": stream_offsets.get("out"), "err": stream_offsets.get("err"),
        }

    def _output_receiver(self, on_output):
        def receive(data, kind):
            offset = self._output_offsets[kind]
            if offset is not None:
                self._output_offsets[kind] = offset + len(data)
            on_output(data, kind)
        return receive

    async def _open_call(self, operation, request, on_output, **options):
        self._call = await self.target.open_call(
            self._operation(operation), request, self._output_receiver(on_output), **options,
        )
        return await self._call.submission()

    async def observe(self, on_output, *, output_requested=None):
        try:
            if self._call is None:
                facts = await self.target.follow(
                    self._operation("follow"), self.ref.token, self._output_receiver(on_output),
                    **self._follow_options, subscription=output_requested,
                )
            elif output_requested is None:
                facts = await self._call.wait()
            else:
                facts = await self._call.wait_with_output(output_requested, self._initial_output)
        except BaseException as exc:
            self._record({"state": "unknown", "error": str(exc)})
            raise
        status = self._record(facts)
        if facts.get("output_error") is not None:
            self.output_error = OSError(facts["output_error"])
            return status
        # A late request can cross the terminal frame. Complete named-stream
        # replay from the bytes already delivered, without another backend wait.
        if (output_requested is not None and output_requested.enabled
                and output_requested.enabled_late and status.state.is_terminal
                and facts.get("output_drained") is not True
                and status.failure_phase not in (JobFailurePhase.LAUNCH, JobFailurePhase.SUBMISSION)):
            paths = {name: self._follow_options.get(name + "_path") for name in ("stdout", "stderr")}
            if any(paths.values()):
                from kkrunner.support.growing_file_tail import replay_named_output
                replay = await self.target.follow(
                    Operation.standalone(replay_named_output), facts, self._output_receiver(on_output),
                    stdout_path=paths["stdout"], stderr_path=paths["stderr"],
                    stdout_offset=self._output_offsets["out"], stderr_offset=self._output_offsets["err"],
                    subscription=output_requested,
                )
                if replay.get("output_error") is not None:
                    self.output_error = OSError(replay["output_error"])
        return status

    async def status(self):
        """Query native facts without replacing the live call's outcome."""
        if (self.ref is None or self.snapshot.state.is_terminal
                or BackendCapability.STATUS not in self.backend.supports):
            return self.snapshot
        status = decode_job_status(await self.target.call(self._operation("status"), self.ref.token))
        # A concurrent live terminal reply is stronger than a liveness probe.
        return self.snapshot if self.snapshot.state.is_terminal else status

    async def close(self):
        if self._call is not None:
            await self._call.close()


class DirectExecution(_Execution):
    """Backend binding for session-owned execution and its native status messages."""

    def __init__(self, backend, transport, *, local_io=None):
        super().__init__(backend, transport)
        self._local_io = local_io

    def configure_observation(self, *, stream_offsets, stream_paths):
        if any(offset not in (None, 0) for offset in stream_offsets.values()):
            raise ValueError("Session-bound execution requires empty output streams")
        super().configure_observation(stream_offsets=stream_offsets, stream_paths=stream_paths)

    async def submit(self, request, on_output, *, output_requested=None):
        request = dict(request, detached=self._detached)
        try:
            prepared = await self.target.call(self._operation("prepare_mpi"), request)
            if prepared["warning"] is not None:
                warnings.warn(prepared["warning"], MpiNotFoundWarning, stacklevel=2)
            request = prepared["request"]
            await self.target.call(self._operation("prepare_target_outputs"), request)
            # Attached Direct streams retain their native session semantics.
            # Detached processes write files; tail only on explicit request.
            self._initial_output = (
                not self._detached or output_requested is None or output_requested.enabled
            )
            facts = await self._open_call(
                "stream", request, on_output, output=self._initial_output,
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

    async def observe(self, on_output, *, output_requested=None):
        # Attached Direct uses native session streams, not subscriptions.
        return await super().observe(
            on_output, output_requested=output_requested if self._detached else None,
        )

    async def cancel(self):
        await self._call.cancel()


class ManagedExecution(_Execution):
    """Backend binding for independently observable supervisor/scheduler jobs."""

    async def submit(self, request, on_output, *, output_requested=None):
        request = dict(request, detached=self._detached)
        try:
            if BackendCapability.SUBMIT_AND_FOLLOW in self.backend.supports:
                self._initial_output = output_requested is None or output_requested.enabled
                facts = await self._open_call(
                    "submit_and_follow", request, on_output, observe_only=True,
                    output=self._initial_output,
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
