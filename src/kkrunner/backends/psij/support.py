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

def _create_executor(options=None):
    options = options or {}
    return PsijExecutor(options.get("executor_name") or "slurm", url=options.get("url"))


async def _follow_scheduler(remote_id, streams, paths, offsets, poll_interval):
    """Follow scheduler-owned files while PSI/J waits for terminal completion."""

    handle = decode_handle(remote_id)
    executor = _create_executor(handle)
    job = executor.attach_job(handle["native_id"])

    def emit(kind, data):
        streams[kind].write(data)
        streams[kind].flush()

    loop = asyncio.get_running_loop()
    finished = loop.create_future()

    def deliver(status):
        if not finished.done():
            finished.set_result(status)

    def status_changed(_job, status):
        if not psij_state(status).is_terminal:
            return
        try:
            loop.call_soon_threadsafe(deliver, status)
        except RuntimeError:
            pass  # Observation has already closed its event loop.

    async def terminal():
        if not finished.done():
            return None
        native = finished.result()
        state = psij_state(native)
        return status_reply(
            state, getattr(native, "exit_code", None),
            failure_phase=(JobFailurePhase.EXECUTION if state == RunState.FAILED else None),
            error=getattr(native, "message", None),
        )

    try:
        job.set_job_status_callback(status_changed)
        # Completion can race with callback registration.
        status_changed(job, job.status)
        return await follow_growing_files(
            paths, emit, terminal, offsets=offsets,
            poll_interval=max(poll_interval, 1.0), wakeup=finished,
        )
    finally:
        finished.cancel()
        # PSI/J versions that wrap None as a callback require a callable.
        # Release the observer closure without cancelling the scheduler job.
        try:
            job.set_job_status_callback(lambda _job, _status: None)
        except Exception:
            logging.getLogger(__name__).exception("Failed to release PSI/J observation callback")
