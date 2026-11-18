"""Installed PSI/J operations, independent of client FileSession."""

from __future__ import annotations

import asyncio
import logging

from kkrunner.backends.contracts import JobFailurePhase, JobStatus
from kkrunner.backends.support.messages import (
    status_reply,
)
from kkrunner.remote.handles import decode_handle
from kkrunner.execution import RunState
from kkrunner.support.growing_file_tail import follow_growing_files

from . import slurm

_STATE_MAP = {
    "NEW": RunState.NEW,
    "STAGE_IN": RunState.QUEUED,
    "QUEUED": RunState.QUEUED,
    "ACTIVE": RunState.RUNNING,
    "STAGE_OUT": RunState.RUNNING,
    "COMPLETED": RunState.FINISHED,
    "FAILED": RunState.FAILED,
    "CANCELED": RunState.CANCELLED,
}

def psij_state(status, *, attached=False, return_code=None):
    """Interpret native PSI/J job facts consistently across all observers.

    A newly attached job reports NEW before PSI/J has received its real
    scheduler status. That is UNKNOWN, not evidence of a new submission.
    A completed job with a nonzero exit code is an execution failure.
    """
    name = getattr(getattr(status, "state", None), "name", None)
    if attached and name == "NEW":
        return RunState.UNKNOWN
    state = _STATE_MAP.get(name, RunState.UNKNOWN)
    if return_code is None:
        return_code = getattr(status, "exit_code", None)
    if state == RunState.FINISHED and return_code not in (None, 0):
        return RunState.FAILED
    return state


def _local_slurm(options):
    return options.get("executor_name", "slurm").lower() == "slurm" and options.get("url") is None


def supports_recovery(options):
    return _local_slurm(options)


def _load_psij():
    try:
        import psij
    except ImportError as exc:
        raise ImportError("PSI/J requires the optional 'psij-python' package") from exc
    return psij


def _create_executor(options):
    psij = _load_psij()
    executor = psij.JobExecutor.get_instance(options.get("executor_name", "slurm"), url=options.get("url"))
    return psij, executor


def _attach_job(handle):
    psij, executor = _create_executor(handle)
    job = psij.Job()
    executor.attach(job, str(handle["native_id"]))
    return job


def job_status(handle, *, native=None, attached=False):
    """Resolve the authoritative status for both STATUS and FOLLOW.

    For local Slurm use scheduler facts. PSI/J attachment callbacks only
    signal that terminal status may now be available; for other executors
    PSI/J itself provides the status.
    """
    if _local_slurm(handle):
        return slurm.status(str(handle["native_id"]))
    if native is None:
        native = _attach_job(handle).status
    state = psij_state(native, attached=attached)
    return JobStatus(
        state, return_code=getattr(native, "exit_code", None),
        failure_phase=JobFailurePhase.EXECUTION if state == RunState.FAILED else None,
        message=getattr(native, "message", None),
    )


async def _follow_scheduler(remote_id, streams, paths, offsets, poll_interval):
    """Follow scheduler-owned files while PSI/J waits for terminal completion."""

    handle = decode_handle(remote_id)
    job = _attach_job(handle)

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
        facts = await asyncio.to_thread(
            job_status, handle, native=finished.result(), attached=True,
        )
        if not facts.state.is_terminal:
            # The callback is a wakeup, not an alternate status authority.
            # The scheduler may still be updating its terminal record.
            return None
        return status_reply(
            facts.state, facts.return_code,
            failure_phase=facts.failure_phase, error=facts.message,
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
