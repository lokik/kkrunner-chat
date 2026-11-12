"""Installed PSI/J operations, independent of client FileSession."""

from __future__ import annotations

import asyncio
import logging

from kkrunner.backends.contracts import JobFailurePhase
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


def supports_recovery(options):
    return options.get("executor_name", "slurm").lower() == "slurm" and options.get("url") is None


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
        native = finished.result()
        state = psij_state(native)
        code = getattr(native, "exit_code", None)
        # PSI/J can report terminal completion before its attached JobStatus
        # carries an exit code. For local Slurm, recover the authoritative
        # scheduler outcome rather than treating a successful job as code=None.
        if (
            code is None and state.is_terminal
            and str(handle["executor_name"]).lower() == "slurm" and handle.get("url") is None
        ):
            scheduler = await asyncio.to_thread(slurm.status, str(handle["native_id"]))
            if scheduler.state.is_terminal and scheduler.return_code is not None:
                return status_reply(
                    scheduler.state, scheduler.return_code,
                    failure_phase=scheduler.failure_phase, error=scheduler.message,
                )
        return status_reply(
            state, code,
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
