"""Installed PSI/J operations, independent of client FileSession."""

from __future__ import annotations

import asyncio
import logging

from kkrunner.remote.handles import decode_handle, encode_handle
from kkrunner.backends.messages import (
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


def cancel(remote_id: str) -> StatusReply:
    handle = decode_handle(remote_id)
    if handle["kind"] != "psij":
        raise ValueError("Not a PSI/J job reference")
    executor = _create_executor(handle)
    job = executor.attach_job(handle["native_id"])
    job.cancel()
    return status(remote_id)


def acknowledge(remote_id: str) -> AcknowledgeReply:
    return {"acknowledged": False}


def acknowledge_submission(run_id: str) -> AcknowledgeReply:
    return {"acknowledged": False}


def recover(
    run_id: str, request: RecoveryRequest, *, submitted_at: str | None = None,
) -> RecoveryReply:
    executor = _create_executor(request.get("backend_options"))
    facts = executor.recover(
        run_id, request["directory"], submitted_at=submitted_at,
    )
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
