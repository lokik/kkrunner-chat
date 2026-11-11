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
from .slurm import status_native

def _psij_executor(options={}):
    return PsijExecutor(options.get("backend") or "slurm", url=options.get("url"))

def _submit_impl(request: ExecutionRequest) -> SubmissionReply:
    executor = _psij_executor(request.get("executor_options"))
    spec = request_to_spec(request)
    runner = executor.create_native_job(spec, submission_key=request.get("id"))
    # PSI/J's _prepare_io() owns cleanup of its complete declared manifest.
    # Running a second cleanup here would duplicate it and risk wildcard caches.
    runner.submit_native()
    backend_id = runner.backend_id
    if backend_id is None:
        raise RuntimeError("PSI/J submission did not return a backend id")
    remote_id = encode_handle(
        {
            "kind": "psij",
            "backend": executor.backend,
            "url": executor.url,
            "native_id": backend_id,
        }
    )
    return {
        "remote_id": remote_id,
        **status_reply(runner.state, runner.return_code, failure_phase=runner.failure_phase),
        "directory": request["directory"],
        "input_file": request.get("input_file"),
        "output_file": runner.result.output_file,
        "error_file": runner.result.error_file,
    }



def _status_impl(remote_id: str) -> StatusReply:
    handle = decode_handle(remote_id)
    if handle["kind"] != "psij":
        raise ValueError("Not a PSI/J job reference")
    if str(handle["backend"]).lower() == "slurm" and handle.get("url") is None:
        status = status_native(handle["native_id"])
    else:
        executor = _psij_executor(handle)
        status = executor.status_native(handle["native_id"])
    return status_reply(
        status.state, status.return_code,
        failure_phase=status.failure_phase, error=status.message,
    )


def _cancel_impl(remote_id: str) -> StatusReply:
    handle = decode_handle(remote_id)
    if handle["kind"] != "psij":
        raise ValueError("Not a PSI/J job reference")
    executor = _psij_executor(handle)
    job = executor.attach_native_job(handle["native_id"])
    job.cancel()
    return _status_impl(remote_id)


def _acknowledge_impl(remote_id: str) -> AcknowledgeReply:
    return {"acknowledged": False}


def _acknowledge_submission_impl(run_id: str) -> AcknowledgeReply:
    return {"acknowledged": False}


def _recover_impl(
    run_id: str, request: RecoveryRequest, *, submitted_at: str | None = None,
) -> RecoveryReply:
    backend = _psij_executor(request.get("executor_options"))
    facts = backend.recover_native(
        run_id, request["directory"], submitted_at=submitted_at,
    )
    if facts is None:
        return None
    native_id = facts["native_id"]
    remote_id = encode_handle(
        {
            "kind": "psij",
            "backend": backend.backend,
            "url": backend.url,
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


async def _follow_scheduler_async(remote_id, streams, paths, offsets, poll_interval):
    """Follow scheduler-owned files while PSI/J waits for terminal completion."""

    handle = decode_handle(remote_id)
    executor = _psij_executor(handle)
    job = executor.attach_native_job(handle["native_id"])

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
    return await _follow_scheduler_async(remote_id, streams, paths, offsets, poll_interval)
