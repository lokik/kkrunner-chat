"""Installed PSI/J operations, independent of client FileSession."""

import asyncio

from kkrunner.remote.handles import decode_handle, encode_handle
from kkrunner.remote.messages import request_to_spec, status_reply
from kkrunner.runs.base import FailurePhase, RunState
from kkrunner.support.growing_file_tail import follow_job_files
from .executor import PsijBackend
from .job import psij_state
from .slurm import status_native

def _psij_executor(options):

    options = options or {}
    return PsijBackend(options.get("backend") or "slurm", url=options.get("url"))

def _submit_impl(request):
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
        "state": runner.state.value,
        "directory": request["directory"],
        "input_file": request.get("input_file"),
        "output_file": runner.result.output_file,
        "error_file": runner.result.error_file,
        "return_code": runner.return_code,
        "failure_phase": (None if runner.failure_phase is None else runner.failure_phase.value),
    }



def _status_impl(remote_id):
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


def _cancel_impl(remote_id):
    handle = decode_handle(remote_id)
    if handle["kind"] != "psij":
        raise ValueError("Not a PSI/J job reference")
    executor = _psij_executor(handle)
    job = executor.attach_native_job(handle["native_id"])
    job.cancel()
    return _status_impl(remote_id)


def _acknowledge_impl(remote_id):
    return {"acknowledged": False}


def _acknowledge_submission_impl(run_id):
    return {"acknowledged": False}


def _recover_impl(run_id, request, *, submitted_at=None):
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
        "state": state.value,
        "return_code": facts["return_code"],
        "failure_phase": (FailurePhase.EXECUTION.value if state == RunState.FAILED else None),
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

    async def terminal(_native_status):
        facts = await asyncio.to_thread(_status_impl, remote_id)
        if RunState(facts["state"]).is_terminal:
            return facts
        native = job.status
        state = psij_state(native)
        return status_reply(
            state,
            getattr(native, "exit_code", None),
            failure_phase=(FailurePhase.EXECUTION if state == RunState.FAILED else None),
            error=getattr(native, "message", None),
        )

    return await follow_job_files(
        paths,
        emit,
        job.wait,
        terminal=terminal,
        offsets=offsets,
        poll_interval=max(poll_interval, 1.0),
    )

def _follow_impl(
    remote_id, stdout, stderr, *, stdout_offset=0, stderr_offset=0,
    poll_interval=0.1, stdout_path=None, stderr_path=None,
    observation_started=None,
):
    handle = decode_handle(remote_id)
    if handle["kind"] != "psij":
        raise ValueError("Not a PSI/J job reference")
    offsets = {"out": stdout_offset, "err": stderr_offset}
    paths = {"out": stdout_path, "err": stderr_path}
    streams = {"out": stdout, "err": stderr}
    return asyncio.run(_follow_scheduler_async(remote_id, streams, paths, offsets, poll_interval))


