"""Status projection and reattachment for local supervised processes.

Backend execution uses the same supervisor operations as remote-agent runs;
client-only Run attachments and file availability remain local here.
"""

from __future__ import annotations

import os
from dataclasses import replace

from kkrunner.backends.handles import _decode_handle
from kkrunner.files.availability import discover_local_files
from kkrunner.runs.base import ExecutionSpec, FailurePhase, RunState, RunStatus
from .operations import status_remote


def local_manifest(files, status):
    """Resolve stream availability, including partial cancelled-run output.

    The supervisor writes streams directly during execution. Cancellation and
    execution failure do not retract already written bytes.
    """
    published = status.state in (RunState.FINISHED, RunState.CANCELLED) or (
        status.state == RunState.FAILED and status.failure_phase == FailurePhase.EXECUTION
    )
    for entry in (files.stdout, files.stderr):
        if entry is None or entry.local_path is None:
            continue
        if status.failure_phase == FailurePhase.ARTIFACTS:
            state, message = "failed", status.message or "Output publication failed"
        elif published:
            exists = os.path.isfile(entry.local_path)
            state, message = (
                ("available", None)
                if exists
                else (
                    "failed",
                    "Published output file is missing",
                )
            )
        else:
            state, message = "skipped", None
        files = files.with_file_availability(entry.local_path, state, message)

    if status.failure_phase in (FailurePhase.LAUNCH, FailurePhase.SUBMISSION):
        return files
    return discover_local_files(files, include_streams=False, require_outputs=status.state == RunState.FINISHED)


def _local_status_from_facts(backend_id, facts, files=None):
    """Project local supervisor facts into a status without querying it again.

    Recovery already has an authoritative reply; asking the supervisor for
    status a second time risks replacing that snapshot with newer/unknown facts.
    """
    status = RunStatus(
        state=RunState(facts["state"]),
        backend_id=backend_id,
        return_code=facts.get("return_code"),
        message=facts.get("error"),
        failure_phase=facts.get("failure_phase"),
    )
    if files is not None and status.state.is_terminal:
        status = replace(status, files=local_manifest(files, status))
    return status


def local_status(backend_id, files=None):
    return _local_status_from_facts(
        backend_id,
        status_remote(backend_id),
        files,
    )


def attach_local(executor, run, *, callback=None, output_listener=None, start=True):
    from .job import SupervisedProcessJob
    if run.execution.endpoint.executor != "local":
        raise ValueError("Run does not belong to the local backend")
    backend_id = run.execution.backend_id
    if _decode_handle(backend_id).get("kind") != "process":
        raise ValueError("Local project does not have a supervisor handle")
    files = run.files
    spec = ExecutionSpec(
        argv=None,
        input_file=files.input_file,
        output_file=files.output_file,
        error_file=None if files.stderr is None else files.stderr.local_path,
        directory=run.execution.directory.local,
    )
    status = local_status(backend_id, files)
    runner = SupervisedProcessJob(
        executor,
        spec,
        callback=callback,
        output_listener=output_listener,
        backend_id=backend_id,
        state=status.state,
        failure_phase=status.failure_phase,
        failure_message=status.message,
    )
    runner.set_manifest(status.files or files)
    runner._return_code = status.return_code
    runner.result.return_code = status.return_code
    if start:
        runner.start()
    return runner
