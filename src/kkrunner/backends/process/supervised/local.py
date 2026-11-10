"""Raw backend status and reattachment for local supervised processes.

Backend execution uses the same supervisor operations as remote-agent runs.
Client-side file availability is projected by RunManager, never by the Job.
"""

from __future__ import annotations

from kkrunner.remote.handles import decode_handle
from kkrunner.runs.base import ExecutionSpec, RunState, RunStatus
from .operations import status_remote


def _local_status_from_facts(backend_id, facts):
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
    return status


def local_status(backend_id):
    return _local_status_from_facts(backend_id, status_remote(backend_id))


def attach_local(executor, context, *, callback=None, output_listener=None, start=True):
    from .job import SupervisedProcessJob
    if context.endpoint.executor != "local-supervised":
        raise ValueError("Run does not belong to the local backend")
    if context.ref.backend != "process":
        raise ValueError("Job belongs to a different backend")
    backend_id = context.ref.token
    if decode_handle(backend_id).get("kind") != "process":
        raise ValueError("Local project does not have a supervisor handle")
    spec = ExecutionSpec(
        argv=None,
        input_file=context.input_file,
        output_file=context.output_file,
        error_file=context.error_file,
        directory=context.directory.local,
    )
    status = local_status(backend_id)
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
    runner._return_code = status.return_code
    runner.result.return_code = status.return_code
    if start:
        runner.start()
    return runner
