"""Project execution facts into the client Run lifecycle."""

from kkrunner.execution import FailurePhase, RunState


def project_run_state(state, *, files_pending=False, processing_pending=False, failure_phase=None):
    """Project native facts into a client phase without changing any state."""
    state = RunState(state)
    if files_pending and (state in (RunState.FINISHED, RunState.CANCELLED) or (
        state == RunState.FAILED and failure_phase not in (
            FailurePhase.PREPROCESSING, FailurePhase.SUBMISSION, FailurePhase.LAUNCH,
        )
    )):
        return RunState.DOWNLOADING
    if state == RunState.FINISHED and processing_pending:
        return RunState.POSTPROCESSING
    return state
