"""Project execution facts into the client Run lifecycle."""

from kkrunner.execution import RunState


def project_run_state(state, *, files_pending=False, processing_pending=False, failure_phase=None):
    """Project native facts into a client phase without changing any state."""
    state = RunState(state)
    phase = getattr(failure_phase, "value", failure_phase)
    if files_pending and (state == RunState.FINISHED or (
        state == RunState.FAILED and phase not in ("submission", "launch")
    )):
        return RunState.DOWNLOADING
    if state == RunState.FINISHED and processing_pending:
        return RunState.POSTPROCESSING
    return state


