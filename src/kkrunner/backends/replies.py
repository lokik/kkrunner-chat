"""Wire representation of backend execution facts (not client Run state)."""

from kkrunner.runs.base import RunState

def _status_reply(state, return_code=None, *, failure_phase=None, error=None):
    """Encode backend status facts for the common remote-service wire contract."""
    return {
        "state": state.value,
        "return_code": return_code,
        "error": error,
        "failure_phase": None if failure_phase is None else failure_phase.value,
    }

