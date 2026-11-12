"""Installed supervisor target operations."""

from __future__ import annotations

from kkrunner.backends.supervised import mailbox
from kkrunner.execution import FailurePhase, RunState


def _process_outcome(receipt_id):
    data = mailbox.read_outcome(receipt_id)
    if data is None:
        return None
    try:
        state = RunState(data["state"])
        return_code = data.get("return_code")
        raw_phase = data.get("failure_phase")
        failure_phase = None if raw_phase is None else FailurePhase(raw_phase)
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError("Invalid remote process outcome artifact") from exc
    return state, return_code, failure_phase, data.get("error")
