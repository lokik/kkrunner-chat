"""Terminal lifecycle semantics are consistent for all execution backends."""

import pytest

from kkrunner.runs.base import RunState


@pytest.mark.parametrize("state", tuple(RunState))
def test_terminal_state_has_single_definition(state):
    assert state.is_terminal is (state in {RunState.FINISHED, RunState.FAILED, RunState.CANCELLED})

