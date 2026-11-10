"""Backend contract invariants, independent of backend implementation."""

from dataclasses import FrozenInstanceError

import pytest

from kkrunner.backends.contracts import (
    JobFailurePhase,
    JobRef,
    JobState,
    JobStatus,
    RecoveryDisposition,
    RecoveryResult,
)


@pytest.mark.parametrize("state", list(JobState))
def test_only_real_terminal_job_states_are_terminal(state):
    assert state.is_terminal == (state in {JobState.FINISHED, JobState.FAILED, JobState.CANCELLED})


def test_client_specific_phases_are_not_job_states():
    assert "downloading" not in JobState._value2member_map_
    assert "postprocessing" not in JobState._value2member_map_


def test_job_ref_contains_backend_scoped_identity():
    ref = JobRef("psij", "opaque-native-job")
    assert ref.backend == "psij"
    assert ref.token == "opaque-native-job"
    with pytest.raises(FrozenInstanceError):
        ref.token = "replacement"
    with pytest.raises(ValueError, match="nonempty"):
        JobRef("", "a")
    with pytest.raises(ValueError, match="nonempty"):
        JobRef("process", "")


def test_recovery_result_requires_job_identity_only_if_found():
    ref = JobRef("process", "opaque")
    result = RecoveryResult.found(ref)
    assert result.disposition is RecoveryDisposition.FOUND
    assert result.ref == ref
    for result in (RecoveryResult.absent(), RecoveryResult.indeterminate(), RecoveryResult.unsupported()):
        assert result.ref is None
    with pytest.raises(ValueError, match="Exactly FOUND"):
        RecoveryResult(RecoveryDisposition.FOUND)
    with pytest.raises(ValueError, match="Exactly FOUND"):
        RecoveryResult(RecoveryDisposition.ABSENT, ref)


def test_status_validates_execution_facts_without_client_phases():
    status = JobStatus("failed", return_code=7, failure_phase="execution", message="exit 7")
    assert status.state is JobState.FAILED
    assert status.failure_phase is JobFailurePhase.EXECUTION
    with pytest.raises(ValueError):
        JobStatus("downloading")
    with pytest.raises(TypeError):
        JobStatus(JobState.FINISHED, return_code=True)
