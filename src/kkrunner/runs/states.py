"""Failure phases in which no native execution or output can be recovered."""

from kkrunner.execution import FailurePhase


NO_EXECUTION_FAILURES = {
    FailurePhase.PREPROCESSING,
    FailurePhase.SUBMISSION,
    FailurePhase.LAUNCH,
}
