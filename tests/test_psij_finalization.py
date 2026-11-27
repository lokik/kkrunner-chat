from kkrunner import RunState
from kkrunner.backends import PsijBackend
from kkrunner.backends.contracts import JobRef
from kkrunner.client.backend_execution import ManagedExecution
from kkrunner.client.job import Job
from kkrunner.runs.base import ExecutionSpec
from kkrunner.targets import LocalTarget


def test_psij_observer_failure_is_uncertain_not_a_proven_backend_failure():
    execution = ManagedExecution(PsijBackend(), LocalTarget(), ref=JobRef("psij", "native-job"),
                                 status={"state": "running"})
    job = Job(execution, ExecutionSpec(("true",)))

    async def fail():
        raise ValueError("result parsing failed")

    job._monitor = fail
    job._thread_main()
    assert isinstance(job.exception, ValueError)
    assert job.state == RunState.UNKNOWN
    assert job.failure_phase is None
    assert job.failure_message == "result parsing failed"
