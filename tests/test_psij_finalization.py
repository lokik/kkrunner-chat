from threading import Event
from types import SimpleNamespace

from kkrunner import RunState
from kkrunner.client.job import Job
from kkrunner.runs.base import OutputObservers


def test_psij_observer_failure_is_uncertain_not_a_proven_backend_failure():
    runner = object.__new__(Job)
    runner._direct_session = None
    runner._backend_id = "native-job"
    runner._state = RunState.RUNNING
    runner._completion_cleanup = None
    runner._exception = None
    runner._failure_phase = None
    runner._failure_message = None
    runner._return_code = None
    runner._observed_state = None
    runner._output_observers = OutputObservers()
    runner._done = Event()
    runner.callback = None
    runner.result = object()

    async def fail():
        raise ValueError("result parsing failed")

    runner._monitor = fail
    runner._thread_main()
    assert isinstance(runner._exception, ValueError)
    assert runner.state == RunState.UNKNOWN
    assert runner.failure_phase is None
    assert runner.failure_message == "result parsing failed"
