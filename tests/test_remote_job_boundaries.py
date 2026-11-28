"""Contract regressions for separated backend jobs and client file lifecycles.

Tests do not require SSH, PSI/J, Slurm, or a background server.
"""

import asyncio
from types import SimpleNamespace

import pytest

from kkrunner import ExecutionOutcome, ExecutionSpec, Run, RunState
from kkrunner.backends.contracts import JobRef, JobStatus
from kkrunner.client.job import Job
from kkrunner.runs.base import FailurePhase


class _BackendExecution:
    """One controllable backend binding; the real Job owns its lifecycle."""

    def __init__(self, *, submit_error=None):
        self.submit_error = submit_error
        self.submit_calls = 0
        self.status_calls = 0
        self.ref = None
        self.snapshot = JobStatus(RunState.NEW)
        self.can_stop_observation = True
        self.output_error = None

    def configure_detached(self, detached):
        pass

    def configure_observation(self, *, stream_offsets, stream_paths):
        if not isinstance(stream_offsets, dict):
            raise TypeError("stream offsets must be a mapping")

    async def submit(self, request, on_output, *, output_requested=None):
        self.submit_calls += 1
        if self.submit_error is not None:
            # A lost reply is not a proof of non-submission.
            self.snapshot = JobStatus(RunState.UNKNOWN, message=str(self.submit_error))
            raise self.submit_error
        self.ref = JobRef("kkrunner", "opaque-job")
        self.snapshot = JobStatus(RunState.QUEUED)
        return self.snapshot

    async def observe(self, on_output, *, output_requested=None):
        self.snapshot = JobStatus(RunState.FINISHED, return_code=0)
        return self.snapshot

    async def status(self):
        self.status_calls += 1
        return self.snapshot

    async def cancel(self):
        return None

    async def close(self):
        return None


def _job(execution, tmp_path):
    return Job(execution, ExecutionSpec(["true"], directory=tmp_path))


def _configure(job, tmp_path):
    job.configure_submission(
        {"directory": str(tmp_path)},
        stream_offsets={"out": None, "err": None},
        stream_paths=(None, None),
    )


def test_unknown_submit_cannot_be_replayed_on_the_same_job(tmp_path):
    execution = _BackendExecution(submit_error=ConnectionError("reply lost"))
    job = _job(execution, tmp_path)
    _configure(job, tmp_path)
    with pytest.raises(ConnectionError, match="reply lost"):
        job.submit()
    assert job.state == RunState.UNKNOWN
    assert job.ref is None
    with pytest.raises(RuntimeError, match="prepared"):
        job.submit()
    assert execution.submit_calls == 1


def test_backend_job_status_excludes_download_and_processing(tmp_path):
    execution = _BackendExecution()
    job = _job(execution, tmp_path)
    _configure(job, tmp_path)
    job.submit()
    result = job.wait()
    assert isinstance(result, JobStatus)
    assert result.return_code == 0
    assert job.ref == JobRef("kkrunner", "opaque-job")
    # status() is passive; refresh_status() explicitly invokes the backend.
    status = job.status()
    assert status.state is RunState.FINISHED
    assert status.return_code == 0
    assert execution.status_calls == 0
    assert job.refresh_status() == status
    assert execution.status_calls == 1
    assert not hasattr(job, "_output_targets")
    assert not hasattr(job, "_cache_targets")
    assert not hasattr(job, "_sync_output_files")


def test_active_caller_owns_job_until_output_decoder_closes(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    execution = _BackendExecution()
    job = _job(execution, tmp_path)
    _configure(job, tmp_path)
    closing, release = Event(), Event()
    observations = []
    original_observe = execution.observe

    async def observe(*args, **kwargs):
        observations.append(True)
        return await original_observe(*args, **kwargs)

    def finish_bytes():
        closing.set()
        assert release.wait(5)

    monkeypatch.setattr(execution, "observe", observe)
    monkeypatch.setattr(job._output_observers, "finish_bytes", finish_bytes)
    with ThreadPoolExecutor(max_workers=1) as pool:
        first = pool.submit(job.run)
        try:
            assert closing.wait(5)
            with pytest.raises(RuntimeError, match="active caller"):
                job.wait()
        finally:
            release.set()
        result = first.result(timeout=5)
    assert job.wait() == result
    assert observations == [True]


def test_pre_submit_configuration_failure_leaves_no_publishable_request(tmp_path):
    job = _job(_BackendExecution(), tmp_path)
    with pytest.raises(TypeError):
        job.configure_submission(
            {"directory": str(tmp_path)},
            stream_offsets=None,
            stream_paths=(None, None),
        )
    assert job._request is None
    _configure(job, tmp_path)
    assert job._request is not None


class _FinishedJob:
    def __init__(self, events):
        # An observed Job still carries its immutable execution specification.
        self.spec = ExecutionSpec(("true",), directory=".")
        self.events = events
        self.state = RunState.NEW
        self.output_error = None
        self.return_code = 0
        self.failure_phase = None
        self.failure_message = None
        self.exception = None
        self.backend_id = "opaque-job"

    def configure_submission(self, request, **observation):
        self.events.append("configure")

    def submit(self):
        self.events.append("submit")
        self.state = RunState.FINISHED

    async def run_async(self, *, on_submitted=None):
        self.submit()
        if on_submitted is not None:
            await on_submitted()
        self.events.append("observe")
        return self.status()

    def wait(self):
        self.events.append("wait")
        return ExecutionOutcome(return_code=0)

    async def wait_async(self):
        return self.wait()

    def status(self):
        from kkrunner.backends.contracts import job_status_from_facts
        return job_status_from_facts(
            self.state, return_code=self.return_code,
            failure_phase=self.failure_phase, message=self.failure_message,
        )


class _Files:
    def __init__(self, tmp_path, events, *, error=None):
        self.events = events
        self.error = error
        self.spec = ExecutionSpec(["true"], directory=tmp_path)
        self.result = ExecutionOutcome(output_file=str(tmp_path / "stdout"), return_code=0)
        self.files = None
        self.is_local = self.files_are_local = False  # Failed transfer is a DOWNLOAD failure.
        self.input_file = self.result.input_file
        self.output_file = self.result.output_file
        self.error_file = self.result.error_file

    def prepare_submission(self):
        self.events.append("stage")

    async def prepare_submission_async(self):
        self.prepare_submission()

    def prepare_job(self):
        self.events.append("prepare")
        return {"directory": "."}

    def observation(self):
        return {"stream_offsets": {},
                "stream_paths": (None, None)}

    async def finalize_async(self, status, *, on_resolved=None):
        return self.finalize(status)

    def finalize(self, status, *, on_resolved=None):
        self.events.append("sync")
        assert status.state == RunState.FINISHED
        try:
            if self.error is not None:
                raise self.error
        finally:
            self.close()

    def mark_transfer_failed(self, message):
        self.events.append("mark-failed")

    def close(self):
        self.events.append("close")


def _run_with_job(job, files=None, **options):
    from kkrunner.client import ExecutionClient
    from kkrunner.client.run_parts import RunParts

    client = ExecutionClient()
    if files is None:
        files = client.file_session(job.spec)
    return Run(RunParts(job, files), client=client, **options)


def test_run_synchronizes_before_postprocessing(tmp_path):
    events = []
    files = _Files(tmp_path, events)
    run = _run_with_job(_FinishedJob(events), files, process_result=lambda outcome: (
        events.append("process") or outcome.return_code
    ))
    assert run.run() == 0
    assert run.state == RunState.FINISHED
    # Owned streams must be flushed/closed before postprocessing sees files.
    assert events.index("observe") < events.index("sync") < events.index("close") < events.index("process")
    assert events.count("observe") == 1
    assert "wait" not in events


def test_download_failure_does_not_change_successful_job_state(tmp_path):
    events = []
    job = _FinishedJob(events)
    files = _Files(tmp_path, events, error=OSError("download unavailable"))
    run = _run_with_job(job, files)
    with pytest.raises(OSError, match="download unavailable"):
        run.run()
    assert job.state == RunState.FINISHED
    assert run.state == RunState.FAILED
    assert run.failure_phase == FailurePhase.DOWNLOAD
    assert events[-1] == "close"

def test_file_preparation_failure_does_not_attempt_backend_submission(tmp_path):
    from kkrunner.runs.base import SubmissionNotStarted

    events = []
    files = _Files(tmp_path, events)
    def fail_preparation():
        raise ValueError("invalid mapped files")
    files.prepare_job = fail_preparation
    job = _FinishedJob(events)
    run = _run_with_job(job, files)
    with pytest.raises(SubmissionNotStarted, match="invalid mapped files"):
        run.run()
    assert run.state == RunState.FAILED
    assert "submit" not in events
    assert events == ["stage", "close"]


@pytest.mark.parametrize(
    "backend_state, expected_state, expected_phase",
    [
        (RunState.FINISHED, RunState.FINISHED, None),
        (RunState.FAILED, RunState.FAILED, FailurePhase.EXECUTION),
        (RunState.CANCELLED, RunState.CANCELLED, None),
    ],
)
def test_local_run_keeps_backend_outcome_despite_observation_failure(
    backend_state, expected_state, expected_phase,
):
    """The authoritative backend result is independent of lost live output."""
    observed = []
    job = _FinishedJob(observed)
    job.state = backend_state
    job.failure_phase = FailurePhase.EXECUTION if backend_state is RunState.FAILED else None
    delivery_error = OSError("local FOLLOW stream interrupted")
    job.output_error = delivery_error
    run = _run_with_job(job)
    asyncio.run(run._finish_async())
    assert job.state is backend_state
    assert run.state is expected_state
    assert run.failure_phase is expected_phase
    if backend_state is RunState.FINISHED:
        assert run.exception is None
        assert job.output_error is delivery_error
        assert run.result.return_code == 0


def test_untracked_run_postprocesses_confirmed_success_after_failed_local_follow():
    observed = []
    job = _FinishedJob(observed)
    job.state = RunState.FINISHED
    job.output_error = RuntimeError("FOLLOW decoder failed")
    run = _run_with_job(job, process_result=lambda outcome: observed.append("processed"))
    asyncio.run(run._finish_async())
    assert run.state is RunState.FINISHED
    assert run.failure_phase is None
    assert observed == ["wait", "processed"]


def test_incomplete_local_follow_still_collects_backend_files(tmp_path):
    from kkrunner.backends import DirectBackend
    from kkrunner.client import ExecutionClient

    spec = ExecutionSpec(
        argv=("true",), directory=str(tmp_path),
        output_file="stdout.txt", error_file="stderr.txt",
        output_files=("other-result.dat",),
    )
    for name in ("stdout.txt", "stderr.txt", "other-result.dat"):
        (tmp_path / name).write_text("authoritative output")
    job = _FinishedJob([])
    job.spec = spec
    job.state = RunState.FINISHED
    job.output_error = OSError("FOLLOW stream truncated")
    files = ExecutionClient(DirectBackend).file_session(spec)
    run = _run_with_job(job, files)
    asyncio.run(run._finish_async())
    assert run.state is RunState.FINISHED
    assert job.output_error is not None
    assert all(entry.availability_state == "available" for entry in run.files.artifacts)


@pytest.mark.parametrize("when", ["before_start", "observing"])
def test_managed_stop_observation_does_not_cancel_native_job(tmp_path, when):
    from threading import Event

    from kkrunner.backends import SupervisedBackend
    from kkrunner.client.backend_execution import ManagedExecution
    from kkrunner.targets import LocalTarget

    started, stopped = Event(), Event()

    class FollowTransport(LocalTarget):
        async def follow(self, operation, identity, on_output, **options):
            started.set()
            try:
                await asyncio.sleep(10)
                pytest.fail("Observation was not interrupted")
            finally:
                stopped.set()

        async def call(self, operation, *args, **kwargs):
            pytest.fail("Stopping observation must not invoke a native operation")

    execution = ManagedExecution(
        SupervisedBackend(), FollowTransport(),
        ref=JobRef("kkrunner", "opaque"), status={"state": "running"},
    )
    job = Job(execution, ExecutionSpec(("true",), directory=str(tmp_path)))
    if when == "before_start":
        job.stop_observation()
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(job.wait)
        if when == "observing":
            assert started.wait(5)
            job.stop_observation()
            job.stop_observation()
        assert future.result(timeout=5).return_code is None
    assert job.state == RunState.UNKNOWN
    assert job.ref == JobRef("kkrunner", "opaque")
    assert started.is_set() == stopped.is_set() == (when == "observing")


def test_late_status_query_does_not_replace_stream_completion(tmp_path):
    from threading import Event, Thread

    execution = _BackendExecution()
    job = _job(execution, tmp_path)
    entered = Event()
    release_query = Event()
    finish = Event()
    refreshed = []

    async def status():
        entered.set()
        assert await asyncio.to_thread(release_query.wait, 5)
        return JobStatus(RunState.RUNNING)

    async def observe(on_output, *, output_requested=None):
        assert await asyncio.to_thread(finish.wait, 5)
        return JobStatus(RunState.FAILED, return_code=7)

    execution.status = status
    execution.observe = observe
    _configure(job, tmp_path)
    job.submit()
    query = Thread(target=lambda: refreshed.append(job.refresh_status()), daemon=True)
    query.start()
    try:
        assert entered.wait(5)
        finish.set()
        assert job.wait().return_code == 7
    finally:
        finish.set()
        release_query.set()
        query.join(5)
    assert not query.is_alive()
    assert refreshed == [JobStatus(RunState.FAILED, return_code=7)]
    assert job.state == RunState.FAILED


@pytest.mark.parametrize("drained", [False, True])
def test_late_subscription_replays_only_if_terminal_output_was_not_drained(tmp_path, drained):
    import asyncio
    from kkrunner.backends import PsijBackend
    from kkrunner.backends.contracts import JobRef
    from kkrunner.client.backend_execution import ManagedExecution
    from kkrunner.client.contracts import OutputSubscription
    from kkrunner.targets import LocalTarget

    path = tmp_path / "out"
    path.write_bytes(b"saved\n")
    calls = []

    class Target(LocalTarget):
        async def follow(self, operation, identity, on_output, *, subscription=None, **options):
            if operation.backend is not None:
                calls.append("native wait")
                assert not subscription.enabled
                subscription.set(True)
                if drained:
                    on_output(b"saved\n", "out")
                return {"state": "finished", "return_code": 0, "output_drained": drained}
            calls.append("file replay")
            return await super().follow(
                operation, identity, on_output, subscription=subscription, **options,
            )

    async def exercise():
        binding = ManagedExecution(PsijBackend(), Target(), ref=JobRef("psij", "test"))
        binding.configure_observation(stream_offsets={"out": 0, "err": None},
                                      stream_paths=(str(path), None))
        seen = []
        status = await binding.observe(lambda data, kind: seen.append((kind, data)),
                                       output_requested=OutputSubscription(False))
        assert status.return_code == 0
        assert seen == [("out", b"saved\n")]
        assert calls == (["native wait"] if drained else ["native wait", "file replay"])

    asyncio.run(exercise())


@pytest.mark.parametrize("reply", [None, {"state": "unknown"}, {"state": "finished", "return_code": None}])
def test_reattached_terminal_result_survives_output_observation_failure(tmp_path, reply):
    from kkrunner.backends import DirectBackend
    from kkrunner.client.backend_execution import bind_execution

    class Transport:
        async def follow(self, *args, **kwargs):
            if reply is None:
                raise OSError("output connection lost")
            return reply

        async def call(self, *args, **kwargs):
            pytest.fail("A known terminal outcome requires no new status query")

    execution = bind_execution(
        DirectBackend(), Transport(), ref=JobRef("direct", "existing"),
        status={"state": "finished", "return_code": 0},
    )
    job = _job(execution, tmp_path)
    result = job.wait()
    assert result.state == RunState.FINISHED
    assert result.return_code == 0
    if reply is None:
        assert isinstance(job.output_error, OSError)


def test_late_cancel_reply_does_not_replace_observed_outcome():
    from kkrunner.backends import SupervisedBackend
    from kkrunner.client.backend_execution import bind_execution

    async def scenario():
        entered, release = asyncio.Event(), asyncio.Event()

        class Transport:
            async def call(self, operation, token):
                entered.set()
                await release.wait()
                return {"state": "running"}

            async def follow(self, *args, **kwargs):
                return {"state": "finished", "return_code": 0}

        backend = SupervisedBackend()
        execution = bind_execution(backend, Transport(), ref=JobRef(backend.name, "existing"),
                                   status={"state": "running"})
        cancellation = asyncio.create_task(execution.cancel())
        await entered.wait()
        result = await execution.observe(lambda *_: None)
        release.set()
        assert (await cancellation).state == RunState.RUNNING
        assert (await execution.status()) == result
        assert result.return_code == 0

    asyncio.run(scenario())


@pytest.mark.parametrize("detached", [False, True])
def test_live_direct_cancellation_uses_process_owning_call(detached):
    from kkrunner.backends import DirectBackend
    from kkrunner.client.backend_execution import DirectExecution

    async def scenario():
        calls = []

        class Call:
            async def cancel(self):
                calls.append("control")

            async def wait(self):
                calls.append("outcome")
                return {"state": "cancelled", "return_code": -15}

        class Transport:
            async def call(self, *args):
                pytest.fail("A live Direct call owns its native process")

        execution = DirectExecution(DirectBackend(), Transport())
        execution.configure_detached(detached)
        # macOS/opaque identities must work while their owning call lives.
        execution.ref = JobRef("direct", "opaque-job")
        execution._call = Call()
        result = await execution.cancel()
        if detached:
            assert calls == ["control", "outcome"]
            assert result.state == RunState.CANCELLED
            assert result.return_code == -15
        else:
            assert calls == ["control"]
            assert result is None

    asyncio.run(scenario())


def test_direct_cancel_after_observation_close_uses_native_identity():
    from kkrunner.backends import DirectBackend
    from kkrunner.client.backend_execution import DirectExecution

    async def scenario():
        calls = []

        class Call:
            async def close(self):
                calls.append("close")

            async def cancel(self):
                pytest.fail("Closed control channel must not be used")

        class Transport:
            async def call(self, operation, token):
                calls.append(token)
                return {"state": "cancelled"}

        execution = DirectExecution(DirectBackend(), Transport())
        execution.configure_detached(True)
        execution.ref = JobRef("direct", "native-job")
        execution._call = Call()
        await execution.close()
        await execution.close()
        assert (await execution.cancel()).state == RunState.CANCELLED
        assert calls == ["close", "native-job"]

    asyncio.run(scenario())


def test_confirmed_terminal_status_survives_weaker_observer_reply(tmp_path):
    from threading import Event

    execution = _BackendExecution()
    execution.ref = JobRef("kkrunner", "existing")
    execution.snapshot = JobStatus(RunState.RUNNING)
    entered, release = Event(), Event()

    async def observe(on_output, *, output_requested=None):
        entered.set()
        assert await asyncio.to_thread(release.wait, 5)
        return JobStatus(RunState.UNKNOWN)

    async def status():
        return JobStatus(RunState.FINISHED, return_code=0)

    execution.observe = observe
    execution.status = status
    job = _job(execution, tmp_path)
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(job.wait)
        try:
            assert entered.wait(5)
            assert job.refresh_status() == JobStatus(RunState.FINISHED, return_code=0)
        finally:
            release.set()
        assert future.result(timeout=5) == JobStatus(RunState.FINISHED, return_code=0)
