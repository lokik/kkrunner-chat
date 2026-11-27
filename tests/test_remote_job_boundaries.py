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

    def configure_detached(self, detached):
        pass

    def configure_observation(self, *, stream_offsets, stream_paths):
        if not isinstance(stream_offsets, dict):
            raise TypeError("stream offsets must be a mapping")

    async def submit(self, request, on_output):
        self.submit_calls += 1
        if self.submit_error is not None:
            # A lost reply is not a proof of non-submission.
            self.snapshot = JobStatus(RunState.UNKNOWN, message=str(self.submit_error))
            raise self.submit_error
        self.ref = JobRef("kkrunner", "opaque-job")
        self.snapshot = JobStatus(RunState.QUEUED)
        return self.snapshot

    async def observe(self, on_output):
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

    def wait(self):
        self.events.append("wait")
        return ExecutionOutcome(return_code=0)

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
        self.is_local = False  # A failed remote transfer is a DOWNLOAD failure.

    def prepare_submission(self):
        self.events.append("stage")

    def prepare_job(self):
        self.events.append("prepare")
        return {"directory": "."}

    def observation(self):
        return {"stream_offsets": {},
                "stream_paths": (None, None)}

    def finalize(self, status):
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
    assert events.index("wait") < events.index("sync") < events.index("close") < events.index("process")


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
    assert "mark-failed" in events
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
        run.submit()
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
    run._finish()
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
    run._finish()
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
    run._finish()
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
    job.start()
    if when == "observing":
        assert started.wait(5)
        job.stop_observation()
        job.stop_observation()
    assert job.wait().return_code is None
    assert job.state == RunState.UNKNOWN
    assert job.ref == JobRef("kkrunner", "opaque")
    assert started.is_set() == stopped.is_set() == (when == "observing")
