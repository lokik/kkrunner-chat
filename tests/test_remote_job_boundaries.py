"""Contract regressions for separated remote execution and client file lifecycle.

These tests use fake transports and do not require SSH, PSI/J, Slurm, or a
background server. They intentionally exercise public Run/Job boundaries.
"""

from types import SimpleNamespace

import pytest

from kkrunner import ExecutionOutcome, ExecutionSpec, Run, RunState
from kkrunner.client.job import Job
from kkrunner.runs.base import FailurePhase


class _TransportContext:
    backend = "process"

    def __init__(self, *, submit_error=None):
        self.submit_error = submit_error
        self.submit_calls = 0
        self.status_calls = 0

    async def _submit(self, request):
        self.submit_calls += 1
        if self.submit_error is not None:
            raise self.submit_error
        return {"state": "queued", "remote_id": "opaque-job", "return_code": None}

    async def _status(self, directory, backend_id):
        self.status_calls += 1
        return {"state": "finished", "return_code": 0}


def _job(context, tmp_path):
    return Job(context, ExecutionSpec(["true"], directory=tmp_path))


def _configure(job, tmp_path):
    job.configure_submission(
        {"directory": str(tmp_path)},
        stream_offsets={"out": None, "err": None},
        stream_writer=lambda data, kind: None,
        stream_paths=(None, None),
    )


def test_unknown_submit_cannot_be_replayed_on_the_same_job(tmp_path):
    context = _TransportContext(submit_error=ConnectionError("reply lost"))
    job = _job(context, tmp_path)
    _configure(job, tmp_path)
    with pytest.raises(ConnectionError, match="reply lost"):
        job.submit()
    assert job.state == RunState.UNKNOWN
    assert job.ref is None
    with pytest.raises(RuntimeError, match="prepared"):
        job.submit()
    assert context.submit_calls == 1


def test_backend_job_status_excludes_download_and_processing(tmp_path):
    context = _TransportContext()
    job = _job(context, tmp_path)
    _configure(job, tmp_path)
    # This test concerns the submission boundary, not the follow monitor.
    job.start = lambda: None
    job.submit()
    assert job.ref.backend == "process"
    assert job.ref.token == "opaque-job"
    status = job.status()
    assert status.state.value == "finished"
    assert status.return_code == 0
    assert context.status_calls == 1
    assert not hasattr(job, "_output_targets")
    assert not hasattr(job, "_cache_targets")
    assert not hasattr(job, "_sync_output_files")


def test_pre_submit_configuration_failure_leaves_no_publishable_request(tmp_path):
    job = _job(_TransportContext(), tmp_path)
    with pytest.raises(TypeError):
        job.configure_submission(
            {"directory": str(tmp_path)},
            stream_offsets=None,
            stream_writer=lambda data, kind: None,
            stream_paths=(None, None),
        )
    assert job._request is None
    _configure(job, tmp_path)
    assert job._request is not None


class _FinishedJob:
    def __init__(self, events):
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


class _Files:
    def __init__(self, tmp_path, events, *, error=None):
        self.events = events
        self.error = error
        self.spec = ExecutionSpec(["true"], directory=tmp_path)
        self.result = ExecutionOutcome(output_file=str(tmp_path / "stdout"), return_code=0)
        self.files = None

    def prepare_job(self):
        self.events.append("prepare")
        return {"directory": "."}

    def observation(self):
        return {"stream_offsets": {}, "stream_writer": lambda data, kind: None,
                "stream_paths": (None, None)}

    def synchronize(self, state):
        self.events.append("sync")
        assert state == RunState.FINISHED
        if self.error is not None:
            raise self.error

    def mark_transfer_failed(self, message):
        self.events.append("mark-failed")

    def close(self):
        self.events.append("close")


def test_run_synchronizes_before_postprocessing(tmp_path):
    events = []
    files = _Files(tmp_path, events)
    run = Run(_FinishedJob(events), file_session=files, process_result=lambda outcome: (
        events.append("process") or outcome.return_code
    ))
    assert run.run() == 0
    assert run.state == RunState.FINISHED
    assert events.index("wait") < events.index("sync") < events.index("process") < events.index("close")


def test_download_failure_does_not_change_successful_job_state(tmp_path):
    events = []
    job = _FinishedJob(events)
    files = _Files(tmp_path, events, error=OSError("download unavailable"))
    run = Run(job, file_session=files)
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
    run = Run(job, file_session=files)
    with pytest.raises(SubmissionNotStarted, match="invalid mapped files"):
        run.submit()
    assert run.state == RunState.FAILED
    assert "submit" not in events
    assert events == ["close"]


def test_run_parts_own_file_cleanup_before_and_after_handoff(tmp_path):
    from kkrunner.client.run_parts import RunParts

    events = []
    files = _Files(tmp_path, events)
    parts = RunParts(_FinishedJob(events), files)
    parts.abort()
    assert events == ["close"]
    events.clear()
    parts = RunParts(_FinishedJob(events), files)
    parts.claim()
    parts.abort()
    assert events == []
    parts.close()
    assert events == ["close"]
