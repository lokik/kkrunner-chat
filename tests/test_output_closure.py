"""Output streams belong to FileSession, not backend Job state."""

import asyncio

import pytest

from kkrunner import ExecutionSpec, Run, RunState
from kkrunner.client.run_parts import RunParts
from kkrunner.backends import DirectBackend, SupervisedBackend
from kkrunner.backends.contracts import JobStatus
from kkrunner.client import ExecutionClient
from kkrunner.client.job import Job
from kkrunner.files.streams import close_output_files
from kkrunner.runs.base import FailurePhase, SubmissionNotStarted


class _TrackedOutput:
    def __init__(self, name=None, error=None):
        self.name = name
        self.error = error
        self.close_calls = 0

    def close(self):
        self.close_calls += 1
        if self.error is not None:
            raise self.error

    def flush(self):
        pass


@pytest.mark.parametrize("backend", (DirectBackend, SupervisedBackend))
@pytest.mark.parametrize("merged", (False, True))
@pytest.mark.parametrize("acquire", (False, True))
def test_local_file_session_closes_only_acquired_output_handles(tmp_path, backend, merged, acquire):
    stdout = _TrackedOutput(str(tmp_path / "stdout.out"))
    stderr = stdout if merged else _TrackedOutput(str(tmp_path / "stderr.err"))
    spec = ExecutionSpec(("true",), directory=tmp_path, output_file=stdout, error_file=stderr)
    parts = ExecutionClient(backend).create_run_parts(spec)
    # Creating an unstarted session only borrows application streams.
    assert stdout.close_calls == stderr.close_calls == 0
    if acquire:
        parts.files.prepare_submission()
        parts.files.prepare_job()
        parts.files.prepare_job()
        assert stdout.close_calls == stderr.close_calls == 0
    parts.close()
    parts.close()
    assert stdout.close_calls == stderr.close_calls == int(acquire)


def test_close_output_files_skips_missing_and_path_declarations():
    first, second = _TrackedOutput(), _TrackedOutput()
    close_output_files(None, "out.txt", first, first, second)
    assert (first.close_calls, second.close_calls) == (1, 1)


def test_close_error_still_closes_remaining_handles():
    first = _TrackedOutput(error=OSError("cannot close output"))
    second = _TrackedOutput()
    with pytest.raises(OSError, match="cannot close output"):
        close_output_files(first, second)
    assert first.close_calls == second.close_calls == 1


def _managed_job(spec):
    from kkrunner.backends.contracts import JobRef
    from kkrunner.client.backend_execution import bind_execution

    client = ExecutionClient(SupervisedBackend)
    binding = bind_execution(
        client.backend, client.target,
        ref=JobRef("process", "job-1"),
        status={"state": "running", "return_code": None},
    )
    return Job(binding, spec)


def test_unknown_backend_monitor_leaves_stream_closure_to_file_session(tmp_path, monkeypatch):
    output = _TrackedOutput(str(tmp_path / "output.out"))
    spec = ExecutionSpec(("true",), directory=tmp_path, output_file=output, error_file=output)
    files = ExecutionClient().file_session(spec)
    files.prepare_submission()
    job = _managed_job(spec)

    async def disconnected(_on_output, *, output_requested=None):
        raise ConnectionError("remote FOLLOW interrupted")

    async def unavailable():
        raise ConnectionError("remote STATUS unavailable")

    monkeypatch.setattr(job.execution, "observe", disconnected)
    monkeypatch.setattr(job.execution, "status", unavailable)
    job.wait()
    assert job.state is RunState.UNKNOWN
    assert output.close_calls == 0
    files.close()
    assert output.close_calls == 1


@pytest.mark.parametrize("monitor_fails,finish_fails", [
    (True, False), (False, True), (True, True),
])
def test_managed_job_cleanup_attempts_all_stages(tmp_path, monkeypatch, monitor_fails, finish_fails):
    """Output decoder closure runs even when backend observation fails."""
    calls = []
    errors = (asyncio.CancelledError("observation interrupted"), OSError("decoder failed"))
    job = _managed_job(ExecutionSpec(("true",), directory=tmp_path))

    async def observe(*args, **kwargs):
        calls.append("observe")
        if monitor_fails:
            raise errors[0]
        return JobStatus(RunState.FINISHED, return_code=0)

    def finish_bytes():
        calls.append("finish")
        if finish_fails:
            raise errors[1]

    monkeypatch.setattr(job.execution, "observe", observe)
    monkeypatch.setattr(job._output_observers, "finish_bytes", finish_bytes)
    if monitor_fails:
        with pytest.raises(asyncio.CancelledError) as caught:
            job.wait()
        assert caught.value is errors[0]
        assert job.state is RunState.UNKNOWN
        with pytest.raises(asyncio.CancelledError):
            job.wait()
    else:
        result = job.wait()
        assert result.state is RunState.FINISHED
        assert job.wait() == result
    assert calls == ["observe", "finish"]
    if finish_fails:
        assert job.output_error is errors[1]


def test_confirmed_native_completion_survives_client_cleanup_error(tmp_path, monkeypatch):
    """A failed output decoder must not replace a confirmed backend outcome."""
    error = OSError("output decoder failed")
    job = _managed_job(ExecutionSpec(("true",), directory=tmp_path))

    async def finished(*args, **kwargs):
        return JobStatus(RunState.FINISHED, return_code=0)

    def close_decoder():
        raise error

    monkeypatch.setattr(job.execution, "observe", finished)
    monkeypatch.setattr(job._output_observers, "finish_bytes", close_decoder)
    result = job.wait()
    assert job.wait() == result
    assert job.state is RunState.FINISHED
    assert job.return_code == 0
    assert job.failure_phase is None
    assert job.output_error is error
    assert job.exception is None


def test_run_reports_file_session_close_error_without_changing_native_outcome(tmp_path, monkeypatch):
    error = OSError("cannot close output")
    output = _TrackedOutput(str(tmp_path / "out.txt"), error=error)
    spec = ExecutionSpec(("true",), directory=tmp_path, output_file=output)
    client = ExecutionClient(SupervisedBackend)
    files = client.file_session(spec)
    files.prepare_submission()
    job = _managed_job(spec)

    async def finished(*args, **kwargs):
        return JobStatus(RunState.FINISHED, return_code=0)

    monkeypatch.setattr(job.execution, "observe", finished)
    run = Run(RunParts(job, files), client=client)
    with pytest.raises(OSError, match="cannot close output"):
        run.wait()
    assert job.state is RunState.FINISHED
    assert job.return_code == 0
    assert job.failure_phase is None
    assert run.state is RunState.FAILED
    assert run.failure_phase is FailurePhase.ARTIFACTS
    assert output.close_calls == 1


@pytest.mark.parametrize("backend", (DirectBackend, SupervisedBackend))
def test_staging_failure_closes_acquired_outputs_once(tmp_path, backend, monkeypatch):
    from kkrunner.client.file_session import FileSession

    output = _TrackedOutput(str(tmp_path / "out"))
    parts = ExecutionClient(backend).create_run_parts(ExecutionSpec(
        ("true",), directory=tmp_path, output_file=output, error_file=output,
    ))

    def fail(_self):
        raise OSError("flush failed")

    monkeypatch.setattr(FileSession, "_flush_declared_handles", fail)
    with pytest.raises(SubmissionNotStarted, match="flush failed"):
        parts.files.prepare_submission()
    parts.close()
    parts.close()
    assert output.close_calls == 1


@pytest.mark.parametrize("symlink", [False, True])
def test_new_output_is_a_new_artifact_without_truncating_previous_reader(tmp_path, symlink):
    from kkrunner.files.streams import open_new_output

    original = tmp_path / "out"
    original.write_bytes(b"old")
    path = original
    if symlink:
        path = tmp_path / "alias"
        path.symlink_to(original)
    with path.open("rb") as previous, open_new_output(path, "w+b", buffering=0) as current:
        current.write(b"new")
        current.seek(0)
        assert current.read() == b"new"
        assert path.read_bytes() == b"new"
        assert previous.read() == b"old"
    assert path.is_symlink() is symlink
    assert original.read_bytes() == b"new"


def test_failed_new_output_replacement_leaves_original_artifact(tmp_path, monkeypatch):
    import os
    from kkrunner.files.streams import open_new_output

    path = tmp_path / "out"
    path.write_bytes(b"old")

    def denied(*args):
        raise PermissionError("cannot publish new output")

    monkeypatch.setattr(os, "replace", denied)
    with pytest.raises(PermissionError, match="cannot publish new output"):
        open_new_output(path)
    assert path.read_bytes() == b"old"
    assert list(tmp_path.iterdir()) == [path]
