"""Output streams belong to FileSession, not backend Job state."""

import asyncio

import pytest

from kkrunner import ExecutionSpec, Run, RunState
from kkrunner.backends import DirectBackend, SupervisedBackend
from kkrunner.client import RunClient
from kkrunner.client.job import Job
from kkrunner.files.streams import close_output_files
from kkrunner.runs.base import FailurePhase


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
def test_local_file_session_closes_unique_output_handles(tmp_path, backend, merged):
    stdout = _TrackedOutput(str(tmp_path / "stdout.out"))
    stderr = stdout if merged else _TrackedOutput(str(tmp_path / "stderr.err"))
    spec = ExecutionSpec(("true",), directory=tmp_path, output_file=stdout, error_file=stderr)
    parts = RunClient(backend).create_run_parts(spec)
    # Creating a backend Job never transfers ownership of application streams.
    assert stdout.close_calls == stderr.close_calls == 0
    parts.close()
    parts.close()
    assert stdout.close_calls == 1
    assert stderr.close_calls == 1


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


def test_unknown_backend_monitor_leaves_stream_closure_to_file_session(tmp_path):
    output = _TrackedOutput(str(tmp_path / "output.out"))

    class LostContext:
        async def _follow(self, *args, **kwargs):
            raise ConnectionError("remote FOLLOW interrupted")

        async def _status(self, *args, **kwargs):
            raise ConnectionError("remote STATUS unavailable")

    spec = ExecutionSpec(("true",), directory=tmp_path, output_file=output, error_file=output)
    files = RunClient().file_session(spec)
    job = Job(LostContext(), spec, backend_id="job-1", remote_directory="/remote/case")
    asyncio.run(job._monitor())
    assert job.state is RunState.UNKNOWN
    assert output.close_calls == 0
    files.close()
    assert output.close_calls == 1


@pytest.mark.parametrize("monitor_fails,finish_fails,cleanup_fails", [
    (True, False, False), (False, True, False), (False, False, True),
    (True, True, True), (False, True, True), (True, False, True),
])
def test_managed_job_cleanup_attempts_all_stages(
    tmp_path, monkeypatch, monitor_fails, finish_fails, cleanup_fails,
):
    calls, callbacks = [], []
    errors = (RuntimeError("monitor failed"), OSError("decoder failed"), OSError("cleanup failed"))
    spec = ExecutionSpec(("true",), directory=tmp_path)

    async def monitor():
        calls.append("monitor")
        if monitor_fails:
            raise errors[0]

    def finish_bytes():
        calls.append("finish")
        if finish_fails:
            raise errors[1]

    def cleanup():
        calls.append("cleanup")
        if cleanup_fails:
            raise errors[2]

    job = Job(object(), spec, backend_id="job", callback=lambda result: callbacks.append(result),
              completion_cleanup=cleanup)
    monkeypatch.setattr(job, "_monitor", monitor)
    monkeypatch.setattr(job._output_observers, "finish_bytes", finish_bytes)
    job._thread_main()
    assert calls == ["monitor", "finish", "cleanup"]
    assert job._done.is_set()
    assert callbacks == [job.result]
    if monitor_fails:
        assert job.exception is errors[0]
    if finish_fails:
        assert job.output_error is errors[1]
    elif cleanup_fails:
        assert job.output_error is errors[2]
    assert job.state is RunState.UNKNOWN


def test_confirmed_native_completion_survives_client_cleanup_error(tmp_path, monkeypatch):
    error = OSError("output cleanup failed")
    job = Job(object(), ExecutionSpec(("true",), directory=tmp_path),
              backend_id="known-job", completion_cleanup=lambda: (_ for _ in ()).throw(error))

    async def finished():
        job._state = RunState.FINISHED
        job._return_code = 0
        job.result.return_code = 0

    monkeypatch.setattr(job, "_monitor", finished)
    job._thread_main()
    assert job.state is RunState.FINISHED
    assert job.return_code == 0
    assert job.failure_phase is None
    assert job.output_error is error
    assert job.exception is None
    assert job._done.is_set()


def test_run_reports_file_session_close_error_without_changing_native_outcome(tmp_path, monkeypatch):
    error = OSError("cannot close output")
    output = _TrackedOutput(str(tmp_path / "out.txt"), error=error)
    spec = ExecutionSpec(("true",), directory=tmp_path, output_file=output)
    client = RunClient(SupervisedBackend)
    files = client.file_session(spec)
    job = Job(client._context(client, spec.staging), spec,
              backend_id="known-job", remote_directory=str(tmp_path))

    async def finished():
        job._state = RunState.FINISHED
        job._return_code = 0
        job.result.return_code = 0

    monkeypatch.setattr(job, "_monitor", finished)
    run = Run(job, client=client, file_session=files)
    run.start()
    with pytest.raises(OSError, match="cannot close output"):
        run.wait()
    assert job.state is RunState.FINISHED
    assert job.return_code == 0
    assert job.failure_phase is None
    assert run.state is RunState.FAILED
    assert run.failure_phase is FailurePhase.ARTIFACTS
    assert output.close_calls == 1
