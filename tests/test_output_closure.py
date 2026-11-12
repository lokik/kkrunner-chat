"""Output streams belong to FileSession, not backend Job state."""

import asyncio

import pytest

from kkrunner import ExecutionSpec, Run, RunState
from kkrunner.backends import DirectBackend, SupervisedBackend
from kkrunner.client import EndpointClient
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
    parts = EndpointClient(backend).create_run_parts(spec)
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


def _managed_job(spec):
    from kkrunner.backends.contracts import JobRef
    from kkrunner.client.backend_execution import bind_execution

    client = EndpointClient(SupervisedBackend)
    binding = bind_execution(
        client.backend, client.transport,
        ref=JobRef("process", "job-1"),
        status={"state": "running", "return_code": None},
    )
    return Job(binding, spec)


def test_unknown_backend_monitor_leaves_stream_closure_to_file_session(tmp_path, monkeypatch):
    output = _TrackedOutput(str(tmp_path / "output.out"))
    spec = ExecutionSpec(("true",), directory=tmp_path, output_file=output, error_file=output)
    files = EndpointClient().file_session(spec)
    job = _managed_job(spec)

    async def disconnected(_on_output):
        raise ConnectionError("remote FOLLOW interrupted")

    async def unavailable():
        raise ConnectionError("remote STATUS unavailable")

    monkeypatch.setattr(job.execution, "observe", disconnected)
    monkeypatch.setattr(job.execution, "status", unavailable)
    asyncio.run(job._monitor())
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
    errors = (RuntimeError("monitor failed"), OSError("decoder failed"))
    job = _managed_job(ExecutionSpec(("true",), directory=tmp_path))

    async def monitor():
        calls.append("monitor")
        if monitor_fails:
            raise errors[0]
        job._state = RunState.FINISHED
        job.result.return_code = 0

    def finish_bytes():
        calls.append("finish")
        if finish_fails:
            raise errors[1]

    monkeypatch.setattr(job, "_monitor", monitor)
    monkeypatch.setattr(job._output_observers, "finish_bytes", finish_bytes)
    job._thread_main()
    assert calls == ["monitor", "finish"]
    assert job._done.is_set()
    if monitor_fails:
        assert job.exception is errors[0]
        assert job.state is RunState.UNKNOWN
    else:
        assert job.state is RunState.FINISHED
    if finish_fails:
        assert job.output_error is errors[1]


def test_confirmed_native_completion_survives_client_cleanup_error(tmp_path, monkeypatch):
    """A failed output decoder must not replace a confirmed backend outcome."""
    error = OSError("output decoder failed")
    job = _managed_job(ExecutionSpec(("true",), directory=tmp_path))

    async def finished():
        job._state = RunState.FINISHED
        job.result.return_code = 0

    def close_decoder():
        raise error

    monkeypatch.setattr(job, "_monitor", finished)
    monkeypatch.setattr(job._output_observers, "finish_bytes", close_decoder)
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
    client = EndpointClient(SupervisedBackend)
    files = client.file_session(spec)
    job = _managed_job(spec)

    async def finished():
        job._state = RunState.FINISHED
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
