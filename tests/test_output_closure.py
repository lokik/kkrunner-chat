"""Output handle lifecycle across process jobs and remote client file sessions."""

import asyncio

import pytest

from kkrunner.backends.process.engine import ProcessJob
from kkrunner.backends.process.managed import SupervisedProcessJob
from kkrunner.client.remote_files import RemoteFileSession
from kkrunner.client.remote_job import RemoteJob
from kkrunner.runs.base import ExecutionSpec, RunState, close_output_files


class _TrackedOutput:
    def __init__(self, name=None):
        self.name = name
        self.close_calls = 0

    def close(self):
        self.close_calls += 1

    def flush(self):
        pass


@pytest.mark.parametrize("backend", ("local", "local_project"))
@pytest.mark.parametrize("merged", (False, True))
def test_local_runners_close_unique_output_handles(monkeypatch, tmp_path, backend, merged):
    stdout = _TrackedOutput(str(tmp_path / "stdout.out"))
    stderr = stdout if merged else _TrackedOutput(str(tmp_path / "stderr.err"))
    spec = ExecutionSpec(
        argv=("true",), directory=str(tmp_path), input_file=None, output_file=stdout, error_file=stderr
    )
    if backend == "local":

        async def fail_to_launch(_spec):
            raise RuntimeError("launch failed")

        monkeypatch.setattr("kkrunner.backends.process.engine.start_local_process", fail_to_launch)
        runner = ProcessJob(spec)
        with pytest.raises(RuntimeError, match="launch failed"):
            asyncio.run(runner._execute())
        assert runner.state == RunState.FAILED
    else:
        runner = SupervisedProcessJob(None, spec)
        monkeypatch.setattr(runner, "_monitor", lambda: None)
        runner._thread_main()
        assert runner._done.is_set()
        assert runner.exception is None
    assert stdout.close_calls == 1
    assert stderr.close_calls == 1


def test_close_output_files_skips_missing_and_path_declarations():
    first, second = (_TrackedOutput(), _TrackedOutput())
    close_output_files(None, "out.txt", first, first, second)
    assert (first.close_calls, second.close_calls) == (1, 1)


def test_close_error_still_closes_remaining_handles():

    class Faulty:
        def close(self):
            raise OSError("cannot close output")

    other = _TrackedOutput()
    with pytest.raises(OSError, match="cannot close output"):
        close_output_files(Faulty(), other)
    assert other.close_calls == 1


def test_remote_monitor_closes_merged_output_once_after_follow_failure(tmp_path):
    output_path = tmp_path / "output.out"
    output_path.touch()
    output = _TrackedOutput(str(output_path))

    class UnreachableEndpoint:
        async def _follow(self, directory, backend_id, *, offsets=None, stdout_path=None, stderr_path=None):
            assert backend_id == "job-1"
            raise ConnectionError("remote follow interrupted")

    spec = ExecutionSpec(
        argv=("true",), directory=str(tmp_path), input_file=None, output_file=output, error_file=output
    )
    files = RemoteFileSession(object(), spec)
    job = RemoteJob(UnreachableEndpoint(), spec, backend_id="job-1", remote_directory="/remote/case")
    asyncio.run(job._monitor())
    assert job.state == RunState.UNKNOWN
    # The backend job never owns local output streams. Only the client file
    # session closes them, and a shared stdout/stderr handle is closed once.
    assert output.close_calls == 0
    files.close()
    assert output.close_calls == 1


@pytest.mark.parametrize(
    "monitor_fails,finish_fails,close_fails",
    [
        (True, False, False),
        (False, True, False),
        (False, False, True),
        (True, True, True),
        (False, True, True),
        (True, False, True),
    ],
)
def test_local_project_cleanup_attempts_all_stages(tmp_path, monkeypatch, monitor_fails, finish_fails, close_fails):
    """The first failure wins, but all cleanup and completion still happen."""
    from kkrunner.runs.base import FailurePhase

    calls = []
    callbacks = []
    errors = (RuntimeError("monitor failed"), OSError("finish bytes failed"), OSError("close output failed"))

    class Output:
        name = str(tmp_path / "out.dat")

        def flush(self):
            pass

        def close(self):
            calls.append("close")
            if close_fails:
                raise errors[2]

    runner = SupervisedProcessJob(
        None,
        ExecutionSpec(argv=("true",), input_file=None, directory=str(tmp_path), output_file=Output()),
        callback=lambda result: callbacks.append(result),
    )

    def monitor():
        calls.append("monitor")
        if monitor_fails:
            raise errors[0]

    def finish_bytes():
        calls.append("finish")
        if finish_fails:
            raise errors[1]

    monkeypatch.setattr(runner, "_monitor", monitor)
    monkeypatch.setattr(runner._output_observers, "finish_bytes", finish_bytes)
    runner._thread_main()
    assert calls == ["monitor", "finish", "close"]
    expected = next((error for failed, error in zip((monitor_fails, finish_fails, close_fails), errors) if failed))
    assert runner.exception is expected
    assert runner.failure_message == str(expected)
    assert runner.failure_phase is None
    assert runner.state is RunState.UNKNOWN
    assert runner._done.is_set()
    assert callbacks == [runner.result]
