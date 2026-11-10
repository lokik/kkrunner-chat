"""Explicit stream persistence must not depend on the other stream."""

import sys

import pytest

import kkrunner.client.direct_session as direct_session
import kkrunner.backends.process.direct.local as local
from kkrunner.backends.process.backend import DirectProcessBackend
from kkrunner.runs.base import ExecutionSpec, RunState


@pytest.mark.parametrize("store_stdout, store_stderr", ((True, True), (True, False), (False, True), (False, False)))
def test_local_stream_targets_are_independent(tmp_path, store_stdout, store_stderr):
    stdout_path = tmp_path / "stdout.txt"
    stderr_path = tmp_path / "stderr.txt"
    stdout = stdout_path.open("wb") if store_stdout else None
    stderr = stderr_path.open("wb") if store_stderr else None
    runner = DirectProcessBackend().create_job(
        ExecutionSpec(
            argv=(sys.executable, "-c", "import sys; print('from stdout'); print('from stderr', file=sys.stderr)"),
            input_file=None,
            output_file=stdout,
            error_file=stderr,
            directory=str(tmp_path),
        )
    )
    observed = {"out": [], "err": []}
    runner.add_output_listener(lambda data, kind: observed[kind].append(data))
    assert runner.run().return_code == 0
    assert runner.state == RunState.FINISHED
    assert "".join(observed["out"]) == "from stdout\n"
    assert "".join(observed["err"]) == "from stderr\n"
    assert stdout_path.exists() is store_stdout
    assert stderr_path.exists() is store_stderr
    if store_stdout:
        assert stdout_path.read_bytes() == b"from stdout\n"
        assert stdout.closed
    if store_stderr:
        assert stderr_path.read_bytes() == b"from stderr\n"
        assert stderr.closed


def test_repeated_cancel_schedules_a_single_stop(tmp_path, monkeypatch):
    stopped = []
    stop = local.stop_local_process

    async def record_stop(process, **kwargs):
        stopped.append(process.pid)
        await stop(process, **kwargs)

    monkeypatch.setattr(local, "stop_local_process", record_stop)
    runner = DirectProcessBackend().create_job(
        ExecutionSpec(
            argv=(sys.executable, "-c", "import time; time.sleep(30)"),
            directory=str(tmp_path),
            input_file=None,
            output_file=None,
        )
    )
    runner.submit()
    pid = int(runner.backend_id)
    runner.cancel()
    runner.cancel()
    runner.wait()
    assert runner.state == RunState.CANCELLED
    assert stopped == [pid]


def test_failed_output_pump_terminates_child_without_wrapper(tmp_path, monkeypatch):
    stopped = []
    stop = local.stop_local_process

    async def record_stop(process, **kwargs):
        stopped.append(process.pid)
        await stop(process, **kwargs)

    async def failing_pump(source, sink, *, eof=None):
        raise RuntimeError("output pump failed")

    monkeypatch.setattr(local, "stop_local_process", record_stop)
    monkeypatch.setattr(direct_session, "pump_stream", failing_pump)
    runner = DirectProcessBackend().create_job(
        ExecutionSpec(
            argv=(sys.executable, "-c", "import time; time.sleep(30)"),
            directory=str(tmp_path),
            input_file=None,
            output_file=None,
        )
    )
    runner.submit()
    pid = int(runner.backend_id)
    with pytest.raises(RuntimeError, match="output pump failed"):
        runner.wait()
    assert runner.state == RunState.FAILED
    assert stopped == [pid]

def test_missing_declared_output_fails_run_but_not_backend_job(tmp_path):
    from kkrunner import Run
    spec = ExecutionSpec(
        argv=(sys.executable, "-c", "print('ok')"),
        directory=str(tmp_path),
        output_files=("missing.output",),
    )
    native = DirectProcessBackend().create_job(spec)
    native.run()
    assert native.state == RunState.FINISHED

    run = Run.direct(DirectProcessBackend(), spec)
    run.run()
    assert run.state == RunState.FAILED
    assert run.files.failed_artifacts
    assert run.failure_phase.value == "artifacts"

def test_local_direct_spec_retains_declared_paths_during_execution(tmp_path):
    from kkrunner import Run, ExecutionSpec

    spec = ExecutionSpec(
        argv=(sys.executable, "-c", "print('ready', flush=True)"),
        directory=str(tmp_path),
        output_file=str(tmp_path / "stdout.txt"),
        error_file=str(tmp_path / "stderr.txt"),
    )
    run = Run.direct(DirectProcessBackend(), spec)
    assert run.spec is spec
    run.submit()
    assert run.spec is spec
    assert run.wait().return_code == 0
    assert run.spec is spec
    assert run.spec.output_file == str(tmp_path / "stdout.txt")
    assert run.spec.error_file == str(tmp_path / "stderr.txt")
    assert (tmp_path / "stdout.txt").read_text() == "ready\n"


def test_local_direct_mpi_resolution_does_not_replace_declaration(tmp_path, monkeypatch):
    from kkrunner import Run, ExecutionSpec
    from kkrunner.runs import LaunchSpec

    monkeypatch.setattr(
        "kkrunner.backends.process.primitives.resolve_mpi_command",
        lambda mpi: (sys.executable, "-c"),
    )
    spec = ExecutionSpec(
        argv=("unused-serial",),
        mpi_argv=("print('from MPI')",),
        launch=LaunchSpec.from_mpi(True),
        directory=str(tmp_path),
        output_file=str(tmp_path / "output.txt"),
    )
    run = Run.direct(DirectProcessBackend(), spec)
    assert run.spec is spec
    assert run.run().return_code == 0
    assert run.spec is spec
    assert run.spec.argv == ("unused-serial",)
    assert (tmp_path / "output.txt").read_text() == "from MPI\n"
