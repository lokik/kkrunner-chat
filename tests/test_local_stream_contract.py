"""Direct local streams, cancellation and immutable declarations."""

import os
import signal
import sys

import pytest

from kkrunner import ExecutionSpec, Run, RunState
from kkrunner.backends import DirectBackend
from kkrunner.client import EndpointClient
from kkrunner.client.transports.calls import OutputSink
from kkrunner.execution import MpiSpec


def _run(spec):
    return Run.untracked(EndpointClient(DirectBackend), spec)


@pytest.mark.parametrize("store_stdout, store_stderr", ((True, True), (True, False), (False, True), (False, False)))
def test_local_stream_targets_are_independent(tmp_path, store_stdout, store_stderr):
    stdout_path = tmp_path / "stdout.txt"
    stderr_path = tmp_path / "stderr.txt"
    stdout = stdout_path.open("wb") if store_stdout else None
    stderr = stderr_path.open("wb") if store_stderr else None
    run = _run(ExecutionSpec(
        argv=(sys.executable, "-c",
              "import sys; print('from stdout'); print('from stderr', file=sys.stderr)"),
        input_file=None, output_file=stdout, error_file=stderr, directory=str(tmp_path),
    ))
    observed = {"out": [], "err": []}
    run.add_output_listener(lambda data, kind: observed[kind].append(data))
    assert run.run().return_code == 0
    assert run.state is RunState.FINISHED
    assert "".join(observed["out"]) == "from stdout\n"
    assert "".join(observed["err"]) == "from stderr\n"
    assert stdout_path.exists() is store_stdout
    assert stderr_path.exists() is store_stderr
    if stdout is not None:
        assert stdout_path.read_bytes() == b"from stdout\n"
        assert stdout.closed
    if stderr is not None:
        assert stderr_path.read_bytes() == b"from stderr\n"
        assert stderr.closed


def test_repeated_cancel_schedules_a_single_stop(tmp_path, monkeypatch):
    calls = []
    killpg = os.killpg

    def observe(pid, sig):
        if sig == signal.SIGTERM:
            calls.append(pid)
        return killpg(pid, sig)

    monkeypatch.setattr(os, "killpg", observe)
    run = _run(ExecutionSpec(
        argv=(sys.executable, "-c", "import time; time.sleep(30)"),
        directory=str(tmp_path), input_file=None, output_file=None,
    ))
    run.submit()
    run.cancel()
    run.cancel()
    run.wait()
    assert run.state is RunState.CANCELLED
    assert len(calls) == 1
    assert calls[0] > 0


def test_failed_output_pump_terminates_child_without_wrapper(tmp_path, monkeypatch):
    terminated = []
    original_killpg = os.killpg

    def record_termination(pid, sig):
        if sig == signal.SIGTERM:
            terminated.append(pid)
        return original_killpg(pid, sig)

    monkeypatch.setattr(os, "killpg", record_termination)

    def failing_write(self, data):
        raise RuntimeError("output pump failed")

    monkeypatch.setattr(OutputSink, "write", failing_write)
    run = _run(ExecutionSpec(
        argv=(sys.executable, "-c", "import time; print('ready', flush=True); time.sleep(30)"),
        directory=str(tmp_path), input_file=None, output_file=None,
    ))
    run.submit()
    result = run.wait()
    assert run.state is RunState.FAILED
    assert run.failure_phase.value == "execution"
    assert "output pump failed" in run.failure_message
    assert result.return_code is None
    assert terminated, "Output-pump failure must terminate the child process group"
    for pid in terminated:
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)


def test_missing_declared_output_fails_run_but_not_backend_job(tmp_path):
    spec = ExecutionSpec(
        argv=(sys.executable, "-c", "print('ok')"), directory=str(tmp_path),
        output_files=("missing.output",),
    )
    client = EndpointClient(DirectBackend)
    parts = client.create_run_parts(spec)
    parts.prepare_submission()
    parts.prepare_job()
    try:
        parts.job.submit()
        assert parts.job.wait().return_code == 0
        assert parts.job.state is RunState.FINISHED
    finally:
        parts.close()

    run = Run.untracked(client, spec)
    with pytest.raises(FileNotFoundError, match="Required output file is missing"):
        run.run()
    assert run.state is RunState.FAILED
    assert run.files.failed_artifacts
    assert run.failure_phase.value == "artifacts"


def test_local_direct_spec_retains_declared_paths_during_execution(tmp_path):
    spec = ExecutionSpec(
        argv=(sys.executable, "-c", "print('ready', flush=True)"),
        directory=str(tmp_path), output_file=str(tmp_path / "stdout.txt"),
        error_file=str(tmp_path / "stderr.txt"),
    )
    run = _run(spec)
    assert run.spec is spec
    run.submit()
    assert run.spec is spec
    assert run.wait().return_code == 0
    assert run.spec is spec
    assert run.spec.output_file == str(tmp_path / "stdout.txt")
    assert run.spec.error_file == str(tmp_path / "stderr.txt")
    assert (tmp_path / "stdout.txt").read_text() == "ready\n"


def test_local_direct_mpi_resolution_does_not_replace_declaration(tmp_path):
    spec = ExecutionSpec(
        argv=("unused-serial",),
        mpi_argv=("print('from MPI')",),
        mpi=MpiSpec(required=True, launcher=(sys.executable, "-c")),
        directory=str(tmp_path), output_file=str(tmp_path / "output.txt"),
    )
    run = _run(spec)
    assert run.spec is spec
    assert run.run().return_code == 0
    assert run.spec is spec
    assert run.spec.argv == ("unused-serial",)
    assert (tmp_path / "output.txt").read_text() == "from MPI\n"
