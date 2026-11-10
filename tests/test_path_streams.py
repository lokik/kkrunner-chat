"""Path-valued specs work without application-owned pre-opened handles."""

import sys

from kkrunner import ExecutionSpec, Run
from kkrunner.runs import LaunchSpec
from kkrunner.backends import DirectProcessBackend
from kkrunner.client import LocalTransport, AgentRunEndpoint


def test_raw_local_paths_and_stdin_are_materialized_at_submission(tmp_path):
    (tmp_path / "input.txt").write_text("answer")
    (tmp_path / "primary.txt").write_text("main input is not stdin")
    spec = ExecutionSpec(
        [sys.executable, "-c", 'import sys; print(sys.stdin.read()); print("err",file=sys.stderr)'],
        directory=tmp_path,
        input_file="primary.txt",
        stdin="input.txt",
        output_file="stdout.txt",
        error_file="stderr.txt",
    )
    run = Run.direct(DirectProcessBackend(), spec)
    assert not (tmp_path / "stdout.txt").exists()
    assert run.run().return_code == 0
    assert (tmp_path / "stdout.txt").read_text() == "answer\n"
    assert (tmp_path / "stderr.txt").read_text() == "err\n"


def test_raw_remote_paths_work_with_only_runner_on_target(tmp_path):
    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    executor = AgentRunEndpoint(LocalTransport(), command=(sys.executable, "-m", "kkrunner"))
    spec = ExecutionSpec(
        [sys.executable, "-c", "print(42)"],
        directory=local,
        output_file="stdout.txt",
        launch=LaunchSpec(local_root=local, remote_root=remote),
    )
    run = Run.direct(executor, spec)
    assert not (local / "stdout.txt").exists()
    assert run.run().return_code == 0
    assert (local / "stdout.txt").read_text() == "42\n"
