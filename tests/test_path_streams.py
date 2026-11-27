"""Path-valued specs work without application-owned pre-opened handles."""

import sys

from kkrunner import ExecutionSpec, Run
from kkrunner.client import ExecutionClient, StagingSpec
from kkrunner.backends import DirectBackend
from kkrunner.client import SshTarget


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
    run = ExecutionClient(DirectBackend).create_run(spec, tracked=False)
    assert not (tmp_path / "stdout.txt").exists()
    assert run.run().return_code == 0
    assert (tmp_path / "stdout.txt").read_text() == "answer\n"
    assert (tmp_path / "stderr.txt").read_text() == "err\n"


def test_raw_remote_paths_work_with_only_runner_on_target(tmp_path):
    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    class LoopbackSshTransport(SshTarget):
        def __init__(self):
            super().__init__("loopback", python_command=(sys.executable,))

        def _command(self, argv, cwd=None):
            assert cwd is None
            return tuple(argv)

    client = ExecutionClient(DirectBackend, target=LoopbackSshTransport())
    spec = ExecutionSpec(
        [sys.executable, "-c", "print(42)"],
        directory=local,
        output_file="stdout.txt",
        staging=StagingSpec(local_root=local, remote_root=remote),
    )
    run = client.create_run(spec, tracked=False)
    assert not (local / "stdout.txt").exists()
    assert run.run().return_code == 0
    assert (local / "stdout.txt").read_text() == "42\n"


def test_remote_live_observation_does_not_touch_local_files(tmp_path, monkeypatch):
    from threading import Event
    from kkrunner.backends import SupervisedBackend

    monkeypatch.setenv("KKRunner_REMOTE_STATE_DIR", str(tmp_path / "state"))
    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    remote.mkdir()

    class LoopbackSshTransport(SshTarget):
        def __init__(self):
            super().__init__("loopback", python_command=(sys.executable,))

        def _command(self, argv, cwd=None):
            assert cwd is None
            return tuple(argv)

    (local / "stdout.txt").write_text("old")
    ready = Event()
    client = ExecutionClient(SupervisedBackend, target=LoopbackSshTransport())
    spec = ExecutionSpec(
        [sys.executable, "-c",
         "from pathlib import Path; import time; print('ready', flush=True); "
         "exec('while not Path(\"release\").exists(): time.sleep(.01)'); print('done')"],
        directory=local, output_file="stdout.txt", error_file="stderr.txt",
        staging=StagingSpec(local_root=local, remote_root=remote),
    )
    run = client.create_run(spec, tracked=False,
                            output_listener=lambda text, kind: ready.set() if "ready" in text else None)
    run.submit()
    try:
        assert ready.wait(5)
        assert (local / "stdout.txt").read_text() == "old"
        assert not (local / "stderr.txt").exists()
        assert (remote / "stdout.txt").read_text() == "ready\n"
    finally:
        (remote / "release").touch()
        outcome = run.wait()
    assert outcome.return_code == 0
    assert (local / "stdout.txt").read_text() == "ready\ndone\n"
    assert (local / "stderr.txt").read_bytes() == b""


def test_shared_workspace_disables_all_file_transfers(tmp_path, monkeypatch):
    from kkrunner.backends import SupervisedBackend

    monkeypatch.setenv("KKRunner_REMOTE_STATE_DIR", str(tmp_path / "state"))

    class SharedSshTransport(SshTarget):
        def __init__(self):
            super().__init__("loopback", python_command=(sys.executable,))

        def _command(self, argv, cwd=None):
            assert cwd is None
            return tuple(argv)

        async def get_file(self, *args, **kwargs):
            raise AssertionError("Shared filesystem does not require downloads")

        async def put_file(self, *args, **kwargs):
            raise AssertionError("Shared filesystem does not require uploads")

    client = ExecutionClient(SupervisedBackend, target=SharedSshTransport())
    spec = ExecutionSpec(
        [sys.executable, "-c",
         "from pathlib import Path; print('out'); Path('result.dat').write_text('result')"],
        directory=tmp_path, output_file="stdout.txt", output_files=("result.dat",),
        staging=StagingSpec(local_root=tmp_path, remote_root=tmp_path,
                            input_sync="never", output_sync="never"),
    )
    run = client.create_run(spec, tracked=False)
    assert run.run().return_code == 0
    assert (tmp_path / "stdout.txt").read_text() == "out\n"
    assert (tmp_path / "result.dat").read_text() == "result"
    assert all(entry.availability_state == "available" for entry in run.files.artifacts)
