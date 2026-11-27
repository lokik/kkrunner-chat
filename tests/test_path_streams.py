"""Path-valued specs work without application-owned pre-opened handles."""

import sys

import pytest

from kkrunner import ExecutionSpec, Run
from kkrunner.client import ExecutionClient, StagingSpec
from kkrunner.backends import DirectBackend, SupervisedBackend
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
    assert not (local / "stdout.txt").exists()
    for message in ("first", "second"):
        spec = ExecutionSpec(
            [sys.executable, "-c", f"import sys; print({message!r}); print({message!r}, file=sys.stderr)"],
            directory=local, output_file="stdout.txt", error_file="stderr.txt",
            staging=StagingSpec(local_root=local, remote_root=remote),
        )
        run = client.create_run(spec, tracked=False)
        assert run.run().return_code == 0
        for directory in (local, remote):
            for filename in ("stdout.txt", "stderr.txt"):
                assert (directory / filename).read_text() == message + "\n"


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

        async def get_files(self, *args, **kwargs):
            raise AssertionError("Shared filesystem does not require downloads")

        async def put_files(self, *args, **kwargs):
            raise AssertionError("Shared filesystem does not require uploads")

    client = ExecutionClient(SupervisedBackend, target=SharedSshTransport())
    spec = ExecutionSpec(
        [sys.executable, "-c",
         "from pathlib import Path; print('out'); Path('result.dat').write_text('result'); "
         "Path('cache.dat').write_text('cache'); Path('restart-a.dat').write_text('restart')"],
        directory=tmp_path, output_file="stdout.txt", output_files=("result.dat",),
        cache_files=("cache.dat", "restart-*.dat"),
        staging=StagingSpec(local_root=tmp_path, remote_root=tmp_path,
                            input_sync="never", output_sync="never"),
    )
    run = client.create_run(spec, tracked=False)
    assert run.run().return_code == 0
    assert (tmp_path / "stdout.txt").read_text() == "out\n"
    assert (tmp_path / "result.dat").read_text() == "result"
    assert (tmp_path / "cache.dat").read_text() == "cache"
    assert (tmp_path / "restart-a.dat").read_text() == "restart"
    assert all(entry.availability_state == "available" for entry in run.files.artifacts)


@pytest.mark.parametrize("remote", [False, True])
@pytest.mark.parametrize("backend,detached,subscription", [
    (SupervisedBackend, False, "never"),
    (SupervisedBackend, False, "during"),
    (SupervisedBackend, True, "convenience"),
    (DirectBackend, True, "convenience"),
    (SupervisedBackend, True, "during"),
    (DirectBackend, True, "never"),
    (DirectBackend, True, "before"),
    (DirectBackend, True, "during"),
])
def test_client_requests_named_output_only_for_a_listener(tmp_path, monkeypatch, remote, backend, detached, subscription):
    from threading import Event
    import time
    from kkrunner.targets import LocalTarget

    monkeypatch.setenv("KKRunner_REMOTE_STATE_DIR", str(tmp_path / "state"))
    local = tmp_path / "local"
    local.mkdir()
    target_root = tmp_path / "remote" if remote else local
    target_root.mkdir(exist_ok=True)
    observed = []

    class Trace:
        async def open_call(self, operation, request, on_output, **options):
            def record(data, kind):
                observed.append((kind, data))
                on_output(data, kind)
            return await super().open_call(operation, request, record, **options)

        async def follow(self, operation, identity, on_output, **options):
            assert operation.backend is not None, "A drained subscription needs no terminal file replay"
            def record(data, kind):
                observed.append((kind, data))
                on_output(data, kind)
            return await super().follow(operation, identity, record, **options)

    class TracedLocal(Trace, LocalTarget):
        pass

    class TracedSsh(Trace, SshTarget):
        def _command(self, argv, cwd=None):
            assert cwd is None
            return tuple(argv)

    target = TracedSsh("loopback", python_command=(sys.executable,)) if remote else TracedLocal()
    spec = ExecutionSpec(
        (sys.executable, "-c",
         "from pathlib import Path; import time; print('ready', flush=True); "
         "exec('while not Path(\"release\").exists(): time.sleep(.01)'); print('done')"),
        directory=local, output_file="stdout.txt",
        staging=StagingSpec(local_root=local, remote_root=target_root) if remote else None,
    )
    ready = Event()
    received = []

    def listener(text, kind):
        received.append(text)
        if "ready" in text:
            ready.set()

    client = ExecutionClient(backend, target=target)
    if subscription == "convenience":
        run = client.submit(
            spec.argv, directory=local, output_file="stdout.txt",
            staging=spec.staging, tracked=False, detached=detached,
        )
    else:
        run = client.create_run(
            spec, tracked=False, detached=detached,
            output_listener=listener if subscription == "before" else None,
        )
        run.submit()
    try:
        deadline = time.monotonic() + 5
        while not (target_root / "stdout.txt").exists() or (
            (target_root / "stdout.txt").read_bytes() != b"ready\n"
        ):
            assert time.monotonic() < deadline
            time.sleep(.01)
        if subscription != "before":
            assert observed == []
        if subscription == "during":
            run.add_output_listener(listener)
        if subscription in ("before", "during"):
            assert ready.wait(5)
    finally:
        (target_root / "release").touch()
        outcome = run.wait()
    assert outcome.return_code == 0
    assert (local / "stdout.txt").read_bytes() == b"ready\ndone\n"
    if subscription in ("before", "during"):
        assert "".join(received) == "ready\ndone\n"
        assert b"".join(data for kind, data in observed if kind == "out") == b"ready\ndone\n"
    else:
        assert observed == []



def test_local_direct_keeps_borrowed_named_output_handle(tmp_path):
    path = tmp_path / "out"
    handle = path.open("wb")
    try:
        spec = ExecutionSpec(
            (sys.executable, "-c", "print('output')"), directory=tmp_path, output_file=handle,
        )
        assert ExecutionClient(DirectBackend).create_run(spec).run().return_code == 0
        assert path.read_bytes() == b"output\n"
    finally:
        handle.close()


def test_detached_direct_restarts_failed_file_reader(tmp_path, monkeypatch):
    import asyncio
    import io
    from kkrunner.support import growing_file_tail

    async def check():
        failed = asyncio.Event()
        received = asyncio.Event()
        original = growing_file_tail.read_output_chunks
        path = tmp_path / "out"
        release = tmp_path / "release"

        def read(*args, **kwargs):
            if not failed.is_set():
                failed.set()
                raise PermissionError("cannot read output")
            return original(*args, **kwargs)

        class Sink(io.BytesIO):
            def write(self, data):
                count = super().write(data)
                received.set()
                return count

        monkeypatch.setattr(growing_file_tail, "read_output_chunks", read)
        control = asyncio.StreamReader()
        out = Sink()
        task = asyncio.create_task(DirectBackend.stream(
            {
                "directory": str(tmp_path),
                "argv": [sys.executable, "-c",
                         "from pathlib import Path; import time; print('first', flush=True); "
                         "exec('while not Path(\\\"release\\\").exists(): time.sleep(.01)'); print('last')"],
                "detached": True,
                "output_file": str(path),
            },
            stdout=out, stderr=io.BytesIO(), control=control,
        ))
        try:
            await asyncio.wait_for(failed.wait(), 3)
            assert not task.done()
            control.feed_data(b"unfollow\nfollow\n")
            await asyncio.wait_for(received.wait(), 3)
            assert out.getvalue() == b"first\n"
            release.touch()
            facts = await asyncio.wait_for(task, 5)
            assert facts["state"] == "finished"
            assert facts["return_code"] == 0
            assert facts["output_drained"] is True
            assert facts["output_error"] == "cannot read output"
            assert out.getvalue() == path.read_bytes() == b"first\nlast\n"
        finally:
            release.touch()
            if not task.done():
                await asyncio.wait_for(task, 5)

    asyncio.run(check())
