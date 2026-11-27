"""Tests for command/file targets."""

import asyncio
import hashlib
import json
import os
import signal
import subprocess
import sys

import pytest

from kkrunner.files.versions import FileVersion, file_digest
from kkrunner.targets import LocalTarget, SshTarget


def test_local_transport_run_start_and_files(tmp_path):

    async def run_test():
        transport = LocalTarget()
        result = await transport.run((sys.executable, "-c", "print('ok')"))
        assert result.stdout == b"ok\n"
        command = await transport.start(
            (sys.executable, "-c", "import sys; print('stream'); print('err', file=sys.stderr)")
        )
        stdout, stderr, returncode = await asyncio.gather(command.stdout.read(), command.stderr.read(), command.wait())
        assert returncode == 0
        assert stdout == b"stream\n"
        assert stderr == b"err\n"
        source = tmp_path / "source"
        source.write_bytes(b"content")
        assert await transport.read_file(source) == b"content"
        assert not transport.supports_file_transfer

    asyncio.run(run_test())


def test_local_transport_drains_stdout_and_stderr_concurrently():
    """Both pipes must be drained while the child is still running."""

    async def run_test():
        transport = LocalTarget()
        chunks = 64
        chunk_size = 8192
        command = await transport.start(
            (
                sys.executable,
                "-c",
                f"import sys\nchunk = b'x' * {chunk_size}\nfor _ in range({chunks}):\n    sys.stdout.buffer.write(chunk)\n    sys.stdout.buffer.flush()\n    sys.stderr.buffer.write(chunk)\n    sys.stderr.buffer.flush()\n",
            )
        )
        stdout, stderr, returncode = await asyncio.wait_for(
            asyncio.gather(command.stdout.read(), command.stderr.read(), command.wait()), timeout=10
        )
        assert returncode == 0
        expected_size = chunks * chunk_size
        assert len(stdout) == expected_size
        assert len(stderr) == expected_size

    asyncio.run(run_test())


def test_ssh_transport_quotes_remote_command():
    transport = SshTarget("cluster", options=("-p", "2222"))
    assert transport._command(("printf", "%s", "a b"), cwd="/tmp/a b") == [
        "ssh",
        "-p",
        "2222",
        "cluster",
        "cd '/tmp/a b' && exec printf %s 'a b'",
    ]


def test_transport_and_file_version_use_identical_digest(tmp_path):
    source = tmp_path / "large.bin"
    payload = b"one" * 400000 + b"two"
    source.write_bytes(payload)
    expected = hashlib.sha256(payload).hexdigest()
    assert file_digest(source) == expected
    assert file_digest(source, missing_ok=True) == expected
    assert FileVersion.capture(source, require_digest=True).digest == expected
    assert asyncio.run(LocalTarget().file_digest(source)) == expected


def test_transport_missing_file_is_optional_but_provenance_digest_is_strict(tmp_path):
    missing = tmp_path / "missing"
    assert file_digest(missing, missing_ok=True) is None
    assert asyncio.run(LocalTarget().file_digest(missing)) is None
    with pytest.raises(FileNotFoundError):
        file_digest(missing)


def test_ssh_and_local_globstar_agree_on_hidden_entries(tmp_path):
    """Exercise both targets, including Python glob's hidden-entry rules."""
    for name in (
        "shown/one.dat",
        "shown/.private.dat",
        ".hidden/other.dat",
        "shown/.hidden/deep.dat",
        "shown/nested/two.dat",
        "shown/nested/.another.dat",
    ):
        destination = tmp_path / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(name)

    async def check():
        for pattern in (
            "**/*.dat",
            "shown/**/*.dat",
            "**/.*.dat",
            "**/.hidden/*.dat",
            ".hidden/*.dat",
            "shown/one.dat",
        ):
            local = await LocalTarget().expand_files(pattern, tmp_path)
            remote = await _LoopbackSshTransport().expand_files(pattern, tmp_path)
            assert remote == local

    asyncio.run(check())


def test_strict_transfer_refuses_wildcard_symlink_outside_source_root(tmp_path):
    import pytest

    from kkrunner.files.paths import expand_local_candidates
    from kkrunner.files.queries import expand_local_files

    root = tmp_path / "root"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    target = outside / "sensitive.dat"
    target.write_text("protected")
    alias = root / "alias.dat"
    alias.symlink_to(target)
    assert expand_local_candidates(str(root / "*.dat")) == (str(alias),)
    with pytest.raises(ValueError, match="outside source root"):
        expand_local_files(str(root / "*.dat"), root)
    assert target.read_text() == "protected"


class _LoopbackSshTransport(SshTarget):
    """Exercise the real SSH transfer implementation without an SSH server."""

    def __init__(self, *, compression="auto"):
        super().__init__("local-test", compression=compression, python_command=(sys.executable,))

    def _command(self, argv, cwd=None):
        assert cwd is None
        return tuple((os.fspath(part) for part in argv))


def test_ssh_batch_tar_upload_and_download_share_pipeline(tmp_path):
    source = tmp_path / "upload source"
    (source / "nested").mkdir(parents=True)
    (source / "nested" / "one.dat").write_text("one")
    (source / "nested" / "two.dat").write_text("two")
    remote = tmp_path / "remote target"
    downloaded = tmp_path / "download destination"
    transport = _LoopbackSshTransport()

    async def check():
        sent = await transport.put_files(("nested/one.dat", "nested/two.dat"), source, remote)
        assert len(sent) == 2
        assert all((os.path.isfile(destination) for _, destination in sent))
        received = await transport.get_files(("nested/one.dat", "nested/two.dat"), remote, downloaded)
        assert len(received) == 2
        assert all((os.path.isfile(destination) for _, destination in received))

    asyncio.run(check())
    for name in ("one.dat", "two.dat"):
        expected = (source / "nested" / name).read_text()
        assert (remote / "nested" / name).read_text() == expected
        assert (downloaded / "nested" / name).read_text() == expected


@pytest.mark.parametrize("failing_side", ["producer", "consumer"])
def test_ssh_batch_tar_pipeline_reports_subprocess_failures(failing_side):
    transport = _LoopbackSshTransport()
    producer = (
        sys.executable,
        "-c",
        f"import sys; sys.stdout.buffer.write(b'data'); sys.stdout.buffer.flush(); sys.stderr.write('producer failed'); sys.exit({(7 if failing_side == 'producer' else 0)})",
    )
    consumer = (
        sys.executable,
        "-c",
        f"import sys; sys.stdin.buffer.read(); sys.stderr.write('consumer failed'); sys.exit({(9 if failing_side == 'consumer' else 0)})",
    )

    async def check():
        with pytest.raises(__import__("subprocess").CalledProcessError) as exc:
            await asyncio.wait_for(transport._transfer_tar(producer, consumer), timeout=5)
        expected = 7 if failing_side == "producer" else 9
        assert exc.value.returncode == expected
        assert (failing_side + " failed").encode() in exc.value.stderr

    asyncio.run(check())


def test_ssh_batch_tar_pipeline_reaps_producer_after_early_receiver_failure():
    producer = (
        sys.executable, "-c",
        "import os; chunk = b'x' * 1048576\nwhile True: os.write(1, chunk)",
    )
    consumer = (
        sys.executable, "-c",
        "import sys; sys.stdin.buffer.read(1); sys.stderr.write('destination full'); sys.exit(9)",
    )

    async def check():
        with pytest.raises(subprocess.CalledProcessError) as error:
            await asyncio.wait_for(_LoopbackSshTransport()._transfer_tar(producer, consumer), timeout=5)
        assert error.value.returncode == 9
        assert b"destination full" in error.value.stderr

    asyncio.run(check())


def test_ssh_batch_tar_pipeline_drains_stderr_during_copy():
    transport = _LoopbackSshTransport()
    producer = (
        sys.executable,
        "-c",
        "import sys; sys.stderr.buffer.write(b'e' * 262144); sys.stderr.buffer.flush(); sys.stdout.buffer.write(b'payload')",
    )
    consumer = (sys.executable, "-c", "import sys; assert sys.stdin.buffer.read() == b'payload'")
    asyncio.run(asyncio.wait_for(transport._transfer_tar(producer, consumer), timeout=5))


async def _wait_for_command_pid(path):
    for _ in range(500):
        if path.exists():
            contents = path.read_text().strip()
            if contents:
                return int(contents)
        await asyncio.sleep(0.01)
    raise AssertionError("Subprocess never reported its PID")


async def _cancel_and_check_reaped(task, pid):
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=5)
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


@pytest.mark.skipif(os.name != "posix", reason="PID cleanup assertion is POSIX-only")
@pytest.mark.parametrize("factory", (LocalTarget, _LoopbackSshTransport))
def test_transport_run_cancellation_reaps_subprocess(tmp_path, factory):
    pid_file = tmp_path / "command.pid"

    async def check():
        command = (
            sys.executable,
            "-c",
            f"import os, time; open({str(pid_file)!r}, 'w').write(str(os.getpid())); time.sleep(30)",
        )
        task = asyncio.create_task(factory().run(command))
        pid = None
        try:
            pid = await _wait_for_command_pid(pid_file)
            await _cancel_and_check_reaped(task, pid)
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            if pid is not None:
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass

    asyncio.run(check())


@pytest.mark.skipif(os.name != "posix", reason="PID cleanup assertion is POSIX-only")
def test_ssh_batch_tar_cancellation_reaps_both_processes(tmp_path):
    producer_pid, consumer_pid = tmp_path / "producer.pid", tmp_path / "consumer.pid"
    producer = (
        sys.executable, "-c",
        f"import os; open({str(producer_pid)!r}, 'w').write(str(os.getpid())); "
        "chunk = b'x' * 1048576\nwhile True: os.write(1, chunk)",
    )
    consumer = (
        sys.executable, "-c",
        f"import os, sys, time; open({str(consumer_pid)!r}, 'w').write(str(os.getpid())); "
        "sys.stdin.buffer.read(1); time.sleep(0.3); sys.stdin.buffer.read()",
    )

    async def check():
        task = asyncio.create_task(_LoopbackSshTransport()._transfer_tar(producer, consumer))
        pids = []
        try:
            for path in (producer_pid, consumer_pid):
                pids.append(await _wait_for_command_pid(path))
            await _cancel_and_check_reaped(task, pids[0])
            with pytest.raises(ProcessLookupError):
                os.kill(pids[1], 0)
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            for pid in pids:
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass

    asyncio.run(check())


@pytest.mark.skipif(os.name != "posix", reason="Requires POSIX shell and PID semantics")
def test_ssh_get_file_cancellation_reaps_process_and_removes_temp(tmp_path):
    pid_file = tmp_path / "download.pid"
    source = tmp_path / "source"
    destination = tmp_path / "downloaded"
    source.write_bytes(b"source bytes")
    destination.write_bytes(b"old destination")

    class SlowDownloadTransport(_LoopbackSshTransport):
        def _command(self, argv, cwd=None):
            if argv[0] == "cat":
                return (
                    sys.executable,
                    "-c",
                    f"import os, time; open({str(pid_file)!r}, 'w').write(str(os.getpid())); time.sleep(30)",
                )
            return super()._command(argv, cwd)

    async def check():
        task = asyncio.create_task(SlowDownloadTransport().get_file(source, destination))
        pid = None
        try:
            pid = await _wait_for_command_pid(pid_file)
            await _cancel_and_check_reaped(task, pid)
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            if pid is not None:
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass

    asyncio.run(check())
    assert destination.read_bytes() == b"old destination"
    assert not tuple(tmp_path.glob(".downloaded.*.tmp"))


@pytest.mark.skipif(os.name != "posix", reason="Requires POSIX shell tools")
def test_ssh_get_file_reports_transport_io_failure(tmp_path):
    source = tmp_path / "source"
    source.write_bytes(b"source bytes")
    destination = tmp_path / "downloaded"

    class FailingDownloadTransport(_LoopbackSshTransport):
        def _command(self, argv, cwd=None):
            if argv[0] == "cat":
                return (sys.executable, "-c", "import sys; sys.stderr.write('download failed'); sys.exit(17)")
            return super()._command(argv, cwd)

    with pytest.raises(OSError, match="exit status 17.*download failed") as failure:
        asyncio.run(FailingDownloadTransport().get_file(source, destination))
    assert isinstance(failure.value.__cause__, subprocess.CalledProcessError)
    assert not destination.exists()
    assert not tuple(tmp_path.glob(".downloaded.*.tmp"))


@pytest.mark.parametrize(
    "method, expected", [("file_digest", "1234abcd"), ("file_signature", (10, 1729000000250000000))]
)
@pytest.mark.parametrize("status", ("ok", "missing", "error"))
def test_ssh_file_queries_distinguish_absence_and_errors(method, expected, status):
    calls = []
    path = "folder name/result.out"
    item = {"path": path, "status": status, "size": 10, "mtime_ns": 1729000000250000000, "type": "file"}
    if status == "ok":
        item["digest"] = "1234abcd"
    elif status == "error":
        item["error"] = {"errno": 13, "message": "permission denied"}

    class ProbedTransport(SshTarget):
        async def run(self, argv, *, cwd=None, input=None, check=True):
            calls.append((argv, json.loads(input), check))
            return subprocess.CompletedProcess(argv, 0, stdout=json.dumps({"version": 1, "result": [item]}).encode(), stderr=b"")

    query = getattr(ProbedTransport("example-host"), method)
    if status == "error":
        with pytest.raises(PermissionError):
            asyncio.run(query(path))
    else:
        assert asyncio.run(query(path)) == (None if status == "missing" else expected)
    assert len(calls) == 1
    command, payload, checked = calls[0]
    assert command[:2] == ("python3", "-c")
    assert "def query_files(" in command[2]
    assert payload == {
        "args": [[path]],
        "kwargs": {"root": None, "digest": method == "file_digest"},
    }
    assert checked is True

def test_ssh_file_query_reports_transport_io_failure():
    class Failed(SshTarget):
        async def start(self, argv, *, cwd=None, stdin=None):
            return await LocalTarget().start(
                (sys.executable, "-c", "import sys; sys.stderr.write('connection failed'); sys.exit(255)"), stdin=stdin
            )

    with pytest.raises(OSError, match="exit status 255.*connection failed") as error:
        asyncio.run(Failed("host").expand_files("*.dat", "/root"))
    assert isinstance(error.value.__cause__, subprocess.CalledProcessError)


@pytest.mark.parametrize("compression", (None, "auto", "gz", "bz2", "xz", "zstd"))
def test_ssh_batch_exact_pairs_and_compression(tmp_path, compression):
    from kkrunner.files.transfer import archive_compressions
    if compression not in (None, "auto") and compression not in archive_compressions():
        pytest.skip(f"{compression} unavailable in this Python")

    source = tmp_path / "source"
    source.mkdir()
    original = source / "one.dat"
    original.write_bytes(b"content")
    timestamp = 1729000000123456789
    os.utime(original, ns=(timestamp, timestamp))
    remote, returned = tmp_path / "remote", tmp_path / "returned"
    transport = _LoopbackSshTransport(compression=compression)

    async def check():
        await transport.put_files([("one.dat", "nested/renamed.dat")], source, remote)
        await transport.get_files([("nested/renamed.dat", "back.dat")], remote, returned)

    asyncio.run(check())
    assert (remote / "nested/renamed.dat").read_bytes() == b"content"
    assert (returned / "back.dat").read_bytes() == b"content"
    assert (returned / "back.dat").stat().st_mtime_ns == timestamp


def test_ssh_batch_incomplete_archive_does_not_publish(tmp_path):
    source, remote = tmp_path / "source", tmp_path / "remote"
    source.mkdir()
    remote.mkdir()
    (source / "first").write_text("new")
    (remote / "first").write_text("old")
    with pytest.raises(OSError, match="file transfer failed"):
        asyncio.run(_LoopbackSshTransport().put_files(
            ["first", "missing"], source, remote,
        ))
    assert (remote / "first").read_text() == "old"
    assert list(remote.iterdir()) == [remote / "first"]


def test_ssh_client_compression_roundtrips_from_transport():
    from kkrunner.client import ExecutionClient
    from kkrunner.backends import DirectBackend

    for compression in (None, "auto", "gz", "bz2", "xz", "zstd"):
        client = ExecutionClient(DirectBackend, target=SshTarget("cluster", compression=compression))
        assert client.target.compression == compression
        restored = ExecutionClient.from_config(client.target.to_dict(), client.backend_config)
        assert restored.target == client.target
        assert restored.target.compression == compression

@pytest.mark.parametrize("factory", (LocalTarget, _LoopbackSshTransport))
@pytest.mark.parametrize("check", (False, True))
@pytest.mark.parametrize("send_input", (False, True))
def test_transport_run_uses_start_and_preserves_completion(factory, check, send_input):

    class Observed(factory):
        def __init__(self):
            super().__init__()
            self.started = []

        async def start(self, argv, *, cwd=None, stdin=None):
            self.started.append((tuple(argv), cwd, stdin))
            return await super().start(argv, cwd=cwd, stdin=stdin)

    transport = Observed()

    async def execute():
        return await transport.run(
            (
                sys.executable,
                "-c",
                "import sys; sys.stdout.buffer.write(sys.stdin.buffer.read()); sys.stderr.write('diagnostic'); sys.exit(7)",
            ),
            input=b"payload" if send_input else None,
            check=check,
        )

    if check:
        with pytest.raises(subprocess.CalledProcessError) as error:
            asyncio.run(execute())
        assert error.value.returncode == 7
        assert error.value.output == (b"payload" if send_input else b"")
        assert error.value.stderr == b"diagnostic"
    else:
        result = asyncio.run(execute())
        assert result.returncode == 7
        assert result.stdout == (b"payload" if send_input else b"")
        assert result.stderr == b"diagnostic"
    assert len(transport.started) == 1
    assert transport.started[0][2] == (asyncio.subprocess.PIPE if send_input else None)


def test_ssh_upload_reports_connection_failure(tmp_path):
    source = tmp_path / "source.txt"
    source.write_bytes(b"payload")
    transport = SshTarget("unused", ssh="/missing-kkrunner-ssh-executable")

    with pytest.raises(FileNotFoundError, match="missing-kkrunner-ssh-executable"):
        asyncio.run(transport.put_file(source, "/remote/destination.txt"))


def test_ssh_download_preserves_original_error_when_temp_unlink_fails(tmp_path, monkeypatch):
    from kkrunner.files import staging as module

    class UnavailableTarget(SshTarget):
        async def file_signature(self, path):
            return (5, 1000000000)

    transport = UnavailableTarget("unused", ssh="/missing-kkrunner-ssh-executable")
    staged = tmp_path / "staged.tmp"
    cleanup_calls = []

    class UndeletableTemporary:
        def open(self, *args, **kwargs):
            return staged.open(*args, **kwargs)

        def unlink(self, *, missing_ok=False):
            cleanup_calls.append(True)
            raise PermissionError("cannot remove staged download")

    monkeypatch.setattr(module, "_local_temporary_path", lambda destination: UndeletableTemporary())

    async def check():
        with pytest.raises(FileNotFoundError) as original:
            await transport.get_file("/remote/output", tmp_path / "result")
        assert "missing-kkrunner-ssh-executable" in str(original.value)

    asyncio.run(check())
    assert cleanup_calls == [True]
    assert not (tmp_path / "result").exists()


def test_agent_ssh_uses_raw_portable_file_operations(tmp_path):
    """File queries and upload publication must never invoke the agent."""
    import asyncio
    from kkrunner.files.versions import file_digest

    transport = _LoopbackSshTransport()
    transport.command = ("__must_not_invoke_agent__",)
    source = tmp_path / "sample"
    source.write_text("abc")
    destination = tmp_path / "published"
    mtime_ns = 1729000000123456789
    os.utime(source, ns=(mtime_ns, mtime_ns))

    async def verify():
        entries = await transport.query_files([str(source)], digest=True)
        assert entries[0]["digest"] == file_digest(source)
        await transport.put_file(source, destination)

    asyncio.run(verify())
    assert destination.read_text() == "abc"
    assert destination.stat().st_mtime_ns == mtime_ns


def test_ssh_client_retains_target_python_configuration():
    from kkrunner.client import ExecutionClient
    from kkrunner.backends import DirectBackend

    client = ExecutionClient(
        DirectBackend, target=SshTarget("cluster", python_command=("python3.12",)),
    )
    assert client.target.python_command == ("python3.12",)
    restored = ExecutionClient.from_config(client.target.to_dict(), client.backend_config)
    assert restored.target == client.target


@pytest.mark.parametrize("existing", [False, True])
def test_local_staging_verifies_before_atomic_publication(tmp_path, existing):
    from kkrunner.files.staging import staged_local_file

    destination = tmp_path / "artifact"
    if existing:
        destination.write_bytes(b"old")

    def verify(path):
        assert path.parent == destination.parent
        assert path.read_bytes() == b"new"
        assert destination.exists() == existing
        raise OSError("historical digest mismatch")

    with pytest.raises(OSError, match="digest mismatch"):
        with staged_local_file(destination, verify=verify, overwrite=False) as temporary:
            temporary.write_bytes(b"new")
    assert destination.exists() == existing
    if existing:
        assert destination.read_bytes() == b"old"
    assert not list(tmp_path.glob(".*.tmp"))


def test_local_staging_does_not_replace_concurrent_destination(tmp_path):
    from kkrunner.files.staging import staged_local_file

    destination = tmp_path / "artifact"
    with pytest.raises(FileExistsError):
        with staged_local_file(destination, overwrite=False) as temporary:
            temporary.write_bytes(b"old result")
            destination.write_bytes(b"new result")
    assert destination.read_bytes() == b"new result"
    assert list(tmp_path.iterdir()) == [destination]


@pytest.mark.parametrize("factory", [_LoopbackSshTransport])
def test_directory_upload_replaces_whole_tree(tmp_path, factory):
    source, target = tmp_path / "source", tmp_path / "target"
    source.mkdir()
    target.mkdir()
    (source / "new").write_text("new")
    (target / "old").write_text("old")

    async def transfer():
        await factory().put_directory(source, target)

    asyncio.run(transfer())
    assert sorted(p.name for p in target.iterdir()) == ["new"]
    assert (target / "new").read_text() == "new"


def test_large_batch_manifest_is_not_passed_in_argv(tmp_path):
    source, remote = tmp_path / "source", tmp_path / "remote"
    transport = _LoopbackSshTransport()
    names = ["TAU/" + str(i).zfill(8) + "x" * 100 for i in range(3000)]
    calls = []

    async def transfer(producer, consumer, *, producer_input):
        calls.append((producer, consumer))
        assert json.load(producer_input) == names
        assert all(names[0] not in argument for argument in (*producer, *consumer))

    transport._transfer_tar = transfer
    result = asyncio.run(transport.put_files(
        names, source_directory=source, destination_directory=remote,
    ))
    assert len(calls) == 1
    assert len(result) == len(names)


@pytest.mark.parametrize("factory", [_LoopbackSshTransport])
def test_exact_batch_does_not_expand_wildcards_and_allows_rename(tmp_path, factory):
    source, remote = tmp_path / "source", tmp_path / "remote"
    source.mkdir()
    (source / "literal[1].dat").write_text("literal")
    (source / "literal1.dat").write_text("other")
    asyncio.run(factory().put_files(
        ["literal[1].dat", ("literal[1].dat", "renamed")], source, remote,
    ))
    assert (remote / "literal[1].dat").read_text() == "literal"
    assert (remote / "renamed").read_text() == "literal"
    assert not (remote / "literal1.dat").exists()


def test_single_ssh_upload_accepts_explicit_symlink_source(tmp_path):
    actual = tmp_path / "source" / "data"
    actual.parent.mkdir()
    actual.write_text("data")
    alias = tmp_path / "alias" / "data"
    alias.parent.mkdir()
    alias.symlink_to(actual)
    target = tmp_path / "remote" / "data"
    asyncio.run(_LoopbackSshTransport().put_file(alias, target))
    assert target.read_text() == "data"


@pytest.mark.parametrize("local,remote,expected", [
    (["gz", "zstd"], ["gz", "zstd"], "zst"),
    (["gz"], ["gz", "zstd"], "gz"),
    (["gz", "zstd"], ["gz"], "gz"),
])
def test_auto_compression_negotiates_and_caches(monkeypatch, local, remote, expected):
    from kkrunner.targets.implementations.ssh import transport as module

    probes = []
    queries = []

    def local_probe():
        probes.append(True)
        return local

    monkeypatch.setattr(module, "archive_compressions", local_probe)

    class Target(_LoopbackSshTransport):
        async def call_function(self, function, *args, **kwargs):
            queries.append(function)
            return remote

    target = Target()
    assert asyncio.run(target._archive_compression()) == expected
    assert asyncio.run(target._archive_compression()) == expected
    assert len(probes) == len(queries) == 1
    target.close()
    assert asyncio.run(target._archive_compression()) == expected
    assert len(probes) == len(queries) == 2


def test_explicit_unsupported_compression_does_not_fall_back(monkeypatch):
    from kkrunner.targets.implementations.ssh import transport as module

    monkeypatch.setattr(module, "archive_compressions", lambda: ["gz", "zstd"])

    class Target(_LoopbackSshTransport):
        async def call_function(self, function, *args, **kwargs):
            return ["gz"]

    with pytest.raises(ValueError, match="zstd.*not supported"):
        asyncio.run(Target(compression="zstd")._archive_compression())


@pytest.mark.parametrize("compression", (None, "auto", "gz", "bz2", "xz", "zstd"))
def test_ssh_directory_roundtrip_uses_native_compression(tmp_path, compression):
    from kkrunner.files.transfer import archive_compressions
    if compression not in (None, "auto") and compression not in archive_compressions():
        pytest.skip(f"{compression} unavailable in this Python")
    source = tmp_path / "source"
    source.mkdir()
    (source / "empty").mkdir()
    (source / "nested").mkdir()
    (source / "nested" / ".file").write_bytes(b"payload")
    target, returned = tmp_path / "target", tmp_path / "returned"
    transport = _LoopbackSshTransport(compression=compression)

    async def transfer():
        await transport.put_directory(source, target)
        await transport.get_directory(target, returned)

    asyncio.run(transfer())
    assert (returned / "empty").is_dir()
    assert (returned / "nested" / ".file").read_bytes() == b"payload"


def test_legacy_persisted_compression_is_readable():
    from kkrunner.targets import target_from_dict

    for old, compression in ((False, None), (True, "zstd")):
        transport = target_from_dict({"kind": "ssh", "host": "cluster", "zstd": old})
        assert transport.compression == compression
        assert "zstd" not in transport.to_dict()



@pytest.mark.parametrize("compression", ["", "gz", "bz2", "xz", "zst"])
@pytest.mark.parametrize("damage", ["truncated", "extra_member", "extra_data"])
def test_archive_receiver_rejects_invalid_end_before_publication(tmp_path, compression, damage):
    import io
    import tarfile
    from kkrunner.files.transfer import archive_compressions, receive_archive

    if compression and ("zstd" if compression == "zst" else compression) not in archive_compressions():
        pytest.skip(f"{compression} unavailable in this Python")

    archive_bytes = io.BytesIO()
    with tarfile.open(fileobj=archive_bytes, mode="w|", format=tarfile.PAX_FORMAT) as archive:
        content = b"new"
        item = tarfile.TarInfo("result")
        item.size = len(content)
        item.pax_headers["kkrunner.mtime_ns"] = "1700000000000000000"
        archive.addfile(item, io.BytesIO(content))
        marker = tarfile.TarInfo(".kkrunner-complete")
        marker.pax_headers["kkrunner.complete"] = "1"
        archive.addfile(marker)
        if damage == "extra_member":
            archive.addfile(tarfile.TarInfo("unexpected"))
    data = archive_bytes.getvalue()
    if damage == "extra_data":
        # A second TAR archive after the original zero-block trailer must
        # not disappear in tarfile's read-ahead buffer.
        data += b"not TAR padding"

    if compression == "gz":
        import gzip
        data = gzip.compress(data)
    elif compression == "bz2":
        import bz2
        data = bz2.compress(data)
    elif compression == "xz":
        import lzma
        data = lzma.compress(data)
    elif compression == "zst":
        from compression.zstd import compress
        data = compress(data)
    if damage == "truncated":
        data = data[:-1]

    destination = tmp_path / "target"
    destination.mkdir()
    (destination / "result").write_text("old")
    transport = _LoopbackSshTransport(compression=None)
    command = transport._archive_command(receive_archive, destination, compression, remote=False)
    process = subprocess.run(command, input=data, capture_output=True)
    assert process.returncode != 0
    assert (destination / "result").read_text() == "old"
    assert list(destination.iterdir()) == [destination / "result"]


def test_gzip_archive_crc_failure_does_not_publish(tmp_path):
    import gzip
    from kkrunner.files.transfer import send_archive, receive_archive

    source, target = tmp_path / "source", tmp_path / "target"
    source.mkdir()
    target.mkdir()
    (source / "file").write_bytes(b"new")
    (target / "file").write_bytes(b"old")
    transport = _LoopbackSshTransport(compression="gz")
    command = transport._archive_command(send_archive, source, "gz", remote=False)
    data = bytearray(subprocess.run(command, input=b'["file"]', capture_output=True, check=True).stdout)
    data[-8] ^= 1  # gzip CRC, not TAR payload
    with pytest.raises(gzip.BadGzipFile):
        gzip.decompress(data)
    command = transport._archive_command(receive_archive, target, "gz", remote=False)
    result = subprocess.run(command, input=data, capture_output=True)
    assert result.returncode != 0
    assert (target / "file").read_bytes() == b"old"
    assert list(target.iterdir()) == [target / "file"]


@pytest.mark.skipif(os.name == "nt", reason="POSIX permission bits")
def test_ssh_directory_transfer_preserves_permissions(tmp_path):
    import stat

    source, target, returned = (tmp_path / name for name in ("source", "target", "returned"))
    nested = source / "read-only"
    nested.mkdir(parents=True)
    script = nested / "script"
    script.write_text("echo hello")
    script.chmod(0o751)
    nested.chmod(0o555)
    source.chmod(0o750)
    transport = _LoopbackSshTransport()

    async def transfer():
        await transport.put_directory(source, target)
        # Replacing a tree with read-only subdirectories must also work.
        await transport.put_directory(source, target)
        await transport.get_directory(target, returned)
        await transport.get_directory(target, returned)

    try:
        asyncio.run(transfer())
        for root in (target, returned):
            assert stat.S_IMODE(root.stat().st_mode) == 0o750
            assert stat.S_IMODE((root / "read-only").stat().st_mode) == 0o555
            assert stat.S_IMODE((root / "read-only" / "script").stat().st_mode) == 0o751
            assert (root / "read-only" / "script").read_text() == "echo hello"
    finally:
        for root in (source, target, returned):
            if (root / "read-only").is_dir():
                (root / "read-only").chmod(0o755)


def test_archive_sender_propagates_directory_read_error(tmp_path, monkeypatch):
    import io
    from types import SimpleNamespace
    from kkrunner.files.transfer import send_archive

    source = tmp_path / "source"
    source.mkdir()

    nested = source / "nested"
    nested.mkdir()
    (nested / "data").write_bytes(b"content")
    scandir = os.scandir

    def failing_scandir(path):
        if os.fspath(path) == str(nested):
            raise PermissionError("cannot read nested directory")
        return scandir(path)

    monkeypatch.setattr(os, "scandir", failing_scandir)
    monkeypatch.setattr(sys, "stdout", SimpleNamespace(buffer=io.BytesIO()))
    # Inject a real traversal error even when the tests run as root.
    with pytest.raises(PermissionError, match="cannot read nested directory"):
        send_archive(source, tree=True)


@pytest.mark.parametrize("failure_phase", ["discovery", "download"])
def test_historical_retrieval_continues_after_ssh_command_failure(tmp_path, monkeypatch, failure_phase):
    from kkrunner import RunManager, RunState, RunStore
    from kkrunner.files import FileEntry, FileManifest
    from kkrunner.files.manifest import FileMapping

    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    bad, good = local / "bad.dat", local / "good.dat"
    for path in (bad, good):
        (remote / path.name).write_text(path.name)

    def command(self, argv, cwd=None):
        assert cwd is None
        if argv[0] == "cat" and os.fspath(argv[-1]) == str(remote / bad.name):
            if failure_phase == "download":
                return (sys.executable, "-c",
                        "import sys; sys.stderr.write('connection lost'); sys.exit(255)")
        return tuple(os.fspath(part) for part in argv)

    monkeypatch.setattr(SshTarget, "_command", command)
    if failure_phase == "discovery":
        original = SshTarget.run

        async def run(self, argv, *, cwd=None, input=None, check=True):
            if input is not None and json.loads(input)["args"][0] == [str(remote / bad.name)]:
                raise subprocess.CalledProcessError(255, argv, stderr=b"connection lost")
            return await original(self, argv, cwd=cwd, input=input, check=check)

        monkeypatch.setattr(SshTarget, "run", run)

    target = SshTarget("loopback", pool_size=0, python_command=(sys.executable,))
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = manager.new_record(
        directory=local, remote_directory=remote, target=target,
        files=FileManifest(
            outputs=tuple(FileEntry(str(path), str(remote / path.name)) for path in (bad, good)),
            mapping=FileMapping(str(local), str(remote)),
        ),
    )
    record = manager.transition(record, state=RunState.FAILED,
                                execution=record.execution.submitted("historical"))
    restored = manager.retrieve_result_files(record)
    assert not bad.exists()
    assert good.read_text() == good.name
    failed, available = restored.files.outputs
    assert failed.availability_state == "failed"
    assert "connection lost" in failed.availability_message
    assert available.availability_state == "available"
