"""Tests for command/file transports."""

import asyncio
import hashlib
import json
import os
import signal
import subprocess
import sys

import pytest

from kkrunner.files.versions import FileVersion, file_digest
from kkrunner.client.transports import LocalTransport, SshTransport


def test_local_transport_run_start_and_files(tmp_path):

    async def run_test():
        transport = LocalTransport()
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
        copied = tmp_path / "subdir" / "copied"
        returned = tmp_path / "returned"
        source.write_bytes(b"content")
        await transport.put_file(source, copied)
        assert await transport.read_file(copied) == b"content"
        await transport.get_file(copied, returned)
        assert returned.read_bytes() == b"content"

    asyncio.run(run_test())


def test_local_transport_preserves_file_signature(tmp_path):

    async def run_test():
        transport = LocalTransport()
        source = tmp_path / "source"
        destination = tmp_path / "destination"
        source.write_bytes(b"content")
        stat = source.stat()
        os.utime(source, ns=(stat.st_atime_ns, stat.st_mtime_ns - 1000000000))
        source_signature = await transport.file_signature(source)
        await transport.get_file(source, destination)
        assert await transport.file_signature(destination) == source_signature

    asyncio.run(run_test())


def test_local_transport_drains_stdout_and_stderr_concurrently():
    """Both pipes must be drained while the child is still running."""

    async def run_test():
        transport = LocalTransport()
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
    transport = SshTransport("cluster", options=("-p", "2222"))
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
    assert asyncio.run(LocalTransport().file_digest(source)) == expected


def test_transport_missing_file_is_optional_but_provenance_digest_is_strict(tmp_path):
    missing = tmp_path / "missing"
    assert file_digest(missing, missing_ok=True) is None
    assert asyncio.run(LocalTransport().file_digest(missing)) is None
    with pytest.raises(FileNotFoundError):
        file_digest(missing)


def test_ssh_and_local_globstar_agree_on_hidden_entries(tmp_path):
    """Exercise both transports, including Python glob's hidden-entry rules."""
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
            local = await LocalTransport().expand_files(pattern, tmp_path)
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


class _LoopbackSshTransport(SshTransport):
    """Exercise the real SSH transfer implementation without an SSH server."""

    def __init__(self, *, zstd=False):
        super().__init__("local-test", zstd=zstd, python_command=(sys.executable,))

    def _command(self, argv, cwd=None):
        assert cwd is None
        return tuple((os.fspath(part) for part in argv))


def test_ssh_batch_tar_upload_and_download_share_pipeline(tmp_path):
    import shutil

    if shutil.which("tar") is None:
        pytest.skip("tar executable unavailable")
    source = tmp_path / "upload source"
    (source / "nested").mkdir(parents=True)
    (source / "nested" / "one.dat").write_text("one")
    (source / "nested" / "two.dat").write_text("two")
    remote = tmp_path / "remote target"
    downloaded = tmp_path / "download destination"
    transport = _LoopbackSshTransport()

    async def check():
        sent = await transport.put_files("nested/*.dat", source, remote)
        assert len(sent) == 2
        assert all((os.path.isfile(destination) for _, destination in sent))
        received = await transport.get_files("nested/*.dat", remote, downloaded)
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
@pytest.mark.parametrize("factory", (LocalTransport, _LoopbackSshTransport))
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
def test_ssh_get_file_preserves_subprocess_failure(tmp_path):
    source = tmp_path / "source"
    source.write_bytes(b"source bytes")
    destination = tmp_path / "downloaded"

    class FailingDownloadTransport(_LoopbackSshTransport):
        def _command(self, argv, cwd=None):
            if argv[0] == "cat":
                return (sys.executable, "-c", "import sys; sys.stderr.write('download failed'); sys.exit(17)")
            return super()._command(argv, cwd)

    with pytest.raises(subprocess.CalledProcessError) as failure:
        asyncio.run(FailingDownloadTransport().get_file(source, destination))
    assert failure.value.returncode == 17
    assert b"download failed" in failure.value.stderr
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

    class ProbedTransport(SshTransport):
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

def test_ssh_file_query_preserves_command_errors():
    class Failed(SshTransport):
        async def start(self, argv, *, cwd=None, stdin=None):
            return await LocalTransport().start(
                (sys.executable, "-c", "import sys; sys.stderr.write('connection failed'); sys.exit(255)"), stdin=stdin
            )

    with pytest.raises(subprocess.CalledProcessError) as error:
        asyncio.run(Failed("host").expand_files("*.dat", "/root"))
    assert error.value.returncode == 255
    assert error.value.stderr == b"connection failed"


@pytest.mark.parametrize("zstd", (False, True))
def test_ssh_batch_tar_uses_explicit_compression_without_probing(tmp_path, zstd):
    source = tmp_path / "source"
    source.mkdir()
    (source / "one.dat").write_bytes(b"content")
    destination = tmp_path / "remote"
    returned = tmp_path / "returned"
    transport = _LoopbackSshTransport(zstd=zstd)
    commands = []

    async def record(producer, consumer):
        commands.append((producer, consumer))

    transport._transfer_tar = record

    async def check():
        await transport.put_files("one.dat", source, destination)
        await transport.get_files("one.dat", source, returned)

    asyncio.run(check())
    assert len(commands) == 2
    for producer, consumer in commands:
        assert ("--zstd" in producer) is zstd
        assert ("--zstd" in consumer) is zstd


def test_ssh_client_compression_roundtrips_from_transport():
    from kkrunner.client import EndpointClient
    from kkrunner.backends import DirectBackend

    for compressed in (False, True):
        client = EndpointClient(DirectBackend, transport=SshTransport("cluster", zstd=compressed))
        assert client.transport.zstd is compressed
        restored = EndpointClient.from_config(client.transport.to_dict(), client.backend_config)
        assert restored.transport == client.transport
        assert restored.transport.zstd is compressed

@pytest.mark.parametrize("factory", (LocalTransport, _LoopbackSshTransport))
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


def test_ssh_upload_preserves_original_error_when_remote_cleanup_fails(tmp_path):
    source = tmp_path / "source.txt"
    source.write_bytes(b"payload")
    commands = []

    class FailingUpload(SshTransport):
        async def run(self, argv, *, cwd=None, input=None, check=True):
            commands.append(tuple(argv))
            if argv[0] == "rm":
                raise OSError("cleanup connection unavailable")
            return subprocess.CompletedProcess(argv, 0, b"", b"")

    transport = FailingUpload("unused", ssh="/missing-kkrunner-ssh-executable")

    async def check():
        with pytest.raises(FileNotFoundError) as original:
            await transport.put_file(source, "/remote/destination.txt")
        assert "missing-kkrunner-ssh-executable" in str(original.value)

    asyncio.run(check())
    assert [command[0] for command in commands] == ["mkdir", "rm"]


def test_ssh_download_preserves_original_error_when_temp_unlink_fails(tmp_path, monkeypatch):
    from kkrunner.client.transports import ssh as module

    class UnavailableTransport(SshTransport):
        async def file_signature(self, path):
            return (5, 1000000000)

    transport = UnavailableTransport("unused", ssh="/missing-kkrunner-ssh-executable")
    staged = tmp_path / "staged.tmp"
    cleanup_calls = []

    class UndeletableTemporary:
        def open(self, *args, **kwargs):
            return staged.open(*args, **kwargs)

        def unlink(self):
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
    temporary = tmp_path / "stage"
    destination = tmp_path / "published"
    temporary.write_text("stored")
    mtime_ns = 1729000000123456789

    async def verify():
        entries = await transport.query_files([str(source)], digest=True)
        assert entries[0]["digest"] == file_digest(source)
        await transport._finalize_upload(str(temporary), str(destination), mtime_ns)

    asyncio.run(verify())
    assert destination.read_text() == "stored"
    assert destination.stat().st_mtime_ns == mtime_ns


def test_ssh_client_retains_target_python_configuration():
    from kkrunner.client import EndpointClient
    from kkrunner.backends import DirectBackend

    client = EndpointClient(
        DirectBackend, transport=SshTransport("cluster", python_command=("python3.12",)),
    )
    assert client.transport.python_command == ("python3.12",)
    restored = EndpointClient.from_config(client.transport.to_dict(), client.backend_config)
    assert restored.transport == client.transport
