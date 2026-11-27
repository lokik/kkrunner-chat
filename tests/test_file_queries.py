"""Portable file service contracts and real CLI round trips."""

import asyncio
import errno
import hashlib
import os
import stat
import subprocess
import sys

import pytest

from kkrunner.files import queries
from kkrunner.targets import LocalTarget

from .test_transport import _LoopbackSshTransport


def test_directory_digest_covers_dotfiles_and_empty_directories(tmp_path):
    first, second = tmp_path / "first", tmp_path / "second"
    for directory in (first, second):
        (directory / "nested" / "empty").mkdir(parents=True)
        (directory / "nested" / ".hidden").write_bytes(b"secret")
        (directory / "plain").write_bytes(b"visible")
    assert queries.directory_digest(first) == queries.directory_digest(second)
    (second / "nested" / ".hidden").write_bytes(b"changed")
    assert queries.directory_digest(first) != queries.directory_digest(second)
    (second / "nested" / ".hidden").write_bytes(b"secret")
    (second / "nested" / "empty").rmdir()
    assert queries.directory_digest(first) != queries.directory_digest(second)
    (second / "nested" / "alias").symlink_to(second / "plain")
    assert queries.directory_digest(second) is None


@pytest.mark.parametrize("error_number", [errno.EACCES, errno.EIO])
def test_directory_digest_propagates_nested_read_failure(tmp_path, monkeypatch, error_number):
    nested = tmp_path / "nested"
    nested.mkdir()
    (nested / "data").write_bytes(b"content")
    scandir = os.scandir

    def failing_scandir(path):
        if os.fspath(path) == str(nested):
            raise OSError(error_number, "cannot read nested directory", str(nested))
        return scandir(path)

    monkeypatch.setattr(os, "scandir", failing_scandir)
    with pytest.raises(OSError) as error:
        queries.directory_digest(tmp_path)
    assert error.value.errno == error_number


def test_directory_digest_portable_over_ssh(tmp_path):
    from kkrunner.targets.contracts import Operation

    directory = tmp_path / "tree"
    directory.mkdir()
    (directory / ".hidden").write_bytes(b"data")
    observed = asyncio.run(
        _LoopbackSshTransport().call(Operation.standalone(queries.directory_digest), str(directory))
    )
    assert observed == queries.directory_digest(directory)


def test_batch_metadata_missing_types_and_nanoseconds(tmp_path):
    regular = tmp_path / "file"
    regular.write_bytes(b"contents")
    os.utime(regular, ns=(1729000000123456789, 1729000000123456789))
    alias = tmp_path / "alias"
    alias.symlink_to(regular)
    broken = tmp_path / "broken"
    broken.symlink_to(tmp_path / "missing")
    paths = [regular, tmp_path, alias, broken, tmp_path / "missing"]
    results = queries.query_files(paths, digest=True)
    assert [item["path"] for item in results] == list(map(str, paths))
    assert results[0] == {
        "path": str(regular),
        "status": "ok",
        "size": 8,
        "mtime_ns": regular.stat().st_mtime_ns,
        "type": "file",
        "digest": hashlib.sha256(b"contents").hexdigest(),
    }
    assert results[1]["type"] == "directory"
    assert results[2]["type"] == "file"  # stat follows links
    assert results[3:] == [{"path": str(path), "status": "missing"} for path in paths[3:]]
    assert queries.query_files([]) == []


@pytest.mark.parametrize("error_number", [errno.EACCES, errno.EIO, errno.ENOTDIR])
@pytest.mark.parametrize("confined", [False, True])
def test_batch_keeps_errors_separate_from_missing(tmp_path, monkeypatch, error_number, confined):
    original = os.stat
    bad = str(tmp_path / "bad")

    def failing_stat(path, *args, **kwargs):
        if path == bad:
            raise OSError(error_number, "query failed", path)
        return original(path, *args, **kwargs)

    monkeypatch.setattr(queries.os, "stat", failing_stat)
    results = queries.query_files([bad, str(tmp_path / "missing")], root=tmp_path if confined else None)
    assert results[0]["status"] == "error"
    assert results[0]["error"]["errno"] == error_number
    assert results[1]["status"] == "missing"
    with pytest.raises(OSError) as error:
        queries.checked_files(results)
    assert error.value.errno == error_number


@pytest.mark.parametrize("factory", [LocalTarget, _LoopbackSshTransport])
def test_query_batch_and_discovery_unusual_names(tmp_path, factory):
    names = ["a b.dat", "tab\tline\nquote'\".dat", "-option.dat", "žluťoučký.dat", "[literal]*.dat"]
    paths = [tmp_path / name for name in names]
    for path in paths:
        path.write_bytes(b"data")
    (tmp_path / ".hidden.dat").write_bytes(b"hidden")
    (tmp_path / "directory.dat").mkdir()
    (tmp_path / "nested").mkdir()
    (tmp_path / "nested" / "deep.dat").write_bytes(b"deep")

    async def check():
        transport = factory()
        results = await transport.query_files(paths)
        assert [item["path"] for item in results] == list(map(str, paths))
        assert all(item["status"] == "ok" and item["size"] == 4 for item in results)
        matches = await transport.expand_files(["**/*.dat", "a b.dat", "absent*.dat"], tmp_path)
        assert matches == tuple(sorted(map(str, [*paths, tmp_path / "nested" / "deep.dat"])))
        assert await transport.expand_files(["missing", "empty*.dat"], tmp_path) == ()
        assert await transport.file_signature(tmp_path / "missing") is None
        assert await transport.file_signature(tmp_path / "directory.dat") is None
        assert await transport.file_digest(tmp_path / "directory.dat") is None
        assert await transport.file_digest(tmp_path / "missing") is None
        assert await transport.file_signature(paths[0]) == (4, paths[0].stat().st_mtime_ns)
        assert await transport.file_digest(paths[0]) == hashlib.sha256(b"data").hexdigest()

    asyncio.run(check())


def test_ssh_combines_globs_and_metadata_in_one_request(tmp_path):
    for name in ("one.dat", "two.dat", "third.txt"):
        (tmp_path / name).write_text(name)
    calls = []

    class Observed(_LoopbackSshTransport):
        async def run(self, argv, **kwargs):
            calls.append(tuple(argv))
            return await super().run(argv, **kwargs)

    items = asyncio.run(Observed().query_files(["*.dat", "*.txt"], root=tmp_path))
    assert len(items) == 3
    assert all(item["mtime_ns"] == os.stat(item["path"]).st_mtime_ns for item in items)
    assert len(calls) == 1
    assert calls[0][1] == "-c"
    assert "def query_files(" in calls[0][2]


@pytest.mark.parametrize("factory", [LocalTarget, _LoopbackSshTransport])
def test_discovery_confines_paths_and_symlinks(tmp_path, factory):
    root = tmp_path / "root"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.write_bytes(b"secret")
    (root / "alias").symlink_to(outside)
    for pattern in ("../outside", str(outside), "a/../../outside", "alias", "ali*"):
        with pytest.raises((ValueError, subprocess.CalledProcessError)):
            asyncio.run(factory().expand_files(pattern, root))


def test_ssh_single_file_transfer_preserves_nanoseconds_and_atomic_publication(tmp_path):
    source = tmp_path / "-source\nwith ' quotes"
    source.write_bytes(b"payload")
    os.utime(source, ns=(1729000000123456789, 1729000000123456789))
    remote = tmp_path / "remote" / "target"
    destination = tmp_path / "downloaded"

    async def check():
        transport = _LoopbackSshTransport()
        await transport.put_file(source, remote)
        assert remote.read_bytes() == source.read_bytes()
        assert remote.stat().st_mtime_ns == source.stat().st_mtime_ns
        await transport.get_file(remote, destination)
        assert destination.read_bytes() == source.read_bytes()
        assert destination.stat().st_mtime_ns == source.stat().st_mtime_ns
        assert await transport.file_digest(remote) == hashlib.sha256(b"payload").hexdigest()
        assert not list(remote.parent.glob(".*.tmp"))

    asyncio.run(check())


def test_remote_endpoint_reuses_configured_target_python_command():
    from kkrunner.client import ExecutionClient, SshTarget
    from kkrunner.backends.config import BackendConfig

    command = (sys.executable,)
    transport = SshTarget("host", python_command=command)
    client = ExecutionClient(target=transport)
    assert client.target.python_command == command
    restored = ExecutionClient.from_config(transport.to_dict(), client.backend_config)
    assert restored.target.python_command == command


def test_special_file_metadata_does_not_open_fifo(tmp_path):
    if not hasattr(os, "mkfifo"):
        pytest.skip("FIFOs unavailable")
    path = tmp_path / "fifo"
    os.mkfifo(path)
    assert stat.S_ISFIFO(path.stat().st_mode)
    assert queries.query_files([path], digest=True)[0]["type"] == "other"


def test_remote_discovery_preserves_symlinked_root_namespace(tmp_path):
    """macOS /tmp and /var aliases must stay inside the client's mapping."""
    real = tmp_path / "real"
    real.mkdir()
    (real / "file.dat").write_bytes(b"payload")
    alias = tmp_path / "alias"
    alias.symlink_to(real, target_is_directory=True)
    destination = tmp_path / "downloaded"

    async def check():
        transport = _LoopbackSshTransport()
        assert await transport.expand_files("*.dat", alias) == (str(alias / "file.dat"),)
        transfers = await transport.get_files("file.dat", alias, destination)
        assert transfers == ((str(alias / "file.dat"), str(destination / "file.dat")),)
        assert (destination / "file.dat").read_bytes() == b"payload"

    asyncio.run(check())


def test_cache_preparation_batches_patterns_and_exact_metadata(tmp_path):
    from kkrunner import ExecutionSpec
    from kkrunner.client import ExecutionClient, StagingSpec

    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    for name in ("one.pot", "two.pot", "three.cache", "exact"):
        (remote / name).write_bytes(b"cache")
    calls = []

    class Observed(LocalTarget):
        is_local = False
        supports_file_transfer = True

        async def query_files(self, sources, *, root=None, digest=False):
            calls.append((tuple(sources), root))
            return await super().query_files(sources, root=root, digest=digest)

    client = ExecutionClient(target=Observed())
    spec = ExecutionSpec(
        argv=("unused",), directory=local,
        cache_files=("*.pot", "*.cache", "exact", "missing"),
        staging=StagingSpec(local_root=local, remote_root=remote, input_sync="never"),
    )
    files = client.create_run_parts(spec).files
    try:
        # Patterns need local candidates for input staging even under NEVER.
        for name in ("one.pot", "two.pot", "three.cache"):
            (local / name).write_bytes(b"cache")
        targets, patterns, before = asyncio.run(
            files._prepare_cache_files({}, files.submission_footprint()[1])
        )
        assert calls == [
            ((str(remote / "*.pot"), str(remote / "*.cache"), str(remote / "exact"), str(remote / "missing")),
             str(remote)),
        ]
        assert set(patterns) == {str(remote / "*.pot"), str(remote / "*.cache")}
        assert before == {
            str(remote / name): queries.local_file_signature(remote / name)
            for name in ("one.pot", "two.pot", "three.cache")
        }
        assert targets[str(local / "exact")][1] == queries.local_file_signature(remote / "exact")
        assert targets[str(local / "missing")][1] is None
    finally:
        files.close()
