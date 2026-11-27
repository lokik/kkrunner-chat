from dataclasses import replace
"""Live snapshots do not disturb run artifacts or historical state."""

import asyncio
from pathlib import Path

import pytest

from kkrunner import RunState, RunStore
from kkrunner.client import ExecutionClient, LocalTarget, SshTarget
from kkrunner.backends import SupervisedBackend
from kkrunner.backends.config import BackendConfig
from kkrunner.files.manifest import CacheEntry, FileEntry, FileManifest, FileMapping
from kkrunner.runs import RunManager

from .test_transport import _LoopbackSshTransport


@pytest.fixture(autouse=True)
def loopback_remote_snapshots(monkeypatch):
    # Check file mapping independently of an external SSH daemon.
    monkeypatch.setattr(SshTarget, "query_files", LocalTarget.query_files)
    monkeypatch.setattr(SshTarget, "snapshot_file", LocalTarget.snapshot_file)
    monkeypatch.setattr(SshTarget, "read_file", LocalTarget.read_file)


def running_run(tmp_path, remote=False, output="result.dat"):
    local = tmp_path / "local" / "job"
    target = tmp_path / "remote" / "job"
    local.mkdir(parents=True)
    if remote:
        target.mkdir(parents=True)
        transport = SshTarget("loopback")
        mapping = FileMapping(str(local.parent), str(target.parent))
    else:
        transport = LocalTarget()
        mapping = None
    manager = RunManager(RunStore(tmp_path / "history"))
    files = FileManifest(
        outputs=(FileEntry(str(local / output), str(target / output) if remote else None),),
        caches=(CacheEntry(FileEntry(str(local / "cache.bin"), str(target / "cache.bin") if remote else None)),),
        inputs=(FileEntry(str(local / "input.dat"), str(target / "input.dat") if remote else None),),
        mapping=mapping,
    )
    record = manager.new_record(
        directory=local, remote_directory=str(target) if remote else None,
        target=transport, backend=BackendConfig("kkrunner"), files=files,
    )
    record = replace(record, state=RunState.RUNNING, execution=record.execution.submitted("job-123"))
    manager.store.save(record)
    return manager, record, local, target if remote else local


@pytest.mark.parametrize("remote", [False, True])
def test_snapshot_growing_file_does_not_change_record_or_canonical_file(tmp_path, remote):
    manager, run, local, source_root = running_run(tmp_path, remote)
    source = source_root / "result.dat"
    source.write_bytes(b"partial\x00")
    assert manager.snapshot_file(run.id, "result.dat") == b"partial\x00"
    assert manager.snapshot_file(run, run.files.outputs[0]) == b"partial\x00"
    destination = tmp_path / "observed.bin"
    assert manager.snapshot_file(run, "result.dat", destination=destination) == destination
    source.write_bytes(b"partial\x00more")
    assert manager.snapshot_file(run, "result.dat") == b"partial\x00more"
    assert destination.read_bytes() == b"partial\x00"
    if remote:
        assert not (local / "result.dat").exists()
    assert manager.store.get(run.id) == run


def test_snapshot_validates_declarations_and_submission(tmp_path):
    manager, run, local, _ = running_run(tmp_path)
    (local / "input.dat").write_bytes(b"input")
    with pytest.raises(ValueError, match="declared output"):
        manager.snapshot_file(run, "input.dat")
    with pytest.raises(ValueError, match="declared output"):
        manager.snapshot_file(run, "unknown.dat")
    with pytest.raises(FileNotFoundError):
        manager.snapshot_file(run, "result.dat")
    with pytest.raises(ValueError, match="concrete file"):
        manager.snapshot_file(run, "*.dat")
    new = manager.new_record(
        directory=local, target=run.execution.target, backend=run.execution.backend,
        files=run.files, remote_directory=run.execution.directory.remote,
    )
    manager.store.save(new)
    with pytest.raises(RuntimeError, match="not been submitted"):
        manager.snapshot_file(new.id, "result.dat")


def test_snapshot_destination_cannot_overwrite_run_files(tmp_path):
    manager, run, local, _ = running_run(tmp_path)
    source = local / "result.dat"
    source.write_bytes(b"output")
    (local / "input.dat").write_bytes(b"input")
    alias = tmp_path / "alias"
    alias.symlink_to(source)
    for forbidden in [source, local / "input.dat", alias]:
        with pytest.raises(ValueError, match="overlaps"):
            manager.snapshot_file(run, "result.dat", destination=forbidden)
    assert source.read_bytes() == b"output"


def test_snapshot_cache_and_wildcard_concrete_file(tmp_path):
    manager, run, local, target = running_run(tmp_path, True, "nested/**/*.dat")
    cache = target / "cache.bin"
    cache.write_bytes(b"cache")
    assert manager.snapshot_file(run, "cache.bin") == b"cache"
    path = target / "nested" / "deep" / "one.dat"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"still writing")
    assert manager.snapshot_file(run, "nested/deep/one.dat") == b"still writing"
    with pytest.raises(ValueError, match="declared output"):
        manager.snapshot_file(run, "nested/deep/one.txt")
    with pytest.raises(ValueError, match="overlaps"):
        manager.snapshot_file(run, "cache.bin", destination=local / "nested" / "other.dat")


@pytest.mark.parametrize("factory", [LocalTarget, _LoopbackSshTransport])
def test_transport_snapshot_replaces_destination_without_stable_transfer_checks(tmp_path, factory):
    source = tmp_path / "source"
    target = tmp_path / "target"
    source.write_bytes(b"first")

    async def verify():
        transport = factory()
        assert await transport.snapshot_file(source) == b"first"
        assert await transport.snapshot_file(source, destination=target) == target
        source.write_bytes(b"second")
        assert await transport.snapshot_file(source, destination=target) == target

    asyncio.run(verify())
    assert target.read_bytes() == b"second"
    assert not list(tmp_path.glob(".*.tmp"))


def test_failed_snapshot_does_not_replace_previous_copy(tmp_path):
    manager, run, _, _ = running_run(tmp_path)
    target = tmp_path / "target"
    target.write_bytes(b"previous")
    with pytest.raises(FileNotFoundError):
        manager.snapshot_file(run, "result.dat", destination=target)
    assert target.read_bytes() == b"previous"


@pytest.mark.parametrize("tracked", [False, True])
def test_snapshot_runner_convenience(tmp_path, tracked):
    import sys
    from kkrunner import ExecutionSpec

    manager = RunManager(RunStore(tmp_path / "history"))
    run = ExecutionClient(SupervisedBackend).create_run(
        ExecutionSpec([sys.executable, "-c", "print('done')"], directory=tmp_path, output_file="stdout.txt"),
        tracked=manager if tracked else False,
    )
    with pytest.raises(RuntimeError, match="not been submitted"):
        run.snapshot_file("stdout.txt")
    run.run()
    assert run.snapshot_file("stdout.txt") == b"done\n"
    if not tracked:
        assert manager.store.runs() == ()


def test_snapshot_destination_cannot_overwrite_declared_directory_contents(tmp_path):
    from kkrunner.files.snapshots import check_destination
    source = tmp_path / "output.dat"
    tree = tmp_path / "TAUDIR"
    manifest = FileManifest(stdout=FileEntry(str(source)), inputs=(
        FileEntry(str(tree), kind="directory"),))
    for destination in (tree / "file.dat", tree / ".hidden" / "file.dat"):
        with pytest.raises(ValueError, match="overlaps"):
            check_destination(manifest, source, destination)
    assert check_destination(manifest, source, tmp_path / "other") == tmp_path / "other"


@pytest.mark.parametrize("remote", [False, True])
def test_client_snapshot_respects_manifest_and_preserves_run_files(tmp_path, remote):
    _, run, local, source_root = running_run(tmp_path, remote)
    client = ExecutionClient(SupervisedBackend, target=run.execution.target)
    (source_root / "result.dat").write_bytes(b"result")
    (source_root / "input.dat").write_bytes(b"input")
    (source_root / "unknown.dat").write_bytes(b"undeclared")
    assert client.snapshot_file(run, "result.dat") == b"result"
    assert client.snapshot_file(run, run.files.outputs[0]) == b"result"

    for forbidden in ("input.dat", "unknown.dat"):
        with pytest.raises(ValueError, match="declared output"):
            client.snapshot_file(run, forbidden)
    with pytest.raises(ValueError, match="concrete file"):
        client.snapshot_file(run, "*.dat")
    (local / "input.dat").write_bytes(b"local input")
    with pytest.raises(ValueError, match="overlaps"):
        client.snapshot_file(run, "result.dat", destination=local / "input.dat")
    assert (local / "input.dat").read_bytes() == b"local input"

    destination = tmp_path / "copy.dat"
    assert client.snapshot_file(run, "result.dat", destination=destination) == destination
    assert destination.read_bytes() == b"result"


@pytest.mark.parametrize("remote", [False, True])
def test_snapshot_rejects_nonregular_source(tmp_path, remote):
    manager, run, _, source_root = running_run(tmp_path, remote)
    client = ExecutionClient(SupervisedBackend, target=run.execution.target)
    (source_root / "result.dat").mkdir()
    destination = tmp_path / "copy.dat"
    destination.write_bytes(b"previous")
    for snapshot in (manager.snapshot_file, client.snapshot_file):
        with pytest.raises(ValueError, match="not a regular file"):
            snapshot(run, "result.dat", destination=destination)
    assert destination.read_bytes() == b"previous"


def test_shared_snapshot_respects_mapped_root_for_both_apis(tmp_path):
    manager, run, local, _ = running_run(tmp_path, remote=True)
    client = ExecutionClient(SupervisedBackend, target=run.execution.target)
    outside = tmp_path / "outside.dat"
    outside.write_bytes(b"outside")
    source = local / "result.dat"
    source.symlink_to(outside)
    files = replace(run.files, mapping=replace(run.files.mapping, shared_filesystem=True))
    run = manager.store.update(run, lambda latest: replace(latest, files=files))
    for snapshot in (manager.snapshot_file, client.snapshot_file):
        with pytest.raises(ValueError, match="outside mapped root"):
            snapshot(run, "result.dat")
    source.unlink()
    source.write_bytes(b"shared result")
    for snapshot in (manager.snapshot_file, client.snapshot_file):
        assert snapshot(run, "result.dat") == b"shared result"
