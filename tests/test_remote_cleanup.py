"""Guarded remote deletion operates on exact persisted files, without a remote install."""
import asyncio
import sys
from types import SimpleNamespace

import pytest

from kkrunner.backends import DirectBackend
from kkrunner.client import EndpointClient
from kkrunner.client.contracts import Operation
from kkrunner.files.manifest import FileEntry, FileManifest
from kkrunner.files.queries import remote_file_cleanup
from kkrunner.runs import ExecutionMetadata, WorkingDirectory

from .test_ssh_direct import LoopbackSshTransport


def _identity(reply):
    return [reply[key] for key in ("mtime_ns", "size", "device", "inode")]


def test_remote_cleanup_guards_changed_and_missing_files(tmp_path):
    root = tmp_path / "run"
    root.mkdir()
    keep = root / "keep.out"
    remove = root / "remove.out"
    keep.write_text("keep")
    remove.write_text("remove")
    preview = remote_file_cleanup([str(keep), str(remove)], str(root))
    assert [item["status"] for item in preview] == ["ok", "ok"]
    keep.write_text("changed")
    result = remote_file_cleanup(
        [str(keep), str(remove)], str(root),
        expected={entry["path"]: _identity(entry) for entry in preview},
    )
    assert [item["status"] for item in result] == ["changed", "deleted"]
    assert keep.exists() and not remove.exists()
    assert remote_file_cleanup([str(remove)], str(root))[0]["status"] == "missing"


def test_remote_cleanup_rejects_unrelated_paths_symlinks_and_directories(tmp_path):
    root = tmp_path / "run"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.write_text("untouched")
    (root / "nested").mkdir()
    (root / "alias").symlink_to(outside)
    (root / "linked").symlink_to(tmp_path, target_is_directory=True)
    paths = [
        str(outside), str(root), str(root / "alias"),
        str(root / "linked" / "outside"), str(root / "nested"),
    ]
    assert all(item["status"] == "skipped" for item in remote_file_cleanup(paths, str(root)))
    assert outside.exists()
    assert (root / "nested").is_dir()


def test_remote_cleanup_uses_ssh_standalone_protocol_without_agent(tmp_path):
    root = tmp_path / "run"
    root.mkdir()
    path = root / "a file 'with spaces'.out"
    path.write_text("data")
    ssh = LoopbackSshTransport()

    async def verify():
        operation = Operation.standalone(remote_file_cleanup)
        preview = await ssh.call(operation, (str(path),), str(root))
        assert preview[0]["status"] == "ok"
        deleted = await ssh.call(
            operation, (str(path),), str(root),
            expected={str(path): _identity(preview[0])},
        )
        assert deleted[0]["status"] == "deleted"

    asyncio.run(verify())
    assert not path.exists()


def test_endpoint_cleanup_rejects_paths_not_in_manifest(tmp_path):
    root = tmp_path / "run"
    root.mkdir()
    path = root / "result.out"
    rogue = root / "not-declared.out"
    path.write_text("result")
    rogue.write_text("important")
    transport = LoopbackSshTransport()
    client = EndpointClient(DirectBackend, transport=transport)
    record = SimpleNamespace(
        execution=ExecutionMetadata(
            transport, WorkingDirectory(str(tmp_path), str(root)),
            backend=client.backend_config,
        ),
        files=FileManifest(outputs=(FileEntry(remote_path=str(path)),)),
    )
    with pytest.raises(ValueError, match="declared remote manifest"):
        client.inspect_remote_files(record, (str(rogue),))
    preview = client.inspect_remote_files(record, (str(path),))
    assert preview[0]["status"] == "ok"
    results = client.delete_remote_files(record, {str(path): _identity(preview[0])})
    assert results[0]["status"] == "deleted"
    assert not path.exists()
    assert rogue.exists()



def test_manifest_file_in_nested_or_external_tau_directory_is_deletable(tmp_path):
    """TAUDIR may live under TAUPATH instead of the execution directory."""
    workdir = tmp_path / "calculation"
    workdir.mkdir()
    nested = workdir / "output" / "partial.dat"
    nested.parent.mkdir()
    nested.write_text("subdirectory")
    taupath = tmp_path / "shared-storage"
    taudir = taupath / "tau-files"
    taudir.mkdir(parents=True)
    taufile = taudir / "Fe.tau"
    taufile.write_text("tau")
    unlisted = taudir / "other-run.tau"
    unlisted.write_text("do not delete")

    client = EndpointClient(DirectBackend, transport=LoopbackSshTransport())
    record = SimpleNamespace(
        execution=ExecutionMetadata(
            client.transport,
            WorkingDirectory(str(tmp_path), str(workdir)),
            backend=client.backend_config,
        ),
        files=FileManifest(outputs=(
            FileEntry(remote_path=str(nested)),
            FileEntry(remote_path=str(taufile)),
        )),
    )
    selected = (str(nested), str(taufile))
    preview = client.inspect_remote_files(record, selected)
    assert {item["path"] for item in preview} == set(selected)
    assert all(item["status"] == "ok" for item in preview)
    with pytest.raises(ValueError, match="declared remote manifest"):
        client.inspect_remote_files(record, (str(unlisted),))

    deleted = client.delete_remote_files(
        record, {item["path"]: _identity(item) for item in preview},
    )
    assert all(item["status"] == "deleted" for item in deleted)
    assert not nested.exists() and not taufile.exists()
    assert unlisted.read_text() == "do not delete"
    assert taudir.is_dir()  # Cleanup never removes whole directories.


def test_rootless_operation_rejects_symlinked_tau_paths(tmp_path):
    """A declared file whose parent is a symlink is not automatically safe."""
    actual = tmp_path / "scratch"
    actual.mkdir()
    tau = actual / "Fe.tau"
    tau.write_text("tau")
    alias = tmp_path / "TAUDIR"
    alias.symlink_to(actual, target_is_directory=True)
    replies = remote_file_cleanup((str(alias / "Fe.tau"),))
    assert replies[0]["status"] == "skipped"
    assert tau.exists()
