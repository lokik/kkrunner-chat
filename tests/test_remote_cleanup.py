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
from kkrunner.runs import WorkingDirectory

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
    outcomes = remote_file_cleanup(
        paths, str(root), kinds={path: ("directory" if path == str(root / "nested") else "file") for path in paths},
    )
    assert [item["status"] for item in outcomes] == [
        "skipped", "skipped", "skipped", "skipped", "ok",
    ]
    assert outcomes[-1]["kind"] == "directory"
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


# Cleanup tests attach an in-memory loopback transport; a persisted record
# intentionally allows only exact reconstructible transport implementations.
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
        execution=SimpleNamespace(
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
        execution=SimpleNamespace(
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



def test_explicit_directory_removes_complete_tree_but_not_adjacent_files(tmp_path):
    import os
    root = tmp_path / "work"
    taudir = root / "tau"
    (taudir / "deep").mkdir(parents=True)
    (taudir / "deep" / "Fe.tau").write_text("tau")
    (taudir / "other.dat").write_text("data")
    outside = root / "neighbor.txt"
    outside.write_text("keep")
    linked_target = tmp_path / "outside.txt"
    linked_target.write_text("also keep")
    (taudir / "alias").symlink_to(linked_target)
    preview = remote_file_cleanup(
        [str(taudir)], root=str(root), kinds={str(taudir): "directory"},
    )
    assert preview[0]["kind"] == "directory"
    assert {os.path.relpath(entry["path"], str(taudir)) for entry in preview[0]["contents"]} == {
        "deep", os.path.join("deep", "Fe.tau"), "other.dat", "alias",
    }
    expected = {str(taudir): {key: preview[0][key] for key in (
        "kind", "mtime_ns", "size", "device", "inode", "contents",
    )}}
    deleted = remote_file_cleanup(
        (str(taudir),), str(root), expected=expected,
        kinds={str(taudir): "directory"},
    )
    assert deleted[0]["status"] == "deleted"
    assert not taudir.exists()
    assert outside.exists() and linked_target.exists()


def test_directory_cleanup_rejects_unreviewed_new_content(tmp_path):
    root = tmp_path / "work"
    taudir = root / "tau"
    taudir.mkdir(parents=True)
    (taudir / "first").write_text("data")
    review = remote_file_cleanup(
        (str(taudir),), kinds={str(taudir): "directory"},
    )[0]
    (taudir / "later").write_text("new data")
    expected = {str(taudir): {key: review[key] for key in (
        "kind", "mtime_ns", "size", "device", "inode", "contents",
    )}}
    outcome = remote_file_cleanup(
        (str(taudir),), expected=expected,
        kinds={str(taudir): "directory"},
    )
    assert outcome[0]["status"] == "changed"
    assert (taudir / "later").exists()


def test_ssh_endpoint_deletes_recorded_directory_recursively(tmp_path):
    local = tmp_path / "local"
    work = tmp_path / "work"
    taupath = tmp_path / "shared"
    taudir = taupath / "tau"
    local.mkdir()
    work.mkdir()
    (taudir / "nested").mkdir(parents=True)
    (taudir / "nested" / "one.tau").write_text("tau")
    other = taupath / "other-project.tau"
    other.write_text("keep")
    client = EndpointClient(DirectBackend, transport=LoopbackSshTransport())
    record = SimpleNamespace(
        execution=SimpleNamespace(
            client.transport, WorkingDirectory(str(local), str(work)),
            backend=client.backend_config,
        ),
        files=FileManifest(outputs=(FileEntry(remote_path=str(taudir), kind="directory"),)),
    )
    preview = client.inspect_remote_files(record, (str(taudir),))
    assert preview[0]["kind"] == "directory"
    assert preview[0]["contents"]
    expected = {str(taudir): {key: preview[0][key] for key in (
        "kind", "mtime_ns", "size", "device", "inode", "contents",
    )}}
    results = client.delete_remote_files(record, expected)
    assert results[0]["status"] == "deleted"
    assert not taudir.exists() and other.exists()



def test_cleanup_never_reinterprets_declared_file_or_directory(tmp_path):
    root = tmp_path / "scratch"
    root.mkdir()
    real_file = root / "metadata.dir"
    real_file.write_text("regular despite suffix")
    real_directory = root / "tau"
    (real_directory / "nested").mkdir(parents=True)
    (real_directory / "nested" / "result.tau").write_text("keep")
    paths = (str(real_file), str(real_directory))
    mismatches = remote_file_cleanup(
        paths, kinds={str(real_file): "directory", str(real_directory): "file"},
    )
    assert all(item["status"] == "skipped" for item in mismatches)
    assert all("Declared" in item["message"] for item in mismatches)
    assert real_file.read_text() == "regular despite suffix"
    assert (real_directory / "nested" / "result.tau").exists()

    preview = remote_file_cleanup(paths, kinds={
        str(real_file): "file", str(real_directory): "directory",
    })
    assert [item["kind"] for item in preview] == ["file", "directory"]
    expected = {
        item["path"]: {k: item[k] for k in (
            "kind", "mtime_ns", "size", "device", "inode", "contents",
        )} for item in preview
    }
    deleted = remote_file_cleanup(
        paths, expected=expected,
        kinds={str(real_file): "directory", str(real_directory): "file"},
    )
    assert all(item["status"] == "skipped" for item in deleted)
    assert real_file.is_file() and real_directory.is_dir()


def test_endpoint_requires_declared_kind_not_actual_filesystem_type(tmp_path):
    root = tmp_path / "scratch"
    root.mkdir()
    declared_file = root / "listed-file"
    declared_file.mkdir()
    declared_directory = root / "listed-directory"
    declared_directory.write_text("file instead of expected directory")
    transport = LoopbackSshTransport()
    client = EndpointClient(DirectBackend, transport=transport)
    record = SimpleNamespace(
        execution=SimpleNamespace(
            transport, WorkingDirectory(str(tmp_path), str(root)),
            backend=client.backend_config,
        ),
        files=FileManifest(outputs=(
            FileEntry(remote_path=str(declared_file), kind="file"),
            FileEntry(remote_path=str(declared_directory), kind="directory"),
        )),
    )
    replies = client.inspect_remote_files(
        record, (str(declared_file), str(declared_directory)),
    )
    assert all(item["status"] == "skipped" for item in replies)
    assert declared_file.is_dir() and declared_directory.is_file()


def test_manifest_retains_directory_type_from_task_declaration(tmp_path):
    from kkrunner import TaskFile
    from kkrunner.files.manifest import FileManifest
    from kkrunner import ExecutionSpec

    spec = ExecutionSpec(
        ["true"], directory=str(tmp_path),
        output_files=(
            TaskFile("tau", kind="directory"),
            TaskFile("filename.dir"),
        ),
    )
    manifest = FileManifest.from_spec(spec)
    kinds = {entry.local_path: entry.kind for entry in manifest.outputs}
    assert kinds[str(tmp_path / "tau")] == "directory"
    assert kinds[str(tmp_path / "filename.dir")] == "file"
    assert FileManifest(outputs=({"remote_path": "/tmp/legacy"},)).outputs[0].kind == "file"
    with pytest.raises(ValueError, match="kind"):
        TaskFile("invalid", kind="other")
