import asyncio

import pytest

from kkrunner.client import StagingSpec, ExecutionClient, SshTarget
from kkrunner.client.contracts import JobAttachment
from kkrunner.client.file_context import FileContext
from kkrunner.client.file_session import FileSession




class _Transport:
    host = "cluster"


def test_run_context_is_staging_policy_not_executor(tmp_path):
    endpoint = ExecutionClient(target=SshTarget("cluster"))
    context = FileContext(endpoint.target, StagingSpec(local_root=tmp_path, remote_root="/scratch/a", input_sync="always", output_sync="missing")
    )
    assert not hasattr(context, 'create_job')
    assert context.target is endpoint.target
    assert context.local_root == tmp_path.resolve()
    assert context.remote_root == "/scratch/a"


def test_different_run_staging_policies_share_one_endpoint(tmp_path):
    endpoint = ExecutionClient(target=SshTarget("cluster"))
    first = FileContext(endpoint.target, StagingSpec(local_root=tmp_path, remote_root="/scratch/one"))
    second = FileContext(endpoint.target, StagingSpec(local_root=tmp_path, remote_root="/scratch/two"))
    assert first is not second
    assert first.target is endpoint.target
    assert second.target is endpoint.target
    assert first.remote_root == "/scratch/one"
    assert second.remote_root == "/scratch/two"


def test_attach_restores_typed_manifest_without_sync_dictionary(tmp_path, monkeypatch):
    from kkrunner import RunState
    from kkrunner.files.manifest import CacheEntry, FileEntry, FileManifest, FileMapping
    from kkrunner.runs.model import ExecutionMetadata, WorkingDirectory
    from kkrunner.runs.records import RunRecord

    local = tmp_path / "local"
    remote = tmp_path / "remote"
    local.mkdir()
    remote.mkdir()

    def entry(name, **kwargs):
        return FileEntry(str(local / name), str(remote / name), **kwargs)

    files = FileManifest(
        stdout=entry("stdout.txt"),
        outputs=(
            entry("exact.out"),
            entry("data-*.out"),
            entry("data-a.out", source_pattern=str(local / "data-*.out")),
            FileEntry(str(local / "local-only.out")),
        ),
        mapping=FileMapping(str(local), str(remote)),
        caches=(
            CacheEntry(entry("cache.dat"), (2, 3)),
            CacheEntry(entry("restart-*.pot")),
            CacheEntry(entry("restart-a.pot", source_pattern=str(local / "restart-*.pot"))),
            CacheEntry(FileEntry(remote_path=str(remote / "remote-only.dat")), (5, 7)),
            CacheEntry(FileEntry(str(local / "local-only-cache.dat"))),
        ),
    )
    endpoint = ExecutionClient(target=SshTarget("cluster"))
    context = FileContext(endpoint.target, StagingSpec(local_root=local, remote_root=remote))
    run = RunRecord(
        id="test-manifest",
        state=RunState.RUNNING,
        created_at="2026-10-13T08:00:00+00:00",
        updated_at="2026-10-13T08:00:00+00:00",
        execution=ExecutionMetadata(endpoint.target, WorkingDirectory(str(local), str(remote)), "opaque-id"),
        files=files,
        metadata={"test": {"task": "SCF"}},
        processing_required=True,
    )

    async def status(*args, **kwargs):
        return {"state": "running"}

    runner = asyncio.run(FileSession.from_attachment(context, JobAttachment(run.execution, run.files)))
    try:
        assert runner.files == files
        assert runner.files.caches[0].before == (2, 3)
        assert runner.files.caches[3].before == (5, 7)
    finally:
        runner.close()


def test_absolute_remote_root_needs_no_remote_home_query(tmp_path, monkeypatch):
    endpoint = ExecutionClient(target=SshTarget("cluster"))
    context = FileContext(endpoint.target, StagingSpec(local_root=tmp_path, remote_root="/scratch/../work"))
    calls = []

    async def remote_home(self):
        calls.append("home")
        raise AssertionError("Absolute remote roots must not query remote HOME")

    monkeypatch.setattr(SshTarget, "home_directory", remote_home)
    directory = tmp_path / "calculation"
    directory.mkdir()
    assert asyncio.run(context.map_path(directory)) == "/work/calculation"
    assert asyncio.run(context.map_directory(directory)) == "/work/calculation"
    assert asyncio.run(context.unmap_path("/work/calculation")) == directory
    assert asyncio.run(context._resolved_remote_root()) == "/work"
    assert calls == []


@pytest.mark.parametrize(
    "root, expected",
    [
        (None, "/home/user"),
        ("~", "/home/user"),
        ("~/projects", "/home/user/projects"),
        ("projects", "/home/user/projects"),
    ],
)
def test_home_relative_remote_root_still_resolves_home_once(tmp_path, monkeypatch, root, expected):
    endpoint = ExecutionClient(target=SshTarget("cluster"))
    context = FileContext(endpoint.target, StagingSpec(local_root=tmp_path, remote_root=root))
    calls = []

    from types import SimpleNamespace

    async def run(self, argv, **kwargs):
        calls.append(argv)
        return SimpleNamespace(stdout=b"/home/user")

    monkeypatch.setattr(SshTarget, "run", run)
    assert asyncio.run(context._resolved_remote_root()) == expected
    second = FileContext(endpoint.target, StagingSpec(local_root=tmp_path, remote_root=root))
    assert asyncio.run(second._resolved_remote_root()) == expected
    assert calls == [("sh", "-c", 'printf "%s" "$HOME"')]


def test_attach_does_not_open_or_create_output_paths(tmp_path):
    """Observation does not need writable client output paths."""

    from kkrunner import RunState
    from kkrunner.files.manifest import FileEntry, FileManifest, FileMapping
    from kkrunner.runs.model import ExecutionMetadata, WorkingDirectory
    from kkrunner.runs.records import RunRecord

    local, remote = (tmp_path / "local", tmp_path / "remote")
    local.mkdir()
    remote.mkdir()
    stdout = local / "stdout.txt"
    stderr = local / "stderr.txt"
    endpoint = ExecutionClient(target=SshTarget("cluster"))
    context = FileContext(endpoint.target, StagingSpec(local_root=local, remote_root=remote))
    run = RunRecord(
        id="stream-open-failure",
        state=RunState.RUNNING,
        created_at="2026-10-13T08:00:00+00:00",
        updated_at="2026-10-13T08:00:00+00:00",
        execution=ExecutionMetadata(endpoint.target, WorkingDirectory(str(local), str(remote)), "remote-id"),
        files=FileManifest(
            mapping=FileMapping(str(local), str(remote)),
            stdout=FileEntry(str(stdout), str(remote / "stdout.txt")),
            stderr=FileEntry(str(stderr), str(remote / "stderr.txt")),
        ),
        metadata={"test": {"task": "SCF"}},
        processing_required=True,
    )

    session = asyncio.run(FileSession.from_attachment(context, JobAttachment(run.execution, run.files)))
    try:
        assert not stdout.exists()
        assert not stderr.exists()
    finally:
        session.close()


def test_unmap_rejects_symlink_parent_outside_local_root(tmp_path):
    root = tmp_path / "local"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (root / "redirect").symlink_to(outside, target_is_directory=True)
    endpoint = ExecutionClient(target=SshTarget("cluster"))
    context = FileContext(endpoint.target, StagingSpec(local_root=root, remote_root="/remote"))
    with pytest.raises(ValueError, match="outside mapped root"):
        asyncio.run(context.unmap_path("/remote/redirect/output.dat"))


def test_unmap_preserves_internal_symlink_spelling(tmp_path):
    root = tmp_path / "local"
    actual = root / "actual"
    actual.mkdir(parents=True)
    (root / "alias").symlink_to(actual, target_is_directory=True)
    endpoint = ExecutionClient(target=SshTarget("cluster"))
    context = FileContext(endpoint.target, StagingSpec(local_root=root, remote_root="/remote"))
    assert asyncio.run(context.unmap_path("/remote/alias/output.dat")) == root / "alias" / "output.dat"
