import pytest

from kkrunner import RunState
from kkrunner.files import FileEntry, FileManifest, FileState, FileVersion
from kkrunner.files.manifest import FileMapping
from kkrunner.client import ExecutionClient, SshTarget, LocalTarget
from kkrunner.runs import ExecutionMetadata, RunRecord, WorkingDirectory

from .transfer_helpers import LoopbackFileTransfer


@pytest.fixture(autouse=True)
def loopback_file_transport(monkeypatch):
    """Use a persistable SSH configuration with local filesystem primitives.

    This test verifies FileAccess path mapping, not the SSH process protocol.
    """
    monkeypatch.setattr(SshTarget, "expand_files", LocalTarget.expand_files)
    monkeypatch.setattr(SshTarget, "put_file", LoopbackFileTransfer.put_file)
    monkeypatch.setattr(SshTarget, "get_file", LoopbackFileTransfer.get_file)


def test_remote_executor_fetches_only_requested_historical_files(tmp_path):
    local_root = tmp_path / "local"
    remote_root = tmp_path / "remote"
    local_directory = local_root / "calculation"
    remote_directory = remote_root / "calculation"
    local_directory.mkdir(parents=True)
    remote_directory.mkdir(parents=True)
    remote_input = remote_directory / "run.inp"
    remote_potential = remote_directory / "start.pot"
    remote_output = remote_directory / "run.out"
    remote_first = remote_directory / "result-1.dat"
    remote_second = remote_directory / "result-2.dat"
    remote_later = remote_directory / "result-later.dat"
    remote_input.write_text("input")
    remote_potential.write_text("potential")
    remote_output.write_text("output")
    remote_first.write_text("one")
    remote_second.write_text("two")
    remote_later.write_text("not part of the historical snapshot")
    local_first = local_directory / "result-1.dat"
    local_first.write_text("keep-local")
    executor = ExecutionClient(target=SshTarget("loopback"))
    run = RunRecord(
        id="historical",
        state=RunState.FINISHED,
        created_at="2026-10-13T08:00:00+00:00",
        updated_at="2026-10-13T08:00:00+00:00",
        execution=ExecutionMetadata(
            executor.target,
            WorkingDirectory(str(local_directory), str(remote_directory)),
            "opaque-historical-handle",
        ),
        files=FileManifest(mapping=FileMapping(str(local_root), str(remote_root))),
        metadata={"example": {"task": "DOS".upper()}},
        processing_required=True,
    )
    local_input = local_directory / "run.inp"
    local_potential = local_directory / "start.pot"
    local_output = local_directory / "run.out"
    local_second = local_directory / "result-2.dat"
    local_later = local_directory / "result-later.dat"
    assert executor.target.supports_file_transfer
    executor.fetch_result_files(run, (local_input, local_potential, local_output, local_second))
    assert local_input.read_text() == "input"
    assert local_potential.read_text() == "potential"
    assert local_output.read_text() == "output"
    assert local_first.read_text() == "keep-local"
    assert local_second.read_text() == "two"
    assert not local_later.exists()
    assert local_output.stat().st_mtime_ns == remote_output.stat().st_mtime_ns
    assert local_second.stat().st_mtime_ns == remote_second.stat().st_mtime_ns


def test_remote_fetch_uses_transport_expansion_for_exact_and_wildcard(tmp_path):
    import os

    expanded, downloaded = [], []
    native_expand = LocalTarget.expand_files
    native_get = LoopbackFileTransfer.get_file

    async def observed_expand(self, sources, root):
        expanded.append(os.fspath(sources))
        return await native_expand(self, sources, root)

    async def observed_get(self, source, destination, **kwargs):
        downloaded.append(os.fspath(source))
        return await native_get(self, source, destination, **kwargs)

    from unittest.mock import patch
    local_root = tmp_path / "local"
    remote_root = tmp_path / "remote"
    (local_root / "calc").mkdir(parents=True)
    remote_dir = remote_root / "calc"
    remote_dir.mkdir(parents=True)
    (remote_dir / "exact.dat").write_text("exact")
    (remote_dir / "part-a.dat").write_text("a")
    (remote_dir / "part-b.dat").write_text("b")
    (remote_dir / ".hidden.dat").write_text("hidden")
    existing = local_root / "calc" / "part-a.dat"
    existing.write_text("keep")
    executor = ExecutionClient(target=SshTarget("loopback"))
    run = RunRecord(
        id="wildcard-fetch",
        state=RunState.FINISHED,
        created_at="2026-10-13T08:00:00+00:00",
        updated_at="2026-10-13T08:00:00+00:00",
        execution=ExecutionMetadata(
            executor.target,
            WorkingDirectory(str(local_root / "calc"), str(remote_dir)),
            "opaque-handle",
        ),
        files=FileManifest(mapping=FileMapping(str(local_root), str(remote_root))),
        metadata={"example": {"task": "DOS".upper()}},
        processing_required=True,
    )
    with patch.object(SshTarget, "expand_files", observed_expand), patch.object(SshTarget, "get_file", observed_get):
        executor.fetch_result_files(run, (local_root / "calc" / "exact.dat", local_root / "calc" / "part-*.dat"))
    assert expanded == [str(remote_dir / "exact.dat"), str(remote_dir / "part-*.dat")]
    assert downloaded == [str(remote_dir / "exact.dat"), str(remote_dir / "part-b.dat")]
    assert (local_root / "calc" / "exact.dat").read_text() == "exact"
    assert existing.read_text() == "keep"
    assert (local_root / "calc" / "part-b.dat").read_text() == "b"
    assert not (local_root / "calc" / ".hidden.dat").exists()


def test_remote_staged_result_keeps_canonical_file_absent(tmp_path):
    local_root = tmp_path / "local"
    remote_root = tmp_path / "remote"
    local_dir = local_root / "calc"
    remote_dir = remote_root / "calc"
    local_dir.mkdir(parents=True)
    remote_dir.mkdir(parents=True)
    local = local_dir / "run.out"
    local.write_bytes(b"original result")
    captured = FileVersion.capture(local, require_digest=True)
    local.unlink()
    (remote_dir / "run.out").write_bytes(b"original result")
    executor = ExecutionClient(target=SshTarget("loopback"))
    run = RunRecord(
        id="staged-result",
        state=RunState.FINISHED,
        created_at="2026-10-13T08:00:00+00:00",
        updated_at="2026-10-13T08:00:00+00:00",
        execution=ExecutionMetadata(
            executor.target, WorkingDirectory(str(local_dir), str(remote_dir)), "backend-id"
        ),
        files=FileManifest(
            mapping=FileMapping(str(local_root), str(remote_root)),
            output_versions=(captured,),
            captured_at="2026-10-13T08:00:00+00:00",
        ),
        metadata={"example": {"task": "DOS".upper()}},
        processing_required=True,
    )
    staged = local_dir / ".staged-run.out"
    executor.fetch_result_files(run, (local,), destinations={str(local): str(staged)})
    assert not local.exists()
    assert captured.state() is FileState.MISSING
    assert staged.read_bytes() == b"original result"


@pytest.mark.parametrize("wildcard", (False, True))
def test_fetch_rejects_local_symlink_escape(tmp_path, wildcard):
    local_root = tmp_path / "local"
    remote_root = tmp_path / "remote"
    outside = tmp_path / "outside"
    local_root.mkdir()
    remote_root.mkdir()
    outside.mkdir()
    (local_root / "alias").symlink_to(outside, target_is_directory=True)
    remote_dir = remote_root / "alias"
    remote_dir.mkdir()
    (remote_dir / "result.dat").write_text("remote result")
    existing = outside / "result.dat"
    existing.write_text("preserved")
    name = "*.dat" if wildcard else "result.dat"
    local_path = local_root / "alias" / name
    remote_path = remote_dir / name
    executor = ExecutionClient(target=SshTarget("loopback"))
    run = RunRecord(
        id="escaped-result",
        state=RunState.FINISHED,
        created_at="2026-10-13T08:00:00+00:00",
        updated_at="2026-10-13T08:00:00+00:00",
        execution=ExecutionMetadata(
            executor.target, WorkingDirectory(str(local_root), str(remote_root)), "backend-id"
        ),
        files=FileManifest(
            outputs=(FileEntry(str(local_path), str(remote_path)),),
            mapping=FileMapping(str(local_root), str(remote_root)),
        ),
        metadata={"example": {"task": "DOS".upper()}},
        processing_required=True,
    )
    with pytest.raises(ValueError, match="outside mapped root"):
        executor.fetch_result_files(run, (local_path,))
    assert existing.read_text() == "preserved"
    assert sorted(path.name for path in outside.iterdir()) == ["result.dat"]


def test_fetch_validates_canonical_path_before_explicit_staging(tmp_path):
    """An explicit destination must not bypass checks on mapped source paths."""
    local = tmp_path / "local"
    remote = tmp_path / "remote"
    outside = tmp_path / "outside"
    local.mkdir()
    remote.mkdir()
    outside.mkdir()
    (local / "bad").symlink_to(outside, target_is_directory=True)
    (remote / "bad").mkdir()
    (remote / "bad" / "output.dat").write_bytes(b"remote")
    canonical = local / "bad" / "output.dat"
    staged = local / "staged-output.dat"
    executor = ExecutionClient(target=SshTarget("loopback"))
    run = RunRecord(
        id="escaped-staged",
        state=RunState.FINISHED,
        created_at="2026-10-13T08:00:00+00:00",
        updated_at="2026-10-13T08:00:00+00:00",
        execution=ExecutionMetadata(executor.target, WorkingDirectory(str(local), str(remote)), "backend-id"),
        files=FileManifest(
            outputs=(FileEntry(str(canonical), str(remote / "bad" / "output.dat")),),
            mapping=FileMapping(str(local), str(remote)),
        ),
        metadata={"example": {"task": "DOS".upper()}},
        processing_required=True,
    )
    with pytest.raises(ValueError, match="outside mapped root"):
        executor.fetch_result_files(run, (canonical,), destinations={str(canonical): str(staged)})
    assert not staged.exists()
    assert not (outside / "output.dat").exists()


def test_missing_historical_auxiliary_does_not_block_other_file_retrieval(tmp_path):
    """A vanished optional DOS must not stop restoration of the main SCF output."""
    from dataclasses import replace
    from kkrunner import RunStore
    from kkrunner.runs.manager import RunManager

    main = tmp_path / "run.out"
    auxiliary = tmp_path / "run_SCFSTART.dos"
    main.write_bytes(b"main output")
    auxiliary.write_bytes(b"auxiliary output")
    versions = tuple(FileVersion.capture(path, require_digest=True) for path in (auxiliary, main))
    main.unlink()
    auxiliary.unlink()

    remote = tmp_path / "remote"
    remote.mkdir()
    (remote / main.name).write_bytes(b"main output")
    manifest = FileManifest(
        stdout=FileEntry(str(main), str(remote / main.name)),
        outputs=(FileEntry(str(auxiliary), str(remote / auxiliary.name), optional=True),),
        mapping=FileMapping(str(tmp_path), str(remote)),
        output_versions=versions,
        captured_at="2026-10-13T08:00:00+00:00",
    )
    record = RunRecord(
        id="missing-auxiliary", state=RunState.FINISHED,
        created_at="2026-10-13T08:00:00+00:00",
        updated_at="2026-10-13T08:00:00+00:00",
        execution=ExecutionMetadata(
            SshTarget("loopback"),
            WorkingDirectory(str(tmp_path), str(remote)),
            "backend-id",
        ),
        files=manifest,
    )

    store = RunStore(tmp_path / "runs")
    record = store.save(record)
    manager = RunManager(store)
    manager.register_client(ExecutionClient(target=SshTarget("loopback")))
    restored = manager.retrieve_result_files(record)
    assert main.read_bytes() == b"main output"
    assert not auxiliary.exists()
    assert restored.files.stdout.availability_state == "available"
    assert restored.files.outputs[0].availability_state == "failed"
    assert "not retrieved" in restored.files.outputs[0].availability_message


@pytest.mark.parametrize("concurrent", ["unrelated", "active", "mismatch", "directory", "remote_writer", "remote_reader"])
def test_historical_transfer_does_not_hold_submission_locks(tmp_path, monkeypatch, concurrent):
    """Reserve from another store during transfer; publish only if still safe."""
    from concurrent.futures import ThreadPoolExecutor
    from pathlib import Path
    from threading import Event

    from kkrunner import MemoryRunStore, RunStore
    from kkrunner.runs import DuplicateSubmissionError
    from kkrunner.runs.manager import RunManager

    disk, memory = RunStore(tmp_path / "runs"), MemoryRunStore()
    manager = RunManager(disk, conflict_stores=(memory,))
    other = RunManager(memory, conflict_stores=(disk,))
    work = tmp_path / "work"
    work.mkdir()
    path = work / "result.out"
    path.write_text("historical")
    version = FileVersion.capture(path, require_digest=True)
    path.unlink()
    remote = tmp_path / "remote"
    remote.mkdir()
    source = remote / "result.out"
    source.write_text("wrong version" if concurrent == "mismatch" else "historical")
    client = ExecutionClient(target=SshTarget("loopback"))
    manager.register_client(client)
    record = manager.new_record(
        directory=work, remote_directory=remote,
        target=client.target, backend=client.backend_config,
        files=FileManifest(
            stdout=FileEntry(str(path), str(source)),
            mapping=FileMapping(str(work), str(remote)),
            output_versions=(version,), captured_at="2026-10-13T08:00:00+00:00",
        ),
    )
    record = manager.complete(record, state=RunState.FINISHED, return_code=0,
                              execution=record.execution.submitted("historical"),
                              artifacts_available=False)
    downloading, release = Event(), Event()

    async def download(self, source_path, destination, **kwargs):
        assert str(source_path) == str(source)
        assert str(destination) == str(path)
        downloading.set()
        assert release.wait(5), "transfer was not released"
        await LoopbackFileTransfer.get_file(self, source_path, destination, **kwargs)

    monkeypatch.setattr(SshTarget, "get_file", download)

    def submit():
        if concurrent in ("remote_writer", "remote_reader"):
            unrelated = tmp_path / "unrelated" / "result.out"
            entry = FileEntry(str(unrelated), str(source))
            files = FileManifest(
                outputs=(entry,) if concurrent == "remote_writer" else (),
                inputs=(entry,) if concurrent == "remote_reader" else (),
                mapping=FileMapping(str(unrelated.parent), str(remote), input_sync="never"),
            )
            proposed = other.new_record(directory=unrelated.parent, remote_directory=remote,
                                        target=client.target, files=files)
            if concurrent == "remote_writer":
                with pytest.raises(DuplicateSubmissionError):
                    other.reserve(proposed)
                return
            return other.reserve(proposed)
        if concurrent == "directory":
            proposed = other.new_record(directory=work, files=FileManifest(
                inputs=(FileEntry(str(work), kind="directory"),),
            ))
            with pytest.raises(DuplicateSubmissionError):
                other.reserve(proposed)
            return
        target = work / "unrelated.out" if concurrent in ("unrelated", "mismatch") else path
        proposed = other.new_record(directory=work, files=FileManifest(stdout=FileEntry(str(target))))
        if concurrent == "active":
            with pytest.raises(DuplicateSubmissionError):
                other.reserve(proposed)
            return
        return other.reserve(proposed)

    with ThreadPoolExecutor(max_workers=2) as pool:
        future = pool.submit(manager.retrieve_result_files, record)
        try:
            assert downloading.wait(5), "transfer did not start"
            assert not path.exists()
            assert disk.runs() == (record,)
            assert not memory.runs()
            # Even an independently configured manager in this process
            # sees the internal reservation. Neither application store owns it.
            independent = RunManager(RunStore(tmp_path / "independent"))
            proposed = independent.new_record(directory=work, files=FileManifest(
                stdout=FileEntry(str(path)),
            ))
            (reservation,) = independent.conflicts(proposed)
            assert manager.conflicts(proposed) == (reservation,)
            assert other.conflicts(proposed) == (reservation,)
            with pytest.raises(DuplicateSubmissionError):
                independent.reserve(proposed)
            run = pool.submit(submit).result(timeout=3)
            if concurrent not in ("directory", "active", "remote_writer"):
                assert run.state == RunState.SUBMITTING
        finally:
            release.set()
        restored = future.result(timeout=5)
        assert restored.state == RunState.FINISHED
        if concurrent == "mismatch":
            assert not path.exists()
            assert restored.files.stdout.availability_state == "failed"
            assert "overwritten" in restored.files.stdout.availability_message
        else:
            assert path.read_text() == "historical"
    assert not tuple(work.glob(".*.tmp"))
    assert [r.id for r in disk.runs()] == [record.id]
    assert reservation not in independent.conflicts(proposed)


@pytest.mark.parametrize("project_scoped", [False, True])
def test_historical_wildcard_download_uses_transport_staging(tmp_path, monkeypatch, project_scoped):
    from kkrunner import RunStore
    from kkrunner.runs.manager import RunManager

    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    for name in ("part-a.dat", "part-b.dat"):
        (remote / name).write_text(name)
    pattern = local / "part-*.dat"
    remote_pattern = remote / pattern.name
    client = ExecutionClient(target=SshTarget("loopback"))
    if project_scoped:
        from kkrunner.projects.manager import ProjectRunManager
        from kkrunner.projects.repository import ProjectRepository

        projects = ProjectRepository(tmp_path / "projects")
        project = projects.create("Historical results")
        manager = ProjectRunManager(project_repository=projects)
        creator = manager.for_project(project)
    else:
        manager = creator = RunManager(RunStore(tmp_path / "runs"))
    manager.register_client(client)
    record = creator.new_record(
        directory=local, target=client.target, backend=client.backend_config,
        remote_directory=remote,
        files=FileManifest(
            outputs=(FileEntry(str(pattern), str(remote_pattern)),),
            mapping=FileMapping(str(local), str(remote)),
        ),
    )
    record = manager.complete(record, state=RunState.FAILED,
                              execution=record.execution.submitted("historical"),
                              artifacts_available=False)

    original = LoopbackFileTransfer.get_file

    async def download(self, source, destination, **kwargs):
        from pathlib import Path
        assert Path(destination).parent == local
        assert not Path(destination).exists()
        assert kwargs["overwrite"] is False
        await original(self, source, destination, **kwargs)

    monkeypatch.setattr(SshTarget, "get_file", download)
    restored = manager.retrieve_result_files(record)
    assert restored.state == RunState.FAILED
    assert sorted(p.name for p in local.iterdir()) == ["part-a.dat", "part-b.dat"]
    assert (local / "part-a.dat").read_text() == "part-a.dat"
    assert (local / "part-b.dat").read_text() == "part-b.dat"


def test_directory_reservation_prevents_historical_download(tmp_path, monkeypatch):
    from kkrunner import RunStore
    from kkrunner.runs import DuplicateSubmissionError
    from kkrunner.runs.manager import RunManager

    manager = RunManager(RunStore(tmp_path / "runs"))
    work = tmp_path / "work"
    work.mkdir()
    path = work / "output.dat"
    remote = tmp_path / "remote"
    remote.mkdir()
    (remote / path.name).write_text("historical")
    client = ExecutionClient(target=SshTarget("loopback"))
    manager.register_client(client)
    record = manager.new_record(
        directory=work, remote_directory=remote, target=client.target, backend=client.backend_config,
        files=FileManifest(stdout=FileEntry(str(path), str(remote / path.name)),
                           mapping=FileMapping(str(work), str(remote))),
    )
    record = manager.complete(record, state=RunState.FAILED,
                              execution=record.execution.submitted("historical"))
    reader = manager.new_record(directory=work, files=FileManifest(
        inputs=(FileEntry(str(work), kind="directory"),),
    ))

    with manager.store.observation_lock(reader):
        active = manager.reserve(reader, preprocessing=True)
        with pytest.raises(DuplicateSubmissionError):
            manager.retrieve_result_files(record)
        assert not tuple(work.iterdir())
        assert manager.store.get(record.id) == record
        manager.complete(active, state=RunState.FAILED)
    # Even abandoned preprocessing is not implicitly recovered by a download.
    abandoned = manager.reserve(manager.new_record(directory=work, files=reader.files), preprocessing=True)
    with pytest.raises(DuplicateSubmissionError):
        manager.retrieve_result_files(record)
    assert manager.store.get(abandoned.id) == abandoned
    assert not path.exists()
    assert manager.refresh((abandoned,))[0].state == RunState.FAILED
    proposed = manager.new_record(directory=work, files=reader.files)

    async def download(self, source, destination, **kwargs):
        assert {r.id for r in manager.store.runs()} == {record.id, reader.id, abandoned.id}
        with pytest.raises(DuplicateSubmissionError):
            manager.reserve(proposed)
        await LoopbackFileTransfer.get_file(self, source, destination, **kwargs)

    monkeypatch.setattr(SshTarget, "get_file", download)
    assert manager.retrieve_result_files(record).files.stdout.availability_state == "available"
    assert not manager.conflicts(proposed)
    assert path.read_text() == "historical"
    assert tuple(work.iterdir()) == (path,)
    assert {r.id for r in manager.store.runs()} == {record.id, reader.id, abandoned.id}


def test_restore_wildcard_alias_conflicts_with_canonical_destination(tmp_path):
    from kkrunner import RunStore
    from kkrunner.runs.manager import RunManager
    from kkrunner.runs import DuplicateSubmissionError

    local = tmp_path / "local"
    remote = tmp_path / "remote"
    (local / "actual").mkdir(parents=True)
    (local / "changed").mkdir()
    (remote / "actual").mkdir(parents=True)
    # The historic mapping refers to actual/, but the alias has moved.
    (local / "alias").symlink_to(local / "changed", target_is_directory=True)
    (remote / "actual" / "result.dat").write_text("historical")
    client = ExecutionClient(target=SshTarget("loopback"))
    manager = RunManager(RunStore(tmp_path / "runs"))
    manager.register_client(client)
    record = manager.new_record(
        directory=local, remote_directory=remote, target=client.target,
        backend=client.backend_config, files=FileManifest(
            outputs=(FileEntry(str(local / "alias" / "*.dat"), str(remote / "actual" / "*.dat")),),
            mapping=FileMapping(str(local), str(remote)),
        ),
    )
    record = manager.complete(record, state=RunState.FAILED,
                              execution=record.execution.submitted("historical"))
    path = local / "actual" / "result.dat"
    proposed = manager.new_record(directory=local, files=FileManifest(
        stdout=FileEntry(str(path)),
    ))
    with manager.store.observation_lock(proposed):
        active = manager.reserve(proposed, preprocessing=True)
        with pytest.raises(DuplicateSubmissionError):
            manager.retrieve_result_files(record)
        assert not path.exists()
        manager.complete(active, state=RunState.FAILED)
    manager.retrieve_result_files(record)
    assert path.read_text() == "historical"
    assert not (local / "changed" / "result.dat").exists()


@pytest.mark.parametrize("existing", ["dangling_link", "file"])
def test_historical_retrieval_never_replaces_existing_destination(tmp_path, existing):
    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    (remote / "result.dat").write_text("historical")
    destination = local / "result.dat"
    if existing == "dangling_link":
        destination.symlink_to(local / "missing")
    else:
        destination.write_text("current")
    client = ExecutionClient(target=SshTarget("loopback"))
    from kkrunner import RunStore
    from kkrunner.runs.manager import RunManager
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = manager.new_record(
        directory=local, remote_directory=remote, target=client.target,
        backend=client.backend_config, files=FileManifest(
            outputs=(FileEntry(str(destination), str(remote / destination.name)),),
            mapping=FileMapping(str(local), str(remote)),
        ),
    )
    client.fetch_result_files(record, (destination,))
    if existing == "dangling_link":
        assert destination.is_symlink()
        assert not (local / "missing").exists()
    else:
        assert destination.read_text() == "current"


@pytest.mark.parametrize("local_directory_exists", [False, True])
def test_uncaptured_history_only_restores_regular_file_declarations(
    tmp_path, monkeypatch, local_directory_exists,
):
    from kkrunner import RunStore
    from kkrunner.runs.manager import RunManager

    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    for name in ("input-tree", "output-tree"):
        (remote / name).mkdir()
        if local_directory_exists:
            (local / name).mkdir()
    # A directory matching a file wildcard is not a restored result.
    (local / "part-directory.dat").mkdir()
    (remote / "part-result.dat").write_text("result")
    client = ExecutionClient(target=SshTarget("loopback"))
    manager = RunManager(RunStore(tmp_path / "runs"))
    manager.register_client(client)
    record = manager.new_record(
        directory=local, remote_directory=remote, target=client.target,
        backend=client.backend_config,
        files=FileManifest(
            inputs=(FileEntry(str(local / "input-tree"), str(remote / "input-tree"),
                              kind="directory"),),
            outputs=(
                FileEntry(str(local / "output-tree"), str(remote / "output-tree"),
                          kind="directory", availability_state="pending"),
                FileEntry(str(local / "part-*.dat"), str(remote / "part-*.dat")),
            ),
            mapping=FileMapping(str(local), str(remote)),
        ),
    )
    record = manager.complete(record, state=RunState.FAILED,
                              execution=record.execution.submitted("historical"),
                              artifacts_available=False)
    expanded = []
    original = LocalTarget.expand_files

    async def expand(self, sources, root):
        expanded.append(sources)
        return await original(self, sources, root)

    monkeypatch.setattr(SshTarget, "expand_files", expand)
    restored = manager.retrieve_result_files(record)
    assert expanded == [str(remote / "part-*.dat")]
    assert (local / "part-result.dat").read_text() == "result"
    assert restored.files.outputs[0] == record.files.outputs[0]
    assert restored.files.inputs == record.files.inputs
    assert (local / "output-tree").exists() == local_directory_exists


@pytest.mark.parametrize("remote_content", ["original", "different"])
def test_restore_does_not_read_existing_result_contents(tmp_path, monkeypatch, remote_content):
    from dataclasses import replace
    from kkrunner import RunStore
    from kkrunner.files import versions
    from kkrunner.runs.manager import RunManager

    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    existing, missing = local / "existing.dat", local / "missing.dat"
    existing.write_text("existing")
    missing.write_text("original")
    captured = (
        replace(FileVersion.capture(existing, require_digest=True), fingerprint=None),
        FileVersion.capture(missing, require_digest=True),
    )
    missing.unlink()
    (remote / missing.name).write_text(remote_content)
    client = ExecutionClient(target=SshTarget("loopback"))
    manager = RunManager(RunStore(tmp_path / "runs"))
    manager.register_client(client)
    record = manager.new_record(
        directory=local, remote_directory=remote, target=client.target,
        backend=client.backend_config,
        files=FileManifest(
            outputs=tuple(FileEntry(str(path), str(remote / path.name))
                          for path in (existing, missing)),
            output_versions=captured,
            captured_at="2026-10-13T08:00:00+00:00",
            mapping=FileMapping(str(local), str(remote)),
        ),
    )
    record = manager.complete(record, state=RunState.FINISHED, return_code=0,
                              execution=record.execution.submitted("historical"))

    original_digest = versions.file_digest

    def digest(path, **kwargs):
        from pathlib import Path
        assert Path(path) != existing, "Recovery must not hash an existing result"
        return original_digest(path, **kwargs)

    monkeypatch.setattr(versions, "file_digest", digest)
    restored = manager.retrieve_result_files(record)
    assert existing.read_text() == "existing"
    assert restored.files.outputs[0] == record.files.outputs[0]
    if remote_content == "original":
        assert missing.read_text() == "original"
        assert restored.files.outputs[1].availability_state == "available"
    else:
        assert not missing.exists()
        assert restored.files.outputs[1].availability_state == "failed"
        assert "overwritten" in restored.files.outputs[1].availability_message


@pytest.mark.parametrize("collision", ["explicit", "implicit", "alias"])
def test_staged_result_destinations_must_be_distinct(tmp_path, collision):
    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    first, second = local / "first.dat", local / "second.dat"
    (remote / first.name).write_text("first")
    (remote / second.name).write_text("second")
    client = ExecutionClient(target=SshTarget("loopback"))
    run = RunRecord(
        id="staged-collision", state=RunState.FINISHED,
        created_at="2026-10-13T08:00:00+00:00",
        updated_at="2026-10-13T08:00:00+00:00",
        execution=ExecutionMetadata(client.target, WorkingDirectory(str(local), str(remote)), "job"),
        files=FileManifest(mapping=FileMapping(str(local), str(remote))),
    )
    target = local / "staged.dat"
    if collision == "implicit":
        destinations = {first: second}
    elif collision == "alias":
        alias = tmp_path / "alias"
        alias.symlink_to(local, target_is_directory=True)
        destinations = {first: target, second: alias / target.name}
    else:
        destinations = {first: target, second: target}
    with pytest.raises(ValueError, match="Different historical sources"):
        client.fetch_result_files(run, (first, second), destinations=destinations)
    assert not tuple(local.iterdir())

    verified = []

    def verify(path, temporary):
        from pathlib import Path
        verified.append((path, Path(temporary).read_text()))

    staged = local / "other.dat"
    restored = client.fetch_result_files(
        run, (first, second), destinations={first: target, second: staged}, verify=verify,
    )
    assert restored == (str(first), str(second))
    assert verified == [(str(first), "first"), (str(second), "second")]
    assert target.read_text() == "first"
    assert staged.read_text() == "second"


@pytest.mark.parametrize("existing", (False, True))
def test_declared_historical_output_glob_excludes_uncollected_files(tmp_path, existing):
    local = tmp_path / "local"
    remote = tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    for name in ("scratch.dat", "result.dat"):
        (remote / name).write_text(name)
    from kkrunner import RunStore
    from kkrunner.runs import RunManager

    if existing:
        (local / "scratch.dat").write_text("local scratch")
    client = ExecutionClient(target=SshTarget("loopback"))
    manager = RunManager(RunStore(tmp_path / "runs"))
    manager.register_client(client)
    pattern = str(local / "*.dat")
    run = manager.new_record(
        directory=local, remote_directory=remote, target=client.target,
        backend=client.backend_config,
        files=FileManifest(
            mapping=FileMapping(str(local), str(remote)),
            outputs=(
                FileEntry(pattern, str(remote / "*.dat")),
                FileEntry(str(local / "scratch.dat"), str(remote / "scratch.dat"), collect=False),
            ),
        ),
    )
    run = manager.complete(run, state=RunState.FAILED,
                           execution=run.execution.submitted("historical"))
    manager.retrieve_result_files(run)
    assert (local / "result.dat").read_text() == "result.dat"
    if existing:
        assert (local / "scratch.dat").read_text() == "local scratch"
    else:
        assert not (local / "scratch.dat").exists()


@pytest.mark.parametrize("captured", [False, True])
@pytest.mark.parametrize("remote_exists", [False, True])
def test_restore_input_does_not_create_output_artifact(tmp_path, captured, remote_exists):
    from kkrunner import RunStore
    from kkrunner.runs.manager import RunManager

    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    source = local / "input.inp"
    source.write_text("original input")
    version = FileVersion.capture(source, require_digest=True)
    source.unlink()
    if remote_exists:
        (remote / source.name).write_text("original input")
    client = ExecutionClient(target=SshTarget("loopback"))
    manager = RunManager(RunStore(tmp_path / "runs"))
    manager.register_client(client)
    record = manager.new_record(
        directory=local, remote_directory=remote, target=client.target,
        backend=client.backend_config,
        files=FileManifest(
            inputs=(FileEntry(str(source), str(remote / source.name)),),
            input_versions=(version,) if captured else (),
            captured_at="2026-10-13T08:00:00+00:00" if captured else None,
            mapping=FileMapping(str(local), str(remote)),
        ),
    )
    record = manager.complete(record, state=RunState.FINISHED, return_code=0,
                              execution=record.execution.submitted("historical"))
    restored = manager.retrieve_result_files(record)
    assert source.exists() == remote_exists
    if remote_exists:
        assert source.read_text() == "original input"
        assert version.state() is FileState.CURRENT
    assert restored == record
    assert restored.files.artifacts == ()
    assert manager.store.get(record.id) == record


def test_history_write_failure_is_not_a_transfer_failure(tmp_path, monkeypatch):
    from kkrunner import RunStore
    from kkrunner.runs.manager import RunManager
    from kkrunner.runs.files import RunFiles

    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    output = local / "output.dat"
    (remote / output.name).write_text("result")
    client = ExecutionClient(target=SshTarget("loopback"))
    manager = RunManager(RunStore(tmp_path / "runs"))
    manager.register_client(client)
    record = manager.new_record(
        directory=local, remote_directory=remote, target=client.target,
        backend=client.backend_config,
        files=FileManifest(
            outputs=(FileEntry(str(output), str(remote / output.name)),),
            mapping=FileMapping(str(local), str(remote)),
        ),
    )
    record = manager.complete(record, state=RunState.FINISHED, return_code=0,
                              execution=record.execution.submitted("historical"))
    def fail_history_write(self, record, path, state, message=None):
        assert state == "available"
        raise OSError("history is read-only")

    monkeypatch.setattr(RunFiles, "update_artifact_availability", fail_history_write)
    with pytest.raises(OSError, match="history is read-only"):
        manager.retrieve_result_files(record)
    assert output.read_text() == "result"


def test_restore_results_without_original_execution_backend(tmp_path, monkeypatch):
    from kkrunner import RunStore
    from kkrunner.backends.config import BackendConfig
    from kkrunner.runs.manager import RunManager

    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    path = local / "result.dat"
    source = remote / path.name
    source.write_text("historical result")
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = manager.new_record(
        directory=local, remote_directory=remote, target=SshTarget("loopback"),
        backend=BackendConfig("removed-backend"),
        files=FileManifest(
            stdout=FileEntry(str(path), str(source)),
            mapping=FileMapping(str(local), str(remote)),
        ),
    )
    record = manager.transition(record, state=RunState.FINISHED, return_code=0,
                                execution=record.execution.submitted("historical"))
    assert not record.execution.available
    monkeypatch.setattr(manager.client_pool, "for_config",
                        lambda *args: pytest.fail("File retrieval must not reconstruct a backend"))
    restored = manager.retrieve_result_files(record)
    assert path.read_text() == "historical result"
    assert restored.files.stdout.availability_state == "available"


def test_historical_output_glob_excludes_input_and_cache_namespaces(tmp_path):
    from kkrunner.files.manifest import CacheEntry

    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    for name in ("input.dat", "cache.dat", "result.dat"):
        (remote / name).write_text(name)
    (remote / "protected").mkdir()
    (remote / "protected" / "inside.dat").write_text("input")
    client = ExecutionClient(target=SshTarget("loopback"))
    def entry(name, **kwargs):
        return FileEntry(str(local / name), str(remote / name), **kwargs)
    run = RunRecord(
        id="protected-glob", state=RunState.FINISHED,
        created_at="2026-10-13T08:00:00+00:00",
        updated_at="2026-10-13T08:00:00+00:00",
        execution=ExecutionMetadata(client.target, WorkingDirectory(str(local), str(remote)), "job"),
        files=FileManifest(
            mapping=FileMapping(str(local), str(remote)),
            inputs=(entry("input.dat"), entry("protected", kind="directory")),
            caches=(CacheEntry(entry("cache.dat")),),
            outputs=(entry("**/*.dat"),),
        ),
    )
    pairs = client.resolve_result_files(run, (str(local / "**/*.dat"),))
    assert {pair.local_path for pair in pairs} == {str(local / "result.dat")}


@pytest.mark.parametrize("changed", [False, True])
def test_restore_captured_inputs_without_captured_outputs(tmp_path, changed):
    from kkrunner import RunStore
    from kkrunner.runs import RunManager

    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    original = local / "input-one.dat"
    original.write_text("original")
    version = FileVersion.capture(original, require_digest=True)
    original.unlink()
    (local / "input-two.dat").write_text("another local input")
    (remote / original.name).write_text("changed" if changed else "original")
    (remote / "input-later.dat").write_text("not used by this run")
    (remote / "output.dat").write_text("diagnostic")
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = manager.new_record(
        directory=local, remote_directory=remote, target=SshTarget("loopback"),
        files=FileManifest(
            inputs=(FileEntry(str(local / "input-*.dat"), str(remote / "input-*.dat")),),
            outputs=(FileEntry(str(local / "output.dat"), str(remote / "output.dat")),),
            input_versions=(version,),
            mapping=FileMapping(str(local), str(remote)),
        ),
    )
    record = manager.transition(record, state=RunState.FAILED,
                                execution=record.execution.submitted("historical"))
    restored = manager.retrieve_result_files(record)
    assert original.exists() == (not changed)
    if not changed:
        assert original.read_text() == "original"
    assert not (local / "input-later.dat").exists()
    assert (local / "input-two.dat").read_text() == "another local input"
    assert (local / "output.dat").read_text() == "diagnostic"
    assert restored.files.input_versions == (version,)
    assert restored.files.outputs[0].availability_state == "available"


def test_overlapping_historical_aliases_share_captured_digest(tmp_path, monkeypatch):
    from kkrunner import RunStore
    from kkrunner.runs import RunManager

    local, remote = tmp_path / "local", tmp_path / "remote"
    (local / "actual").mkdir(parents=True)
    (remote / "actual").mkdir(parents=True)
    (local / "alias").symlink_to(local / "actual", target_is_directory=True)
    alias = local / "alias" / "result.dat"
    alias.write_text("original")
    version = FileVersion.capture(alias, require_digest=True)
    alias.unlink()
    (remote / "actual" / "result.dat").write_text("changed")
    (remote / "actual" / "second.dat").write_text("second result")
    attempts = []

    async def download(self, source, destination, **kwargs):
        attempts.append(str(destination))
        await LoopbackFileTransfer.get_file(self, source, destination, **kwargs)

    monkeypatch.setattr(SshTarget, "get_file", download)
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = manager.new_record(
        directory=local, remote_directory=remote, target=SshTarget("loopback"),
        files=FileManifest(
            outputs=(
                FileEntry(str(local / "actual" / "*.dat"), str(remote / "actual" / "*.dat")),
                FileEntry(str(alias), str(remote / "actual" / "result.dat")),
            ),
            output_versions=(version,),
            mapping=FileMapping(str(local), str(remote)),
        ),
    )
    record = manager.transition(record, state=RunState.FAILED,
                                execution=record.execution.submitted("historical"))
    restored = manager.retrieve_result_files(record)
    assert not alias.exists()
    assert (local / "actual" / "second.dat").read_text() == "second result"
    assert attempts.count(str(local / "actual" / "result.dat")) == 1
    assert len(attempts) == 2
    assert all(entry.availability_state == "failed" for entry in restored.files.outputs)
    assert all("overwritten" in entry.availability_message for entry in restored.files.outputs)


def test_history_does_not_claim_file_created_during_retrieval(tmp_path, monkeypatch):
    from pathlib import Path
    from kkrunner import RunStore
    from kkrunner.runs import RunManager

    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    output = local / "result.dat"
    output.write_text("original")
    version = FileVersion.capture(output, require_digest=True)
    output.unlink()
    source = remote / output.name
    source.write_text("original")
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = manager.new_record(
        directory=local, remote_directory=remote, target=SshTarget("loopback"),
        files=FileManifest(
            stdout=FileEntry(str(output), str(source)),
            output_versions=(version,),
            captured_at="2026-10-13T08:00:00+00:00",
            mapping=FileMapping(str(local), str(remote)),
        ),
    )
    record = manager.transition(record, state=RunState.FINISHED, return_code=0,
                                execution=record.execution.submitted("historical"))

    async def concurrent_writer(self, source, destination, **kwargs):
        Path(destination).write_text("another producer")
        await LoopbackFileTransfer.get_file(self, source, destination, **kwargs)

    monkeypatch.setattr(SshTarget, "get_file", concurrent_writer)
    restored = manager.retrieve_result_files(record)
    assert output.read_text() == "another producer"
    assert restored.files.stdout.availability_state == "failed"
    assert "already exists" in restored.files.stdout.availability_message
    assert restored.files.output_versions == (version,)


@pytest.mark.parametrize("state", [RunState.CANCELLED, RunState.FINISHED])
def test_ended_file_collection_persists_partial_manifest(tmp_path, monkeypatch, state):
    from kkrunner import RunStore
    from kkrunner.runs import RunManager, RunStatus
    from kkrunner.client.pool import ClientPool

    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    for name in ("stdout", "stderr"):
        (remote / name).write_text(name)

    monkeypatch.setattr(SshTarget, "query_files", LocalTarget.query_files)

    async def download(self, source, destination, **kwargs):
        if str(source) == str(remote / "stderr"):
            raise OSError("stderr transfer failed")
        await LoopbackFileTransfer.get_file(self, source, destination, **kwargs)

    monkeypatch.setattr(SshTarget, "get_file", download)
    client = ExecutionClient(target=SshTarget("loopback"))
    manager = RunManager(RunStore(tmp_path / "runs"), client_pool=ClientPool())
    manager.register_client(client)
    record = manager.new_record(
        directory=local, remote_directory=remote, target=client.target,
        backend=client.backend_config,
        files=FileManifest(
            stdout=FileEntry(local / "stdout", remote / "stdout"),
            stderr=FileEntry(local / "stderr", remote / "stderr"),
            mapping=FileMapping(local, remote),
        ).initialize_availability(),
    )
    record = manager.transition(record, state=RunState.RUNNING,
                                execution=record.execution.submitted("native-job"))
    monkeypatch.setattr(client, "status", lambda *runs: (
        RunStatus(state, backend_id="native-job"),
    ))
    updated = manager.refresh((record,), retrieve_ended=True)[0]
    saved = manager.store.get(record.id)
    assert saved == updated
    assert saved.state == (RunState.CANCELLED if state == RunState.CANCELLED else RunState.DOWNLOADING)
    assert (local / "stdout").read_text() == "stdout"
    assert not (local / "stderr").exists()
    assert saved.files.stdout.availability_state == "available"
    assert saved.files.stderr.availability_state == "failed"
    assert saved.files.stderr.availability_message == "stderr transfer failed"
    if state == RunState.CANCELLED:
        assert saved.files.captured_at is not None
        assert {v.path.name for v in saved.files.output_versions} == {"stdout"}
    else:
        assert saved.files.captured_at is None
        # The first attempt already published stdout. Retrying stderr must
        # not invalidate that result if its target-side copy has disappeared.
        (remote / "stdout").unlink()
        monkeypatch.setattr(SshTarget, "get_file", LoopbackFileTransfer.get_file)
        finished = manager.refresh((saved,), retrieve_ended=True)[0]
        assert finished.state == RunState.FINISHED
        assert finished.files.stderr.availability_state == "available"
        assert (local / "stderr").read_text() == "stderr"
        assert finished.files.stdout.availability_state == "available"
        assert (local / "stdout").read_text() == "stdout"
        assert {v.path.name for v in finished.files.output_versions} == {"stdout", "stderr"}
        assert finished.message == "Process ended; exit code unavailable"


def test_client_collects_ended_files_without_original_backend(tmp_path, monkeypatch):
    from kkrunner import RunStore
    from kkrunner.backends.config import BackendConfig
    from kkrunner.runs import RunManager, RunStatus

    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    (remote / "stdout").write_text("result")
    monkeypatch.setattr(SshTarget, "query_files", LocalTarget.query_files)
    client = ExecutionClient(target=SshTarget("loopback"))
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = manager.new_record(
        directory=local, remote_directory=remote, target=client.target,
        backend=BackendConfig("unavailable-test-backend"),
        files=FileManifest(
            stdout=FileEntry(local / "stdout", remote / "stdout"),
            mapping=FileMapping(local, remote),
        ).initialize_availability(),
    )
    # File collection needs neither a backend reference nor an installed backend.
    files, error = client.collect_ended_files(record, RunStatus(RunState.CANCELLED))
    assert error is None
    assert (local / "stdout").read_text() == "result"
    assert files.stdout.availability_state == "available"
    assert files.output_versions[0].is_current()


def test_ended_file_collection_reports_session_setup_failure(tmp_path):
    from kkrunner.runs import RunManager, RunStatus
    from kkrunner.client.file_access import FileAccess

    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    target = SshTarget("loopback")
    record = RunManager().new_record(
        directory=local, target=target,
        files=FileManifest(
            stdout=FileEntry(local / "stdout", remote / "stdout"),
            mapping=FileMapping(local, remote),
        ).initialize_availability(),
    )
    files, error = FileAccess(target).collect_ended_files(
        record, RunStatus(RunState.CANCELLED),
    )
    assert isinstance(error, ValueError)
    assert "working directory" in str(error)
    assert files.stdout.availability_state == "failed"
    assert files.stdout.availability_message == str(error)
    assert not (local / "stdout").exists()


def test_ended_collection_retry_rechecks_failed_pattern_members(tmp_path, monkeypatch):
    from kkrunner import RunStore
    from kkrunner.runs import RunManager, RunStatus

    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    target = SshTarget("loopback")
    client = ExecutionClient(target=target)
    manager = RunManager(RunStore(tmp_path / "runs"))
    pattern = local / "*.dat"
    record = manager.new_record(
        directory=local, remote_directory=remote, target=target,
        files=FileManifest(
            outputs=(
                FileEntry(pattern, remote / "*.dat", availability_state="failed"),
                FileEntry(local / "missing.dat", remote / "missing.dat",
                          source_pattern=pattern, availability_state="failed"),
            ),
            mapping=FileMapping(local, remote),
        ),
    )
    monkeypatch.setattr(SshTarget, "query_files", LocalTarget.query_files)
    files, error = client.collect_ended_files(record, RunStatus(RunState.FINISHED))
    assert error is None
    assert all(entry.availability_state == "skipped" for entry in files.outputs)
    assert files.captured_at is not None


@pytest.mark.parametrize("via_client", [False, True])
def test_finalization_preserves_captured_manifest_and_files(tmp_path, monkeypatch, via_client):
    import asyncio
    from kkrunner import RunStore
    from kkrunner.runs import RunManager, RunStatus
    from kkrunner.client.file_context import FileContext
    from kkrunner.client.file_session import FileSession
    from kkrunner.client.staging import StagingSpec
    from kkrunner.files.availability import capture_result_files

    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    output = local / "result.dat"
    output.write_text("original result")
    (remote / output.name).write_text("another run's result")
    (remote / "new.dat").write_text("another run's new output")
    pattern = local / "*.dat"
    files = capture_result_files(FileManifest(
        stdout=FileEntry(local / "stdout", remote / "stdout",
                         availability_state="failed", availability_message="original failure"),
        outputs=(
            FileEntry(pattern, remote / "*.dat", availability_state="available"),
            FileEntry(output, remote / output.name, source_pattern=pattern,
                      availability_state="available"),
        ),
        mapping=FileMapping(local, remote),
    ), require_outputs=False)
    target = SshTarget("loopback")
    client = ExecutionClient(target=target)
    record = RunManager(RunStore(tmp_path / "runs")).new_record(
        directory=local, remote_directory=remote, target=target, files=files,
    )
    status = RunStatus(RunState.CANCELLED)

    async def unexpected_io(*args, **kwargs):
        pytest.fail("Captured results must not query or transfer remote files")

    monkeypatch.setattr(SshTarget, "query_files", unexpected_io)
    monkeypatch.setattr(SshTarget, "get_file", unexpected_io)
    monkeypatch.setattr(SshTarget, "get_files", unexpected_io)

    if via_client:
        finalized, error = client.collect_ended_files(record, status)
        assert error is None
    else:
        context = FileContext(target, StagingSpec.from_mapping(files.mapping))
        session = asyncio.run(FileSession.from_attachment(context, record))
        finalized = session.finalize(status)

    assert finalized == files
    assert output.read_text() == "original result"
    assert not (local / "new.dat").exists()
    assert files.output_versions[0].is_current()
    assert finalized.stdout.availability_state == "failed"
    assert finalized.stdout.availability_message == "original failure"

@pytest.mark.parametrize("policy", ["missing", "always"])
def test_ended_collection_retry_keeps_published_cache(tmp_path, monkeypatch, policy):
    from dataclasses import replace
    from kkrunner.files.manifest import CacheEntry
    from kkrunner.runs import RunManager, RunStatus

    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    for name in ("first.cache", "second.cache"):
        (remote / name).write_text(name)
    target = SshTarget("loopback")
    client = ExecutionClient(target=target)
    record = RunManager().new_record(
        directory=local, remote_directory=remote, target=target,
        files=FileManifest(
            caches=tuple(CacheEntry(FileEntry(local / name, remote / name))
                         for name in ("first.cache", "second.cache")),
            mapping=FileMapping(local, remote, output_sync=policy),
        ).initialize_availability(),
    )
    monkeypatch.setattr(SshTarget, "query_files", LocalTarget.query_files)
    calls = []
    fail_second = True

    async def download(self, source, destination, **kwargs):
        calls.append(str(source))
        if fail_second and str(source) == str(remote / "second.cache"):
            raise OSError("second cache transfer failed")
        await LoopbackFileTransfer.get_file(self, source, destination, **kwargs)

    monkeypatch.setattr(SshTarget, "get_file", download)
    status = RunStatus(RunState.FINISHED)
    partial, error = client.collect_ended_files(record, status)
    assert isinstance(error, OSError)
    assert partial.caches[0].file.availability_state == "available"
    assert partial.caches[1].file.availability_state == "failed"

    fail_second = False
    completed, error = client.collect_ended_files(replace(record, files=partial), status)
    assert error is None
    assert all(cache.file.availability_state == "available" for cache in completed.caches)
    assert calls == [str(remote / name) for name in ("first.cache", "second.cache", "second.cache")]
    assert (local / "first.cache").read_text() == "first.cache"
    assert (local / "second.cache").read_text() == "second.cache"
    assert {version.path.name for version in completed.cache_versions} == {"first.cache", "second.cache"}


@pytest.mark.parametrize("kind", ["output", "cache"])
@pytest.mark.parametrize("remove_source", [False, True])
def test_ended_collection_retry_keeps_published_pattern_members(tmp_path, monkeypatch, kind, remove_source):
    from dataclasses import replace
    from kkrunner.files.manifest import CacheEntry
    from kkrunner.runs import RunManager, RunStatus

    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    for name in ("first-1.dat", "second-1.dat"):
        (remote / name).write_text(name)
    target = SshTarget("loopback")
    client = ExecutionClient(target=target)
    declarations = tuple(FileEntry(local / pattern, remote / pattern)
                         for pattern in ("first-*.dat", "second-*.dat"))
    files = FileManifest(
        outputs=declarations if kind == "output" else (),
        caches=tuple(CacheEntry(entry) for entry in declarations) if kind == "cache" else (),
        mapping=FileMapping(local, remote, output_sync="always"),
    ).initialize_availability()
    record = RunManager().new_record(
        directory=local, remote_directory=remote, target=target, files=files,
    )
    monkeypatch.setattr(SshTarget, "query_files", LocalTarget.query_files)
    calls = []
    fail_second = True

    async def download(self, pairs, source_directory, destination_directory):
        for source, destination in pairs:
            calls.append(str(source))
            if fail_second and str(source) == str(remote / "second-1.dat"):
                raise OSError("second pattern transfer failed")
            await LoopbackFileTransfer.get_file(self, source, destination)

    monkeypatch.setattr(SshTarget, "get_files", download)
    status = RunStatus(RunState.FINISHED, return_code=0)
    partial, error = client.collect_ended_files(record, status)
    assert isinstance(error, OSError)
    assert (local / "first-1.dat").read_text() == "first-1.dat"
    assert not (local / "second-1.dat").exists()
    if remove_source:
        (remote / "first-1.dat").unlink()

    fail_second = False
    completed, error = client.collect_ended_files(replace(record, files=partial), status)
    assert error is None
    assert {entry.availability_state for entry in completed.artifacts} == {"available"}
    assert calls == [str(remote / name) for name in ("first-1.dat", "second-1.dat", "second-1.dat")]
    assert (local / "first-1.dat").read_text() == "first-1.dat"
    assert (local / "second-1.dat").read_text() == "second-1.dat"
    if kind == "output":
        assert {version.path.name for version in completed.output_versions} == {"first-1.dat", "second-1.dat"}


def test_published_directory_does_not_satisfy_required_file_pattern(tmp_path, monkeypatch):
    from kkrunner.runs import RunManager, RunStatus

    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    (local / "tree.dat").mkdir()
    target = SshTarget("loopback")
    record = RunManager().new_record(
        directory=local, remote_directory=remote, target=target,
        files=FileManifest(
            outputs=(
                FileEntry(local / "tree.dat", remote / "tree.dat", kind="directory",
                          availability_state="available"),
                FileEntry(local / "*.dat", remote / "*.dat"),
            ),
            mapping=FileMapping(local, remote),
        ).initialize_availability(),
    )
    monkeypatch.setattr(SshTarget, "query_files", LocalTarget.query_files)
    files, error = ExecutionClient(target=target).collect_ended_files(
        record, RunStatus(RunState.FINISHED, return_code=0),
    )
    assert isinstance(error, FileNotFoundError)
    assert files.outputs[0].availability_state == "available"
    assert files.outputs[1].availability_state == "failed"
