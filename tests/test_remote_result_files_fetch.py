import pytest

from kkrunner import RunState
from kkrunner.files import FileEntry, FileManifest, FileState, FileVersion
from kkrunner.files.manifest import FileMapping
from kkrunner.client import EndpointClient, SshTransport, LocalTransport
from kkrunner.runs import ExecutionMetadata, RunRecord, WorkingDirectory


@pytest.fixture(autouse=True)
def loopback_file_transport(monkeypatch):
    """Use a persistable SSH configuration with local filesystem primitives.

    This test verifies FileAccess path mapping, not the SSH process protocol.
    """
    monkeypatch.setattr(SshTransport, "expand_files", LocalTransport.expand_files)
    monkeypatch.setattr(SshTransport, "put_file", LocalTransport.put_file)
    monkeypatch.setattr(SshTransport, "get_file", LocalTransport.get_file)


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
    executor = EndpointClient(transport=SshTransport("loopback"))
    run = RunRecord(
        id="historical",
        state=RunState.FINISHED,
        created_at="2026-10-13T08:00:00+00:00",
        updated_at="2026-10-13T08:00:00+00:00",
        execution=ExecutionMetadata(
            executor.transport,
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
    assert executor.supports_result_file_retrieval is True
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
    native_expand = LocalTransport.expand_files
    native_get = LocalTransport.get_file

    async def observed_expand(self, sources, root):
        expanded.append(os.fspath(sources))
        return await native_expand(self, sources, root)

    async def observed_get(self, source, destination):
        downloaded.append(os.fspath(source))
        return await native_get(self, source, destination)

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
    executor = EndpointClient(transport=SshTransport("loopback"))
    run = RunRecord(
        id="wildcard-fetch",
        state=RunState.FINISHED,
        created_at="2026-10-13T08:00:00+00:00",
        updated_at="2026-10-13T08:00:00+00:00",
        execution=ExecutionMetadata(
            executor.transport,
            WorkingDirectory(str(local_root / "calc"), str(remote_dir)),
            "opaque-handle",
        ),
        files=FileManifest(mapping=FileMapping(str(local_root), str(remote_root))),
        metadata={"example": {"task": "DOS".upper()}},
        processing_required=True,
    )
    with patch.object(SshTransport, "expand_files", observed_expand), patch.object(SshTransport, "get_file", observed_get):
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
    executor = EndpointClient(transport=SshTransport("loopback"))
    run = RunRecord(
        id="staged-result",
        state=RunState.FINISHED,
        created_at="2026-10-13T08:00:00+00:00",
        updated_at="2026-10-13T08:00:00+00:00",
        execution=ExecutionMetadata(
            executor.transport, WorkingDirectory(str(local_dir), str(remote_dir)), "backend-id"
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
    executor = EndpointClient(transport=SshTransport("loopback"))
    run = RunRecord(
        id="escaped-result",
        state=RunState.FINISHED,
        created_at="2026-10-13T08:00:00+00:00",
        updated_at="2026-10-13T08:00:00+00:00",
        execution=ExecutionMetadata(
            executor.transport, WorkingDirectory(str(local_root), str(remote_root)), "backend-id"
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
    executor = EndpointClient(transport=SshTransport("loopback"))
    run = RunRecord(
        id="escaped-staged",
        state=RunState.FINISHED,
        created_at="2026-10-13T08:00:00+00:00",
        updated_at="2026-10-13T08:00:00+00:00",
        execution=ExecutionMetadata(executor.transport, WorkingDirectory(str(local), str(remote)), "backend-id"),
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
    from kkrunner.runs.files import RunFiles

    main = tmp_path / "run.out"
    auxiliary = tmp_path / "run_SCFSTART.dos"
    main.write_bytes(b"main output")
    auxiliary.write_bytes(b"auxiliary output")
    versions = tuple(FileVersion.capture(path, require_digest=True) for path in (auxiliary, main))
    main.unlink()
    auxiliary.unlink()

    manifest = FileManifest(
        stdout=FileEntry(str(main)),
        outputs=(FileEntry(str(auxiliary), optional=True),),
        output_versions=versions,
        captured_at="2026-10-13T08:00:00+00:00",
    )
    record = RunRecord(
        id="missing-auxiliary", state=RunState.FINISHED,
        created_at="2026-10-13T08:00:00+00:00",
        updated_at="2026-10-13T08:00:00+00:00",
        execution=ExecutionMetadata(
            SshTransport("loopback"),
            WorkingDirectory(str(tmp_path), "/remote"),
            "backend-id",
        ),
        files=manifest,
    )

    class Store:
        current = record

        def get(self, _run):
            return self.current

        def update(self, _run, updater):
            self.current = updater(self.current)
            return self.current

    class Client:
        supports_result_file_retrieval = True

        def fetch_result_files(self, _record, paths, *, destinations=None):
            path = str(paths[0])
            if path == str(main):
                target = destinations[path] if destinations else path
                from pathlib import Path
                Path(target).write_bytes(b"main output")
            # Missing auxiliary deliberately produces no staged file.

    store = Store()
    service = RunFiles(store=store, client_pool=None)
    service._client_for = lambda _record: Client()
    restored = service.retrieve_result_files(record)
    assert main.read_bytes() == b"main output"
    assert not auxiliary.exists()
    assert restored.files.stdout.availability_state == "available"
    assert restored.files.outputs[0].availability_state == "failed"
    assert "not retrieved" in restored.files.outputs[0].availability_message
