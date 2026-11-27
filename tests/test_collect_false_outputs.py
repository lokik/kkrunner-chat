"""Uncollected output files still reserve and clear their paths."""
import asyncio
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from kkrunner import ExecutionSpec, TaskFile
from kkrunner.client.execution_request import execution_request
from kkrunner.client.file_session import FileSession
from kkrunner.files.availability import discover_local_files, prepare_target_outputs
from kkrunner.files.manifest import FileEntry, FileManifest, FileSyncPolicy
from kkrunner.files.snapshots import matching_file
from kkrunner.runs.conflicts import _manifest_accesses
from kkrunner.files.availability import capture_result_files


def spec_for(tmp_path):
    return ExecutionSpec(
        ["python", "-c", "print('ok')"],
        directory=tmp_path,
        output_files=(
            TaskFile("unused.dos", collect=False),
            TaskFile("result.dat", optional=True),
        ),
    )


def test_task_file_collect_is_part_of_identity_and_validated():
    assert TaskFile("a") == "a"
    assert TaskFile("a", collect=False) != TaskFile("a")
    assert TaskFile("a", collect=False) != "a"
    assert len({TaskFile("a", collect=False), TaskFile("a")}) == 2
    with pytest.raises(TypeError, match="collect"):
        TaskFile("a", collect=0)
    with pytest.raises(TypeError, match="collect"):
        FileEntry(local_path="/tmp/a", collect=0)


def test_uncollected_file_stays_reserved_but_is_not_an_artifact(tmp_path):
    spec = spec_for(tmp_path)
    files = FileManifest.from_spec(spec)
    unused = str(tmp_path / "unused.dos")
    result = str(tmp_path / "result.dat")
    assert files.outputs[0].local_path == unused
    assert files.outputs[0].collect is False
    assert files.outputs[0].availability_state is None
    assert unused not in files.output_files
    assert unused not in files.manifests
    assert unused not in [x.local_path for x in files.artifacts]
    assert result in files.output_files
    assert matching_file(files, unused) is None
    assert matching_file(files, result) is not None

    record = SimpleNamespace(
        files=files,
        execution=SimpleNamespace(
            directory=SimpleNamespace(local=str(tmp_path), remote=None, target=str(tmp_path)),
        ),
    )
    reads, writes = _manifest_accesses(record, remote=False)
    assert unused in writes
    assert result in writes


def test_uncollected_file_is_cleared_normally_before_launch(tmp_path):
    spec = spec_for(tmp_path)
    unused = tmp_path / "unused.dos"
    result = tmp_path / "result.dat"
    unused.write_text("stale")
    result.write_text("stale")
    prepare_target_outputs(execution_request(spec))
    assert not unused.exists()
    assert not result.exists()


def test_uncollected_file_is_never_required_or_captured(tmp_path):
    files = FileManifest.from_spec(spec_for(tmp_path))
    unused = tmp_path / "unused.dos"
    unused.write_text("not a result")
    # A missing ordinary optional output must not be treated as required.
    files = discover_local_files(files, require_outputs=True)
    assert files.outputs[0].availability_state is None
    assert files.outputs[1].availability_state == "skipped"
    captured = capture_result_files(files)
    assert captured.captured_at is not None
    assert all(version.path != unused for version in captured.output_versions)
    assert not captured.output_versions
    assert captured.outputs[0].availability_state is None


def test_uncollected_outputs_not_downloaded_even_after_reattach(tmp_path):
    files = FileManifest.from_spec(spec_for(tmp_path))
    remote_root = "/remote/project"
    files = replace(files, outputs=tuple(
        replace(entry, remote_path=remote_root + "/" + Path(entry.local_path).name)
        for entry in files.outputs
    ))

    session = object.__new__(FileSession)
    session.context = SimpleNamespace(output_files=FileSyncPolicy.CHANGED)
    session._files = files
    session._staging = {
        "output_targets": {entry.local_path: entry.remote_path for entry in files.outputs},
        "output_patterns": (),
    }
    session.result = SimpleNamespace(output_file=None, error_file=None)
    attempted = []

    async def capture(local, remote, required, **kwargs):
        attempted.append((local, remote, required))

    session._sync_output_entry = capture
    asyncio.run(session._sync_output_files(required=True))
    assert [Path(local).name for local, _, _ in attempted] == ["result.dat"]

    # The stored FileEntry carries collect=False; no ExecutionSpec declarations
    # are available after reconstruction of a historical run.
    session._staging = None
    attempted.clear()
    asyncio.run(session._sync_output_files(required=True))
    assert [Path(local).name for local, _, _ in attempted] == ["result.dat"]


def test_collect_false_rejected_for_inputs_caches_and_primary_stream(tmp_path):
    with pytest.raises(ValueError, match="collect=False"):
        FileManifest.from_spec(ExecutionSpec(
            ["python"], directory=tmp_path,
            input_files=(TaskFile("input", collect=False),),
        ))
    with pytest.raises(ValueError, match="collect=False"):
        FileManifest.from_spec(ExecutionSpec(
            ["python"], directory=tmp_path,
            cache_files=(TaskFile("cache", collect=False),),
        ))
    with pytest.raises(ValueError, match="collect=False"):
        FileManifest.from_spec(ExecutionSpec(
            ["python"], directory=tmp_path,
            output_file=TaskFile("stdout", collect=False),
        ))


@pytest.mark.parametrize("excluded", ("scratch.dat", "scratch*.dat"))
def test_output_glob_does_not_collect_excluded_files(tmp_path, excluded):
    ignored = tmp_path / "scratch.dat"
    result = tmp_path / "result.dat"
    ignored.write_text("temporary")
    result.write_text("result")
    spec = ExecutionSpec(
        ("true",), directory=tmp_path,
        output_files=(TaskFile(excluded, collect=False), "*.dat"),
    )
    files = capture_result_files(FileManifest.from_spec(spec))
    assert {version.path for version in files.output_versions} == {result}
    assert ignored not in {Path(entry.local_path) for entry in files.artifacts}
    assert matching_file(files, ignored) is None
    assert matching_file(files, result) is not None


def test_output_glob_only_matching_excluded_files_is_not_an_artifact_failure(tmp_path):
    (tmp_path / "scratch.dat").write_text("temporary")
    files = FileManifest.from_spec(ExecutionSpec(
        ("true",), directory=tmp_path,
        output_files=(TaskFile("scratch.dat", collect=False), "*.dat"),
    ))
    files = capture_result_files(files)
    assert not files.failed_artifacts
    assert not files.output_versions
    assert files.outputs[1].availability_state == "skipped"


@pytest.mark.parametrize("excluded", ("scratch.dat", "scratch*.dat"))
def test_remote_output_glob_does_not_download_excluded_files(tmp_path, excluded):
    from .test_remote import _text_run, _processed_run

    executor, spec, workdir, remote_root = _text_run(
        tmp_path, declared_output_files=(TaskFile(excluded, collect=False), "*.dat"),
        code="from pathlib import Path; Path('scratch.dat').write_text('temporary'); Path('result.dat').write_text('result')",
    )
    runner = _processed_run(executor, spec)
    runner.run()
    assert (remote_root / "calculation" / "scratch.dat").read_text() == "temporary"
    assert not (workdir / "scratch.dat").exists()
    assert (workdir / "result.dat").read_text() == "result"
    assert matching_file(runner.files, workdir / "scratch.dat") is None


def test_remote_output_glob_exclusion_uses_mapped_path(tmp_path):
    from .test_remote import _text_run, _processed_run

    workdir = tmp_path / "local" / "calculation"
    workdir.mkdir(parents=True)
    (workdir / "alias").symlink_to(workdir, target_is_directory=True)
    executor, spec, workdir, remote_root = _text_run(
        tmp_path, declared_output_files=(
            TaskFile(workdir / "alias" / "scratch*.dat", collect=False),
            "*.dat",
        ),
        code="from pathlib import Path; Path('scratch.dat').write_text('temporary'); Path('result.dat').write_text('result')",
    )
    runner = _processed_run(executor, spec)
    runner.run()
    assert (remote_root / "calculation" / "scratch.dat").exists()
    assert not (workdir / "scratch.dat").exists()
    assert (workdir / "result.dat").exists()
