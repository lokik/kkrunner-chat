"""Clearing a promoted directory cache must retain file safety checks."""

import pytest

from kkrunner.files import FileEntry, FileManifest
from kkrunner.files.availability import prepare_target_outputs, discover_local_files
from kkrunner.runs.conflicts import submission_conflicts
from kkrunner.runs.manager import RunManager
from kkrunner.runs.records import RunStore


def request(root, directory, *, input_file=None):
    return {
        "directory": str(root), "input_file": None if input_file is None else str(input_file),
        "stdin": None, "protected_files": [], "output_file": None,
        "error_file": None, "protected_patterns": [], "output_files": [str(directory)],
        "output_directories": [str(directory)],
    }


def test_directory_output_removes_stale_cache(tmp_path):
    directory = tmp_path / "Fe.tau.dir"
    directory.mkdir()
    (directory / "P1E1").write_text("stale")
    input_file = tmp_path / "host.pot"
    input_file.write_text("input")
    prepare_target_outputs(request(tmp_path, directory, input_file=input_file))
    assert not directory.exists()
    assert input_file.read_text() == "input"


def test_directory_cleanup_rejects_inputs_inside(tmp_path):
    directory = tmp_path / "Fe.tau.dir"
    directory.mkdir()
    input_file = directory / "P1E1"
    input_file.write_text("protected")
    with pytest.raises(ValueError, match="contains input/cache"):
        prepare_target_outputs(request(tmp_path, directory, input_file=input_file))
    assert input_file.read_text() == "protected"


def test_directory_discovery_recognizes_optional_output(tmp_path):
    directory = tmp_path / "Fe.tau.dir"
    declaration = FileEntry(local_path=str(directory), kind="directory", optional=True)
    manifest = FileManifest(outputs=(declaration,))
    assert discover_local_files(manifest).outputs[0].availability_state == "skipped"
    directory.mkdir()
    assert discover_local_files(manifest).outputs[0].availability_state == "available"


def test_directory_reservation_conflicts_with_child_file(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    directory = tmp_path / "Fe.tau.dir"
    output = manager.new_record(
        directory=tmp_path,
        files=FileManifest(outputs=(FileEntry(str(directory), kind="directory", optional=True),)),
    )
    reader = manager.new_record(
        directory=tmp_path,
        files=FileManifest(inputs=(FileEntry(str(directory / "P1E1")),)),
    )
    assert submission_conflicts(output, reader)


def test_target_request_declares_directory_outputs(tmp_path):
    from kkrunner.client.execution_request import execution_request
    from kkrunner.files import TaskFile
    from kkrunner.runs.base import ExecutionSpec

    spec = ExecutionSpec(
        argv=("true",), directory=str(tmp_path),
        output_files=(TaskFile("Fe.tau.dir", kind="directory", optional=True),),
    )
    request = execution_request(spec)
    assert request["output_directories"] == [str(tmp_path / "Fe.tau.dir")]
    assert request["output_files"] == [str(tmp_path / "Fe.tau.dir")]


def test_directory_output_refuses_to_remove_working_directory(tmp_path):
    with pytest.raises(ValueError, match="working directory"):
        prepare_target_outputs(request(tmp_path, tmp_path))


def test_directory_output_rejects_symlink_without_removing_target(tmp_path):
    actual = tmp_path / "actual"
    actual.mkdir()
    (actual / "keep").write_text("safe")
    link = tmp_path / "Fe.tau.dir"
    link.symlink_to(actual, target_is_directory=True)
    with pytest.raises(ValueError, match="symbolic link"):
        prepare_target_outputs(request(tmp_path, link))
    assert (actual / "keep").read_text() == "safe"
