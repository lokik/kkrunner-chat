"""Collision reporting exposes exact (or conservatively overlapping) file paths."""
from kkrunner import RunManager, RunStore
from kkrunner.files import FileEntry, FileManifest
from kkrunner.runs.conflicts import conflicting_files, submission_conflicts


def test_file_conflict_details_include_access_and_path(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    first = manager.new_record(
        directory=tmp_path,
        files=FileManifest(stdout=FileEntry(str(tmp_path / "shared.out"))),
    )
    second = manager.new_record(
        directory=tmp_path,
        files=FileManifest(stdout=FileEntry(str(tmp_path / "shared.out"))),
    )
    details = conflicting_files(first, second)
    assert submission_conflicts(first, second)
    assert any(
        item.first_path == str(tmp_path / "shared.out")
        and item.second_path == str(tmp_path / "shared.out")
        and item.first_access == item.second_access == "write"
        for item in details
    )


def test_unrelated_output_files_have_no_conflict_details(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    first = manager.new_record(
        directory=tmp_path,
        files=FileManifest(stdout=FileEntry(str(tmp_path / "first.out"))),
    )
    second = manager.new_record(
        directory=tmp_path,
        files=FileManifest(stdout=FileEntry(str(tmp_path / "second.out"))),
    )
    assert not submission_conflicts(first, second)
    assert conflicting_files(first, second) == ()
