"""Collision reporting exposes exact (or conservatively overlapping) file paths."""
import pytest

from kkrunner import RunManager, RunStore
from kkrunner.files import FileEntry, FileManifest
from kkrunner.runs.conflicts import conflicting_files, file_reference_overlaps, submission_conflicts


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


@pytest.mark.parametrize("path,reference,directory,expected", (
    ("/scratch/Fe.dos", "/scratch/*.dos", False, True),
    ("/scratch/Fe.dos", "/scratch/*.out", False, False),
    ("/scratch/job", "/scratch/job/*.dos", True, True),
    ("/scratch/job/a.dat", "/scratch/job", False, True),
    ("/scratch/job", "/scratch/job2/a.dat", True, False),
))
def test_cleanup_reference_overlap(path, reference, directory, expected):
    assert file_reference_overlaps(path, reference, directory=directory) == expected


@pytest.mark.parametrize("pattern,relative,expected", [
    ("nested/**/*.dat", "nested/one.dat", True),
    ("nested/*.dat", "nested/deep/one.dat", False),
    ("nested/**/*.dat", "nested/deep/one.dat", True),
])
def test_recursive_glob_reservations(tmp_path, pattern, relative, expected):
    manager = RunManager(RunStore(tmp_path / "runs"))
    first = manager.new_record(directory=tmp_path, files=FileManifest(
        outputs=(FileEntry(str(tmp_path / pattern)),)))
    second = manager.new_record(directory=tmp_path, files=FileManifest(
        stdout=FileEntry(str(tmp_path / relative))))
    assert submission_conflicts(first, second) == expected
    assert bool(conflicting_files(first, second)) == expected


def test_directory_reservation_covers_hidden_descendants(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    first = manager.new_record(directory=tmp_path, files=FileManifest(
        outputs=(FileEntry(str(tmp_path / "tree"), kind="directory"),)))
    second = manager.new_record(directory=tmp_path, files=FileManifest(
        stdout=FileEntry(str(tmp_path / "tree/.hidden/deep.dat"))))
    assert submission_conflicts(first, second)
