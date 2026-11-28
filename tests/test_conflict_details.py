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
    assert len(details) == 1
    item = details[0]
    assert item.namespace == "local"
    assert item.first_path == item.second_path == str(tmp_path / "shared.out")
    assert item.first_access == item.second_access == "write"


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


def test_distinct_local_paths_conflict_on_same_target_path(tmp_path):
    from kkrunner.files import FileMapping
    from kkrunner.targets import SshTarget

    manager = RunManager(RunStore(tmp_path / "runs"))

    def record(name):
        local = tmp_path / name
        return manager.new_record(
            directory=local, remote_directory="/work", target=SshTarget("cluster"),
            files=FileManifest(
                stdout=FileEntry(str(local / "result"), "/work/result"),
                mapping=FileMapping(str(local), "/work"),
            ),
        )

    first, second = record("first"), record("second")
    assert submission_conflicts(first, second)
    details = conflicting_files(first, second)
    assert len(details) == 1
    assert details[0].namespace == "target"
    assert details[0].first_path == details[0].second_path == "/work/result"
    assert details[0].first_access == details[0].second_access == "write"


@pytest.mark.parametrize("name,other,expected", [
    ("result[1].log", "result1.log", False),
    ("result[1].log", "result[1].log", True),
    ("result*.log", "result2.log", False),
    ("result?.log", "result?.log", True),
])
def test_named_stream_reservations_are_literal(tmp_path, name, other, expected):
    manager = RunManager(RunStore(tmp_path / "runs"))
    first = manager.new_record(directory=tmp_path, files=FileManifest(
        stdout=FileEntry(str(tmp_path / name))))
    second = manager.new_record(directory=tmp_path, files=FileManifest(
        stdout=FileEntry(str(tmp_path / other))))
    assert submission_conflicts(first, second) == expected
    assert bool(conflicting_files(first, second)) == expected


def test_literal_stream_conflicts_with_matching_output_glob(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    first = manager.new_record(directory=tmp_path, files=FileManifest(
        stdout=FileEntry(str(tmp_path / "result[1].log"))))
    second = manager.new_record(directory=tmp_path, files=FileManifest(
        outputs=(FileEntry(str(tmp_path / "result*.log")),)))
    assert submission_conflicts(first, second)
    assert submission_conflicts(second, first)


def test_literal_directory_reserves_its_tree_not_a_similarly_named_tree(tmp_path):
    from kkrunner.runs.conflicts import referenced_paths

    manager = RunManager(RunStore(tmp_path / "runs"))
    directory = tmp_path / "tree[1]"
    first = manager.new_record(directory=tmp_path, files=FileManifest(
        outputs=(FileEntry(str(directory), kind="directory"),)))
    references = referenced_paths(first)
    for path, expected in ((directory / ".hidden" / "data", True),
                           (tmp_path / "tree1" / "data", False)):
        second = manager.new_record(directory=tmp_path, files=FileManifest(
            stdout=FileEntry(str(path))))
        assert submission_conflicts(first, second) == expected
        assert any(file_reference_overlaps(str(path), ref) for ref in references) == expected
