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


def test_file_mutation_guard_rejects_active_references_across_stores(tmp_path):
    from kkrunner import MemoryRunStore
    store = RunStore(tmp_path / "runs")
    memory = MemoryRunStore()
    manager = RunManager(store, conflict_stores=(memory,))
    other = RunManager(memory, conflict_stores=(store,))
    path = tmp_path / "shared.out"
    record = other.new_record(directory=tmp_path, files=FileManifest(stdout=FileEntry(str(path))))
    other.reserve(record)
    with pytest.raises(OSError, match="active run"):
        with manager.file_mutation_guard({str(path): "file"}):
            pytest.fail("Deletion of an actively reserved file was allowed")
    with manager.file_mutation_guard({str(tmp_path / "unrelated.out"): "file"}):
        pass


def test_file_mutation_guard_checks_client_paths_for_remote_reservations(tmp_path):
    from kkrunner.transports import SshTransport
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = manager.new_record(
        directory=tmp_path, transport=SshTransport("cluster"),
        remote_directory="/remote/work",
        files=FileManifest(stdout=FileEntry(local_path="relative.out",
                                           remote_path="/remote/work/relative.out")),
    )
    manager.reserve(record)
    with pytest.raises(OSError, match="active run"):
        with manager.file_mutation_guard({str(tmp_path / "relative.out"): "file"}):
            pytest.fail("Deletion of a reserved local output was allowed")
    assert "/remote/work/relative.out" in manager.file_mutation_conflicts(
        {"/remote/work/relative.out": "file"}, target=record,
    )


def test_best_effort_cleanup_reports_only_blocked_paths(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    path = tmp_path / "shared.out"
    alias = tmp_path / "alias.out"
    path.write_text("shared")
    alias.symlink_to(path)
    record = manager.new_record(directory=tmp_path,
                                files=FileManifest(inputs=(FileEntry(str(alias)),)))
    manager.reserve(record)
    other = tmp_path / "unrelated.out"
    blocked = manager.file_mutation_conflicts({str(path): "file", str(other): "file"})
    assert set(blocked) == {str(path)}
    assert record.id in blocked[str(path)]


def test_file_mutation_guard_serializes_submissions_without_blocking_record_reads(tmp_path):
    from concurrent.futures import ThreadPoolExecutor, TimeoutError
    from threading import Event
    from kkrunner import MemoryRunStore, RunState
    disk, memory = RunStore(tmp_path / "runs"), MemoryRunStore()
    manager = RunManager(disk, conflict_stores=(memory,))
    other = RunManager(memory, conflict_stores=(disk,))
    path = tmp_path / "future.out"
    record = other.new_record(directory=tmp_path, files=FileManifest(stdout=FileEntry(str(path))))
    started = Event()

    def reserve():
        started.set()
        return other.reserve(record)

    with ThreadPoolExecutor(max_workers=2) as pool:
        with manager.file_mutation_guard({str(path): "file"}):
            assert pool.submit(disk.runs).result(timeout=2) == ()
            assert pool.submit(memory.runs).result(timeout=2) == ()
            submission = pool.submit(reserve)
            assert started.wait(2)
            with pytest.raises(TimeoutError):
                submission.result(timeout=0.1)
        assert submission.result(timeout=5).state == RunState.SUBMITTING


def test_download_retry_obeys_same_reservations(tmp_path):
    from kkrunner import RunState
    from kkrunner.runs import FailurePhase, DuplicateSubmissionError
    manager = RunManager(RunStore(tmp_path / "runs"))
    files = FileManifest(stdout=FileEntry(str(tmp_path / "shared.out")))
    failed = manager.new_record(directory=tmp_path, files=files)
    failed = manager.transition(failed, state=RunState.FAILED, return_code=0,
                                failure_phase=FailurePhase.DOWNLOAD,
                                execution=failed.execution.submitted("native"))
    active = manager.reserve(manager.new_record(directory=tmp_path, files=files))
    with pytest.raises(DuplicateSubmissionError):
        manager.resume_download(failed)
    assert manager.store.get(failed.id).state is RunState.FAILED
    manager.transition(active, state=RunState.CANCELLED)
    assert manager.resume_download(failed).state is RunState.DOWNLOADING


def test_postprocessing_retry_obeys_same_reservations(tmp_path):
    from kkrunner import RunState
    from kkrunner.runs import FailurePhase, DuplicateSubmissionError
    from kkrunner.runs.files import capture_result_files
    manager = RunManager(RunStore(tmp_path / "runs"))
    path = tmp_path / "shared.out"
    path.write_text("result")
    files = FileManifest(stdout=FileEntry(str(path)))
    failed = manager.new_record(directory=tmp_path, files=files, processing_required=True)
    failed = manager.transition(failed, state=RunState.FAILED, return_code=0,
                                failure_phase=FailurePhase.POSTPROCESSING,
                                files=capture_result_files(files))
    active = manager.reserve(manager.new_record(directory=tmp_path, files=files))
    with pytest.raises(DuplicateSubmissionError):
        manager.retry_postprocessing(failed, lambda _: pytest.fail("Processed a reserved file"))
    assert manager.store.get(failed.id).state is RunState.FAILED
    manager.transition(active, state=RunState.CANCELLED)
    assert manager.retry_postprocessing(failed, lambda _: "parsed") == "parsed"
