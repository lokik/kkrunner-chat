from threading import Event
from types import SimpleNamespace

import pytest

from kkrunner.runs import DuplicateSubmissionError
from kkrunner.client import EndpointClient
from kkrunner.backends import SupervisedBackend
from kkrunner.backends.contracts import JobStatus
from kkrunner import ExecutionSpec
from kkrunner.files import FileEntry, FileManifest
from kkrunner import RunState, RunStore
from kkrunner.runs.manager import RunManager


class _Runner:
    def __init__(self, callback=None):
        self.callback = callback
        self.state = RunState.NEW
        self.backend_id = None
        self._done = Event()
        self.result = SimpleNamespace(return_code=None)
        self.exception = None
        self.output_error = None
        self.failure_phase = None
        self.failure_message = None

    @property
    def return_code(self):
        return self.result.return_code

    @property
    def requires_stream_retrieval(self):
        return False

    def configure_detached(self, detached):
        pass

    def configure_submission(self, request, **observation):
        pass

    def status(self):
        return JobStatus(self.state, return_code=self.return_code)

    def add_output_error_listener(self, callback):
        pass

    def submit(self):
        self.backend_id = "job"
        self.state = RunState.QUEUED

    def wait(self):
        self._done.wait(2)
        return self.result

    def run(self):
        self.submit()
        return self.wait()

    def cancel(self):
        self.state = RunState.CANCELLED
        self._done.set()

    def add_output_listener(self, callback):
        pass

    def finish(self):
        self.result.return_code = 0
        self.state = RunState.FINISHED
        self._done.set()
        if self.callback:
            self.callback(self.result)


class _Executor(EndpointClient):
    def __init__(self):
        super().__init__(SupervisedBackend)
        self.runners = []

    def acknowledge_submission(self, record):
        pass

    def acknowledge(self, record):
        pass

    def create_run_parts(self, spec, *, submission_key=None, callback=None):
        parts = super().create_run_parts(spec, submission_key=submission_key, callback=callback)
        runner = _Runner(callback)
        runner.spec = spec
        self.runners.append(runner)
        parts.job = runner
        return parts


def _run(manager, executor, directory, *, task="DOS", dataset="case"):
    return manager.create_run(
        executor,
        spec=ExecutionSpec(argv=("unused",), directory=str(directory), output_file=str(directory / f"{dataset}.out")),
        process_result=lambda outcome: outcome,
        metadata={"test": {"task": str(task).upper(), "dataset": dataset}},
    )


def test_duplicate_task_and_dataset_is_rejected_across_managers(tmp_path):
    directory = tmp_path / "work"
    store_path = tmp_path / "runs"
    first_manager = RunManager(RunStore(store_path))
    second_manager = RunManager(RunStore(store_path))
    first = _run(first_manager, _Executor(), directory)
    second = _run(second_manager, _Executor(), directory)
    first.submit()
    with pytest.raises(DuplicateSubmissionError) as exc:
        second.submit()
    assert exc.value.conflicts[0].id == first.id
    assert second.record.state == RunState.NEW


def test_different_dataset_can_share_non_scf_directory(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    executor = _Executor()
    first = _run(manager, executor, tmp_path, dataset="one")
    second = _run(manager, executor, tmp_path, dataset="two")
    first.submit()
    second.submit()
    assert first.record.state == RunState.QUEUED
    assert second.record.state == RunState.QUEUED


def test_task_names_do_not_create_conflicts_with_disjoint_manifests(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    executor = _Executor()
    scf = _run(manager, executor, tmp_path, task="SCF", dataset="scf")
    dos = _run(manager, executor, tmp_path, task="DOS", dataset="dos")
    scf.submit()
    dos.submit()
    assert dos.record.state == RunState.QUEUED


def test_final_run_releases_submission_namespace(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    executor = _Executor()
    first = _run(manager, executor, tmp_path)
    first.submit()
    (tmp_path / "case.out").write_text("result")
    executor.runners[0].finish()
    first.wait()
    assert manager.store.get(first.id).state == RunState.FINISHED
    second = _run(manager, executor, tmp_path)
    second.submit()
    assert second.record.state == RunState.QUEUED


def test_submitting_record_contains_submission_key_before_backend_submit(tmp_path):
    store = RunStore(tmp_path / "runs")
    manager = RunManager(store)
    executor = _Executor()
    run = _run(manager, executor, tmp_path)
    backend = executor.runners[0]
    seen = []
    original_submit = backend.submit

    def submit():
        record = store.get(run.id)
        seen.append(record)
        original_submit()

    backend.submit = submit
    run.submit()
    assert seen[0].state == RunState.SUBMITTING
    assert seen[0].metadata["test"]["task"] == "DOS"
    assert seen[0].metadata["test"]["dataset"] == "case"
    assert seen[0].execution.directory.local == str(tmp_path.resolve())


@pytest.mark.parametrize("overlap", (True, False), ids=("same-file", "separate-files"))
def test_declared_exact_outputs_conflict_only_if_they_share_a_target(tmp_path, overlap):
    manager = RunManager(RunStore(tmp_path / "runs"))
    first_path = str(tmp_path / "one.out")
    second_path = first_path if overlap else str(tmp_path / "two.out")
    first = manager.new_record(
        directory=tmp_path,
        files=FileManifest(stdout=FileEntry(local_path=first_path)),
        metadata={"test": {"task": "DOS", "dataset": "one"}},
        processing_required=True,
    )
    second = manager.new_record(
        directory=tmp_path,
        files=FileManifest(outputs=(FileEntry(local_path=second_path),))
        if overlap
        else FileManifest(stdout=FileEntry(local_path=second_path)),
        metadata={"test": {"task": str("BSF" if overlap else "DOS").upper(), "dataset": "two" if overlap else "one"}},
        processing_required=True,
    )
    manager.reserve(first)
    if overlap:
        with pytest.raises(DuplicateSubmissionError):
            manager.reserve(second)
    else:
        assert manager.reserve(second).state == RunState.SUBMITTING


@pytest.mark.parametrize(
    "first_access, same_directory, conflict",
    [
        pytest.param("output", True, True, id="writer-reader-same-directory"),
        pytest.param("output", False, True, id="writer-reader-different-directories"),
        pytest.param("input", False, False, id="two-readers-different-directories"),
    ],
)
def test_declared_read_write_conflicts_respect_access_and_absolute_paths(
    tmp_path, first_access, same_directory, conflict
):
    manager = RunManager(RunStore(tmp_path / "runs"))
    shared = tmp_path / ("shared.pot" if same_directory or first_access == "input" else "shared.out")
    if same_directory:
        shared.write_text("density")
    first_directory = tmp_path if same_directory else tmp_path / "one"
    second_directory = tmp_path if same_directory else tmp_path / "two"
    entry = FileEntry(local_path=str(shared))
    first_files = FileManifest(outputs=(entry,)) if first_access == "output" else FileManifest(inputs=(entry,))
    second_files = FileManifest(
        inputs=(FileEntry(local_path=str(shared)),),
        stdout=FileEntry(local_path=str(tmp_path / "other.out")) if same_directory else None,
    )
    first = manager.new_record(
        directory=first_directory,
        files=first_files,
        metadata={"test": {"task": "DOS", "dataset": "one"}},
        processing_required=True,
    )
    second = manager.new_record(
        directory=second_directory,
        files=second_files,
        metadata={"test": {"task": "BSF", "dataset": "two"}},
        processing_required=True,
    )
    manager.reserve(first)
    if conflict:
        with pytest.raises(DuplicateSubmissionError):
            manager.reserve(second)
    else:
        assert manager.reserve(second).state == RunState.SUBMITTING


@pytest.mark.parametrize(
    "first_pattern, second_pattern, second_is_stdout, conflict",
    [
        pytest.param("result-*.dat", "result-1.dat", True, True, id="wildcard-versus-exact-stdout"),
        pytest.param("spectrum-*.dat", "spectrum-?.dat", False, True, id="overlapping-wildcard-patterns"),
        pytest.param("bsf-*.dat", "dos-*.dat", False, False, id="disjoint-wildcard-patterns"),
    ],
)
def test_wildcard_output_collisions_follow_pattern_overlap(
    tmp_path, first_pattern, second_pattern, second_is_stdout, conflict
):
    manager = RunManager(RunStore(tmp_path / "runs"))
    first = manager.new_record(
        directory=tmp_path,
        files=FileManifest(outputs=(FileEntry(local_path=str(tmp_path / first_pattern)),)),
        metadata={"test": {"task": "DOS"}},
        processing_required=True,
    )
    second_entry = FileEntry(local_path=str(tmp_path / second_pattern))
    second = manager.new_record(
        directory=tmp_path,
        files=FileManifest(stdout=second_entry) if second_is_stdout else FileManifest(outputs=(second_entry,)),
        metadata={"test": {"task": "BSF"}},
        processing_required=True,
    )
    manager.reserve(first)
    if conflict:
        with pytest.raises(DuplicateSubmissionError):
            manager.reserve(second)
    else:
        assert manager.reserve(second).state == RunState.SUBMITTING


def test_remote_conflicts_use_declared_target_paths_before_staging(tmp_path):
    from kkrunner.client import SshTransport

    manager = RunManager(RunStore(tmp_path / "runs"))
    transport = SshTransport("cluster")
    first = manager.new_record(
        directory=tmp_path / "one",
        remote_directory="/jobs/shared",
        transport=transport,
        files=FileManifest(stdout=FileEntry(local_path=str(tmp_path / "one/result.txt"))),
    )
    second = manager.new_record(
        directory=tmp_path / "two",
        remote_directory="/jobs/shared",
        transport=transport,
        files=FileManifest(inputs=(FileEntry(local_path=str(tmp_path / "two/result.txt")),)),
    )
    manager.reserve(first)
    with pytest.raises(DuplicateSubmissionError):
        manager.reserve(second)
