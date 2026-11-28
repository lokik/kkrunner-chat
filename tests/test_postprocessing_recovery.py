"""Durable completion and retry ownership without application result readers."""

from threading import Event
from concurrent.futures import ThreadPoolExecutor

import pytest

from kkrunner import RunFileState
from kkrunner.runs import RunStatus
from kkrunner import ExecutionOutcome, RunState, RunStore
from kkrunner.runs import FailurePhase
from kkrunner.client import ExecutionClient
from kkrunner.backends import SupervisedBackend
from kkrunner.backends.contracts import JobStatus
from kkrunner import ExecutionSpec
from kkrunner.runs.manager import RunManager


class _RawRunner:
    def __init__(self):
        self.state = RunState.NEW
        self.backend_id = None
        self.result = ExecutionOutcome(return_code=0)
        self.exception = None
        self.failure_phase = None
        self.failure_message = None
        self.output_error = None

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

    async def run_async(self, *, on_submitted=None):
        self.backend_id = "job-1"
        self.state = RunState.FINISHED
        if on_submitted is not None:
            await on_submitted()
        return self.status()

    async def wait_async(self):
        return self.status()

    def cancel(self):
        self.state = RunState.CANCELLED

    def add_output_listener(self, callback):
        pass

    def add_output_error_listener(self, callback):
        pass


class _Executor(ExecutionClient):
    def __init__(self):
        super().__init__(SupervisedBackend)

    async def acknowledge_submission_async(self, record):
        pass

    async def acknowledge_async(self, record):
        pass

    def create_run_parts(self, spec, *, submission_key=None, callback=None):
        parts = super().create_run_parts(spec, submission_key=submission_key, callback=callback)
        self.runner = _RawRunner()
        self.runner.spec = spec
        parts.job = self.runner
        return parts


def test_finished_run_is_persisted_before_callback(tmp_path):
    store = RunStore(tmp_path / "runs")
    manager = RunManager(store)
    finished = Event()
    run = _Executor().create_run(
        ExecutionSpec(argv=("unused",), directory=str(tmp_path)), tracked=manager,
        process_result=lambda outcome: outcome,
        callback=lambda result: finished.set(),
        metadata={"example": {"task": "DOS".upper()}},
    )
    run.run()
    assert finished.is_set()
    record = store.get(run.id)
    assert record.state == RunState.FINISHED
    assert record.finished_at is not None


def test_result_processor_failure_is_postprocessing_failure(tmp_path):
    store = RunStore(tmp_path / "runs")
    manager = RunManager(store)
    executor = _Executor()

    def fail(_outcome):
        raise ValueError("result parsing failed")

    run = executor.create_run(
        ExecutionSpec(argv=("unused",), directory=str(tmp_path)), tracked=manager,
        process_result=fail,
        metadata={"example": {"task": "DOS".upper()}},
    )
    with pytest.raises(ValueError, match="result parsing failed"):
        run.run()
    record = store.get(run.id)
    assert record.state == RunState.FINISHED
    assert record.file_state == RunFileState.FAILED
    assert record.file_failure_phase == FailurePhase.POSTPROCESSING
    assert record.file_message == "result parsing failed"
    assert record.finished_at is not None



@pytest.mark.parametrize("processor", [None, 42])
def test_retry_without_processor_preserves_failed_result(tmp_path, processor):
    from kkrunner.files import FileEntry, FileManifest

    manager = RunManager(RunStore(tmp_path / "runs"))
    output = tmp_path / "out"
    output.write_text("result")
    record = manager.new_record(
        directory=tmp_path, processing_required=True,
        files=FileManifest(stdout=FileEntry(str(output))),
    )
    failed = manager.complete(
        record, state=RunState.FINISHED, return_code=0,
        file_state=RunFileState.FAILED,
        file_failure_phase=FailurePhase.POSTPROCESSING,
        file_message="original parser error", artifacts_available=True,
        execution=record.execution.submitted("native"),
    )
    with pytest.raises(TypeError, match="callable result processor"):
        manager.retry_postprocessing(failed, processor)
    assert manager.store.get(failed.id) == failed
    assert not manager.store.runs(active=True)
    assert manager.retry_postprocessing(failed, lambda outcome: "parsed") == "parsed"


@pytest.mark.parametrize("during_retry", ["finished", "failed", "refreshed"])
def test_retry_does_not_overwrite_concurrent_record_update(tmp_path, during_retry):
    from kkrunner.files import FileEntry, FileManifest

    manager = RunManager(RunStore(tmp_path / "runs"))
    output = tmp_path / "run.out"
    output.write_text("saved data")
    record = manager.new_record(
        directory=tmp_path,
        files=FileManifest(stdout=FileEntry(local_path=str(output))),
        metadata={"example": {"task": "DOS".upper()}},
        processing_required=True,
    )
    record = manager.transition(
        record, state=RunState.RUNNING, execution=record.execution.submitted("existing-backend")
    )
    failed = manager.complete(
        record,
        state=RunState.FINISHED,
        return_code=0,
        file_state=RunFileState.FAILED,
        file_failure_phase=FailurePhase.POSTPROCESSING,
        artifacts_available=True,
    )

    def concurrent_change(outcome):
        current = manager.store.get(failed.id)
        if during_retry == "refreshed":
            manager.update(current, RunStatus(RunState.FAILED, return_code=7))
        else:
            with pytest.raises(RuntimeError, match="live client"):
                manager.complete(
                    current, state=RunState.FINISHED,
                    file_state=RunFileState.POSTPROCESSED if during_retry == "finished" else RunFileState.FAILED,
                    return_code=0,
                )
        return outcome

    # A competing observer cannot replace the processing owner's state,
    # even if it attempts to publish a terminal result mid-callback.
    manager.retry_postprocessing(failed.id, concurrent_change)
    latest = manager.store.get(failed.id)
    assert latest.state == RunState.FINISHED
    assert latest.message is None


def test_retry_exception_does_not_overwrite_concurrent_completion(tmp_path):
    from kkrunner.files import FileEntry, FileManifest

    manager = RunManager(RunStore(tmp_path / "runs"))
    output = tmp_path / "run.out"
    output.write_text("saved data")
    record = manager.new_record(
        directory=tmp_path,
        files=FileManifest(stdout=FileEntry(local_path=str(output))),
        metadata={"example": {"task": "DOS".upper()}},
        processing_required=True,
    )
    record = manager.transition(
        record, state=RunState.RUNNING, execution=record.execution.submitted("existing-backend")
    )
    failed = manager.complete(
        record,
        state=RunState.FINISHED,
        return_code=0,
        file_state=RunFileState.FAILED,
        file_failure_phase=FailurePhase.POSTPROCESSING,
        artifacts_available=True,
    )

    def interrupted(outcome):
        with pytest.raises(RuntimeError, match="live client"):
            manager.complete(
                manager.store.get(failed.id), state=RunState.FINISHED, return_code=0,
                file_state=RunFileState.POSTPROCESSED,
            )
        raise ValueError("parser error")

    with pytest.raises(ValueError, match="parser error"):
        manager.retry_postprocessing(failed.id, interrupted)
    current = manager.store.get(failed.id)
    assert current.state == RunState.FINISHED
    assert current.file_state == RunFileState.FAILED
    assert current.file_failure_phase == FailurePhase.POSTPROCESSING
    assert current.file_message == "parser error"


@pytest.mark.parametrize("input_change", ("overwritten", "missing"))
def test_postprocessing_retry_accepts_unchanged_output_with_stale_input(tmp_path, input_change):
    """Captured output remains retryable when the original input is stale."""
    from kkrunner.files import FileEntry, FileManifest, FileState, FileVersion

    manager = RunManager(RunStore(tmp_path / "runs"))
    output = tmp_path / "result.out"
    source = tmp_path / "original.inp"
    output.write_bytes(b"captured result")
    source.write_bytes(b"original input")
    record = manager.new_record(
        directory=tmp_path,
        files=FileManifest(
            inputs=(FileEntry(local_path=str(source)),), stdout=FileEntry(local_path=str(output)),
            input_versions=(FileVersion.capture(source),),
        ),
        metadata={"example": {"task": "DOS".upper()}},
        processing_required=True,
    )
    prepared = manager.transition(
        record, state=RunState.RUNNING, execution=record.execution.submitted("existing-backend")
    )
    if input_change == "missing":
        source.unlink()
    else:
        source.write_bytes(b"changed input")
    failed = manager.complete(
        prepared,
        state=RunState.FINISHED,
        return_code=0,
        file_state=RunFileState.FAILED,
        file_failure_phase=FailurePhase.POSTPROCESSING,
        artifacts_available=True,
    )
    assert failed.files.output_versions
    assert failed.files.input_versions == record.files.input_versions
    assert all(item.state() is FileState.CURRENT for item in failed.files.output_versions)
    assert any(item.state() is not FileState.CURRENT for item in failed.files.input_versions)
    seen = []

    def parse(outcome):
        seen.append(outcome)
        return output.read_bytes()

    assert manager.retry_postprocessing(failed.id, parse) == b"captured result"
    assert len(seen) == 1
    assert seen[0].input_file is None
    assert seen[0].output_file == str(output)
    current = manager.store.get(failed.id)
    assert current.state is RunState.FINISHED
    assert current.files == failed.files


def test_historical_retry_releases_global_locks_before_file_validation(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from kkrunner import MemoryRunStore
    from kkrunner.files import FileEntry, FileManifest, FileVersion
    from kkrunner.runs import DuplicateSubmissionError

    disk, memory = RunStore(tmp_path / "runs"), MemoryRunStore()
    manager = RunManager(disk, conflict_stores=(memory,))
    other = RunManager(memory, conflict_stores=(disk,))
    output = tmp_path / "result.out"
    output.write_text("saved result")
    manifest = FileManifest(stdout=FileEntry(str(output)))
    record = manager.new_record(directory=tmp_path, files=manifest, processing_required=True)
    failed = manager.complete(
        record, state=RunState.FINISHED, return_code=0,
        file_state=RunFileState.FAILED,
        file_failure_phase=FailurePhase.POSTPROCESSING, artifacts_available=True,
        execution=record.execution.submitted("original"),
    )

    validating, release = Event(), Event()
    original_state = FileVersion.state

    def state(version):
        if str(version.path) == str(output) and not release.is_set():
            validating.set()
            assert release.wait(5)
        return original_state(version)

    monkeypatch.setattr(FileVersion, "state", state)

    def reserve_other():
        unrelated = other.new_record(directory=tmp_path, files=FileManifest(
            stdout=FileEntry(str(tmp_path / "unrelated.out")),
        ))
        assert other.reserve(unrelated).state == RunState.SUBMITTING
        competing = other.new_record(directory=tmp_path, files=manifest)
        with pytest.raises(DuplicateSubmissionError):
            other.reserve(competing)

    with ThreadPoolExecutor(max_workers=2) as pool:
        future = pool.submit(manager.retry_postprocessing, failed, lambda outcome: "parsed")
        try:
            assert validating.wait(5)
            assert disk.get(record.id).file_state == RunFileState.POSTPROCESSING
            pool.submit(reserve_other).result(timeout=3)
        finally:
            release.set()
        assert future.result(timeout=5) == "parsed"
    assert disk.get(record.id).state == RunState.FINISHED


def test_retry_cannot_commit_success_after_last_moment_artifact_failure(tmp_path, monkeypatch):
    from kkrunner.files import FileEntry, FileManifest, ArtifactAvailabilityState

    manager = RunManager(RunStore(tmp_path / "runs"))
    output = tmp_path / "out"
    output.write_text("answer")
    record = manager.complete(manager.new_record(directory=tmp_path, processing_required=True,
        files=FileManifest(stdout=FileEntry(str(output)))),
        state=RunState.FINISHED, return_code=0, artifacts_available=True)
    transition = manager._commit

    def change_before_commit(record, **kwargs):
        if kwargs.get("file_state") == RunFileState.POSTPROCESSED:
            manager.update_artifact_availability(
                record, output, ArtifactAvailabilityState.FAILED, "late artifact failure",
            )
        return transition(record, **kwargs)

    monkeypatch.setattr(manager, "_commit", change_before_commit)
    with pytest.raises(OSError, match="late artifact failure"):
        manager.retry_postprocessing(record, lambda outcome: "parsed")
    saved = manager.store.get(record.id)
    assert saved.state == RunState.FINISHED
    assert saved.file_state == RunFileState.FAILED
    assert saved.file_failure_phase == FailurePhase.POSTPROCESSING


@pytest.mark.parametrize("shared", [False, True])
def test_processing_reservation_protects_reads_not_original_producer(tmp_path, shared):
    from dataclasses import replace
    from kkrunner import MemoryRunStore
    from kkrunner.files import FileEntry, FileManifest, FileMapping
    from kkrunner.runs import DuplicateSubmissionError
    from kkrunner.runs.conflicts import referenced_paths
    from kkrunner.targets import SshTarget

    manager = RunManager(MemoryRunStore())
    peer = RunManager(manager.store)
    target = SshTarget("cluster")
    original_root = tmp_path / "old"
    original_root.mkdir()
    output = original_root / "part-one.dat"
    output.write_text("answer")
    manifest = FileManifest(
        outputs=(FileEntry(original_root / "part-*.dat", "/work/part-*.dat"),),
        mapping=FileMapping(original_root, "/work", shared_filesystem=shared),
    )
    manifest = manifest.with_file_availability(
        output, "available", remote_path="/work/part-one.dat",
        source_pattern=str(original_root / "part-*.dat"),
    )
    manifest = manifest.with_file_availability(original_root / "part-*.dat", "available")
    manifest = replace(manifest, remote_patterns_resolved=True)
    record = manager.new_record(
        directory=original_root, remote_directory="/work", target=target,
        processing_required=True, files=manifest,
    )
    failed = manager.complete(
        record, state=RunState.FINISHED, return_code=0, artifacts_available=True,
        file_state=RunFileState.FAILED, file_failure_phase=FailurePhase.POSTPROCESSING,
    )
    assert failed.files.output_versions[0].path == output

    # A new producer using the original remote workspace but a different
    # local workspace can coexist unless that workspace is shared storage.
    other_root = tmp_path / "new"
    remote_writer = peer.new_record(
        directory=other_root, remote_directory="/work", target=target,
        files=FileManifest(
            outputs=(FileEntry(other_root / output.name, "/work/part-one.dat"),),
            mapping=FileMapping(other_root, "/work", shared_filesystem=shared),
        ),
    )
    if not shared:
        peer.reserve(remote_writer)

    def process(outcome):
        current = manager.store.get(failed.id)
        assert current.file_state == RunFileState.POSTPROCESSING
        assert "/work/part-one.dat" in referenced_paths(current)
        reader = peer.new_record(directory=original_root, files=FileManifest(
            inputs=(FileEntry(output),),
        ))
        peer.reserve(reader)
        writer = peer.new_record(directory=original_root, files=FileManifest(
            outputs=(FileEntry(output),),
        ))
        with pytest.raises(DuplicateSubmissionError):
            peer.reserve(writer)
        if shared:
            with pytest.raises(DuplicateSubmissionError):
                peer.reserve(remote_writer)
        # A local wildcard declaration no longer reserves files that were
        # never collected for this run.
        peer.reserve(peer.new_record(directory=original_root, files=FileManifest(
            outputs=(FileEntry(original_root / "part-later.dat"),),
        )))
        return output.read_text()

    assert manager.retry_postprocessing(failed, process) == "answer"
    assert manager.store.get(failed.id).file_state == RunFileState.POSTPROCESSED
