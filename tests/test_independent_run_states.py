"""The execution outcome and client file lifecycle are independent."""

from dataclasses import replace
from concurrent.futures import ThreadPoolExecutor
from threading import Event
import sys

import pytest

from kkrunner import ExecutionSpec, MemoryRunStore, RunFileState, RunManager, RunState, RunStore
from kkrunner.backends import DirectBackend
from kkrunner.client import ExecutionClient
from kkrunner.runs import DuplicateSubmissionError
from kkrunner.execution import FailurePhase


@pytest.mark.parametrize("durable", [False, True])
def test_cancelled_execution_keeps_its_file_reservation(tmp_path, durable):
    store = RunStore(tmp_path / "records") if durable else MemoryRunStore()
    manager = RunManager(store)
    record = manager.new_record(directory=tmp_path)
    cancelled = replace(record, state=RunState.CANCELLED, file_state=RunFileState.COLLECTING)
    store.save(cancelled)
    assert store.get(record.id).state is RunState.CANCELLED
    assert store.runs(active=True) == (cancelled,)
    completed = store.update(cancelled, lambda latest: replace(latest, file_state=RunFileState.POSTPROCESSED))
    assert completed.state is RunState.CANCELLED
    assert store.runs(active=True) == ()
    assert store.runs(active=False) == (completed,)


@pytest.mark.parametrize("tracked", [False, True])
@pytest.mark.parametrize("code", [0, 7])
def test_processor_receives_native_outcome(tmp_path, tracked, code):
    manager = RunManager(MemoryRunStore()) if tracked else False
    states = []
    run = ExecutionClient(DirectBackend).create_run(
        ExecutionSpec((sys.executable, "-c", f"raise SystemExit({code})"), directory=tmp_path),
        tracked=manager, process_result=lambda outcome: states.append((outcome.state, outcome.return_code)),
    )
    run.run()
    expected = RunState.FINISHED if code == 0 else RunState.FAILED
    assert states == [(expected, code)]
    assert run.state is expected
    assert run.file_state is RunFileState.POSTPROCESSED
    if tracked:
        record = manager.store.get(run.id)
        assert record.state is expected
        assert record.file_state is RunFileState.POSTPROCESSED


@pytest.mark.parametrize("tracked", [False, True])
def test_processor_failure_does_not_change_successful_execution(tmp_path, tracked):
    manager = RunManager(MemoryRunStore()) if tracked else False

    def fail(outcome):
        assert outcome.state is RunState.FINISHED
        raise ValueError("Cannot parse output")

    run = ExecutionClient(DirectBackend).create_run(
        ExecutionSpec((sys.executable, "-c", "pass"), directory=tmp_path),
        tracked=manager, process_result=fail,
    )
    with pytest.raises(ValueError, match="Cannot parse"):
        run.run()
    assert run.state is RunState.FINISHED
    assert run.return_code == 0
    assert run.file_state is RunFileState.FAILED
    assert run.file_failure_phase is FailurePhase.POSTPROCESSING
    if tracked:
        record = manager.store.get(run.id)
        assert record.state is RunState.FINISHED
        assert record.failure_phase is None
        assert record.file_state is RunFileState.FAILED
        assert record.file_failure_phase is FailurePhase.POSTPROCESSING
        assert manager.retry_postprocessing(record, lambda outcome: outcome.return_code) == 0
        record = manager.store.get(run.id)
        assert record.state is RunState.FINISHED
        assert record.file_state is RunFileState.POSTPROCESSED


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Independent native cancellation needs Linux identity")
def test_second_client_cancellation_preserves_file_ownership_and_processing(tmp_path):
    root = tmp_path / "records"
    first, second = RunManager(RunStore(root)), RunManager(RunStore(root))
    entered, release = Event(), Event()
    processed = []
    run = ExecutionClient(DirectBackend).create_run(
        ExecutionSpec((sys.executable, "-c", "import time; print('partial', flush=True); time.sleep(20)"),
                      directory=tmp_path, output_file="output"),
        tracked=first, detached=True,
        process_result=lambda outcome: processed.append(outcome.state),
    )

    def accepted(_):
        entered.set()
        assert release.wait(10)

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(run.run, on_submitted=accepted)
        try:
            assert entered.wait(5)
            cancelled = second.cancel(run.id)
            assert cancelled.state is RunState.CANCELLED
            assert cancelled.file_state is RunFileState.WAITING
            assert second.store.get(run.id).state is RunState.CANCELLED
            assert second.store.runs(active=True) == (cancelled,)
            conflict = second.new_record(directory=tmp_path, files=run.files)
            with pytest.raises(DuplicateSubmissionError):
                second.reserve(conflict)
        finally:
            release.set()
        future.result(timeout=10)
    final = second.store.get(run.id)
    assert final.state is RunState.CANCELLED
    assert final.file_state is RunFileState.POSTPROCESSED
    assert final.return_code is not None
    assert processed == [RunState.CANCELLED]
    assert second.store.runs(active=True) == ()

def test_native_exit_waits_for_backend_output_publication(tmp_path):
    from kkrunner.runs import RunStatus

    manager = RunManager(MemoryRunStore())
    record = manager.new_record(directory=tmp_path)
    record = manager.transition(record, state=RunState.RUNNING, file_state=RunFileState.WAITING)
    record = manager.update(record, RunStatus(RunState.FINISHED, return_code=0, output_pending=True))
    assert record.state is RunState.FINISHED
    assert record.file_state is RunFileState.WAITING
    assert record.is_active
    record = manager.update(record, RunStatus(RunState.FINISHED, return_code=0))
    assert record.file_state is RunFileState.COLLECTING
    assert record.is_active


def test_output_publication_failure_does_not_change_native_exit(tmp_path):
    from kkrunner.runs import RunStatus

    manager = RunManager(MemoryRunStore())
    record = manager.new_record(directory=tmp_path)
    record = manager.transition(record, state=RunState.RUNNING, file_state=RunFileState.WAITING)
    record = manager.update(record, RunStatus(RunState.FINISHED, return_code=0, file_error="disk full"))
    assert record.state is RunState.FINISHED
    assert record.return_code == 0
    assert record.failure_phase is None
    assert record.file_state is RunFileState.FAILED
    assert record.file_failure_phase is FailurePhase.OUTPUT
    assert record.file_message == "disk full"
    assert not record.is_active


@pytest.mark.parametrize("marker", [True, False])
def test_dead_supervisor_reports_failed_publication_without_losing_exit(tmp_path, monkeypatch, marker):
    import json
    from kkrunner.backends.implementations.supervised import backend
    from kkrunner.runs import RunStatus

    if marker:
        (tmp_path / "active").write_text(json.dumps({
            "state": "finished", "return_code": 0, "output_pending": True,
        }))
    monkeypatch.setattr(backend, "decode_handle", lambda _: {
        "kind": "kkrunner", "receipt_id": "test", "socket_path": str(tmp_path / "socket"),
        "identity": {},
    })
    monkeypatch.setattr(backend, "_process_outcome", lambda _: None)
    from types import SimpleNamespace
    monkeypatch.setattr(backend, "ProcessIdentity", SimpleNamespace(
        from_dict=lambda _: SimpleNamespace(is_running=lambda: False),
    ))
    facts = backend.SupervisedBackend.status("test")
    assert facts["state"] == ("finished" if marker else "unknown")
    assert not facts.get("output_pending")
    assert facts["file_error"]

    manager = RunManager(MemoryRunStore())
    record = manager.new_record(directory=tmp_path)
    record = manager.transition(record, state=RunState.FINISHED,
                                return_code=0, file_state=RunFileState.WAITING)
    record = manager.update(record, RunStatus(
        RunState(facts["state"]), return_code=facts.get("return_code"), file_error=facts["file_error"],
    ))
    assert record.state == RunState.FINISHED
    assert record.return_code == 0
    assert record.file_state == RunFileState.FAILED
    assert not record.is_active


def test_confirmed_failed_exit_is_not_overwritten_by_late_cancellation(tmp_path):
    manager = RunManager(MemoryRunStore())
    record = manager.new_record(directory=tmp_path)
    record = manager.transition(record, state=RunState.FAILED, return_code=7)
    assert manager.transition(record, state=RunState.CANCELLED) == record


def test_cannot_finalize_files_of_running_execution(tmp_path):
    manager = RunManager(MemoryRunStore())
    record = manager.transition(manager.new_record(directory=tmp_path),
                                state=RunState.RUNNING, file_state=RunFileState.WAITING)
    with pytest.raises(ValueError, match="confirmed native termination"):
        manager.transition(record, file_state=RunFileState.POSTPROCESSED)


def test_wait_raises_if_another_observer_failed_file_collection(tmp_path):
    manager = RunManager(MemoryRunStore())
    run = ExecutionClient(DirectBackend).create_run(
        ExecutionSpec((sys.executable, "-c", "pass"), directory=tmp_path),
        tracked=manager, detached=True,
    )
    run.submit()
    manager.complete(run.record, state=RunState.FINISHED, return_code=0,
                     file_state=RunFileState.FAILED, file_failure_phase=FailurePhase.DOWNLOAD,
                     file_message="test transfer failure")
    with pytest.raises(RuntimeError, match="test transfer failure"):
        run.wait()


def test_lost_publisher_preserves_known_job_exit(tmp_path, monkeypatch):
    from kkrunner.backends.contracts import JobStatus

    run = ExecutionClient(DirectBackend).create_run(
        ExecutionSpec((sys.executable, "-c", "pass"), directory=tmp_path),
        tracked=False,
    )
    job = run._job

    async def pending():
        return JobStatus(RunState.FINISHED, return_code=0, output_pending=True)

    async def lost():
        return JobStatus(RunState.UNKNOWN, file_error="publisher lost")

    monkeypatch.setattr(job.execution, "status", pending)
    assert job.refresh_status().output_pending
    monkeypatch.setattr(job.execution, "status", lost)
    status = job.refresh_status()
    assert status.state == RunState.FINISHED
    assert status.return_code == 0
    assert not status.output_pending
    assert status.file_error == "publisher lost"
    run.close()


@pytest.mark.parametrize("observe", ["update", "refresh"])
def test_failed_supplied_manifest_is_not_ready(tmp_path, observe):
    from kkrunner.files import FileManifest, FileEntry
    from kkrunner.runs import RunStatus

    manager = RunManager(MemoryRunStore())
    record = manager.new_record(directory=tmp_path, processing_required=True,
        files=FileManifest(stdout=FileEntry(local_path=str(tmp_path / "out"))))
    record = manager.transition(record, state=RunState.RUNNING, file_state=RunFileState.WAITING)
    failed = record.files.with_file_availability(str(tmp_path / "out"), "failed", "missing output")
    status = RunStatus(RunState.FINISHED, return_code=0, files=failed)
    if observe == "update":
        saved = manager.update(record, status)
    else:
        from kkrunner.support.async_utils import run_coro_sync
        saved = run_coro_sync(manager._observe_async(record, status))
    assert saved.state == RunState.FINISHED
    assert saved.file_state == RunFileState.FAILED
    assert saved.file_failure_phase == FailurePhase.ARTIFACTS
    assert saved.files.captured_at is None


def test_late_publication_error_cannot_undo_confirmed_collection(tmp_path):
    from kkrunner.runs import RunStatus

    manager = RunManager(MemoryRunStore())
    record = manager.transition(manager.new_record(directory=tmp_path),
                                state=RunState.FINISHED, return_code=0,
                                file_state=RunFileState.COLLECTING)
    status = RunStatus(RunState.UNKNOWN, file_error="receipt missing")
    assert manager.update(record, status) == record


def test_capture_discovery_failure_does_not_freeze_partial_manifest(tmp_path):
    from kkrunner.files import FileManifest, FileEntry
    from kkrunner.files.availability import capture_result_files

    first, second = tmp_path / "present", tmp_path / "missing"
    first.write_text("available")
    files = FileManifest(outputs=(FileEntry(local_path=str(first)), FileEntry(local_path=str(second))))
    captured = capture_result_files(files)
    assert captured.failed_artifacts
    assert captured.captured_at is None
    assert not captured.output_versions


def test_processing_does_not_read_skipped_artifacts_from_old_run(tmp_path):
    from kkrunner.files import FileManifest, FileEntry

    output, error = tmp_path / "out", tmp_path / "err"
    output.write_text("unrelated old output")
    error.write_text("unrelated old error")
    manager = RunManager(MemoryRunStore())
    record = manager.new_record(directory=tmp_path, processing_required=True,
        files=FileManifest(
            stdout=FileEntry(local_path=str(output), availability_state="skipped"),
            stderr=FileEntry(local_path=str(error), availability_state="skipped"),
        ))
    record = manager.complete(record, state=RunState.CANCELLED, artifacts_available=True)
    assert record.file_state == RunFileState.READY
    assert record.files.captured_at is not None
    assert not record.files.output_versions
    assert manager.retry_postprocessing(record, lambda outcome: (
        outcome.state, outcome.output_file, outcome.error_file,
    )) == (RunState.CANCELLED, None, None)


def test_confirmed_local_collection_does_not_require_backend_identity(tmp_path, monkeypatch):
    from kkrunner.backends import BackendConfig
    from kkrunner.files import FileManifest, FileEntry

    output = tmp_path / "out"
    output.write_text("finished output")
    manager = RunManager(MemoryRunStore())
    record = manager.new_record(directory=tmp_path, processing_required=True,
        backend=BackendConfig("not-installed"),
        files=FileManifest(stdout=FileEntry(local_path=str(output))))
    record = manager.transition(record, state=RunState.FINISHED, return_code=0,
                                file_state=RunFileState.COLLECTING)
    assert not record.execution.available
    assert record.execution.backend_id is None
    monkeypatch.setattr(manager, "client_for", lambda *_: pytest.fail("No execution client needed"))
    saved, = manager.refresh((record,))
    assert saved.file_state == RunFileState.READY
    assert saved.files.captured_at is not None
    assert manager.retry_postprocessing(saved, lambda outcome: outcome.output_file) == str(output)


def test_collect_files_uses_current_record_and_requires_ownership(tmp_path, monkeypatch):
    from kkrunner.backends import BackendConfig
    from kkrunner.files import FileManifest, FileEntry

    output = tmp_path / "out"
    output.write_text("finished output")
    manager = RunManager(MemoryRunStore())
    stale = manager.new_record(directory=tmp_path, processing_required=True,
        backend=BackendConfig("not-installed"),
        files=FileManifest(stdout=FileEntry(local_path=str(output))))
    record = manager.transition(stale, state=RunState.FINISHED, return_code=0,
                                file_state=RunFileState.COLLECTING)
    monkeypatch.setattr(manager, "client_for", lambda *_: pytest.fail("No execution client needed"))
    with manager.store.observation_lock(record):
        with pytest.raises(RuntimeError, match="live client"):
            manager.collect_files(stale)
    from kkrunner import ProcessingPending
    with pytest.raises(ProcessingPending, match="collect_files"):
        manager.attach(stale)
    saved = manager.collect_files(stale)
    assert saved.file_state == RunFileState.READY
    assert saved.files.captured_at is not None
    assert manager.collect_files(saved) == saved
    assert manager.retry_postprocessing(saved, lambda outcome: outcome.output_file) == str(output)


@pytest.mark.parametrize("tracked", [False, True])
@pytest.mark.parametrize("process", [False, True])
def test_fresh_run_only_exposes_captured_output(tmp_path, monkeypatch, tracked, process):
    from kkrunner import ExecutionSpec
    from kkrunner.backends import DirectBackend
    from kkrunner.client import ExecutionClient
    from kkrunner.files.availability import capture_result_files

    output = tmp_path / "out"
    output.write_text("unrelated old output")
    manager = RunManager(MemoryRunStore()) if tracked else False
    run = ExecutionClient(DirectBackend).create_run(
        ExecutionSpec((sys.executable, "-c", "pass"), directory=tmp_path, output_file=output),
        tracked=manager, process_result=(lambda outcome: outcome) if process else None,
    )

    async def finalize(status, *, on_resolved=None):
        # A collection policy can skip an output even when a local path exists.
        session = run._file_session
        session.close()
        session._files = capture_result_files(
            session.files.with_file_availability(output, "skipped"), require_outputs=False,
        )
        return session.files

    monkeypatch.setattr(run._file_session, "finalize_async", finalize)
    result = run.run()
    assert result.state == RunState.FINISHED
    assert result.return_code == 0
    assert result.output_file is None
    assert run.file_state == (RunFileState.POSTPROCESSED if process else RunFileState.DONE)


@pytest.mark.parametrize("tracked", [False, True])
def test_normal_completion_does_not_import_local_files(tmp_path, monkeypatch, tracked):
    from kkrunner.files import availability
    from kkrunner.runs import manager as manager_module

    def unexpected(*args, **kwargs):
        pytest.fail("Normal file collection must not use the historical import path")

    monkeypatch.setattr(availability, "capture_result_files", unexpected)
    monkeypatch.setattr(manager_module, "capture_result_files", unexpected)
    manager = RunManager(MemoryRunStore()) if tracked else False
    run = ExecutionClient(DirectBackend).create_run(
        ExecutionSpec((sys.executable, "-c", "print('output')"), directory=tmp_path,
                      output_file="out"),
        tracked=manager,
    )
    result = run.run()
    assert result.return_code == 0
    assert run.file_state == RunFileState.DONE
    assert run.files.captured_at is not None
    assert len(run.files.output_versions) == 1
    assert (tmp_path / "out").read_text() == "output\n"
    if tracked:
        assert manager.store.get(run.id).files == run.files


def test_recovered_collection_does_not_import_local_files(tmp_path, monkeypatch):
    from kkrunner.files import FileEntry, FileManifest
    from kkrunner.runs import manager as manager_module

    output = tmp_path / "out"
    output.write_text("finished")
    manager = RunManager(MemoryRunStore())
    record = manager.new_record(directory=tmp_path,
        files=FileManifest(stdout=FileEntry(output)))
    record = manager.transition(record, state=RunState.FINISHED, return_code=0,
                                file_state=RunFileState.COLLECTING)
    monkeypatch.setattr(manager_module, "capture_result_files",
                        lambda *a, **k: pytest.fail("collection must not import"))
    completed = manager.collect_files(record)
    assert completed.file_state == RunFileState.DONE
    assert len(completed.files.output_versions) == 1
    assert completed.files.output_versions[0].is_current()


def test_wait_resumes_latest_collection_manifest(tmp_path):
    from kkrunner.files import FileEntry, TaskFile

    first = RunManager(MemoryRunStore())
    second = RunManager(first.store)
    pattern = str(tmp_path / "part-*.dat")
    original = str(tmp_path / "part-one.dat")
    unrelated = str(tmp_path / "part-later.dat")
    run = ExecutionClient(DirectBackend).create_run(
        ExecutionSpec((sys.executable, "-c", "pass"), directory=tmp_path,
                      output_files=(TaskFile(pattern),)),
        tracked=first, detached=True,
    )
    run.submit()
    # A previous owner has finished discovery but has not yet captured the
    # manifest. The original submitter must not start discovery again.
    with open(original, "w") as stream:
        stream.write("original")
    manifest = run.record.files.with_file_availability(pattern, "available")
    manifest = manifest.with_file_availability(
        original, "available", source_pattern=pattern, kind="output",
    )
    saved = second.transition(
        run.record, state=RunState.FINISHED, return_code=0,
        file_state=RunFileState.COLLECTING, files=manifest,
    )
    assert saved.files != run._file_session.files
    with open(unrelated, "w") as stream:
        stream.write("not part of this collection")
    run.wait()
    completed = first.store.get(run.id)
    assert completed.file_state == RunFileState.DONE
    assert {str(version.path) for version in completed.files.output_versions} == {original}


@pytest.mark.parametrize("remote", [False, True])
@pytest.mark.parametrize("pending", [False, True])
def test_manager_cancel_records_publication_without_collecting_files(tmp_path, monkeypatch, remote, pending):
    from kkrunner.backends import SupervisedBackend
    from kkrunner.client import FileAccess
    from kkrunner.files import FileEntry, FileManifest, FileMapping
    from kkrunner.runs import RunStatus
    from kkrunner.targets import LocalTarget, SshTarget

    target = SshTarget("cluster") if remote else LocalTarget()
    manager = RunManager(MemoryRunStore())
    client = ExecutionClient(SupervisedBackend, target=target)
    manager.register_client(client)
    output = tmp_path / "out"
    record = manager.new_record(
        directory=tmp_path, remote_directory="/work" if remote else None,
        target=target, backend=client.backend_config, processing_required=True,
        files=FileManifest(
            stdout=FileEntry(output, "/work/out" if remote else None),
            mapping=FileMapping(tmp_path, "/work") if remote else None,
        ),
    )
    record = manager.transition(record, state=RunState.RUNNING, file_state=RunFileState.WAITING,
                                execution=record.execution.submitted("native-job"))

    async def cancel(attachment):
        assert attachment.execution.backend_id == "native-job"
        return RunStatus(RunState.CANCELLED, backend_id="native-job",
                         return_code=-15, output_pending=pending)

    async def unexpected(*args, **kwargs):
        pytest.fail("Cancellation must not collect files or acknowledge the result")

    monkeypatch.setattr(client, "cancel_async", cancel)
    monkeypatch.setattr(client, "acknowledge_async", unexpected)
    monkeypatch.setattr(FileAccess, "collect_ended_files_async", unexpected)
    cancelled = manager.cancel(record.id)
    assert cancelled.state == RunState.CANCELLED
    assert cancelled.return_code == -15
    assert cancelled.file_state == (RunFileState.WAITING if pending else RunFileState.COLLECTING)
    assert cancelled.files.captured_at is None
    assert cancelled.is_active
    assert manager.store.get(record.id) == cancelled
    assert manager.cancel(record.id) == cancelled


def test_file_completion_does_not_reinterpret_late_launch_failure(tmp_path):
    from kkrunner.files import FileEntry, FileManifest

    manager = RunManager(MemoryRunStore())
    output = tmp_path / "out"
    record = manager.new_record(
        directory=tmp_path, files=FileManifest(
            stdout=FileEntry(output, availability_state="pending"),
        ),
    )
    record = manager.transition(record, state=RunState.FINISHED, return_code=0,
                                file_state=RunFileState.COLLECTING)
    # This delayed report contradicts the already confirmed native outcome.
    # It cannot establish that the collection should skip the output.
    current = manager.complete(
        record, state=RunState.FAILED, failure_phase=FailurePhase.LAUNCH,
        file_state=RunFileState.COLLECTING, artifacts_available=False,
    )
    assert current.state == RunState.FINISHED
    assert current.return_code == 0
    assert current.failure_phase is None
    assert current.files == record.files
    assert current.file_state == RunFileState.COLLECTING

    output.write_text("confirmed output")
    completed = manager.collect_files(current)
    assert completed.file_state == RunFileState.DONE
    assert completed.files.stdout.availability_state == "available"
    assert completed.files.output_versions[0].is_current()
