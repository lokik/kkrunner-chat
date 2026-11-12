"""Independent self-review probes; assertions express the intended contract."""

import json
from pathlib import Path
from dataclasses import replace
from threading import Event, Thread

import pytest

from kkrunner.runs import CorruptRunRecord
from kkrunner import ExecutionOutcome, ExecutionSpec, RunState, RunStore
from kkrunner.backends import DirectBackend, SupervisedBackend
from kkrunner.backends.config import BackendConfig
from kkrunner.client import EndpointClient, LocalTransport, SshTransport, StagingSpec
from kkrunner.backends.contracts import JobStatus
from kkrunner.client.backend_execution import DirectExecution
from kkrunner.client.run_parts import RunParts
from kkrunner.runs.run import Run
from kkrunner.files import FileEntry, FileManifest
from kkrunner.runs.conflicts import submission_conflicts
from kkrunner.runs.manager import RunManager


def manager_at(tmp_path):
    return RunManager(RunStore(tmp_path / "runs"))


def test_save_cannot_replace_immutable_metadata(tmp_path):
    manager = manager_at(tmp_path)
    record = manager.new_record(directory=tmp_path, metadata={"owner": {"id": 1}})
    manager.store.save(record)
    with pytest.raises(ValueError):
        manager.store.save(replace(record, metadata={"owner": {"id": 2}}))
    assert manager.store.get(record.id).metadata == record.metadata


@pytest.mark.parametrize("data", [[], None, 42])
def test_invalid_json_root_has_domain_error(tmp_path, data):
    manager = manager_at(tmp_path)
    record = manager.new_record(directory=tmp_path)
    manager.store.save(record)
    manager.store._path(record).write_text(json.dumps(data))
    with pytest.raises(CorruptRunRecord):
        manager.store.get(record.id)


def test_nested_execution_is_validated(tmp_path):
    manager = manager_at(tmp_path)
    record = manager.new_record(directory=tmp_path)
    manager.store.save(record)
    path = manager.store._path(record)
    data = json.loads(path.read_text())
    data["execution"]["transport"] = []
    path.write_text(json.dumps(data))
    with pytest.raises(CorruptRunRecord):
        manager.store.get(record.id)


def test_fast_processor_can_finish_during_submit(tmp_path, monkeypatch):
    manager = manager_at(tmp_path)
    entered, release = Event(), Event()

    class Raw:
        state = RunState.NEW
        backend_id = None
        exception = None
        return_code = 0
        result = ExecutionOutcome(return_code=0)

        def submit(self):
            self.backend_id = "instant-job"
            self.state = RunState.FINISHED

        def wait(self):
            return self.result

        @property
        def requires_stream_retrieval(self):
            return False

        def configure_detached(self, detached):
            pass

        def configure_submission(self, request, **options):
            pass

        def status(self):
            return JobStatus(self.state, return_code=self.return_code)

    class Client(EndpointClient):
        def __init__(self):
            super().__init__(SupervisedBackend)

        def create_run_parts(self, spec, *, submission_key=None, callback=None):
            parts = super().create_run_parts(spec, submission_key=submission_key, callback=callback)
            raw = Raw()
            raw.spec = spec
            parts.job = raw
            return parts

    def process(outcome):
        entered.set()
        assert release.wait(3)
        return 42

    original = Run._start_completion

    def start_and_yield(self):
        original(self)
        assert entered.wait(3)

    monkeypatch.setattr(Run, "_start_completion", start_and_yield)
    run = manager.create_run(Client(), ExecutionSpec(["instant"], directory=tmp_path), process_result=process)
    try:
        run.submit()
    finally:
        release.set()
    assert run.wait() == 42
    assert manager.store.get(run.id).state == RunState.FINISHED


def test_cancel_and_failed_submit_do_not_deadlock():
    from .test_cancellation import _controlled_job

    job, execution = _controlled_job(fail_submit=True)
    errors, finished = [], Event()

    def submit():
        try:
            job.submit()
        except BaseException as exc:
            errors.append(exc)
        finally:
            finished.set()

    submitting = Thread(target=submit, daemon=True)
    submitting.start()
    try:
        assert execution.entered.wait(3)
        future = job.cancel()
    finally:
        execution.release.set()
    assert finished.wait(3), "submission deadlocked with cancellation"
    submitting.join(3)
    assert not submitting.is_alive()
    assert len(errors) == 1
    assert isinstance(errors[0], RuntimeError)
    assert str(errors[0]) == "submission rejected"
    assert future.result(timeout=3) is None


@pytest.mark.parametrize(
    "transport, backend",
    [
        (LocalTransport(), BackendConfig("psij")),
        (SshTransport("other-host"), BackendConfig("direct")),
    ],
)
def test_local_publication_conflict_is_independent_of_backend(tmp_path, transport, backend):
    manager = manager_at(tmp_path)
    files = FileManifest(stdout=FileEntry(local_path=str(tmp_path / "same.out")))
    first = manager.new_record(directory=tmp_path, files=files)
    second = manager.new_record(
        directory=tmp_path,
        files=files,
        transport=transport,
        backend=backend,
        remote_directory="/remote" if not transport.is_local else None,
    )
    assert submission_conflicts(first, second)


def test_stdin_is_part_of_read_manifest(tmp_path):
    manager = manager_at(tmp_path)
    spec = ExecutionSpec(["reader"], stdin=tmp_path / "data", directory=tmp_path)
    reader = manager.new_record(directory=tmp_path, files=FileManifest.from_spec(spec))
    writer = manager.new_record(
        directory=tmp_path, files=FileManifest(outputs=(FileEntry(local_path=str(tmp_path / "data")),))
    )
    assert submission_conflicts(reader, writer)


def test_remote_preflight_streams_close_on_submit_failure(tmp_path, monkeypatch):
    from .test_transport import _LoopbackSshTransport

    local = tmp_path / "local"
    local.mkdir()
    output = (local / "stdout").open("wb")
    spec = ExecutionSpec(
        ["unused"], directory=local, output_file=output,
        staging=StagingSpec(local_root=local, remote_root=tmp_path / "remote"),
    )
    run = Run.untracked(EndpointClient(DirectBackend, transport=_LoopbackSshTransport()), spec)

    async def rejected(self, request, on_output):
        raise RuntimeError("lost submit response")

    monkeypatch.setattr(DirectExecution, "submit", rejected)
    with pytest.raises(RuntimeError, match="lost submit response"):
        run.submit()
    assert output.closed


def test_reused_run_id_is_rejected_before_second_backend_submit(tmp_path):
    manager = manager_at(tmp_path)
    release = Event()

    class Raw:
        state = RunState.NEW
        backend_id = None
        exception = None
        return_code = 0
        result = ExecutionOutcome(return_code=0)
        output_error = None

        def submit(self):
            backend.submissions += 1
            self.backend_id = str(backend.submissions)
            self.state = RunState.RUNNING

        def wait(self):
            release.wait(3)
            # This fake backend must publish the declared output before its
            # FINISHED status can be committed by the client file session.
            Path(spec.output_file).write_text("written")
            self.state = RunState.FINISHED
            return self.result

        @property
        def requires_stream_retrieval(self):
            return False

        def configure_detached(self, detached):
            pass

        def configure_submission(self, request, **options):
            pass

        def status(self):
            return JobStatus(self.state, return_code=self.return_code)

    class Client(EndpointClient):
        submissions = 0

        def __init__(self):
            super().__init__(SupervisedBackend)

        def create_run_parts(self, spec, *, submission_key=None, callback=None):
            parts = super().create_run_parts(spec, submission_key=submission_key, callback=callback)
            raw = Raw()
            raw.spec = spec
            parts.job = raw
            return parts

    backend = Client()
    spec = ExecutionSpec(["unused"], directory=tmp_path, output_file=tmp_path / "same-output")
    first = manager.create_run(backend, spec, run_id="same-id")
    second = manager.create_run(backend, spec, run_id="same-id")
    try:
        first.submit()
        with pytest.raises((RuntimeError, ValueError)):
            second.submit()
        assert backend.submissions == 1
    finally:
        release.set()
        first.wait()
