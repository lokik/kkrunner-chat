"""Independent self-review probes; assertions express the intended contract."""

import json
from dataclasses import replace
from threading import Event, Thread

import pytest

from kkrunner.runs import CorruptRunRecord
from kkrunner import ExecutionOutcome, ExecutionSpec, RunState, RunStore
from kkrunner.runs.base import ExecutionResources, ResourceOwnedRunner
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

    class Backend:
        name = "instant"

        def create_run_parts(self, spec, *, submission_key=None, callback=None):
            raw = Raw()
            raw.spec = spec
            return RunParts(raw)

    def process(outcome):
        entered.set()
        assert release.wait(3)
        return 42

    original = Run._start_completion

    def start_and_yield(self):
        original(self)
        assert entered.wait(3)

    monkeypatch.setattr(Run, "_start_completion", start_and_yield)
    run = manager.create_run(Backend(), ExecutionSpec(["instant"], directory=tmp_path), process_result=process)
    try:
        run.submit()
    finally:
        release.set()
    assert run.wait() == 42
    assert manager.store.get(run.id).state == RunState.FINISHED


def test_cancel_and_failed_submit_do_not_deadlock(tmp_path):
    entered, release, cancelled, finished = Event(), Event(), Event(), Event()
    resources = ExecutionResources()

    class Raw:
        state = RunState.NEW

        def submit(self):
            entered.set()
            assert release.wait(3)
            raise RuntimeError("submission rejected")

    run = ResourceOwnedRunner(Raw(), resources)

    def submit():
        try:
            run.submit()
        except RuntimeError:
            pass
        finally:
            finished.set()

    def cancel():
        run.cancel()
        cancelled.set()

    submitting = Thread(target=submit, daemon=True)
    submitting.start()
    assert entered.wait(3)
    Thread(target=cancel, daemon=True).start()
    assert run._cancel_requested.wait(3)
    release.set()
    assert finished.wait(0.5), "submit deadlocked with its own state property"
    assert cancelled.wait(0.5)
    assert resources.closed


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
    from kkrunner.client.ssh import SshRunEndpoint
    from kkrunner.client.remote_files import RemoteFileSession
    from kkrunner.client.transport import LocalTransport

    executor = SshRunEndpoint(LocalTransport())
    raw = Run.direct(executor, ExecutionSpec(["unused"], directory=tmp_path, output_file=tmp_path / "stdout"))

    async def prepared(self):
        return {"directory": "/remote"}, FileManifest.from_spec(self.spec)

    async def rejected(_request):
        raise RuntimeError("lost submit response")

    monkeypatch.setattr(RemoteFileSession, "_prepare_async", prepared)
    monkeypatch.setattr(executor, "_submit", rejected)
    try:
        with pytest.raises(RuntimeError, match="lost submit response"):
            raw.submit()
        assert raw.spec.output_file.closed
    finally:
        raw.spec.output_file.close()


def test_reused_run_id_is_rejected_before_second_backend_submit(tmp_path):
    manager = manager_at(tmp_path)
    release = Event()

    class Raw:
        state = RunState.NEW
        backend_id = None
        exception = None
        return_code = 0
        result = ExecutionOutcome(return_code=0)

        def submit(self):
            backend.submissions += 1
            self.backend_id = str(backend.submissions)
            self.state = RunState.RUNNING

        def wait(self):
            release.wait(3)
            self.state = RunState.FINISHED
            return self.result

    class Backend:
        name = "fake"
        submissions = 0

        def create_run_parts(self, spec, *, submission_key=None, callback=None):
            raw = Raw()
            raw.spec = spec
            return RunParts(raw)

    backend = Backend()
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
