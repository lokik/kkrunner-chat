"""Reservations protect files and identities before any backend side effects."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from kkrunner.runs import DuplicateSubmissionError, LaunchSpec
from kkrunner import ExecutionSpec, RunManager, RunStore
from kkrunner.client.endpoint import ExecutionEndpointConfig
from kkrunner.files import FileEntry, FileManifest
from kkrunner.client import SshRunEndpoint, SshTransport
from kkrunner.runs.conflicts import submission_conflicts


def test_default_remote_root_resolves_before_reservation_without_staging(tmp_path):
    calls = []

    class Transport(SshTransport):
        async def run(self, command, **kwargs):
            calls.append(command)
            assert command == ("sh", "-c", 'printf "%s" "$HOME"')
            return SimpleNamespace(stdout=b"/home/tester\n")

    executor = SshRunEndpoint(Transport("cluster"))
    manager = RunManager(RunStore(tmp_path / "runs"))
    spec = ExecutionSpec(
        ["unused"],
        directory=tmp_path / "case",
        output_file=tmp_path / "case/stdout",
        launch=LaunchSpec(local_root=tmp_path),
    )
    first = manager.create_run(executor, spec)
    resolved = executor.resolve_submission(first.record, spec)
    manager.reserve(resolved)

    second = manager.create_run(executor, spec)
    with pytest.raises(DuplicateSubmissionError):
        second.submit()
    assert resolved.execution.directory.remote == "/home/tester/case"
    assert resolved.files.stdout.remote_path == "/home/tester/case/stdout"
    assert len(calls) == 1  # HOME lookup is cached; no mkdir, upload or submit.
    assert not (tmp_path / "case/stdout").exists()


def test_explicit_shared_filesystem_connects_different_host_aliases(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    first = manager.new_record(
        directory=tmp_path / "one",
        remote_directory="/shared/work",
        endpoint=ExecutionEndpointConfig(location="ssh", host="host-a", filesystem_id="shared"),
        files=FileManifest(stdout=FileEntry(str(tmp_path / "one/out"), "/shared/work/out")),
    )
    second = manager.new_record(
        directory=tmp_path / "two",
        remote_directory="/shared/work",
        endpoint=ExecutionEndpointConfig(location="ssh", host="host-b", filesystem_id="shared"),
        files=FileManifest(stdout=FileEntry(str(tmp_path / "two/out"), "/shared/work/out")),
    )
    assert submission_conflicts(first, second)
    other = replace(
        second,
        execution=replace(
            second.execution,
            endpoint=replace(
                second.execution.endpoint,
                filesystem_id="separate",
            ),
        ),
    )
    assert not submission_conflicts(first, other)


def test_harmless_observation_does_not_steal_processing_claim(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    output = tmp_path / "out"
    output.write_text("answer")
    from kkrunner import RunState

    record = manager.new_record(
        directory=tmp_path, processing_required=True, files=FileManifest(stdout=FileEntry(str(output)))
    )
    record = manager.complete(record, state=RunState.POSTPROCESSING, return_code=0, artifacts_available=True)

    def process(outcome):
        manager.transition(manager.store.get(record.id), message="Independent observation")
        return 42

    assert manager.retry_postprocessing(record.id, process) == 42
    assert manager.store.get(record.id).state == RunState.FINISHED
