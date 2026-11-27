"""Reservations protect files and identities before any backend side effects."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from kkrunner.runs import DuplicateSubmissionError
from kkrunner import ExecutionSpec, RunManager, RunStore
from kkrunner.files import FileEntry, FileManifest, FileMapping
from kkrunner.client import ExecutionClient, SshTarget, StagingSpec
from kkrunner.runs.conflicts import submission_conflicts


def test_default_remote_root_resolves_before_reservation_without_staging(tmp_path, monkeypatch):
    calls = []

    async def fake_run(self, command, **kwargs):
        calls.append(command)
        assert command == ("sh", "-c", 'printf "%s" "$HOME"')
        return SimpleNamespace(stdout=b"/home/tester\n")

    monkeypatch.setattr(SshTarget, "run", fake_run)
    client = ExecutionClient(target=SshTarget("cluster"))
    manager = RunManager(RunStore(tmp_path / "runs"))
    spec = ExecutionSpec(
        ["unused"],
        directory=tmp_path / "case",
        output_file=tmp_path / "case/stdout",
        staging=StagingSpec(local_root=tmp_path),
    )
    first = client.create_run(spec, persistent=manager)
    directory, files = first._file_session.submission_footprint()
    resolved = replace(first.record, execution=replace(first.record.execution, directory=directory), files=files)
    manager.reserve(resolved)

    second = client.create_run(spec, persistent=manager)
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
        target=SshTarget("host-a"),
        files=FileManifest(
            stdout=FileEntry(str(tmp_path / "one/out"), "/shared/work/out"),
            mapping=FileMapping(str(tmp_path / "one"), "/shared/work", filesystem_id="shared"),
        ),
    )
    second = manager.new_record(
        directory=tmp_path / "two",
        remote_directory="/shared/work",
        target=SshTarget("host-b"),
        files=FileManifest(
            stdout=FileEntry(str(tmp_path / "two/out"), "/shared/work/out"),
            mapping=FileMapping(str(tmp_path / "two"), "/shared/work", filesystem_id="shared"),
        ),
    )
    assert submission_conflicts(first, second)
    other = replace(
        second,
        files=replace(
            second.files,
            mapping=replace(second.files.mapping, filesystem_id="separate"),
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


@pytest.mark.parametrize("policy", ["always", "changed", "missing", "never"])
def test_distinct_local_inputs_mapped_to_same_target_conflict_if_uploaded(tmp_path, policy):
    from kkrunner.runs.conflicts import conflicting_files

    manager = RunManager(RunStore(tmp_path / "runs"))
    def record(name):
        root = tmp_path / name
        return manager.new_record(
            directory=root, remote_directory="/shared", target=SshTarget("cluster"),
            files=FileManifest(
                inputs=(FileEntry(str(root / "Fe.pot"), "/shared/Fe.pot"),),
                stdout=FileEntry(str(root / f"{name}.out"), f"/shared/{name}.out"),
                mapping=FileMapping(str(root), "/shared", input_sync=policy),
            ),
        )
    first, second = record("one"), record("two")
    assert submission_conflicts(first, second) == (policy != "never")
    assert bool(conflicting_files(first, second)) == (policy != "never")


def test_local_alias_and_canonical_wildcard_conflict(tmp_path):
    actual = tmp_path / "actual"
    actual.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(actual, target_is_directory=True)
    manager = RunManager(RunStore(tmp_path / "runs"))
    def record(path, *, output):
        return manager.new_record(directory=tmp_path, files=FileManifest.from_spec(
            ExecutionSpec(argv=(), directory=tmp_path,
                          **({"output_files": (path,)} if output else {"input_files": (path,)}))))
    assert submission_conflicts(record(alias / "*.dat", output=True),
                                record(actual / "result.dat", output=False))
