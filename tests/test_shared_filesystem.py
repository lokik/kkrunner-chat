"""Shared staging uses a single filesystem and retains the remote execution paths."""

from dataclasses import asdict
from pathlib import Path

import pytest

from kkrunner import ExecutionSpec, RunState, TaskFile
from kkrunner.client import ExecutionClient, LocalTarget, StagingSpec
from kkrunner.files.manifest import FileManifest
from kkrunner.runs.base import RunStatus, SubmissionNotStarted


class SharedMount(LocalTarget):
    """Loopback target exposing the same files at a different root."""

    is_local = False

    async def run(self, argv, **kwargs):
        assert argv[:3] == ("mkdir", "-p", "--")
        Path(argv[3]).mkdir(parents=True, exist_ok=True)

    async def query_files(self, *args, **kwargs):
        pytest.fail("Shared files must not be queried remotely")

    async def call(self, *args, **kwargs):
        pytest.fail("Shared files must not be hashed remotely")

    async def put_file(self, *args, **kwargs):
        pytest.fail("Unexpected upload")

    async def get_file(self, *args, **kwargs):
        pytest.fail("Unexpected download")


def test_shared_filesystem_staging_and_finalization(tmp_path):
    local = tmp_path / "local"
    local.mkdir()
    remote = tmp_path / "remote"
    remote.symlink_to(local, target_is_directory=True)
    (local / "in").write_text("input")
    (local / "tree").mkdir()
    (local / "tree" / "data").write_text("data")
    staging = StagingSpec(
        local_root=local, remote_root=remote, shared_filesystem=True,
        input_sync="always", output_sync="always",
    )
    assert staging.input_sync == staging.output_sync == "never"
    spec = ExecutionSpec(
        ("true", str(local / "in")), directory=str(local), input_file=local / "in",
        output_file=local / "stdout", error_file=local / "stderr",
        input_files=(TaskFile(local / "tree", kind="directory"),),
        output_files=(local / "*.dat", TaskFile(local / "outdir", kind="directory")),
        cache_files=(local / "cache",), staging=staging,
    )
    session = ExecutionClient(target=SharedMount()).file_session(spec)
    session.prepare_submission()
    request = session.prepare_job()
    assert request["directory"] == str(remote)
    assert request["input_file"] == str(remote / "in")
    assert request["output_file"] == str(remote / "stdout")
    assert request["argv"][-1] == str(remote / "in")
    assert session.files.input_version(local / "in").is_current()
    assert session.files.input_version(local / "tree").is_current()
    assert not (local / "stdout").exists()

    (remote / "stdout").write_text("out")
    (remote / "stderr").write_text("err")
    (remote / "result.dat").write_text("artifact")
    (remote / "cache").write_text("cache")
    (remote / "outdir").mkdir()
    (remote / "outdir" / "result").write_text("directory")
    files = session.finalize(RunStatus(RunState.FINISHED, return_code=0))
    assert files.shared_filesystem
    assert not files.failed_artifacts
    assert {version.path.name for version in files.output_versions} == {
        "stdout", "stderr", "result.dat", "outdir",
    }
    assert {version.path.name for version in files.cache_versions} == {"cache"}
    assert all(version.is_current() for version in files.all_versions)
    assert StagingSpec.from_mapping(FileManifest(**asdict(files)).mapping) == staging


def test_shared_missing_required_input_is_not_a_transfer(tmp_path):
    spec = ExecutionSpec(
        ("true",), directory=str(tmp_path), input_file=tmp_path / "missing",
        staging=StagingSpec(local_root=tmp_path, remote_root=tmp_path, shared_filesystem=True),
    )
    session = ExecutionClient(target=SharedMount()).file_session(spec)
    with pytest.raises(SubmissionNotStarted, match="Shared input file does not exist"):
        session.prepare_submission()
    session.close()


def test_shared_missing_required_output_is_not_skipped(tmp_path):
    spec = ExecutionSpec(
        ("true",), directory=str(tmp_path), output_file=tmp_path / "missing",
        staging=StagingSpec(local_root=tmp_path, remote_root=tmp_path, shared_filesystem=True),
    )
    session = ExecutionClient(target=SharedMount()).file_session(spec)
    session.prepare_submission()
    session.prepare_job()
    with pytest.raises(FileNotFoundError, match="Published output file is missing"):
        session.finalize(RunStatus(RunState.FINISHED, return_code=0))


def test_shared_finished_status_uses_local_artifacts_and_snapshot(tmp_path, monkeypatch):
    from kkrunner import RunManager, RunStore
    from kkrunner.client import SshTarget
    from kkrunner.files.manifest import FileEntry, FileMapping
    from kkrunner.runs import FailurePhase

    manager = RunManager(RunStore(tmp_path / "runs"))
    client = ExecutionClient(target=SshTarget("cluster"))
    manager.register_client(client)
    output = tmp_path / "result.out"
    files = FileManifest(
        stdout=FileEntry(str(output), "/work/result.out"),
        mapping=FileMapping(str(tmp_path), "/work", shared_filesystem=True),
    )
    record = manager.new_record(
        directory=tmp_path, remote_directory="/work", target=client.target,
        backend=client.backend_config, files=files,
    )
    record = manager.transition(
        record, state=RunState.RUNNING,
        execution=record.execution.submitted("shared-job"),
    )
    monkeypatch.setattr(client, "status", lambda *runs: (
        RunStatus(RunState.FINISHED, return_code=0, backend_id="shared-job"),
    ))
    monkeypatch.setattr(client, "collect_ended_files",
                        lambda *args: pytest.fail("No download of shared files"))
    monkeypatch.setattr(client, "acknowledge", lambda *args: None)

    output.write_text("shared result")
    finished = manager.refresh((record,))[0]
    assert finished.state == RunState.FINISHED
    assert finished.files.shared_filesystem
    assert finished.files.output_versions[0].is_current()
    assert manager.snapshot_file(finished, output) == b"shared result"

    # No separate copy is available if a shared result is later deleted.
    output.unlink()
    monkeypatch.setattr(client, "resolve_result_files",
                        lambda *args: pytest.fail("No separate shared file copy"))
    assert manager.retrieve_result_files(finished) == finished

    missing = manager.new_record(
        directory=tmp_path, remote_directory="/work", target=client.target,
        backend=client.backend_config, files=files,
    )
    missing = manager.transition(
        missing, state=RunState.RUNNING,
        execution=missing.execution.submitted("shared-job"),
    )
    failed = manager.refresh((missing,))[0]
    assert failed.state == RunState.FAILED
    assert failed.failure_phase == FailurePhase.ARTIFACTS
