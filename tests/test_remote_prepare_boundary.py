"""Staging and persistence contracts at the backend submission boundary."""

import asyncio

import pytest

from kkrunner import ExecutionSpec, RunState, RunStore, TaskFile
from kkrunner.client import ExecutionClient, SshTarget, StagingSpec
from kkrunner.client.file_context import FileContext
from kkrunner.client.file_session import FileSession
from kkrunner.files import FileManifest
from kkrunner.runs.base import SubmissionNotStarted
from kkrunner.runs.manager import RunManager
from kkrunner.runs.run import Run

from .transfer_helpers import LoopbackFileTransfer


def test_prepared_manifest_is_durable_before_remote_backend_submit(tmp_path, monkeypatch):
    """The persisted SUBMITTING record precedes the irreversible Job.submit call."""
    local = tmp_path / "local"
    local.mkdir()
    remote = tmp_path / "remote"
    spec = ExecutionSpec(
        argv=("ignored",), directory=str(local), output_file="run.out",
        staging=StagingSpec(local_root=str(tmp_path), remote_root=str(remote)),
    )
    client = ExecutionClient("kkrunner", target=SshTarget("cluster"))
    manager = RunManager(RunStore(tmp_path / "runs"))
    run = client.create_run(spec, tracked=manager, metadata={"example": {"task": "DOS"}})
    expected = asyncio.run(FileContext(client.target, spec.staging).map_manifest(FileManifest.from_spec(spec)))
    target = str(remote / "local")
    request = {
        "argv": ["ignored"], "directory": target,
        "input_file": None, "output_file": str(remote / "local" / "run.out"),
        "error_file": None,
    }

    async def prepared(_session):
        return request, expected

    monkeypatch.setattr(FileSession, "_prepare_async", prepared)
    snapshots = []

    def stop_at_submission():
        snapshots.append(manager.store.get(run.id))
        raise SubmissionNotStarted("test stopped before launching backend")

    monkeypatch.setattr(run._job, "submit", stop_at_submission)
    with pytest.raises(SubmissionNotStarted, match="test stopped"):
        run.submit()

    assert len(snapshots) == 1
    snapshot = snapshots[0]
    assert snapshot.state is RunState.SUBMITTING
    assert snapshot.execution.backend_id is None
    assert snapshot.execution.directory.remote == target
    assert snapshot.files == expected
    # A known rejection is recorded as failure; it is never mistaken for a
    # confirmed submission or retried under the old reservation.
    assert manager.store.get(run.id).state is RunState.FAILED


def test_remote_manifest_uses_same_declarations_as_local(tmp_path):
    directory = tmp_path / "local"
    directory.mkdir()
    remote = tmp_path / "target"
    spec = ExecutionSpec(
        argv=("ignored",), directory=str(directory),
        input_file="main.inp",
        input_files=("main.inp", "main.inp", "extra.inp", "extra.inp"),
        output_file="main.out", error_file="main.err",
        output_files=("main.out", "main.err", "one.dat", "two.dat"),
        cache_files=("restart.pot",),
        staging=StagingSpec(local_root=str(tmp_path), remote_root=str(remote)),
    )
    local = FileManifest.from_spec(spec)
    client = ExecutionClient("kkrunner", target=SshTarget("cluster"))
    manifest = asyncio.run(FileContext(client.target, spec.staging).map_manifest(local))

    assert tuple(e.local_path for e in manifest.inputs) == (
        str(directory / "main.inp"), str(directory / "extra.inp"),
    )
    assert tuple(e.remote_path for e in manifest.inputs) == (
        str(remote / "local" / "main.inp"), str(remote / "local" / "extra.inp"),
    )
    assert tuple(e.local_path for e in manifest.outputs) == tuple(e.local_path for e in local.outputs)
    assert tuple(e.local_path for e in manifest.outputs) == (
        str(directory / "one.dat"), str(directory / "two.dat"),
    )
    assert manifest.stdout.remote_path == str(remote / "local" / "main.out")
    assert manifest.stderr.remote_path == str(remote / "local" / "main.err")
    assert manifest.outputs[0].remote_path == str(remote / "local" / "one.dat")
    assert manifest.caches[0].file.remote_path == str(remote / "local" / "restart.pot")
    assert len(manifest.caches) == 1
    assert manifest.mapping.local_root == str(tmp_path)
    assert manifest.mapping.remote_root == str(remote)


def test_remote_file_preparation_uses_validated_manifest(tmp_path):
    """Preparation retains exact and wildcard file identities without target I/O."""
    directory = tmp_path / "work"
    directory.mkdir()
    remote = tmp_path / "remote"
    spec = ExecutionSpec(
        argv=("ignored",), directory=str(directory), output_file="stdout.out",
        output_files=(None, "stdout.out", "one.dat", "cut-*.dat"),
        cache_files=(None, "restart.dat", "checkpoint-*.dat"),
        staging=StagingSpec(
            local_root=str(tmp_path), remote_root=str(remote),
            input_sync="never", output_sync="never",
        ),
    )
    class NoTransferTransport(SshTarget):
        async def run(self, *args, **kwargs):
            pass

        async def query_files(self, sources, **kwargs):
            return ()

    client = ExecutionClient("kkrunner", target=NoTransferTransport("cluster"))
    session = client.create_run_parts(spec).files
    session.prepare_submission()
    request = session.prepare_job()
    try:
        assert request["output_files"] == [
            str(remote / "work" / "one.dat"), str(remote / "work" / "cut-*.dat"),
        ]
        assert request["protected_files"] == [str(remote / "work" / "restart.dat")]
        assert request["protected_patterns"] == [str(remote / "work" / "checkpoint-*.dat")]
        assert [entry.remote_path for entry in session.files.outputs] == request["output_files"]
        assert [cache.file.remote_path for cache in session.files.caches] == [
            *request["protected_files"], *request["protected_patterns"],
        ]
    finally:
        session.close()


@pytest.mark.parametrize("method", ("submit", "start"))
def test_postprocessing_rejected_submit_preserves_error_and_does_not_monitor(tmp_path, monkeypatch, method):
    """The original preflight failure survives without a completion observer."""
    failure = SubmissionNotStarted("preflight refused")
    client = ExecutionClient("direct")
    run = client.create_run(ExecutionSpec(("ignored",), directory=str(tmp_path)), tracked=False,
                        process_result=lambda outcome: outcome)

    def rejected():
        raise failure

    if method == "submit":
        monkeypatch.setattr(run._file_session, "prepare_submission", rejected)
    else:
        monkeypatch.setattr(run._job, "start", rejected)
    with pytest.raises(SubmissionNotStarted, match="preflight refused") as caught:
        getattr(run, method)()
    assert caught.value is failure
    assert run._started is False
    assert run._thread is None


@pytest.mark.parametrize("conflict", ["mixed_input_optionality", "duplicate_output", "duplicate_cache"])
def test_invalid_manifest_rejected_before_remote_side_effects(tmp_path, conflict):
    """Invalid declarations fail at construction, before any transport access."""
    class NoSideEffectTransport(SshTarget):
        async def run(self, *args, **kwargs):
            pytest.fail("Remote command executed before manifest validation")

        async def put_file(self, *args, **kwargs):
            pytest.fail("Input uploaded before manifest validation")

        async def file_digest(self, *args, **kwargs):
            pytest.fail("Target probed before manifest validation")

    local = tmp_path / "local"
    local.mkdir()
    input_path = local / "main.inp"
    input_path.write_text("TASK SCF")
    shared = local / "shared.dat"
    input_files = (input_path,)
    output_files = ()
    cache_files = ()
    if conflict == "mixed_input_optionality":
        input_files += (TaskFile(input_path, optional=True),)
    elif conflict == "duplicate_output":
        output_files = (shared, shared)
    else:
        cache_files = (shared, shared)
    spec = ExecutionSpec(
        argv=("ignored",), directory=str(local),
        input_file=input_path, input_files=input_files,
        output_file=str(local / "stdout.out"),
        output_files=output_files, cache_files=cache_files,
        staging=StagingSpec(local_root=str(tmp_path), remote_root=str(tmp_path / "remote")),
    )
    client = ExecutionClient("kkrunner", target=NoSideEffectTransport("cluster"))
    with pytest.raises(ValueError, match="Conflicting optionality|Duplicate file declaration"):
        client.create_run_parts(spec)


def test_reserved_manifest_is_the_only_mapping_used_for_staging(tmp_path, monkeypatch):
    from kkrunner.client import LocalTarget

    local = tmp_path / "local"
    target = tmp_path / "target"
    local.mkdir()
    (local / "input").write_text("data")
    async def local_run(self, argv, **kwargs):
        return await LocalTarget().run(argv, **kwargs)

    monkeypatch.setattr(SshTarget, "run", local_run)
    monkeypatch.setattr(SshTarget, "file_digest", LocalTarget.file_digest)
    monkeypatch.setattr(SshTarget, "put_file", LoopbackFileTransfer.put_file)
    monkeypatch.setattr(SshTarget, "query_files", LocalTarget.query_files)
    client = ExecutionClient(target=SshTarget("loopback"))
    spec = ExecutionSpec(
        argv=("unused",), directory=local, input_file="input", output_file="out",
        staging=StagingSpec(local_root=local, remote_root=target),
    )
    manager = RunManager(RunStore(tmp_path / "runs"))
    run = client.create_run(spec, tracked=manager)
    calls = []
    original = FileContext.map_manifest

    async def mapped(self, manifest):
        result = await original(self, manifest)
        calls.append(result)
        return result

    monkeypatch.setattr(FileContext, "map_manifest", mapped)
    def stop():
        record = manager.store.get(run.id)
        assert record.files.inputs == calls[0].inputs == run.files.inputs
        assert record.files.stdout == calls[0].stdout == run.files.stdout
        assert len(calls) == 1
        assert (target / "input").read_text() == "data"
        raise SubmissionNotStarted("stop before native execution")

    monkeypatch.setattr(run._job, "submit", stop)
    with pytest.raises(SubmissionNotStarted, match="stop before native"):
        run.submit()


def test_prepare_job_requires_explicit_file_staging(tmp_path):
    client = ExecutionClient("direct")
    session = client.create_run_parts(ExecutionSpec(("ignored",), directory=tmp_path)).files
    try:
        with pytest.raises(SubmissionNotStarted, match="Files must be staged"):
            session.prepare_job()
        session.prepare_submission()
        assert session.prepare_job()["directory"] == str(tmp_path)
    finally:
        session.close()
