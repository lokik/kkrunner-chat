"""Staging and persistence contracts at the backend submission boundary."""

import asyncio

import pytest

from kkrunner import ExecutionSpec, RunState, RunStore, TaskFile
from kkrunner.client import EndpointClient, SshTransport, StagingSpec
from kkrunner.client.file_context import FileContext
from kkrunner.client.file_session import FileSession
from kkrunner.files import FileManifest
from kkrunner.runs.base import SubmissionNotStarted
from kkrunner.runs.manager import RunManager
from kkrunner.runs.run import Run


def test_prepared_manifest_is_durable_before_remote_backend_submit(tmp_path, monkeypatch):
    """The persisted SUBMITTING record precedes the irreversible Job.submit call."""
    local = tmp_path / "local"
    local.mkdir()
    remote = tmp_path / "remote"
    spec = ExecutionSpec(
        argv=("ignored",), directory=str(local), output_file="run.out",
        staging=StagingSpec(local_root=str(tmp_path), remote_root=str(remote)),
    )
    client = EndpointClient("process", transport=SshTransport("cluster"))
    manager = RunManager(RunStore(tmp_path / "runs"))
    run = manager.create_run(client, spec, metadata={"example": {"task": "DOS"}})
    expected = asyncio.run(FileContext(client, spec.staging).map_manifest(FileManifest.from_spec(spec)))
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
    client = EndpointClient("process", transport=SshTransport("cluster"))
    manifest = asyncio.run(FileContext(client, spec.staging).map_manifest(local))

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
    client = EndpointClient("process", transport=SshTransport("cluster"))
    session = client.create_run_parts(spec).files
    manifest = FileManifest.from_spec(spec)

    async def prepare():
        used = {}
        await session._prepare_cache_files(used, manifest)
        await session._prepare_output_files(manifest)
        return used

    used = asyncio.run(prepare())
    assert session._output_targets == {
        str(directory / "one.dat"): str(remote / "work" / "one.dat"),
    }
    assert session._output_patterns == (str(remote / "work" / "cut-*.dat"),)
    assert session._cache_targets == {
        str(directory / "restart.dat"): (str(remote / "work" / "restart.dat"), None),
    }
    assert session._cache_patterns == (str(remote / "work" / "checkpoint-*.dat"),)
    assert used == {str(remote / "work" / "restart.dat"): str(directory / "restart.dat")}


@pytest.mark.parametrize("method", ("submit", "start"))
def test_postprocessing_rejected_submit_preserves_error_and_does_not_monitor(tmp_path, monkeypatch, method):
    """The original preflight failure survives without a completion observer."""
    failure = SubmissionNotStarted("preflight refused")
    client = EndpointClient("direct")
    run = Run.untracked(client, ExecutionSpec(("ignored",), directory=str(tmp_path)),
                        process_result=lambda outcome: outcome)

    def rejected():
        raise failure

    if method == "submit":
        monkeypatch.setattr(run._parts, "prepare_submission", rejected)
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
    class NoSideEffectTransport(SshTransport):
        async def run(self, *args, **kwargs):
            pytest.fail("Remote command executed before manifest validation")

        async def put_file(self, *args, **kwargs):
            pytest.fail("Input uploaded before manifest validation")

        async def file_digest(self, *args, **kwargs):
            pytest.fail("Transport probed before manifest validation")

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
    client = EndpointClient("process", transport=NoSideEffectTransport("cluster"))
    with pytest.raises(ValueError, match="Conflicting optionality|Duplicate file declaration"):
        client.create_run_parts(spec)
