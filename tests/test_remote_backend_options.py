"""Backend selection and file staging have one target-only wire request."""

import asyncio
from dataclasses import asdict

from kkrunner import ExecutionSpec
from kkrunner.execution import MpiSpec
from kkrunner.client import EndpointClient, StagingSpec
from kkrunner.backends import PsijBackend, DirectBackend
from kkrunner.files import FileManifest

from .test_transport import _LoopbackSshTransport


def test_remote_request_carries_target_backend_options_and_mpi_intent(tmp_path):
    local_root, remote_root = tmp_path / "local", tmp_path / "remote"
    directory = local_root / "calculation"
    directory.mkdir(parents=True)
    remote_root.mkdir()
    input_file = directory / "run.inp"
    input_file.write_text("input")
    potential = directory / "start.pot"
    potential.write_text("potential")

    client = EndpointClient(
        PsijBackend(executor_name="slurm"),
        transport=_LoopbackSshTransport(),
    )
    spec = ExecutionSpec(
        argv=("program",),
        mpi_argv=("programMPI", str(input_file)),
        mpi=MpiSpec.from_value(16),
        staging=StagingSpec(local_root=local_root, remote_root=remote_root),
        input_file=input_file,
        output_file=directory / "run.out",
        directory=str(directory),
        stdin=input_file,
        input_files=(input_file, potential),
    )
    parts = client.create_run_parts(spec, submission_key="test-run")
    request, manifest = asyncio.run(parts.files._prepare_async())
    request = parts.submission_builder(request)
    remote_directory = remote_root / "calculation"
    assert request["backend"] == "psij"
    assert request["backend_options"] == {"executor_name": "slurm", "url": None}
    assert request["submission_key"] == "test-run"
    assert request["argv"] == ["program"]
    assert request["mpi_uses_stdin"] is True
    assert request["mpi"] == asdict(spec.mpi)
    assert request["mpi_argv"][0] == "programMPI"
    assert request["mpi_argv"][1] == str(remote_directory / "run.inp")
    assert request["directory"] == str(remote_directory)
    assert "staging" not in request
    assert "local_root" not in request
    assert "client_sync" not in request
    assert request["input_file"] == str(remote_directory / "run.inp")
    assert request["stdin"] == str(remote_directory / "run.inp")
    assert request["output_file"] == str(remote_directory / "run.out")
    assert request["output_files"] == []
    assert request["cache_files"] == []
    assert (remote_directory / "run.inp").read_text() == "input"
    assert (remote_directory / "start.pot").read_text() == "potential"
    assert manifest.mapping.local_root == str(local_root)
    assert manifest.mapping.remote_root == str(remote_root)
    assert manifest.inputs


def test_untracked_remote_request_has_its_own_submission_key(tmp_path):
    root, remote_root = tmp_path / "local", tmp_path / "remote"
    directory = root / "work"
    directory.mkdir(parents=True)
    remote_root.mkdir()
    client = EndpointClient(PsijBackend(), transport=_LoopbackSshTransport())
    parts = client.create_run_parts(ExecutionSpec(
        argv=("program",), directory=directory,
        staging=StagingSpec(local_root=root, remote_root=remote_root),
    ))
    request, _manifest = asyncio.run(parts.files._prepare_async())
    complete = parts.submission_builder(request)
    assert complete["submission_key"]
    assert complete["backend"] == "psij"


def test_remote_file_declarations_are_target_only_and_single_sourced(tmp_path):
    root, remote_root = tmp_path / "local", tmp_path / "remote"
    directory = root / "work"
    directory.mkdir(parents=True)
    remote_root.mkdir()
    cache = directory / "restart.dat"
    cache.write_text("restart")
    client = EndpointClient(DirectBackend, transport=_LoopbackSshTransport())
    spec = ExecutionSpec(
        argv=("program",), directory=directory,
        output_file=directory / "run.out",
        output_files=(directory / "plot-*.dat",),
        cache_files=(cache,),
        staging=StagingSpec(local_root=root, remote_root=remote_root),
    )
    parts = client.create_run_parts(spec)
    request, manifest = asyncio.run(parts.files._prepare_async())
    complete = parts.submission_builder(request)
    remote_dir = remote_root / "work"
    assert complete["output_files"] == [str(remote_dir / "plot-*.dat")]
    assert complete["cache_files"] == [str(remote_dir / "restart.dat")]
    assert complete["output_file"] == str(remote_dir / "run.out")
    assert "client_sync" not in complete
    assert "staging" not in complete
    assert manifest.outputs[0].remote_path == str(remote_dir / "plot-*.dat")
    assert manifest.caches[0].file.remote_path == str(remote_dir / "restart.dat")
