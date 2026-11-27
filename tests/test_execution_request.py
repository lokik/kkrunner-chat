"""A target request is complete, serializable and never mutates ExecutionSpec."""

import io
import json
from pathlib import Path

import pytest

from kkrunner.client.execution_request import execution_request
from kkrunner.runs.base import ExecutionSpec


def test_named_file_is_mapped_without_flushing_or_modifying_spec(tmp_path):
    source = tmp_path / "stdout.log"
    with source.open("wb") as stream:
        stream.write(b"before submission")
        spec = ExecutionSpec(("true",), directory=str(tmp_path), output_file=stream)
        result = execution_request(
            spec,
            directory="/remote/job",
            map_path=lambda path: "/remote/job/" + Path(path).name,
        )
        assert result["output_file"] == "/remote/job/stdout.log"
        assert result["directory"] == "/remote/job"
        assert spec.output_file is stream
        assert source.read_bytes() == b""
        assert json.loads(json.dumps(result)) == result
        stream.flush()
        assert source.read_bytes() == b"before submission"


@pytest.mark.parametrize("stream", [io.StringIO(), io.BytesIO(), 3])
def test_anonymous_streams_fail_before_submission(tmp_path, stream):
    spec = ExecutionSpec(("true",), directory=str(tmp_path), output_file=stream)
    with pytest.raises((ValueError, TypeError)):
        execution_request(spec)


def test_remote_paths_are_never_guessed(tmp_path):
    spec = ExecutionSpec(
        ("true",), directory=str(tmp_path),
        input_file="input.dat", output_file="output.dat",
    )
    target = execution_request(spec, directory="/remote/job",
                               map_path=lambda path: "/remote/job/" + Path(path).name)
    assert target["input_file"] == "/remote/job/input.dat"
    assert target["output_file"] == "/remote/job/output.dat"
    assert spec.input_file == "input.dat"


def test_partial_spec_cannot_submit(tmp_path):
    spec = ExecutionSpec(None, directory=str(tmp_path))
    with pytest.raises(ValueError, match="partial"):
        execution_request(spec)


def test_remote_directory_requires_explicit_mapping(tmp_path):
    spec = ExecutionSpec(("true",), directory=str(tmp_path), output_file="stdout.log")
    with pytest.raises(ValueError, match="mapping"):
        execution_request(spec, directory="/remote/job")
    # A remote command without file declarations needs no file mapping.
    plain = ExecutionSpec(("true",), directory=str(tmp_path))
    assert execution_request(plain, directory="/remote/job")["directory"] == "/remote/job"


def test_file_declaration_metadata_agrees_with_manifest(tmp_path):
    from kkrunner.files import FileManifest, TaskFile

    spec = ExecutionSpec(
        ("true",), directory=tmp_path,
        input_files=(TaskFile("data", kind="directory", optional=True),),
        output_files=("log", TaskFile("tree", kind="directory", collect=False)),
        cache_files=(TaskFile("cache", optional=True),),
    )
    manifest = FileManifest.from_spec(spec)
    assert manifest.inputs[0].kind == "directory" and manifest.inputs[0].optional
    assert manifest.outputs[0].kind == "file" and manifest.outputs[0].collect
    assert manifest.outputs[1].kind == "directory" and not manifest.outputs[1].collect
    assert manifest.caches[0].file.optional
    assert execution_request(spec)["output_directories"] == [str(tmp_path / "tree")]


@pytest.mark.parametrize("command", ["program with spaces", Path("program with spaces")])
def test_single_argument_and_file_declarations(command):
    from kkrunner import TaskFile
    from kkrunner.execution import MpiSpec

    artifact = TaskFile("cache.bin", optional=True)
    spec = ExecutionSpec(
        command, mpi_argv=Path("mpi-program"), mpi=8,
        input_files="input.dat", output_files=Path("result.dat"), cache_files=artifact,
    )
    assert spec.argv == ("program with spaces",)
    assert spec.mpi_argv == ("mpi-program",)
    assert spec.mpi == MpiSpec.from_value(8)
    assert spec.input_files == ("input.dat",)
    assert spec.output_files == ("result.dat",)
    assert spec.cache_files == (artifact,)


@pytest.mark.parametrize("mpi", [None, False, True, 4, "auto", "mpiexec", ("mpiexec", "-n", "4")])
def test_mpi_shortcuts_normalize_to_mpi_spec(mpi):
    from kkrunner.execution import MpiSpec

    assert ExecutionSpec("program", mpi=mpi).mpi == MpiSpec.from_value(mpi)


def test_default_run_store_uses_existing_user_state_directory(tmp_path, monkeypatch):
    from kkrunner import RunStore

    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    assert RunStore().directory == tmp_path / "kkrunner" / "runs"
