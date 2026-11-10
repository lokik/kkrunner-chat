"""A target request is complete, serializable and never mutates ExecutionSpec."""

import io
import json
from pathlib import Path

import pytest

from kkrunner.client.execution_request import execution_request
from kkrunner.runs.base import ExecutionSpec


def test_named_file_is_mapped_after_flush_without_modifying_spec(tmp_path):
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
        assert source.read_bytes() == b"before submission"
        assert json.loads(json.dumps(result)) == result


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
