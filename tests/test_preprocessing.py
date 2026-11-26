"""Client preprocessing owns its reservation before generating input files."""

import sys
from dataclasses import replace

import pytest

from kkrunner import ExecutionSpec, RunManager, RunState, RunStore
from kkrunner.client import EndpointClient
from kkrunner.runs import DuplicateSubmissionError
from kkrunner.runs.base import FailurePhase
from kkrunner.files import FileManifest


def test_preprocessing_reserves_generated_input_before_writing(tmp_path):
    store = tmp_path / "runs"
    manager = RunManager(RunStore(store), submitting_grace=0)
    other = RunManager(RunStore(store), submitting_grace=0)
    input_file = tmp_path / "generated.inp"
    client = EndpointClient()
    spec = ExecutionSpec(
        (sys.executable, "-c", "from pathlib import Path; print(Path('generated.inp').read_text())"),
        directory=tmp_path, input_file=input_file, output_file="out",
        preprocessing_files=(input_file,),
    )
    competitor_spec = replace(spec, output_file="other.out", preprocessing_files=())
    competitor = other.create_run(client, competitor_spec)

    def preprocess():
        assert run.state == RunState.PREPROCESSING
        assert run.record.state == RunState.PREPROCESSING
        assert other.refresh((run.record,))[0].state == RunState.PREPROCESSING
        assert not input_file.exists()
        with pytest.raises(DuplicateSubmissionError):
            competitor.submit()
        input_file.write_text("generated")

    run = manager.create_run(client, spec, preprocess=preprocess)
    assert not input_file.exists()
    run.run()
    assert run.state == RunState.FINISHED
    assert (tmp_path / "out").read_text().strip() == "generated"
    assert manager.store.get(run.id).files.preprocessing_files == FileManifest.from_spec(spec).preprocessing_files


def test_preprocessing_failure_never_submits_and_releases_reservation(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    client = EndpointClient()
    spec = ExecutionSpec(
        (sys.executable, "-c", "pass"), directory=tmp_path, output_file="out",
        preprocessing_files=("generated.inp",),
    )

    def preprocess():
        raise ValueError("invalid input")

    run = manager.create_run(client, spec, preprocess=preprocess)
    with pytest.raises(ValueError, match="invalid input"):
        run.submit()
    assert run.backend_id is None
    assert run.state == RunState.FAILED
    record = manager.store.get(run.id)
    assert record.failure_phase == FailurePhase.PREPROCESSING
    assert record.files.captured_at is None
    assert not (tmp_path / "out").exists()
    replacement = manager.create_run(client, replace(spec, preprocessing_files=()))
    replacement.run()
    assert replacement.state == RunState.FINISHED


def test_orphan_preprocessing_is_recovered_without_backend_query(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = manager.new_record(directory=tmp_path)
    with manager.store.preprocessing_lock(record):
        reserved = manager.reserve(record, preprocessing=True)
        assert manager.refresh((reserved,))[0].state == RunState.PREPROCESSING
    recovered = manager.refresh((reserved,))[0]
    assert recovered.state == RunState.FAILED
    assert recovered.failure_phase == FailurePhase.PREPROCESSING
    assert recovered.execution.backend_id is None
