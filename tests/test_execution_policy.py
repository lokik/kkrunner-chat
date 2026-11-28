import asyncio
import sys

import pytest

from kkrunner import RunFileState
from kkrunner.backends import DirectBackend
from kkrunner.client import ExecutionClient, LocalTarget
from kkrunner.client.execution_request import command_request, execution_request
from kkrunner.client.run_parts import RunParts
from kkrunner.execution import MpiSpec
from kkrunner.files.manifest import ArtifactAvailabilityState, FileEntry, FileManifest
from kkrunner.files.paths import file_path
from kkrunner.runs.base import ExecutionSpec, FailurePhase, RunState, SubmissionNotStarted
from kkrunner.runs.manager import RunManager
from kkrunner.runs.run import Run
from kkrunner.runs.records import RunStore


def _local_spec(tmp_path, code="print('ok')"):
    return ExecutionSpec(
        argv=(sys.executable, "-c", code),
        input_file=None,
        output_file=(tmp_path / "out.txt").open("w+b"),
        error_file=(tmp_path / "err.txt").open("w+b"),
        directory=str(tmp_path),
    )


def test_output_observer_failure_is_reported_but_does_not_fail_process(tmp_path):
    runner = ExecutionClient(DirectBackend).create_run(_local_spec(tmp_path), tracked=False)
    observer_errors = []

    def broken_observer(*_):
        raise ValueError("observer failed")

    runner.add_output_listener(broken_observer)
    runner.add_output_error_listener(lambda callback, exc: observer_errors.append((callback, exc)))
    result = runner.run()
    assert runner.state == RunState.FINISHED
    assert result.return_code == 0
    assert len(observer_errors) == 1
    assert observer_errors[0][0] is broken_observer
    assert str(observer_errors[0][1]) == "observer failed"


def test_postprocessing_failure_does_not_change_raw_execution_state(tmp_path):
    def fail(_outcome):
        raise ValueError("parser failed")

    runner = ExecutionClient(DirectBackend).create_run(_local_spec(tmp_path), tracked=False, process_result=fail)
    raw = runner._job
    with pytest.raises(ValueError, match="parser failed"):
        runner.run()
    assert raw.state == RunState.FINISHED
    assert runner.state == RunState.FINISHED
    assert runner.file_state == RunFileState.FAILED
    assert runner.file_failure_phase == FailurePhase.POSTPROCESSING


def test_callback_failure_does_not_change_local_execution_state(tmp_path):
    runner = ExecutionClient(DirectBackend).create_run(_local_spec(tmp_path), tracked=False,
        callback=lambda result: (_ for _ in ()).throw(ValueError("callback failed")),
    )
    with pytest.raises(ValueError, match="callback failed"):
        runner.run()
    assert runner.state == RunState.FINISHED


class _UnknownRunner:
    result = object()
    return_code = None
    backend_id = None
    state = RunState.NEW
    exception = None
    failure_phase = None
    failure_message = None

    def configure_submission(self, request, **options):
        pass

    async def run_async(self, *, on_submitted=None):
        self.backend_id = "native"
        self.state = RunState.UNKNOWN
        if on_submitted is not None:
            await on_submitted()
        return self.result

    async def wait_async(self):
        return self.result

    def add_output_listener(self, callback):
        pass

    def add_output_error_listener(self, callback):
        pass


def test_unknown_raw_state_is_preserved_and_not_postprocessed():
    raw = _UnknownRunner()
    processed = []
    client = ExecutionClient(DirectBackend)
    spec = ExecutionSpec(argv=("unused",), directory=".")
    parts = client.create_run_parts(spec)
    parts.job = raw
    runner = Run(parts, client=client,
                 process_result=lambda outcome: processed.append(outcome))
    outcome = runner.run()
    assert outcome.return_code is None
    assert processed == []
    assert runner.result is outcome
    assert runner.state == RunState.UNKNOWN
    assert runner.failure_phase is None


class _NotStartedRunner:
    backend_id = None
    result = None
    state = RunState.NEW
    exception = None
    failure_phase = None
    failure_message = None

    def configure_submission(self, request, **options):
        pass

    def configure_detached(self, detached):
        pass

    async def run_async(self, *, on_submitted=None):
        raise SubmissionNotStarted("input staging failed")

    def add_output_listener(self, callback):
        pass

    def add_output_error_listener(self, callback):
        pass


class _UncertainRunner(_NotStartedRunner):
    async def run_async(self, *, on_submitted=None):
        raise OSError("connection lost during submit")


def _project_wrapper(tmp_path, runner):
    manager = RunManager(RunStore(tmp_path / "runs"))
    client = ExecutionClient(DirectBackend)
    spec = ExecutionSpec(argv=("unused",), directory=str(tmp_path))
    parts = client.create_run_parts(spec)
    parts.job = runner
    record = manager.new_record(
        directory=tmp_path, target=client.target, backend=client.backend_config,
        metadata={"test": {"task": "SCF"}}, processing_required=True,
    )
    return manager, Run(parts, client=client, manager=manager, record=record)


def test_definite_pre_submit_failure_becomes_submission_failure(tmp_path):
    manager, runner = _project_wrapper(tmp_path, _NotStartedRunner())
    with pytest.raises(SubmissionNotStarted, match="input staging failed"):
        runner.run()
    record = manager.store.get(runner.id)
    assert record.state == RunState.NEW
    assert record.file_state == RunFileState.FAILED
    assert record.file_failure_phase == FailurePhase.PREPROCESSING
    assert record.execution.backend_id is None
    assert record.file_message == "input staging failed"


def test_uncertain_submit_without_reference_stays_submitting(tmp_path):
    manager, runner = _project_wrapper(tmp_path, _UncertainRunner())
    with pytest.raises(OSError, match="connection lost during submit"):
        runner.run()
    record = manager.store.get(runner.id)
    assert record.state == RunState.SUBMITTING
    assert record.execution.backend_id is None
    assert record.message == "connection lost during submit"
    assert runner.state == record.state
    assert runner.failure_message == record.message


def test_artifact_availability_failure_does_not_change_finished_state(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = manager.new_record(
        directory=tmp_path,
        files=FileManifest(stdout=FileEntry(local_path=str(tmp_path / "out"))),
        metadata={"test": {"task": "SCF"}},
        processing_required=True,
    )
    record = manager.store.save(record)
    record = manager.transition(record, state=RunState.FINISHED, file_state=RunFileState.DONE)
    record = manager.update_artifact_availability(
        record, str(tmp_path / "out"), ArtifactAvailabilityState.FAILED, "ssh disconnected"
    )
    assert record.state == RunState.FINISHED
    assert record.files.availability_state == ArtifactAvailabilityState.FAILED
    assert record.files.availability_message == "ssh disconnected"


def test_file_path_resolves_named_files_and_tolerates_unnamed(tmp_path):
    output = tmp_path / "output.dat"
    output.write_text("a")
    with output.open("rb") as stream:
        assert file_path(stream, tmp_path) == str(output)
    assert file_path("relative.dat", tmp_path) == str(tmp_path / "relative.dat")
    assert file_path(None, tmp_path) is None
    assert file_path(1, tmp_path) is None
    assert file_path(object(), tmp_path) is None


def test_file_path_preserves_pathlike_directory(tmp_path):
    from pathlib import Path

    absolute = tmp_path / "different" / "result.dat"
    assert file_path(absolute, tmp_path / "work") == str(absolute)
    assert file_path(Path("subdir/result.dat"), tmp_path) == str(tmp_path / "subdir" / "result.dat")


def test_file_path_flush_is_explicit(tmp_path):

    class FileLike:
        name = "output.dat"

        def __init__(self):
            self.flush_calls = 0

        def flush(self):
            self.flush_calls += 1

    value = FileLike()
    assert file_path(value, tmp_path) == str(tmp_path / value.name)
    assert value.flush_calls == 0
    assert file_path(value, tmp_path, flush=True) == str(tmp_path / value.name)
    assert value.flush_calls == 1


def test_file_path_strict_contract_preserves_optional_none(tmp_path):
    from kkrunner.files.paths import file_path

    assert file_path(None, tmp_path, required=True) is None
    assert file_path("case.out", tmp_path, required=True) == str(tmp_path / "case.out")
    with pytest.raises(ValueError, match="named file"):
        file_path(1, tmp_path, required=True)
    with pytest.raises(ValueError, match="named file"):
        file_path(object(), tmp_path, required=True)


def test_strict_runner_path_errors_remain_backend_specific(tmp_path):
    from kkrunner.client import SshTarget
    from kkrunner.client.file_session import FileSession

    spec = ExecutionSpec(argv=(), directory=str(tmp_path))
    session = ExecutionClient(DirectBackend, target=SshTarget("example.org")).file_session(spec)
    with pytest.raises(ValueError, match="Remote execution requires named files"):
        session._local_path(object())
    with pytest.raises(ValueError, match="Remote execution requires named files"):
        session._local_path(1)
    assert session._local_path(None) is None
    with pytest.raises(ValueError, match="named file"):
        execution_request(ExecutionSpec(argv=("prog",), directory=str(tmp_path), stdin=object()))


def test_common_command_request_preserves_mpi_and_launch_contract(tmp_path):
    from kkrunner import ExecutionSpec

    spec = ExecutionSpec(
        argv=("serial", "input.inp"),
        mpi_argv=("parallel", "input.inp"),
        mpi_uses_stdin=False,
        input_file=None,
        output_file=None,
        directory=str(tmp_path),
        mpi=MpiSpec(required=True, autodetect=True, process_count=2),
    )
    request = command_request(spec)
    assert request["argv"] == ["serial", "input.inp"]
    assert request["mpi_argv"] == ["parallel", "input.inp"]
    assert request["mpi_uses_stdin"] is False
    transformed = command_request(spec, lambda command: [arg.upper() for arg in command])
    assert transformed["argv"] == ["SERIAL", "INPUT.INP"]
    assert transformed["mpi_argv"] == ["PARALLEL", "INPUT.INP"]
    assert transformed["mpi"] == request["mpi"]
    assert transformed["mpi_uses_stdin"] is False
    assert spec.argv == ("serial", "input.inp")

def test_partial_spec_distinguishes_unknown_from_empty_argv(tmp_path):
    from dataclasses import replace
    from kkrunner import ExecutionSpec

    empty = ExecutionSpec(argv=(), directory=str(tmp_path))
    partial = replace(empty, argv=None)
    assert empty.argv == ()
    assert not empty.is_partial
    assert partial.argv is None
    assert partial.is_partial
    assert partial.directory == empty.directory
    with pytest.raises(ValueError, match="partial ExecutionSpec"):
        command_request(partial)
    with pytest.raises(ValueError, match="partial ExecutionSpec"):
        partial.require_complete()
    assert command_request(empty)["argv"] == []


def test_partial_spec_cannot_be_submitted_to_public_backend_apis(tmp_path):
    spec = ExecutionSpec(argv=None, directory=str(tmp_path))
    client = ExecutionClient(DirectBackend)
    with pytest.raises(ValueError, match="partial ExecutionSpec"):
        client.create_run(spec, tracked=False)
    with pytest.raises(ValueError, match="partial ExecutionSpec"):
        client.create_run_parts(spec)
    manager = RunManager(RunStore(tmp_path / "store"))
    with pytest.raises(ValueError, match="partial ExecutionSpec"):
        client.create_run(spec, tracked=manager)
