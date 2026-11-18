"""Missing MPI payloads must fail before an MPI launcher obscures the error."""

import asyncio
import json
import os
import subprocess
import sys

import pytest

from kkrunner.backends import DirectBackend, SupervisedBackend
from kkrunner.backends.contracts import SubmissionNotStarted
from kkrunner.backends.support.process import prepare_mpi, validate_executable
from kkrunner.client.contracts import Operation
from kkrunner.client.backend_execution import DirectExecution
from kkrunner.transports import LocalTransport
from kkrunner.transports.implementations.ssh.rpc import operation_command
from kkrunner.transports.rpc import operation_result


def mpi_request(directory, executable):
    return {
        "argv": [sys.executable], "mpi_argv": [str(executable)],
        "directory": str(directory),
        "mpi": {"required": True, "launcher": [sys.executable]},
    }


def test_missing_mpi_binary_reports_target_and_working_directory(tmp_path):
    missing = tmp_path / "embscfMPI"
    with pytest.raises(FileNotFoundError, match="Executable not found") as error:
        prepare_mpi(mpi_request(tmp_path, missing))
    assert error.value.filename == str(missing)
    assert str(tmp_path) in str(error.value)


def test_missing_explicit_mpi_launcher_is_reported_before_submission(tmp_path):
    request = mpi_request(tmp_path, sys.executable)
    missing = tmp_path / "custom-mpirun"
    request["mpi"]["launcher"] = [str(missing)]
    with pytest.raises(FileNotFoundError, match="Executable not found") as error:
        prepare_mpi(request)
    assert error.value.filename == str(missing)


@pytest.mark.parametrize("relative", ["./program", "bin/program", "program"])
def test_relative_executable_uses_target_cwd_and_path(monkeypatch, tmp_path, relative):
    target = tmp_path / relative
    target.parent.mkdir(exist_ok=True)
    target.write_text("#!/bin/sh\nexit 0\n")
    target.chmod(0o755)
    monkeypatch.setenv("PATH", ".")
    validate_executable(relative, directory=tmp_path)


def test_bare_command_is_not_implicitly_searched_in_working_directory(monkeypatch, tmp_path):
    target = tmp_path / "program"
    target.write_text("program")
    target.chmod(0o755)
    monkeypatch.setenv("PATH", str(tmp_path / "elsewhere"))
    with pytest.raises(FileNotFoundError, match="Executable not found"):
        validate_executable("program", directory=tmp_path)


@pytest.mark.skipif(os.name != "posix", reason="POSIX executable permissions")
def test_existing_nonexecutable_file_is_reported_as_permission_error(tmp_path):
    target = tmp_path / "embscfMPI"
    target.write_text("not executable")
    target.chmod(0o644)
    with pytest.raises(PermissionError, match="not runnable") as error:
        prepare_mpi(mpi_request(tmp_path, target))
    assert error.value.filename == str(target)


def test_command_resolution_alone_does_not_validate_future_payload(tmp_path):
    request = mpi_request(tmp_path, tmp_path / "compute-node-only")
    prepared = prepare_mpi(request, resolve_only=True)
    assert prepared["request"]["argv"] == [sys.executable, str(tmp_path / "compute-node-only")]


def test_missing_mpi_program_survives_source_delivery_without_installed_kkrunner(tmp_path):
    missing = tmp_path / "remote-embscfMPI"
    command = operation_command(Operation.for_backend(DirectBackend, "prepare_mpi"),
                                python=(sys.executable,))
    result = subprocess.run(
        [sys.executable, "-I", *command[1:]],
        input=json.dumps({"args": [mpi_request(tmp_path, missing)], "kwargs": {}}),
        capture_output=True, text=True, check=True,
    )
    with pytest.raises(FileNotFoundError, match="Executable not found") as error:
        operation_result(result.stdout)
    assert str(missing) in str(error.value)


def test_local_transport_preserves_missing_program_message(tmp_path):
    operation = Operation.for_backend(DirectBackend, "prepare_mpi")
    with pytest.raises(FileNotFoundError, match="Executable not found"):
        asyncio.run(LocalTransport().call(operation, mpi_request(tmp_path, "missing-program")))


@pytest.mark.parametrize("use_mpi", [False, True])
def test_client_reports_missing_program_before_opening_execution_stream(monkeypatch, tmp_path, use_mpi):
    async def unexpected_stream(*_args, **_kwargs):
        pytest.fail("A missing MPI program must not reach the execution stream")

    monkeypatch.setattr(LocalTransport, "open_call", unexpected_stream)
    execution = DirectExecution(DirectBackend(), LocalTransport())
    missing = tmp_path / "embscfMPI"
    request = mpi_request(tmp_path, missing)
    if not use_mpi:
        request.update(argv=[str(missing)], mpi_argv=None)
    with pytest.raises(SubmissionNotStarted, match="Executable not found") as error:
        asyncio.run(execution.submit(request, lambda *_args: None))
    assert str(missing) in str(error.value)
    assert execution.ref is None


@pytest.mark.parametrize("use_mpi", [False, True])
def test_supervised_missing_program_never_starts_supervisor(monkeypatch, tmp_path, use_mpi):
    def unexpected_launch(*_args, **_kwargs):
        pytest.fail("A missing MPI payload must be rejected before supervisor launch")

    monkeypatch.setattr(subprocess, "Popen", unexpected_launch)
    request = mpi_request(tmp_path, tmp_path / "embscfMPI")
    if not use_mpi:
        request.update(argv=[str(tmp_path / "embscf")], mpi_argv=None)
    with pytest.raises(SubmissionNotStarted, match="Executable not found"):
        SupervisedBackend.submit(request)
