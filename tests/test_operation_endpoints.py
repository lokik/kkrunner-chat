"""A backend operation has the same contract in both execution locations."""

import asyncio
import json
import os
import sys

import pytest

from kkrunner.client.operations import BackendOperation
from kkrunner.client.transport import LocalTransport, SshTransport


def portable_add(a, b):
    return a + b


def portable_stream(text):
    import sys
    sys.stdout.write(text)
    sys.stdout.flush()


class LoopbackSshTransport(SshTransport):
    """Execute the real target Python commands without an SSH daemon."""

    def __init__(self):
        super().__init__("loopback", python_command=(sys.executable,))

    def _command(self, argv, cwd=None):
        assert cwd is None
        return tuple(argv)


@pytest.mark.parametrize("endpoint_type", [LocalTransport, LoopbackSshTransport])
def test_portable_call(endpoint_type):
    endpoint = endpoint_type()
    async def verify():
        result = await endpoint.call(BackendOperation.standalone(portable_add), 19, 23)
        assert result == 42
    asyncio.run(verify())


@pytest.mark.parametrize("endpoint_type", [LocalTransport, LoopbackSshTransport])
def test_installed_call(endpoint_type):
    endpoint = endpoint_type()
    async def verify():
        operation = BackendOperation.installed("operator", "add")
        assert await endpoint.call(operation, 19, 23) == 42
    asyncio.run(verify())


@pytest.mark.parametrize("endpoint_type", [LocalTransport, LoopbackSshTransport])
def test_portable_stream(endpoint_type):
    endpoint = endpoint_type()
    async def verify():
        running = await endpoint.start_operation(BackendOperation.standalone(portable_stream), "live\\n")
        out, err = await running.communicate()
        assert out == b"live\\n"
        assert err == b""
        assert running.returncode == 0
    asyncio.run(verify())


def test_reject_nonportable_operation_before_submission():
    captured = 10
    def nested(value):
        return captured + value
    with pytest.raises(TypeError):
        BackendOperation.standalone(nested)
    with pytest.raises(ValueError):
        BackendOperation.installed("bad-module!", "run")


@pytest.mark.parametrize("endpoint_type", [LocalTransport, LoopbackSshTransport])
def test_installed_backend_operation_receives_only_explicit_json_arguments(endpoint_type):
    endpoint = endpoint_type()
    async def verify():
        operation = BackendOperation.installed("kkrunner.backends.support", "resolve_mpi_options")
        # Explicit launcher avoids dependence on target environment/MPI.
        result = await endpoint.call(
            operation,
            {"required": True, "launcher": ["mpiexec", "--bind"], "process_count": 2},
        )
        assert list(result) == ["mpiexec", "--bind", "-np", "2"]
    asyncio.run(verify())


def test_local_streaming_operation_preserves_terminal_result_out_of_band():
    async def verify():
        process = await LocalTransport().start_operation(
            BackendOperation.installed("operator", "add"), 19, 23,
        )
        from pathlib import Path
        receipt = Path(process._receipt_path)
        assert receipt.is_file()
        out, err = await process.communicate()
        assert not receipt.exists()
        assert out == err == b""
        assert process.returncode == 0
        assert process.terminal == 42
        assert await process.wait() == 0
        assert process.terminal == 42

    asyncio.run(verify())


@pytest.mark.parametrize("transport_type", [LocalTransport, LoopbackSshTransport])
def test_class_backend_operation_runs_locally_and_over_ssh(transport_type, tmp_path, monkeypatch):
    """The generated import-only helper must execute a real installed backend."""
    from kkrunner.backends import backend_class
    from kkrunner.client.operations import BackendOperation

    monkeypatch.setenv("KKRunner_REMOTE_STATE_DIR", str(tmp_path / "mailbox"))
    operation = BackendOperation.for_backend(
        backend_class("process"), "acknowledge_submission"
    )

    async def verify():
        reply = await transport_type().call(operation, "missing-submission")
        assert reply == {"acknowledged": False}

    asyncio.run(verify())


def test_installed_class_helper_source_never_contains_supervisor_implementation():
    from kkrunner.backends import backend_class
    from kkrunner.client.operations import BackendOperation, operation_command

    op = BackendOperation.for_backend(backend_class("process"), "submit")
    command = operation_command(op, python=(sys.executable,), ssh=True)
    script = command[-1]
    compile(script, "<ssh import-only helper>", "exec")
    assert "from kkrunner.backends.supervised import SupervisedBackend" in script
    assert "def status(" not in script
    assert "class SupervisedBackendSsh:" not in script
    assert "_local_submit" not in script
    assert "class SupervisorStreams" not in script
