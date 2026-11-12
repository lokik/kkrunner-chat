"""A backend operation has the same contract in both execution locations."""

import asyncio
import json
import os
import sys

import pytest

from kkrunner.client.contracts import Operation
from kkrunner.client.transports import LocalTransport, SshTransport


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
        result = await endpoint.call(Operation.standalone(portable_add), 19, 23)
        assert result == 42
    asyncio.run(verify())


@pytest.mark.parametrize("endpoint_type", [LocalTransport, LoopbackSshTransport])
def test_installed_call(endpoint_type):
    endpoint = endpoint_type()
    async def verify():
        from kkrunner.backends import SupervisedBackend
        operation = Operation.for_backend(SupervisedBackend, "acknowledge_submission")
        assert await endpoint.call(operation, "missing-submission") == {"acknowledged": False}
    asyncio.run(verify())


def test_portable_stream_on_ssh_transport():
    endpoint = LoopbackSshTransport()
    async def verify():
        running = await endpoint.start_operation(Operation.standalone(portable_stream), "live\\n")
        out, err = await running.communicate()
        assert out == b"live\\n"
        assert err == b""
        assert running.returncode == 0
    asyncio.run(verify())


def test_reject_nonportable_operation_before_submission():
    captured = 10
    def nested(value):
        return captured + value
    # Portability is validated when preparing target source, not when creating
    # the client-side operation descriptor (local calls need no source).
    from kkrunner.client.transports.rpc import operation_command
    with pytest.raises(TypeError, match="top-level Python function"):
        operation_command(Operation.standalone(nested))
    from kkrunner.backends import SupervisedBackend
    with pytest.raises(ValueError, match="static backend operation"):
        Operation.for_backend(SupervisedBackend, "not-an-operation")


@pytest.mark.parametrize("endpoint_type", [LocalTransport, LoopbackSshTransport])
def test_installed_backend_operation_receives_only_explicit_json_arguments(endpoint_type):
    endpoint = endpoint_type()
    async def verify():
        from kkrunner.backends import DirectBackend
        operation = Operation.for_backend(DirectBackend, "prepare_mpi")
        # The installed target operation has the same explicit JSON arguments
        # locally and over SSH, without shipping a function's module globals.
        request = {
            "argv": [], "mpi_argv": [],
            "mpi": {"required": True, "launcher": ["mpiexec", "--bind"], "process_count": 2},
        }
        result = await endpoint.call(operation, request, resolve_only=True)
        assert result["launcher"] == ["mpiexec", "--bind", "-np", "2"]
        assert result["request"]["argv"] == ["mpiexec", "--bind", "-np", "2"]
    asyncio.run(verify())


def test_ssh_loopback_streaming_operation_preserves_live_output():
    """Streaming standalone functions use pipes and a process exit code."""
    async def verify():
        process = await LoopbackSshTransport().start_operation(
            Operation.standalone(portable_stream), "streamed",
        )
        out, err = await process.communicate()
        assert out == b"streamed"
        assert err == b""
        assert process.returncode == 0
        assert await process.wait() == 0

    asyncio.run(verify())

@pytest.mark.parametrize("transport_type", [LocalTransport, LoopbackSshTransport])
def test_class_backend_operation_runs_locally_and_over_ssh(transport_type, tmp_path, monkeypatch):
    """The generated import-only helper must execute a real installed backend."""
    from kkrunner.backends import backend_class

    monkeypatch.setenv("KKRunner_REMOTE_STATE_DIR", str(tmp_path / "mailbox"))
    operation = Operation.for_backend(
        backend_class("process"), "acknowledge_submission"
    )

    async def verify():
        reply = await transport_type().call(operation, "missing-submission")
        assert reply == {"acknowledged": False}

    asyncio.run(verify())


def test_installed_class_helper_source_never_contains_supervisor_implementation():
    from kkrunner.backends import backend_class
    from kkrunner.client.transports.rpc import operation_command

    op = Operation.for_backend(backend_class("process"), "submit")
    command = operation_command(op, python=(sys.executable,))
    script = command[-1]
    compile(script, "<ssh import-only helper>", "exec")
    assert "from kkrunner.backends.supervised.backend import SupervisedBackend" in script
    assert "_local_submit" not in script
    assert "class SupervisorStreams" not in script
