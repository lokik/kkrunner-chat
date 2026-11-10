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
        operation = BackendOperation.installed("kkrunner.backends.mpi", "resolve_mpi_options")
        # Explicit launcher avoids dependence on target environment/MPI.
        result = await endpoint.call(
            operation,
            {"required": True, "launcher": ["mpiexec", "--bind"], "process_count": 2},
        )
        assert list(result) == ["mpiexec", "--bind", "-np", "2"]
    asyncio.run(verify())
