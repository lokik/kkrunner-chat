"""Backend operations are static methods, independently dispatchable via SSH."""

import json
import subprocess
import sys

import pytest

from kkrunner.backends import backend_class
from kkrunner.backends.support.class_helper import static_methods
from kkrunner.client.contracts import Operation
from kkrunner.client.transports.rpc import operation_command

MANAGED_OPERATIONS = {"submit", "status", "cancel", "recover", "follow",
                      "stream", "follow_framed", "acknowledge", "acknowledge_submission"}


@pytest.mark.parametrize("kind", ("process", "psij"))
def test_managed_backend_exposes_static_operations(kind):
    assert set(static_methods(backend_class(kind))) == MANAGED_OPERATIONS


@pytest.mark.parametrize("kind", ("process", "psij"))
def test_backend_imports_in_fresh_interpreter(kind):
    backend = backend_class(kind)
    script = ("import importlib,json,sys\n"
              "cls=getattr(importlib.import_module(sys.argv[1]),sys.argv[2])\n"
              "print(json.dumps(sorted(k for k,v in vars(cls).items() if isinstance(v,staticmethod))))\n")
    result = subprocess.run([sys.executable, "-c", script, backend.__module__, backend.__name__],
                            check=True, capture_output=True, text=True)
    assert set(json.loads(result.stdout)) == MANAGED_OPERATIONS


def test_direct_and_managed_operations_have_distinct_contracts():
    assert set(static_methods(backend_class("direct"))) == {"prepare_mpi", "prepare_target_outputs", "stream"}
    assert set(static_methods(backend_class("process"))) == MANAGED_OPERATIONS


@pytest.mark.parametrize("kind", ("process", "psij"))
def test_ssh_helper_imports_one_backend_method(kind):
    backend = backend_class(kind)
    helper = backend.SshHelper
    assert set(static_methods(helper)) == set(static_methods(backend))
    source = helper.submit.__source__
    assert f"from {backend.__module__} import {backend.__name__}" in source
    assert f"return {backend.__name__}.submit(*args, **kwargs)" in source
    assert "def _local_submit" not in source


@pytest.mark.parametrize("kind", ("process", "psij"))
def test_operation_command_dispatches_static_method(kind):
    backend = backend_class(kind)
    operation = Operation.for_backend(backend, "status")
    command = operation_command(operation)
    assert command[:2] == ("python3", "-c")
    compile(command[2], "<backend operation>", "exec")
    assert f"return {backend.__name__}.status" in command[2]


@pytest.mark.parametrize("kind", ("direct", "process", "psij"))
def test_backend_operation_reference_is_well_formed(kind):
    backend = backend_class(kind)
    operation = Operation.for_backend(backend, "stream")
    assert operation.backend is backend
    assert operation.method == "stream"
    assert operation.function is None


def test_portable_direct_stream_is_self_contained():
    backend = backend_class("direct")
    command = operation_command(Operation.for_backend(backend, "stream"), streaming=True)
    code = command[-1]
    compile(code, "<portable direct stream>", "exec")
    assert "async def stream(request" in code
    assert "from kkrunner" not in code
