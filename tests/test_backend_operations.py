"""Static backend methods are the only execution operation interface."""

import json
import subprocess
import sys

import pytest

from kkrunner.backends import backend_class
from kkrunner.backends.class_helper import static_methods
from kkrunner.client.operations import BackendOperation, operation_command

OPERATIONS = {"submit", "status", "cancel", "recover", "follow",
              "stream", "follow_framed", "acknowledge", "acknowledge_submission"}


@pytest.mark.parametrize("kind", ("process", "psij"))
def test_installed_backend_declares_only_static_operations(kind):
    backend = backend_class(kind)
    assert set(static_methods(backend)) == OPERATIONS


@pytest.mark.parametrize("kind", ("process", "psij"))
def test_backend_class_imports_in_fresh_python(kind):
    backend = backend_class(kind)
    script = (
        "import importlib, json, sys\n"
        "backend = getattr(importlib.import_module(sys.argv[1]), sys.argv[2])\n"
        "print(json.dumps(sorted(name for name, value in backend.__dict__.items() "
        "if isinstance(value, staticmethod))))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script, backend.__module__, backend.__name__],
        capture_output=True, text=True, check=True,
    )
    assert set(json.loads(result.stdout)) == OPERATIONS



def test_backend_interfaces_use_static_methods():
    from kkrunner.backends import backend_class
    from kkrunner.backends.class_helper import static_methods
    for kind in ("direct", "process", "psij"):
        backend = backend_class(kind)
        assert "stream" in static_methods(backend)
        assert backend.SshHelper is not None


def test_installed_ssh_helper_delegates_without_copying_implementation():
    from kkrunner.backends import backend_class
    from kkrunner.backends.class_helper import method_source, static_methods
    for kind in ("process", "psij"):
        backend = backend_class(kind)
        helper = backend.SshHelper(backend)
        assert set(static_methods(helper)) == set(static_methods(backend))
        source = method_source(helper, "submit")
        assert f"from {backend.__module__} import {backend.__name__}" in source
        assert "def submit(*args, **kwargs):" in source
        assert "def _local_submit" not in source


def test_backend_ssh_source_is_python_and_uses_class_dispatch():
    from kkrunner.backends import backend_class
    from kkrunner.client.operations import BackendOperation, operation_command
    operation = BackendOperation.for_backend(backend_class("process"), "status")
    command = operation_command(operation, python=("python3",), ssh=True)
    assert command[:2] == ("python3", "-c")
    compile(command[2], "<backend ssh>", "exec")
    assert "def status(*args, **kwargs):" in command[2]
    assert "class SupervisedBackendSsh:" not in command[2]
    assert "def submit(*args, **kwargs):" not in command[2]


def test_backend_operation_reference_is_well_formed():
    from kkrunner.backends import backend_class
    from kkrunner.client.operations import BackendOperation
    for kind in ("direct", "process", "psij"):
        operation = BackendOperation.for_backend(backend_class(kind), "stream")
        assert operation.backend is backend_class(kind)
        assert operation.function == "stream"


def test_portable_direct_ssh_backend_is_self_contained():
    from kkrunner.backends import backend_class
    from kkrunner.client.operations import BackendOperation, operation_command
    backend = backend_class("direct")
    code = operation_command(BackendOperation.for_backend(backend, "stream"), ssh=True)[-1]
    compile(code, "<direct ssh backend>", "exec")
    assert "def execute(request):" in code
    assert "from kkrunner" not in code


def test_native_operations_do_not_call_removed_module_status_functions():
    """All native cancellation/recovery paths use the internal status owner."""
    import ast
    import inspect
    from kkrunner.native.supervised import operations as supervised
    from kkrunner.native.psij import operations as psij
    for module in (supervised, psij):
        tree = ast.parse(inspect.getsource(module))
        unresolved = [
            node for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
            and node.func.id == "status"
        ]
        assert not unresolved, module.__name__
