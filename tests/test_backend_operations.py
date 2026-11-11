"""Backend operations are independently callable after importing only a module."""

import importlib
import json
import subprocess
import sys

import pytest

from kkrunner.backends import installed_backend_names, operation_module
from kkrunner.client.operations import BackendOperation
from kkrunner.client.transport import LocalTransport


@pytest.mark.parametrize("backend", ["process", "psij"])
def test_each_backend_exposes_same_context_free_function_names(backend):
    module = importlib.import_module(operation_module(backend))
    for name in ("submit", "status", "cancel", "recover", "follow",
                 "stream", "acknowledge", "acknowledge_submission"):
        fn = getattr(module, name)
        assert fn.__module__ == module.__name__
        assert callable(fn)
        # Public functions must have no hidden closure or imported globals.
        import inspect
        closure = inspect.getclosurevars(fn)
        assert not closure.nonlocals
        assert not closure.globals


@pytest.mark.parametrize("backend", ["process", "psij"])
def test_backend_operation_can_be_imported_in_fresh_python(backend):
    module = operation_module(backend)
    code = """import importlib, json, sys
mod = importlib.import_module(sys.argv[1])
print(json.dumps(sorted(name for name in
    ('submit', 'status', 'cancel', 'recover', 'follow',
     'stream', 'acknowledge', 'acknowledge_submission')
    if callable(getattr(mod, name, None)))))
"""
    result = subprocess.run(
        [sys.executable, "-c", code, module], capture_output=True, text=True, check=True,
    )
    assert json.loads(result.stdout) == [
        "acknowledge", "acknowledge_submission", "cancel", "follow",
        "recover", "status", "stream", "submit",
    ]



def test_direct_backend_exposes_one_portable_execution_entry():
    module = importlib.import_module(operation_module("direct"))
    assert callable(module.stream)
    assert callable(module.resolve_mpi_options)
    assert "def execute(request):" in module.WORKER_SOURCE


def test_backend_interfaces_use_static_methods():
    from kkrunner.backends import backend_class
    from kkrunner.backends.class_helper import static_methods
    for kind in ("direct", "process", "psij"):
        backend = backend_class(kind)
        assert "stream" in static_methods(backend)
        assert backend.SshHelper is not None


def test_installed_ssh_helper_delegates_without_copying_implementation():
    from kkrunner.backends import backend_class
    from kkrunner.backends.class_helper import class_source, static_methods
    for kind in ("process", "psij"):
        backend = backend_class(kind)
        helper = backend.SshHelper(backend)
        assert set(static_methods(helper)) == set(static_methods(backend))
        source = class_source(helper)
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
    assert "SupervisedBackendSsh.status" in command[2]
