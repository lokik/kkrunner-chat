"""Backend operations are independently callable after importing only a module."""

import importlib
import json
import subprocess
import sys

import pytest

from kkrunner.backends.registry import installed_backend_names, operation_module
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
