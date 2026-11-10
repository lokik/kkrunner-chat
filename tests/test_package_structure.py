"""Public names and dependency boundaries for the standalone package."""

import ast
import importlib
import pkgutil
import subprocess
import sys
from pathlib import Path

import pytest

import kkrunner
import kkrunner.projects
import kkrunner.remote


def test_public_managers_have_one_unambiguous_name():
    assert kkrunner.RunManager.__module__ == "kkrunner.runs.manager"
    assert kkrunner.projects.ProjectExecutionManager.__module__ == "kkrunner.projects.manager"
    from kkrunner.client.agent import AgentBackendProxy
    from kkrunner.runs.run import Run

    assert AgentBackendProxy.__module__ == "kkrunner.client.agent"
    assert Run.__module__ == "kkrunner.runs.run"
    assert not hasattr(kkrunner, "ExecutionManager")
    assert not hasattr(kkrunner, "RemoteExecutor")
    assert not hasattr(importlib.import_module("kkrunner.runs.manager"), "ExecutionManager")
    assert not hasattr(importlib.import_module("kkrunner.projects.manager"), "ExecutionManager")


def test_run_and_job_ownership_boundaries_are_explicit():
    from kkrunner.backends import ProcessBackend, PsijBackend
    from kkrunner.client.agent import AgentBackendProxy
    from kkrunner.client.remote_files import RemoteFileSession
    from kkrunner.client.remote_job import RemoteJob
    from kkrunner.client.run_parts import RunParts
    from kkrunner.runs.run import Run

    assert Run.__module__ == "kkrunner.runs.run"
    assert RemoteJob.__module__ == "kkrunner.client.remote_job"
    assert RemoteFileSession.__module__ == "kkrunner.client.remote_files"
    for backend in (ProcessBackend, PsijBackend, AgentBackendProxy):
        assert callable(backend.create_run_parts)
        assert callable(backend.open_run_parts)
        assert not hasattr(backend, "create_managed_run")
    assert RunParts.__dataclass_fields__.keys() == {"job", "files"}



def test_all_runtime_modules_import_without_optional_scheduler_dependency():
    for module in pkgutil.walk_packages(kkrunner.__path__, kkrunner.__name__ + "."):
        if not module.name.endswith("__main__"):
            importlib.import_module(module.name)


def test_file_and_support_layers_do_not_depend_on_orchestration():
    root = Path(kkrunner.__file__).parent
    for package in ("files", "support"):
        for path in (root / package).glob("*.py"):
            for node in ast.walk(ast.parse(path.read_text())):
                if isinstance(node, ast.ImportFrom) and node.module:
                    assert not node.module.startswith(
                        (
                            "kkrunner.runs",
                            "kkrunner.projects",
                            "kkrunner.remote",
                            "kkrunner.backends",
                        )
                    ), (path.name, node.module)


def test_root_has_only_backend_independent_entry_points():
    assert set(kkrunner.__all__) - {"__version__"} == {
        "ExecutionSpec",
        "ExecutionOutcome",
        "RunManager",
        "RunStore",
        "RunState",
        "TaskFile",
        "ProcessingPending",
        "Run",
    }
    for name in (
        "LocalExecutor",
        "PsijExecutor",
        "RemoteEndpointExecutor",
        "ProjectRepository",
        "LocalRunner",
        "PsijRunner",
        "RemoteRunner",
        "PostprocessingRunner",
    ):
        assert not hasattr(kkrunner, name)


def test_contract_import_does_not_load_backend_implementations():
    code = """
import sys
from kkrunner.files import TaskFile
from kkrunner import ExecutionSpec, RunStore
from kkrunner import backends
assert 'ProcessBackend' in dir(backends)
assert all(name not in sys.modules for name in (
    'kkrunner.backends.process.engine',
    'kkrunner.backends.psij.backend',
    'kkrunner.client.agent', 'kkrunner.projects', 'psij',
))
from kkrunner.backends import ProcessBackend
from kkrunner.backends.process.engine import ProcessBackend as implementation
assert ProcessBackend is implementation
assert backends.ProcessBackend is ProcessBackend
assert 'kkrunner.backends.psij.backend' not in sys.modules
"""
    subprocess.run([sys.executable, "-c", code], check=True, timeout=10)



def test_facade_exports_resolve_without_optional_scheduler_dependency():
    for package in (kkrunner.runs, kkrunner.files, kkrunner.backends, kkrunner.projects, kkrunner.remote):
        for name in package.__all__:
            assert getattr(package, name) is not None, (package.__name__, name)
    with pytest.raises(AttributeError, match="has no attribute"):
        _ = kkrunner.backends.not_a_backend


def test_generic_manager_has_no_application_methods():
    from kkrunner.runs.manager import RunManager

    assert not hasattr(RunManager, "load_result")
    assert not hasattr(RunManager, "attach_sprkkr")


def test_root_import_is_lazy():
    code = """\
import kkrunner
import sys
assert all(name not in sys.modules for name in (
    'kkrunner.runs', 'kkrunner.files', 'kkrunner.backends',
    'kkrunner.remote', 'kkrunner.projects', 'psutil',
))
assert 'ExecutionSpec' in dir(kkrunner)
"""
    subprocess.run([sys.executable, "-c", code], check=True, timeout=10)


@pytest.mark.parametrize(
    "module_name",
    ("kkrunner", "kkrunner.files", "kkrunner.runs", "kkrunner.remote", "kkrunner.backends"),
)
def test_lazy_facade_exports_are_cached(module_name):
    # Exercise all public names in a fresh process; other tests have already
    # imported implementation modules and would conceal broken lazy paths.
    code = """
import importlib
import sys
package = importlib.import_module(sys.argv[1])
resolved = {}
for name in package.__all__:
    value = getattr(package, name)
    assert package.__dict__[name] is value, name
    resolved[name] = value

def forbidden(name):
    raise AssertionError("cached attribute unexpectedly resolved again: " + name)

original = package.__getattr__
package.__getattr__ = forbidden
try:
    for name, value in resolved.items():
        assert getattr(package, name) is value, name
finally:
    package.__getattr__ = original
"""
    subprocess.run([sys.executable, "-c", code, module_name], check=True, timeout=10)
