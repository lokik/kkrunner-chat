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
    assert kkrunner.projects.ProjectRunManager.__module__ == "kkrunner.projects.manager"
    from kkrunner.client.endpoint import EndpointClient
    from kkrunner.client.transports.ssh import SshTransport
    from kkrunner.runs.run import Run

    assert EndpointClient.__module__ == "kkrunner.client.endpoint"
    assert SshTransport.__module__ == "kkrunner.client.transports.ssh"
    assert Run.__module__ == "kkrunner.runs.run"
    assert not hasattr(kkrunner, "ExecutionManager")
    assert not hasattr(kkrunner, "RemoteExecutor")
    assert not hasattr(importlib.import_module("kkrunner.runs.manager"), "ExecutionManager")
    assert not hasattr(importlib.import_module("kkrunner.projects.manager"), "ExecutionManager")


def test_run_and_job_ownership_boundaries_are_explicit():
    from kkrunner.backends import DirectBackend, SupervisedBackend, PsijBackend
    from kkrunner.client import EndpointClient, LocalTransport
    from kkrunner.backends.contracts import BackendCapability

    for backend in (DirectBackend, SupervisedBackend, PsijBackend):
        assert not hasattr(backend, "create_job")
        client = EndpointClient(backend, transport=LocalTransport())
        assert callable(client.create_run_parts)
    assert BackendCapability.REATTACH in DirectBackend().supports
    assert BackendCapability.REATTACH in SupervisedBackend().supports



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
from kkrunner import ExecutionSpec, RunStore, backends
assert 'DirectBackend' in dir(backends)
assert all(name not in sys.modules for name in (
    'kkrunner.backends.psij', 'kkrunner.client.transports.ssh', 'kkrunner.projects', 'psij',
))
from kkrunner.backends import DirectBackend
assert backends.DirectBackend is DirectBackend
assert 'kkrunner.backends.psij' not in sys.modules
"""
    subprocess.run([sys.executable, "-c", code], check=True, timeout=10)



def test_process_strategies_have_distinct_implementations():
    from kkrunner.backends import DirectBackend, SupervisedBackend
    assert DirectBackend.__module__ == "kkrunner.backends.direct.backend"
    assert SupervisedBackend.__module__ == "kkrunner.backends.supervised.backend"


def test_local_process_backend_selects_one_execution_strategy(tmp_path):
    from kkrunner import ExecutionSpec
    from kkrunner.client import EndpointClient
    from kkrunner.backends import SupervisedBackend
    from kkrunner.client.job import Job

    client = EndpointClient()
    spec = ExecutionSpec(argv=("dummy",), directory=str(tmp_path), input_file=None)
    assert isinstance(client.create_run_parts(spec).job, Job)
    assert isinstance(client.create_run_parts(spec, submission_key="run-1").job, Job)
    assert isinstance(EndpointClient(SupervisedBackend).create_run_parts(spec, submission_key="run-1").job, Job)


def test_remote_process_service_reuses_supervised_backend_operations():
    from kkrunner.backends import backend_class
    from kkrunner.backends.supervised import SupervisedBackend
    from kkrunner.remote import service

    assert backend_class("process") is SupervisedBackend
    assert service._backend_for_kind("process") is SupervisedBackend


def test_direct_process_import_does_not_load_supervisor():
    code = """
import sys
from kkrunner.client.job import Job
assert 'kkrunner.backends.supervised' not in sys.modules
"""
    subprocess.run([sys.executable, "-c", code], check=True, timeout=10)


def test_slurm_native_module_does_not_load_psij_executor():
    code = """
import sys
from kkrunner.backends.psij import slurm
assert callable(slurm.status)
assert 'psij' not in sys.modules
assert 'kkrunner.backends.psij.executor' not in sys.modules
"""
    subprocess.run([sys.executable, "-c", code], check=True, timeout=10)


def test_psij_agent_import_does_not_load_executor_implementation():
    # Agent command definitions should load without constructing or importing
    # the reusable PSI/J executor; it is only needed on actual submission.
    code = """
import sys
from kkrunner.backends.psij import PsijBackend
assert callable(PsijBackend.follow)
assert 'kkrunner.backends.psij' in sys.modules
assert 'psij' not in sys.modules
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

original = getattr(package, "__getattr__", None)
if original is not None:
    package.__getattr__ = forbidden
try:
    for name, value in resolved.items():
        assert getattr(package, name) is value, name
finally:
    if original is not None:
        package.__getattr__ = original
"""
    subprocess.run([sys.executable, "-c", code, module_name], check=True, timeout=10)


def test_all_backends_have_local_packages():
    from pathlib import Path
    import kkrunner.backends
    root = Path(kkrunner.backends.__file__).parent
    for name in ("direct", "supervised", "psij"):
        assert (root / name / "backend.py").is_file()
    assert (root / "support").is_dir()


def test_agent_and_direct_have_distinct_supported_operations():
    from kkrunner.backends import available_backend_names, installed_backend_names
    assert available_backend_names() == {"direct", "process", "psij"}
    assert installed_backend_names() == {"process", "psij"}
