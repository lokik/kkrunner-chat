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
    from kkrunner.transports.implementations.ssh.transport import SshTransport
    from kkrunner.runs.run import Run

    assert EndpointClient.__module__ == "kkrunner.client.endpoint"
    assert SshTransport.__module__ == "kkrunner.transports.implementations.ssh.transport"
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
    'kkrunner.backends.implementations.psij', 'kkrunner.transports.implementations.ssh.transport', 'kkrunner.projects', 'psij',
))
from kkrunner.backends import DirectBackend
assert backends.DirectBackend is DirectBackend
assert 'kkrunner.backends.implementations.psij' not in sys.modules
"""
    subprocess.run([sys.executable, "-c", code], check=True, timeout=10)



def test_process_strategies_have_distinct_implementations():
    from kkrunner.backends import DirectBackend, SupervisedBackend
    assert DirectBackend.__module__ == "kkrunner.backends.implementations.direct.backend"
    assert SupervisedBackend.__module__ == "kkrunner.backends.implementations.supervised.backend"


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
    from kkrunner.backends.implementations.supervised import SupervisedBackend
    from kkrunner.remote import service

    assert backend_class("kkrunner") is SupervisedBackend
    assert backend_class("kkrunner") is SupervisedBackend
    assert service._backend_for_kind("kkrunner") is SupervisedBackend
    assert service._backend_for_kind("kkrunner") is SupervisedBackend


def test_direct_process_import_does_not_load_supervisor():
    code = """
import sys
from kkrunner.client.job import Job
assert 'kkrunner.backends.implementations.supervised' not in sys.modules
"""
    subprocess.run([sys.executable, "-c", code], check=True, timeout=10)


def test_slurm_native_module_does_not_load_psij_executor():
    code = """
import sys
from kkrunner.backends.implementations.psij import slurm
assert callable(slurm.status)
assert 'psij' not in sys.modules
assert 'kkrunner.backends.implementations.psij.executor' not in sys.modules
"""
    subprocess.run([sys.executable, "-c", code], check=True, timeout=10)


def test_psij_agent_import_does_not_load_executor_implementation():
    # Agent command definitions should load without constructing or importing
    # the reusable PSI/J executor; it is only needed on actual submission.
    code = """
import sys
from kkrunner.backends.implementations.psij import PsijBackend
assert callable(PsijBackend.follow)
assert 'kkrunner.backends.implementations.psij' in sys.modules
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
        assert (root / "implementations" / name / "backend.py").is_file()
    assert (root / "support").is_dir()


def test_agent_and_direct_have_distinct_supported_operations():
    from kkrunner.backends import available_backend_names, installed_backend_names
    assert available_backend_names() == {"direct", "kkrunner", "psij"}
    assert installed_backend_names() == {"kkrunner", "psij"}


def test_concrete_implementations_are_separated_from_contracts():
    import kkrunner.backends
    import kkrunner.transports
    from kkrunner.backends import DirectBackend, SupervisedBackend, PsijBackend
    from kkrunner.transports import LocalTransport, SshTransport

    root = Path(kkrunner.__file__).parent
    backend_impls = root / "backends" / "implementations"
    transport_impls = root / "transports" / "implementations"
    assert {p.name for p in backend_impls.iterdir() if p.is_dir() and p.name != "__pycache__"} == {
        "direct", "supervised", "psij"
    }
    assert (transport_impls / "local" / "transport.py").is_file()
    assert (transport_impls / "local" / "call.py").is_file()
    assert (transport_impls / "ssh" / "transport.py").is_file()
    assert (transport_impls / "ssh" / "call.py").is_file()
    assert not (transport_impls / "local.py").exists()
    assert not (transport_impls / "local_call.py").exists()
    assert (transport_impls / "unavailable.py").is_file()
    # Ignored __pycache__ artifacts can survive a package move in a local tree.
    # Assert the old Python sources are gone without requiring empty directories.
    assert not list((root / "client" / "transports").rglob("*.py"))
    assert all(cls.__module__.startswith("kkrunner.backends.implementations.")
               for cls in (DirectBackend, SupervisedBackend, PsijBackend))
    assert all(cls.__module__.startswith("kkrunner.transports.implementations.")
               for cls in (LocalTransport, SshTransport))
    assert all(not hasattr(cls, "SshHelper") for cls in (DirectBackend, SupervisedBackend, PsijBackend))


def test_transport_contracts_are_lazy_without_concrete_backends():
    script = """
import sys
from kkrunner.transports import Transport
from kkrunner.transports.contracts import Operation, RunningCommand
assert 'kkrunner.backends.implementations.direct' not in sys.modules
assert 'kkrunner.backends.implementations.psij' not in sys.modules
assert 'kkrunner.transports.implementations.ssh.transport' not in sys.modules
assert Transport is not None and Operation is not None and RunningCommand is not None
"""
    subprocess.run([sys.executable, "-c", script], check=True, timeout=10)


def test_transport_implementations_have_symmetric_package_exports():
    from kkrunner.transports import LocalTransport, SshTransport, transport_from_dict
    from kkrunner.client import LocalTransport as ClientLocalTransport
    from kkrunner.transports.implementations.local import LocalTransport as LocalFromPackage
    from kkrunner.transports.implementations.ssh import SshTransport as SshFromPackage
    from kkrunner.transports.implementations.local.call import LocalCall
    from kkrunner.transports.implementations.ssh.call import SshCall

    assert LocalFromPackage is LocalTransport is ClientLocalTransport
    assert SshFromPackage is SshTransport
    assert LocalTransport.__module__ == "kkrunner.transports.implementations.local.transport"
    assert SshTransport.__module__ == "kkrunner.transports.implementations.ssh.transport"
    assert LocalCall.__module__ == "kkrunner.transports.implementations.local.call"
    assert SshCall.__module__ == "kkrunner.transports.implementations.ssh.call"
    assert isinstance(transport_from_dict({"kind": "local"}), LocalTransport)
    assert isinstance(transport_from_dict({"kind": "ssh", "host": "example"}), SshTransport)
