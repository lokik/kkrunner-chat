"""The one-source native services must run without an installed kkrunner."""

import asyncio
import json
import os
import subprocess
import sys

import pytest

from kkrunner.backends.support.process import prepare_mpi, resolve_mpi_options
from kkrunner.client import EndpointClient
from kkrunner.client.contracts import Operation
from kkrunner.client.transports.rpc import operation_command, operation_result
from kkrunner.files.availability import prepare_target_outputs
from kkrunner.files.queries import finalize_upload, query_files


def _operation_script(function):
    """Exercise the actual transported JSON operation, not a legacy wrapper."""
    return operation_command(Operation.standalone(function), python=(sys.executable,))[-1]


def _isolated(script, *, input=None, args=()):
    return subprocess.run(
        (sys.executable, "-I", "-c", script, *args), input=input,
        capture_output=True, text=True, check=True,
    ).stdout


def test_native_backend_does_not_construct_client_session(tmp_path):
    from kkrunner.backends import DirectBackend
    assert not hasattr(DirectBackend, "create_job")
    assert not hasattr(DirectBackend, "create_run_parts")
    endpoint = EndpointClient(DirectBackend)
    assert isinstance(endpoint.backend, DirectBackend)


def test_portable_file_query_matches_native_batch(tmp_path):
    target = tmp_path / "data.txt"
    target.write_text("some data")
    paths = [str(target), str(tmp_path / "absent")]
    kwargs = {"root": str(tmp_path), "digest": True}
    native = query_files(paths, **kwargs)
    isolated = _isolated(
        _operation_script(query_files),
        input=json.dumps({"args": [paths], "kwargs": kwargs}),
    )
    assert operation_result(isolated) == native


def test_portable_file_query_rejects_escaping_symlink(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    (root / "outside").symlink_to(tmp_path / "elsewhere")
    (tmp_path / "elsewhere").write_text("secret")
    payload = json.dumps({"args": [["outside"]], "kwargs": {"root": str(root)}})
    result = subprocess.run(
        (sys.executable, "-I", "-c", _operation_script(query_files)),
        input=payload, text=True, capture_output=True,
    )
    assert result.returncode == 0
    with pytest.raises(ValueError, match="outside source root"):
        operation_result(result.stdout)


def test_portable_finalize_upload_preserves_mtime(tmp_path):
    temporary, destination = tmp_path / "temporary", tmp_path / "final"
    temporary.write_text("payload")
    timestamp = 1729000000123456789
    payload = json.dumps({"args": [str(temporary), str(destination), timestamp], "kwargs": {}})
    assert operation_result(_isolated(_operation_script(finalize_upload), input=payload)) is None
    assert destination.read_text() == "payload"
    assert destination.stat().st_mtime_ns == timestamp


def test_portable_mpi_options_matches_local_resolver(monkeypatch):
    import shutil
    monkeypatch.setenv("SLURM_NODELIST", "node0")
    monkeypatch.setattr(shutil, "which", lambda name: "/usr/bin/srun" if name == "srun" else None)
    request = {"required": True, "autodetect": True, "launcher": [], "process_count": 3}
    native = resolve_mpi_options(request)
    # The explicit launcher path tests the same implementation in isolation
    # without relying on the CI host having any MPI software.
    isolated_request = {**request, "launcher": ["custom-mpi", "--bind"]}
    result = _isolated(
        _operation_script(prepare_mpi),
        input=json.dumps({"args": [{"argv": [], "mpi_argv": [], "mpi": isolated_request}],
                          "kwargs": {"resolve_only": True}}),
    )
    assert native == ("srun", "-n", "3")
    assert operation_result(result)["launcher"] == ["custom-mpi", "--bind", "-np", "3"]


def test_portable_output_cleanup_preserves_wildcard_cache(tmp_path):
    cache, stale = tmp_path / "saved.pot", tmp_path / "stale.pot"
    cache.write_text("saved")
    stale.write_text("stale")
    request = {
        "output_file": None,
        "cache_files": [str(tmp_path / "saved*.pot")],
        "output_files": [str(tmp_path / "*.pot")],
    }
    payload = json.dumps({"args": [request], "kwargs": {}})
    assert operation_result(_isolated(_operation_script(prepare_target_outputs), input=payload)) is None
    assert cache.read_text() == "saved"
    assert not stale.exists()


def test_remote_function_rejects_module_globals():
    def nested():
        return 1
    with pytest.raises(TypeError, match="top-level"):
        _operation_script(nested)
