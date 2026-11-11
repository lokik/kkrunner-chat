"""The one-source native services must run without an installed kkrunner."""

import asyncio
import json
import os
import subprocess
import sys

import pytest

from kkrunner.backends.support.process import resolve_mpi_options
from kkrunner.client.local import LocalRunEndpoint
from kkrunner.client.run_parts import run_endpoint_for_executor
from kkrunner.client.portable_functions import function_source
from kkrunner.files.availability import prepare_target_outputs
from kkrunner.files.queries import finalize_upload, query_files


def _isolated(script, *, input=None, args=()):
    return subprocess.run(
        (sys.executable, "-I", "-c", script, *args), input=input,
        capture_output=True, text=True, check=True,
    ).stdout


def test_native_backend_does_not_construct_client_session(tmp_path):
    from kkrunner.backends import DirectBackend
    assert not hasattr(DirectBackend, "create_job")
    assert not hasattr(DirectBackend, "create_run_parts")
    endpoint = LocalRunEndpoint(DirectBackend)
    assert run_endpoint_for_executor(endpoint) is endpoint


def test_portable_file_query_matches_native_batch(tmp_path):
    target = tmp_path / "data.txt"
    target.write_text("some data")
    paths = [str(target), str(tmp_path / "absent")]
    kwargs = {"root": str(tmp_path), "digest": True}
    native = query_files(paths, **kwargs)
    isolated = _isolated(
        function_source(query_files),
        input=json.dumps({"args": [paths], "kwargs": kwargs}),
    )
    assert json.loads(isolated) == native


def test_portable_file_query_rejects_escaping_symlink(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    (root / "outside").symlink_to(tmp_path / "elsewhere")
    (tmp_path / "elsewhere").write_text("secret")
    payload = json.dumps({"args": [["outside"]], "kwargs": {"root": str(root)}})
    result = subprocess.run(
        (sys.executable, "-I", "-c", function_source(query_files)),
        input=payload, text=True, capture_output=True,
    )
    assert result.returncode != 0
    assert "outside source root" in result.stderr


def test_portable_finalize_upload_preserves_mtime(tmp_path):
    temporary, destination = tmp_path / "temporary", tmp_path / "final"
    temporary.write_text("payload")
    timestamp = 1729000000123456789
    payload = json.dumps({"args": [str(temporary), str(destination), timestamp], "kwargs": {}})
    assert json.loads(_isolated(function_source(finalize_upload), input=payload)) is None
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
        function_source(resolve_mpi_options),
        input=json.dumps({"args": [isolated_request], "kwargs": {}}),
    )
    assert native == ("srun", "-n", "3")
    assert json.loads(result) == ["custom-mpi", "--bind", "-np", "3"]


def test_portable_output_cleanup_preserves_wildcard_cache(tmp_path):
    cache, stale = tmp_path / "saved.pot", tmp_path / "stale.pot"
    cache.write_text("saved")
    stale.write_text("stale")
    request = {
        "output_file": None,
        "client_sync": {
            "cache_patterns": [str(tmp_path / "saved*.pot")],
            "output_patterns": [str(tmp_path / "*.pot")],
        },
    }
    payload = json.dumps({"args": [request], "kwargs": {}})
    assert json.loads(_isolated(function_source(prepare_target_outputs), input=payload)) is None
    assert cache.read_text() == "saved"
    assert not stale.exists()


def test_remote_function_rejects_module_globals():
    def nested():
        return 1
    with pytest.raises(TypeError, match="top-level"):
        function_source(nested)
