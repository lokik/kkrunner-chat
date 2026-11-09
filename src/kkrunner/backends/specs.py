"""Convert agent protocol submission requests to backend specifications."""

import os

from kkrunner.runs.base import ExecutionSpec, LaunchSpec

def _request_spec(request):
    mpi_argv = request.get("mpi_argv")
    sync = request.get("client_sync") or {}
    input_files = tuple(request.get("input_files", ()))
    output_files = tuple(
        dict.fromkeys(
            (
                *request.get("output_files", ()),
                *sync.get("output_files", ()),
                *sync.get("output_patterns", ()),
            )
        )
    )
    cache_files = tuple(
        dict.fromkeys(
            (
                *request.get("cache_files", ()),
                *(item["path"] for item in sync.get("cache_files", ())),
                *sync.get("cache_patterns", ()),
            )
        )
    )
    return ExecutionSpec(
        argv=tuple(request["argv"]),
        input_file=request.get("input_file"),
        output_file=request.get("output_file"),
        error_file=request.get("error_file"),
        directory=os.fspath(request["directory"]),
        stdin=request.get("stdin"),
        input_files=input_files,
        output_files=output_files,
        cache_files=cache_files,
        mpi_argv=None if mpi_argv is None else tuple(mpi_argv),
        launch=LaunchSpec(**(request.get("launch") or {})),
        mpi_uses_stdin=request.get("mpi_uses_stdin", True),
    )

