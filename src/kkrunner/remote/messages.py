"""Agent wire-message conversion, separate from backend execution contracts."""

import os

from kkrunner.runs.base import ExecutionSpec, LaunchSpec


def request_to_spec(request):
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


def status_reply(state, return_code=None, *, failure_phase=None, error=None):
    """Encode backend status facts for the common remote-service wire contract."""
    return {
        "state": state.value,
        "return_code": return_code,
        "error": error,
        "failure_phase": None if failure_phase is None else failure_phase.value,
    }

