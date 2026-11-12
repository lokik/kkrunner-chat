"""Prepare an execution command for an independently invoked target operation.

ExecutionSpec is the immutable client declaration, which may contain live file
handles. A target command may only receive names in the target filesystem.
Mapping from local to remote paths is supplied by the FileSession, never
guessed from the file handle's local .name.
"""

from __future__ import annotations

import os
from dataclasses import asdict
from typing import Callable

from kkrunner.files.paths import file_path


def _named_path(value, directory, mapping):
    if value is None:
        return None
    # A file object with .name is supported, but an anonymous pipe, fd or
    # in-memory stream cannot silently become a missing target path.
    path = file_path(value, directory, flush=True, required=True)
    if not isinstance(path, str):
        path = os.fsdecode(path)
    if mapping is not None:
        path = mapping(path)
    if not isinstance(path, (str, os.PathLike)):
        raise TypeError("File mapping must return a target pathname")
    return os.fspath(path)


def command_request(spec, transform=list):
    """Project client command intent to target-only JSON execution fields."""
    spec.require_complete()
    return {
        "argv": transform(spec.argv),
        "mpi_argv": None if spec.mpi_argv is None else transform(spec.mpi_argv),
        "mpi_uses_stdin": spec.mpi_uses_stdin,
        "mpi": asdict(spec.mpi),
    }


def execution_request(spec, *, directory=None, map_path: Callable | None = None):
    """Materialize a JSON-safe target request without mutating *spec*.

    For remote execution, *map_path* must resolve each local pathname to the
    staged target path. Without a mapping the request is local-only.
    """
    spec.require_complete()
    target_dir = os.fspath(spec.directory if directory is None else directory)
    if not target_dir:
        raise ValueError("Execution directory must not be empty")
    if map_path is None and directory is not None and target_dir != spec.directory:
        declared = (
            spec.input_file, spec.stdin, spec.output_file, spec.error_file,
            *spec.input_files, *spec.output_files, *spec.cache_files,
        )
        if any(value is not None for value in declared):
            raise ValueError("A target path mapping is required for remote file declarations")
    def path(value):
        return _named_path(value, spec.directory, map_path)
    def paths(values):
        return [path(value) for value in values if value is not None]

    request = {
        **command_request(spec),
        "directory": target_dir,
        "input_file": path(spec.input_file),
        "stdin": path(spec.stdin),
        "output_file": path(spec.output_file),
        "error_file": path(spec.error_file),
        "input_files": paths(spec.input_files),
        "output_files": paths(spec.output_files),
        "cache_files": paths(spec.cache_files),
    }
    import json
    # Validate before contacting a target, not after a possibly accepted submit.
    return json.loads(json.dumps(request, allow_nan=False))
