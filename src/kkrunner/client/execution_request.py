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

from kkrunner.files.manifest import FileManifest


def command_request(spec, transform=list):
    """Project client command intent to target-only JSON execution fields."""
    spec.require_complete()
    return {
        "argv": transform(spec.argv),
        "mpi_argv": None if spec.mpi_argv is None else transform(spec.mpi_argv),
        "mpi_uses_stdin": spec.mpi_uses_stdin,
        "mpi": asdict(spec.mpi),
    }


def execution_request(spec, *, directory=None, map_path: Callable | None = None,
                      manifest=None, allow_live_streams=False):
    """Materialize a JSON-safe target request without mutating *spec*.

    For remote execution, *map_path* must resolve each local pathname to the
    staged target path. Without a mapping the request is local-only.
    The existing manifest, when supplied, owns all file declaration semantics.
    Only a local in-process invocation may use unnamed live streams.
    """
    spec.require_complete()
    target_dir = os.fspath(spec.directory if directory is None else directory)
    if not target_dir:
        raise ValueError("Execution directory must not be empty")
    files = FileManifest.from_spec(spec) if manifest is None else manifest
    if map_path is None and directory is not None and target_dir != spec.directory and files.entries:
        raise ValueError("A target path mapping is required for remote file declarations")

    for value, entry, live in (
        (spec.input_file, files.input, False),
        (spec.stdin, files.stdin, allow_live_streams),
        (spec.output_file, files.stdout, allow_live_streams),
        (spec.error_file, files.stderr, allow_live_streams),
    ):
        if value is not None and (entry is None or entry.local_path is None):
            if not live or map_path is not None or target_dir != spec.directory:
                raise ValueError("A named file or path is required")

    def path(entry):
        if entry is None:
            return None
        name = os.fsdecode(entry.local_path)
        if map_path is not None:
            name = map_path(name)
        if not isinstance(name, (str, os.PathLike)):
            raise TypeError("File mapping must return a target pathname")
        return os.fspath(name)

    protected_files, protected_patterns = [], []
    for entry in (*files.inputs, *(cache.file for cache in files.caches)):
        (protected_patterns if files.is_pattern(entry) else protected_files).append(path(entry))

    return target_request(
        spec, target_dir, input_file=path(files.input), stdin=path(files.stdin),
        output_file=path(files.stdout), error_file=path(files.stderr),
        protected_files=protected_files, protected_patterns=protected_patterns,
        output_files=(path(entry) for entry in files.outputs),
        output_directories=(path(entry) for entry in files.outputs if entry.kind == "directory"),
    )


def target_request(spec, directory, *, input_file, stdin, output_file, error_file,
                   protected_files, protected_patterns, output_files, output_directories, transform=list):
    """Build the wire request from already selected target paths."""
    request = {
        **command_request(spec, transform),
        "directory": directory,
        "input_file": input_file,
        "stdin": stdin,
        "output_file": output_file,
        "error_file": error_file,
        "protected_files": list(dict.fromkeys(protected_files)),
        "protected_patterns": list(dict.fromkeys(protected_patterns)),
        "output_files": list(output_files),
        "output_directories": list(output_directories),
    }
    import json
    # Validate before contacting a target, not after a possibly accepted submit.
    return json.loads(json.dumps(request, allow_nan=False))
