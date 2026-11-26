"""Local pathname resolution and declaration checks, independent of execution."""

import glob
import os

from kkrunner.files.task_file import TaskFile


def file_path(value, directory=None, *, flush=False, required=False):
    """Resolve a named file or path, returning None for unnamed descriptors."""
    if value is None:
        return None
    if flush and hasattr(value, "flush"):
        value.flush()
    if not isinstance(value, (str, bytes, os.PathLike)):
        value = getattr(value, "name", value)
    if isinstance(value, int):
        if required:
            raise ValueError("A named file or path is required")
        return None
    try:
        path = os.fspath(value)
    except TypeError as exc:
        if required:
            raise ValueError("A named file or path is required") from exc
        return None
    if directory is not None and not os.path.isabs(path):
        path = os.path.join(directory, path)
    return os.path.abspath(path)


def declared_file_paths(values, directory, *, excluded=(), flush=False):
    """Combine identical input declarations; reject contradictory optionality.

    The execution layer accepts ordinary filenames, file objects and TaskFile
    metadata without depending on the task-schema implementation.
    """
    merged = {}
    excluded = set(excluded)
    for value in values:
        path = file_path(value, directory, flush=flush)
        if path is None or path in excluded:
            continue
        required = not (isinstance(value, TaskFile) and value.optional)
        previous = merged.get(path)
        if previous is not None and previous != required:
            raise ValueError(
                f"Conflicting optionality for file {path}: the same input cannot be both required and optional"
            )
        merged[path] = required
    return tuple(merged.items())


def has_file_magic(path):
    """Return whether *path* contains shell-style wildcard characters."""
    return glob.has_magic(os.fspath(path))


def expand_local_candidates(path):
    """Enumerate path candidates; preserve exact names even when absent.

    No root validation or symlink resolution happens here. The operation
    consuming the paths is responsible for enforcing its own safety policy.
    """
    path = os.fspath(path)
    if has_file_magic(path):
        return tuple(sorted(glob.glob(path, recursive=True)))
    return (path,)
