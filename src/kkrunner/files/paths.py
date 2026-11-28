"""Local pathname resolution and declaration checks, independent of execution."""

import glob
import os
from fnmatch import fnmatchcase
from functools import lru_cache
from pathlib import Path, PurePath

from kkrunner.files.task_file import TaskFile


def file_path(value, directory=None, *, required=False):
    """Resolve a named file or path without performing any file I/O."""
    if value is None:
        return None
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


def relative_local_path(path, local_root):
    """Resolve a local path and enforce the run's mapped root boundary."""
    path = Path(path).expanduser().resolve()
    root = Path(local_root).expanduser().resolve()
    try:
        return path.relative_to(root)
    except ValueError as exc:
        raise ValueError(
            f"Local path {path} is outside mapped root {root}; specify local_root/remote_root explicitly"
        ) from exc


def declared_file_paths(values, directory, *, excluded=()):
    """Combine identical input declarations; reject contradictory optionality.

    The execution layer accepts ordinary filenames, file objects and TaskFile
    metadata without depending on the task-schema implementation.
    """
    merged = {}
    excluded = set(excluded)
    for value in values:
        path = file_path(value, directory)
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



def path_matches(path, pattern, *, include_hidden=False):
    """Match a concrete pathname using filesystem glob component semantics."""
    path, pattern = os.path.normpath(path), os.path.normpath(pattern)
    if not has_file_magic(pattern):
        return path == pattern
    components, parts = PurePath(path).parts, PurePath(pattern).parts

    @lru_cache(None)
    def match(i, j):
        if j == len(parts):
            return i == len(components)
        if parts[j] == "**":
            return match(i, j + 1) or (
                i < len(components)
                and (include_hidden or not components[i].startswith("."))
                and match(i + 1, j)
            )
        return (
            i < len(components)
            and (include_hidden or not components[i].startswith(".") or parts[j].startswith("."))
            and fnmatchcase(components[i], parts[j])
            and match(i + 1, j + 1)
        )

    return match(0, 0)


def declaration_matches(path, declaration, *, pattern=False, directory=False, include_hidden=False):
    """Test a concrete pathname against a file, directory tree or glob."""
    path, declaration = os.path.normpath(path), os.path.normpath(declaration)
    if directory:
        # Directory declarations are literal, including hidden descendants.
        return path == declaration or PurePath(declaration) in PurePath(path).parents
    if pattern:
        return path_matches(path, declaration, include_hidden=include_hidden)
    return path == declaration


def paths_overlap(first, second):
    """Conservative overlap: exact/glob uses the shared component matcher."""
    first, second = os.path.normpath(first), os.path.normpath(second)
    first_magic, second_magic = has_file_magic(first), has_file_magic(second)
    if not first_magic:
        return path_matches(first, second, include_hidden=True)
    if not second_magic:
        return path_matches(second, first, include_hidden=True)
    if first == second:
        return True

    # A recursive wildcard may match zero path components. The literal
    # prefix before the first wildcard is still a necessary prefix.
    def prefix(pattern):
        return pattern[:min(pattern.find(c) for c in "*?[" if c in pattern)]

    a, b = prefix(first), prefix(second)
    return a.startswith(b) or b.startswith(a)


def declaration_paths(path, *, directory=False):
    """An exact directory reserves the entire tree, including hidden files."""
    path = os.path.normpath(path)
    return (path, os.path.join(path, "**")) if directory else (path,)
