"""Observational copies of files still being written by a producer."""

from contextlib import contextmanager
from fnmatch import fnmatchcase
from functools import lru_cache
import os
from pathlib import Path
from uuid import uuid4

from kkrunner.files.paths import has_file_magic


def matching_file(manifest, path):
    """Find a declared concrete output, stream or cache without mutating the manifest."""
    path = os.path.abspath(os.fspath(path))
    entries = (manifest.stdout, manifest.stderr, *(x for x in manifest.outputs if x.collect),
               *(c.file for c in manifest.caches))
    entries = tuple(e for e in entries if e is not None and e.local_path is not None)
    for entry in entries:
        if os.path.abspath(entry.local_path) == path:
            return entry

    components = Path(path).parts
    for entry in entries:
        pattern = os.path.abspath(entry.local_path)
        if not has_file_magic(pattern):
            continue
        parts = Path(pattern).parts

        @lru_cache(None)
        def match(i, j):
            if j == len(parts):
                return i == len(components)
            if parts[j] == "**":
                return match(i, j + 1) or (
                    i < len(components) and not components[i].startswith(".") and match(i + 1, j)
                )
            return (
                i < len(components)
                and (not components[i].startswith(".") or parts[j].startswith("."))
                and fnmatchcase(components[i], parts[j])
                and match(i + 1, j + 1)
            )

        if match(0, 0):
            return entry
    return None


@contextmanager
def snapshot_destination(destination):
    """Publish a copy atomically without overwriting the existing file on failure."""
    path = Path(destination)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        with temp.open("wb") as output:
            yield output
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def check_destination(manifest, source, destination):
    """Reject destinations in the run's declared file namespace."""
    target = os.path.abspath(os.fspath(destination))
    originals = (
        manifest.stdin, *manifest.inputs, manifest.stdout, manifest.stderr,
        *manifest.outputs, *(c.file for c in manifest.caches),
    )
    if os.path.realpath(source) == os.path.realpath(target):
        raise ValueError("Snapshot destination overlaps a declared file")
    if matching_file(manifest, target) is not None:
        raise ValueError("Snapshot destination overlaps a declared file")
    for entry in originals:
        if entry is not None and entry.local_path is not None:
            if os.path.realpath(target) == os.path.realpath(entry.local_path):
                raise ValueError("Snapshot destination overlaps a declared file")
            if has_file_magic(entry.local_path):
                # Protect wildcard input declarations as well.
                from kkrunner.files.manifest import FileManifest
                if matching_file(FileManifest(outputs=(entry,)), target):
                    raise ValueError("Snapshot destination overlaps a declared file")
    return Path(destination)


def local_snapshot(path, *, destination=None):
    """Read existing file bytes or stream an atomic observational copy."""
    if destination is None:
        return Path(path).read_bytes()
    import shutil
    with snapshot_destination(destination) as output:
        with open(path, "rb") as source:
            shutil.copyfileobj(source, output)
    return Path(destination)
