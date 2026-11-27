"""Observational copies of files still being written by a producer."""

from contextlib import contextmanager
import os
import stat
from pathlib import Path
from kkrunner.files.staging import staged_local_file

from kkrunner.files.manifest import FileEntry
from kkrunner.files.paths import declaration_paths, file_path, has_file_magic, path_matches, paths_overlap


def matching_file(manifest, path):
    """Find a declared concrete output, stream or cache without mutating the manifest."""
    path = os.path.abspath(os.fspath(path))
    entries = (manifest.stdout, manifest.stderr, *(x for x in manifest.outputs if x.collect),
               *(c.file for c in manifest.caches))
    entries = tuple(e for e in entries if e is not None and e.local_path is not None)
    for entry in entries:
        if os.path.abspath(entry.local_path) == path:
            return entry

    for entry in entries:
        if entry in manifest.outputs and manifest.output_excluded(path):
            continue
        if path_matches(path, os.path.abspath(entry.local_path)):
            return entry
    return None


def snapshot_request(manifest, directory, file, destination):
    """Resolve and validate one observational copy against its declaration."""
    selected = file.local_path if isinstance(file, FileEntry) else file
    path = file_path(selected, directory, required=True)
    if path is None or has_file_magic(path):
        raise ValueError("Snapshot requires one concrete file, not a wildcard")
    entry = matching_file(manifest, path)
    if entry is None:
        raise ValueError(f"File is not a declared output or cache: {path}")
    if destination is not None:
        destination = check_destination(manifest, path, destination)
    return path, entry, destination


@contextmanager
def snapshot_destination(destination):
    """Publish a copy atomically without overwriting the existing file on failure."""
    with staged_local_file(destination) as temporary:
        with temporary.open("wb") as output:
            yield output


def check_destination(manifest, source, destination):
    """Reject destinations in the run's declared file namespace."""
    target = os.path.abspath(os.fspath(destination))
    originals = (
        manifest.stdin, *manifest.inputs, manifest.stdout, manifest.stderr,
        *manifest.outputs, *(c.file for c in manifest.caches),
    )
    if os.path.realpath(source) == os.path.realpath(target):
        raise ValueError("Snapshot destination overlaps a declared file")
    targets = (target, os.path.realpath(target))
    for entry in originals:
        if entry is None or entry.local_path is None:
            continue
        path = os.path.abspath(entry.local_path)
        for identity in (path, os.path.realpath(path)):
            for reference in declaration_paths(identity, directory=entry.kind == "directory"):
                if any(paths_overlap(candidate, reference) for candidate in targets):
                    raise ValueError("Snapshot destination overlaps a declared file")
    return Path(destination)


def local_snapshot(path, *, destination=None):
    """Read existing file bytes or stream an atomic observational copy."""
    path = Path(path)
    if not stat.S_ISREG(path.stat().st_mode):
        raise ValueError(f"Snapshot source is not a regular file: {path}")
    if destination is None:
        return path.read_bytes()
    import shutil
    with snapshot_destination(destination) as output:
        with open(path, "rb") as source:
            shutil.copyfileobj(source, output)
    return Path(destination)
