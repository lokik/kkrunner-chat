"""Read/write reservations across local and target filesystem namespaces."""

from __future__ import annotations

import fnmatch
import os
from dataclasses import dataclass

from kkrunner.transports import LocalTransport, SshTransport
from kkrunner.files.paths import has_file_magic
from kkrunner.runs.records import RunRecord


def _manifest_accesses(record, *, remote=None):
    """Resolve declared read/write paths on the execution host."""
    files = record.files
    directory = record.execution.directory
    remote = directory.remote is not None if remote is None else remote

    def path(entry):
        if entry is None:
            return None
        value = entry.remote_path if remote else entry.local_path
        if remote and value is None and entry.local_path is not None:
            # Before staging, declarations already determine the target footprint.
            # Resolve from the mapped working directory without transferring files.
            local = entry.local_path
            if not os.path.isabs(local):
                local = os.path.join(directory.local, local)
            relative = os.path.relpath(local, directory.local)
            return os.path.normpath(os.path.join(directory.remote, relative))
        if value is None:
            return None
        base = directory.target if remote else directory.local
        return os.path.normpath(value if os.path.isabs(value) else os.path.join(base, value))

    def accesses(entries):
        result = set()
        for entry in entries:
            if entry is None:
                continue
            target = path(entry)
            if target is None:
                continue
            result.add(target)
            if entry.kind == "directory":
                # Directory reservations cover descendant files as well.
                result.add(os.path.join(target, "*"))
        return result

    caches = tuple(x.file for x in files.caches)
    reads = accesses((*files.input_entries, *caches))
    writes = accesses((*files.output_entries, *caches))
    return reads - {None}, writes - {None}


def _path_overlap(a, b):
    """Conservatively compare literal paths and glob declarations."""
    a_magic, b_magic = has_file_magic(a), has_file_magic(b)
    if not a_magic:
        return fnmatch.fnmatchcase(a, b) if b_magic else a == b
    if not b_magic:
        return fnmatch.fnmatchcase(b, a)
    if a == b:
        return True

    # Glob/glob intersection is not generally decidable by fnmatch.
    # Only disjoint literal prefixes can prove that no collision is possible.
    def prefix(pattern):
        return pattern[: min((pattern.find(c) for c in "*?[" if c in pattern), default=len(pattern))]

    pa, pb = prefix(a), prefix(b)
    return pa.startswith(pb) or pb.startswith(pa)


def _paths_overlap(first, second):
    return any(_path_overlap(a, b) for a in first for b in second)


def filesystem_namespace(record):
    """Identity of the execution filesystem, shared by reservations and cleanup."""
    transport = record.execution.transport
    if isinstance(transport, LocalTransport):
        return ("local",)
    mapping = record.files.mapping
    if mapping is not None and mapping.filesystem_id is not None:
        return ("filesystem", mapping.filesystem_id)
    if isinstance(transport, SshTransport):
        return ("ssh", str(transport.host).rsplit("@", 1)[-1].lower())
    # An unsupported transport must not be mistaken for the local filesystem.
    return ("unknown",)


def referenced_paths(record, *, remote=None):
    """All declared reads and writes, including globs; default to the execution filesystem."""
    reads, writes = _manifest_accesses(record, remote=remote)
    return reads | writes


def _possibly_same_filesystem(left, right):
    first, second = filesystem_namespace(left), filesystem_namespace(right)
    return first == second or ("unknown",) in (first, second)


def file_reference_overlaps(path, reference, *, directory=False):
    """Whether removing a file/directory would affect a declared reference."""
    path, reference = os.path.normpath(path), os.path.normpath(reference)
    if _path_overlap(path, reference):
        return True
    if directory and _path_overlap(os.path.join(path, "*"), reference):
        return True
    # A declaration can also reference a containing directory (or its glob).
    parent = os.path.dirname(path)
    while parent and parent != path:
        if _path_overlap(parent, reference):
            return True
        path, parent = parent, os.path.dirname(parent)
    return False


@dataclass(frozen=True)
class FileConflict:
    """A conflicting pair of declared file accesses, on one filesystem."""

    namespace: str
    first_path: str
    first_access: str
    second_path: str
    second_access: str


def conflicting_files(left: RunRecord, right: RunRecord) -> tuple[FileConflict, ...]:
    """Describe collisions, including conservative wildcard overlaps.

    Paths refer to the declared files and are not fabricated by a filesystem
    probe; wildcard-vs-wildcard overlaps can be conservative.
    """
    def matches(first, second, namespace):
        reads1, writes1 = first
        reads2, writes2 = second
        result = []
        for access1, paths1 in (("write", writes1), ("read", reads1)):
            for access2, paths2 in (("write", writes2), ("read", reads2)):
                if access1 == access2 == "read":
                    continue
                for path1 in sorted(paths1):
                    for path2 in sorted(paths2):
                        if _path_overlap(path1, path2):
                            result.append(FileConflict(
                                namespace, path1, access1, path2, access2,
                            ))
        return result

    collisions = matches(
        _manifest_accesses(left, remote=False),
        _manifest_accesses(right, remote=False),
        "local",
    )
    if _possibly_same_filesystem(left, right):
        collisions.extend(matches(
            _manifest_accesses(left), _manifest_accesses(right),
            "target",
        ))
    return tuple(dict.fromkeys(collisions))


def submission_conflicts(left: RunRecord, right: RunRecord) -> bool:
    """Conservatively exclude overlapping declared read/write operations."""

    def overlaps(first, second):
        left_reads, left_writes = first
        right_reads, right_writes = second
        return _paths_overlap(left_writes, right_reads | right_writes) or _paths_overlap(right_writes, left_reads)

    # Every client also reads/uploads and publishes/streams in its local
    # filesystem, regardless of which backend performs the calculation.
    if overlaps(_manifest_accesses(left, remote=False), _manifest_accesses(right, remote=False)):
        return True

    return _possibly_same_filesystem(left, right) and overlaps(_manifest_accesses(left), _manifest_accesses(right))
