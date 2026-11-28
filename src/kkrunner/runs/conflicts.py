"""Read/write reservations across local and target filesystem namespaces."""

from __future__ import annotations

import os
from dataclasses import dataclass

from kkrunner.targets import LocalTarget, SshTarget
from kkrunner.execution import RunFileState
from kkrunner.files.paths import declaration_paths, paths_overlap
from kkrunner.files.manifest import FileSyncPolicy
from kkrunner.runs.records import RunRecord


def _manifest_accesses(record, *, remote=None, collecting=False):
    """Resolve producer or collection accesses in the requested filesystem."""
    files = record.files
    directory = record.execution.directory
    remote = directory.remote is not None if remote is None else remote

    def path(entry):
        if entry is None:
            return None
        value = entry.remote_path if remote else entry.local_path
        if remote and value is None and entry.local_path is not None and files.mapping is None:
            # Unmapped legacy declarations are relative to the working
            # directory. A resolved mapping is explicit: a missing side
            # means that side is not accessed (e.g. historical retrieval).
            local = entry.local_path
            if not os.path.isabs(local):
                local = os.path.join(directory.local, local)
            relative = os.path.relpath(local, directory.local)
            return os.path.normpath(os.path.join(directory.remote, relative))
        if value is None:
            return None
        return os.path.normpath(value if os.path.isabs(value) else os.path.join(directory.remote if remote else directory.local, value))

    def accesses(entries):
        result = set()
        for entry in entries:
            if entry is None:
                continue
            target = path(entry)
            if target is None:
                continue
            # Include the spelling and canonical local identity. An alias
            # may be replaced as a directory entry or opened through its target.
            identities = (target,) if remote else (target, os.path.realpath(target))
            for identity in identities:
                result.update(declaration_paths(identity, directory=entry.kind == "directory"))
        return result

    caches = tuple(cache.file for cache in files.caches)
    outputs = tuple(entry for entry in (files.stdout, files.stderr, *files.outputs)
                    if entry is not None)
    if collecting:
        # Collection only reads the producer's published artifacts. The
        # client may still need local inputs for subsequent interpretation.
        artifacts = (*(entry for entry in outputs if entry.collect), *caches)
        separate = directory.remote is not None and not files.shared_filesystem
        if remote and separate:
            return accesses(artifacts), set()
        reads = accesses(files.input_entries)
        if separate and (files.mapping is None or files.mapping.output_sync != FileSyncPolicy.NEVER):
            return reads, accesses(artifacts)
        return reads | accesses(artifacts), set()

    reads = accesses((*files.input_entries, *caches))
    if (not remote and directory.remote is not None and files.mapping is not None
            and not files.shared_filesystem):
        # Only collected artifacts can write a separate client filesystem.
        writes = accesses((*(entry for entry in outputs if entry.collect), *caches))
        if files.mapping.output_sync == FileSyncPolicy.NEVER:
            reads |= writes
            writes = set()
    else:
        # A local or shared producer writes the local paths themselves.
        writes = accesses((*outputs, *caches))
    if not remote:
        writes |= accesses(entry for entry in (*files.input_entries, *outputs, *caches)
                           if entry.write)
    elif files.mapping is not None and files.mapping.input_sync != FileSyncPolicy.NEVER:
        # Uploads may replace target inputs even when the local source is read-only.
        writes |= accesses(files.input_entries)
    return reads - {None}, writes - {None}


def _reservation_accesses(record, *, remote=None):
    """Reserve the accesses of the current file phase, not historical references."""
    files = record.files
    if record.file_state == RunFileState.COLLECTING:
        return _manifest_accesses(record, remote=remote, collecting=True)
    if (record.file_state not in (RunFileState.READY, RunFileState.POSTPROCESSING)
            or files.captured_at is None):
        return _manifest_accesses(record, remote=remote)
    remote = record.execution.directory.remote is not None if remote is None else remote
    if remote:
        if not files.shared_filesystem:
            return set(), set()
        # Shared files may be accessed through a different local mount by
        # another client. Retain the declared target aliases conservatively.
        reads, writes = _manifest_accesses(record, remote=True)
        return reads | writes, set()

    reads = set()
    for version in (*files.input_versions, *files.output_versions, *files.cache_versions):
        path = os.fspath(version.path)
        for identity in (path, os.path.realpath(path)):
            reads.update(declaration_paths(identity, directory=version.kind == "directory"))
    return reads, set()


def _paths_overlap(first, second):
    return any(paths_overlap(a, b) for a in first for b in second)


def filesystem_namespace(record):
    """Identity of the execution filesystem, shared by reservations and cleanup."""
    transport = record.execution.target
    if isinstance(transport, LocalTarget):
        return ("local",)
    mapping = record.files.mapping
    if mapping is not None and mapping.filesystem_id is not None:
        return ("filesystem", mapping.filesystem_id)
    if isinstance(transport, SshTarget):
        return ("ssh", str(transport.host).rsplit("@", 1)[-1].lower())
    # An unsupported transport must not be mistaken for the local filesystem.
    return ("unknown",)


def referenced_paths(record):
    """All declared reads and writes on the execution filesystem, including globs."""
    reads, writes = _manifest_accesses(record)
    return reads | writes


def _compare_target_filesystems(left, right):
    """Compare target paths only when they add a namespace beyond local paths."""
    if left.execution.directory.remote is None and right.execution.directory.remote is None:
        return False
    first, second = filesystem_namespace(left), filesystem_namespace(right)
    return first == second or ("unknown",) in (first, second)


def file_reference_overlaps(path, reference, *, directory=False):
    """Whether removing a file/directory would affect a declared reference."""
    path, reference = os.path.normpath(path), os.path.normpath(reference)
    if paths_overlap(path, reference):
        return True
    if directory and paths_overlap(os.path.join(path, "**"), reference):
        return True
    # A declaration can also reference a containing directory (or its glob).
    parent = os.path.dirname(path)
    while parent and parent != path:
        if paths_overlap(parent, reference):
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
                        if paths_overlap(path1, path2):
                            result.append(FileConflict(
                                namespace, path1, access1, path2, access2,
                            ))
        return result

    collisions = matches(
        _reservation_accesses(left, remote=False),
        _reservation_accesses(right, remote=False),
        "local",
    )
    if _compare_target_filesystems(left, right):
        collisions.extend(matches(
            _reservation_accesses(left), _reservation_accesses(right),
            "target",
        ))
    return tuple(dict.fromkeys(collisions))


def submission_conflicts(left: RunRecord, right: RunRecord) -> bool:
    """Conservatively exclude overlapping declared read/write operations."""

    def overlaps(first, second):
        left_reads, left_writes = first
        right_reads, right_writes = second
        return _paths_overlap(left_writes, right_reads | right_writes) or _paths_overlap(right_writes, left_reads)

    # Shared target files are also represented by their mapped local paths.
    if overlaps(_reservation_accesses(left, remote=False), _reservation_accesses(right, remote=False)):
        return True

    return _compare_target_filesystems(left, right) and overlaps(_reservation_accesses(left), _reservation_accesses(right))
