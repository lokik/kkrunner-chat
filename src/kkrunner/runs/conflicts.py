"""Read/write reservations across local and target filesystem namespaces."""

from __future__ import annotations

import os
from dataclasses import dataclass

from kkrunner.targets import LocalTarget, SshTarget
from kkrunner.files.paths import local_path_identities, paths_overlap
from kkrunner.files.manifest import FileSyncPolicy
from kkrunner.runs.records import RunRecord


@dataclass(frozen=True)
class FileReference:
    """An ephemeral filesystem reference, with its declaration semantics."""

    path: str
    pattern: bool = False
    directory: bool = False

    def overlaps(self, other):
        return paths_overlap(
            self.path, other.path, first_pattern=self.pattern, second_pattern=other.pattern,
            first_directory=self.directory, second_directory=other.directory,
        )


def _entry_references(record, entries, *, remote):
    files = record.files
    directory = record.execution.directory

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

    result = set()
    for entry in entries:
        target = path(entry)
        if target is None:
            continue
        identities = (target,) if remote else local_path_identities(target)
        result.update(FileReference(identity, files.is_pattern(entry), entry.kind == "directory")
                      for identity in identities)
    return result


def _manifest_accesses(record, *, remote=None):
    """Resolve declared read/write paths on the execution host."""
    files = record.files
    directory = record.execution.directory
    remote = directory.remote is not None if remote is None else remote

    def accesses(entries):
        return _entry_references(record, entries, remote=remote)

    caches = tuple(cache.file for cache in files.caches)
    outputs = tuple(entry for entry in (files.stdout, files.stderr, *files.outputs)
                    if entry is not None)
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
    return reads, writes


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


def referenced_paths(record, *, remote=None):
    """All declared references and captured local versions, without expanding globs."""
    if remote is None:
        remote = record.execution.directory.remote is not None
    references = _entry_references(record, record.files.entries, remote=remote)
    if not remote:
        for version in record.files.all_versions:
            path = os.path.join(record.execution.directory.local, os.fspath(version.path))
            references.update(FileReference(identity, directory=version.kind == "directory")
                              for identity in local_path_identities(path))
    return references


def _compare_target_filesystems(left, right):
    """Compare target paths only when they add a namespace beyond local paths."""
    if left.execution.directory.remote is None and right.execution.directory.remote is None:
        return False
    first, second = filesystem_namespace(left), filesystem_namespace(right)
    return first == second or ("unknown",) in (first, second)


def file_reference_overlaps(path, reference, *, directory=False):
    """Whether removing an exact file or directory would affect a reference."""
    return FileReference(path, directory=directory).overlaps(reference)


@dataclass(frozen=True)
class FileConflict:
    """A conflicting pair of declared file accesses, on one filesystem."""

    namespace: str
    first_path: str
    first_access: str
    second_path: str
    second_access: str


def _conflicting_accesses(left, right):
    """Yield conflicting accesses lazily; both decisions and reports use this rule."""
    def matches(first, second, namespace):
        reads1, writes1 = first
        reads2, writes2 = second
        for access1, paths1, access2, paths2 in (
            ("write", writes1, "write", writes2),
            ("write", writes1, "read", reads2),
            ("read", reads1, "write", writes2),
        ):
            for ref1 in paths1:
                for ref2 in paths2:
                    if ref1.overlaps(ref2):
                        yield FileConflict(namespace, ref1.path, access1, ref2.path, access2)

    yield from matches(
        _manifest_accesses(left, remote=False),
        _manifest_accesses(right, remote=False),
        "local",
    )
    if _compare_target_filesystems(left, right):
        yield from matches(
            _manifest_accesses(left), _manifest_accesses(right), "target",
        )


def conflicting_files(left: RunRecord, right: RunRecord) -> tuple[FileConflict, ...]:
    """Describe collisions, including conservative wildcard overlaps."""
    return tuple(sorted(set(_conflicting_accesses(left, right)), key=lambda item: (
        item.namespace, item.first_access != "write", item.second_access != "write",
        item.first_path, item.second_path,
    )))


def submission_conflicts(left: RunRecord, right: RunRecord) -> bool:
    """Conservatively exclude overlapping declared read/write operations."""
    return next(_conflicting_accesses(left, right), None) is not None
