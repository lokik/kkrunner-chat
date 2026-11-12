"""Read/write reservations across local and target filesystem namespaces."""

from __future__ import annotations

import fnmatch
import os

from kkrunner.client.transports import LocalTransport, SshTransport
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
        return os.path.normpath(value if os.path.isabs(value) else os.path.join(directory.target, value))

    reads = {path(entry) for entry in (*files.inputs, files.stdin)}
    reads.update(path(cache.file) for cache in files.caches)
    writes = {path(entry) for entry in (files.stdout, files.stderr, *files.outputs)}
    writes.update(path(cache.file) for cache in files.caches)
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

    def namespace(record):
        transport = record.execution.transport
        if isinstance(transport, LocalTransport):
            return "local"
        mapping = record.files.mapping
        if mapping is not None and mapping.filesystem_id is not None:
            return ("filesystem", mapping.filesystem_id)
        if isinstance(transport, SshTransport):
            # Connection flags, commands and scheduler choices do not partition
            # the host's filesystem. Host aliases/shared mounts use filesystem_id.
            return ("ssh", str(transport.host).rsplit("@", 1)[-1].lower())
        return "local"

    return namespace(left) == namespace(right) and overlaps(_manifest_accesses(left), _manifest_accesses(right))
