"""File declarations, transfer policy and captured artifact availability."""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
from typing import Optional

from kkrunner.files.versions import FileState, FileVersion


class ArtifactAvailabilityState(str, Enum):
    """Availability of a declared local artifact."""

    PENDING = "pending"
    AVAILABLE = "available"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class FileEntry:
    """Declared filesystem entry; legacy records default to a regular file."""

    local_path: Optional[str] = None
    remote_path: Optional[str] = None
    availability_state: Optional[str] = None
    availability_message: Optional[str] = None
    source_pattern: Optional[str] = None
    optional: bool = False
    kind: str = "file"
    collect: bool = True
    write: bool = False

    def __post_init__(self):
        import os

        if self.source_pattern is not None:
            object.__setattr__(self, "source_pattern", os.fspath(self.source_pattern))

        for name in ("local_path", "remote_path"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, os.fspath(value))
        if type(self.collect) is not bool:
            raise TypeError("FileEntry.collect must be a bool")
        if type(self.optional) is not bool:
            raise TypeError("FileEntry.optional must be a bool")
        if type(self.write) is not bool:
            raise TypeError("FileEntry.write must be a bool")
        if self.kind not in ("file", "directory"):
            raise ValueError("FileEntry kind must be file or directory")
        if self.local_path is None and self.remote_path is None:
            raise ValueError("A file entry must have at least one location")
        if self.availability_state is not None:
            try:
                availability = ArtifactAvailabilityState(self.availability_state)
            except ValueError as exc:
                raise ValueError("Invalid per-file availability state") from exc
            # Persist JSON-compatible strings, not implementation-specific enums.
            object.__setattr__(self, "availability_state", availability.value)
        if self.availability_message is not None and self.availability_state != ArtifactAvailabilityState.FAILED:
            raise ValueError("An availability error belongs to a failed file")


@dataclass(frozen=True)
class CacheEntry:
    """Cache identity and its pre-execution remote signature."""

    file: FileEntry
    before: Optional[tuple] = None

    def __post_init__(self):
        if isinstance(self.file, dict):
            object.__setattr__(self, "file", FileEntry(**self.file))
        if not isinstance(self.file, FileEntry):
            raise TypeError("CacheEntry.file must be a FileEntry")
        if self.before is not None:
            object.__setattr__(self, "before", tuple(self.before))


class FileSyncPolicy(str, Enum):
    """Policy controlling transfers between local and remote workspaces."""

    CHANGED = "changed"
    ALWAYS = "always"
    MISSING = "missing"
    NEVER = "never"

    def should_transfer(self, source_digest, destination_digest):
        """Apply the direction-independent transfer rule to file digests.

        The caller handles missing mandatory files separately; a missing
        source is never transferable, including with ALWAYS.
        """
        if self == FileSyncPolicy.NEVER or source_digest is None:
            return False
        if self == FileSyncPolicy.ALWAYS:
            return True
        if self == FileSyncPolicy.MISSING:
            return destination_digest is None
        return source_digest != destination_digest


@dataclass(frozen=True)
class FileMapping:
    """Resolved mapping and transfer policy of this run's file manifest."""

    local_root: str
    remote_root: str
    input_sync: str = FileSyncPolicy.CHANGED.value
    output_sync: str = FileSyncPolicy.CHANGED.value
    filesystem_id: Optional[str] = None
    shared_filesystem: bool = False

    def __post_init__(self):
        import os

        if type(self.shared_filesystem) is not bool:
            raise TypeError("shared_filesystem must be a bool")
        if self.filesystem_id is not None and (not isinstance(self.filesystem_id, str) or not self.filesystem_id):
            raise ValueError("filesystem_id must be a nonempty string or None")
        object.__setattr__(self, "local_root", os.fspath(self.local_root))
        object.__setattr__(self, "remote_root", os.fspath(self.remote_root))
        try:
            input_sync = FileSyncPolicy(self.input_sync)
            output_sync = FileSyncPolicy(self.output_sync)
        except ValueError as exc:
            raise ValueError("Invalid file synchronization policy") from exc
        object.__setattr__(self, "input_sync", (FileSyncPolicy.NEVER if self.shared_filesystem else input_sync).value)
        object.__setattr__(self, "output_sync", (FileSyncPolicy.NEVER if self.shared_filesystem else output_sync).value)


@dataclass(frozen=True)
class FileManifest:
    """All paths and cache baselines required for transfer and reconnect."""

    inputs: tuple[FileEntry, ...] = ()
    outputs: tuple[FileEntry, ...] = ()
    caches: tuple[CacheEntry, ...] = ()
    stdout: Optional[FileEntry] = None
    stderr: Optional[FileEntry] = None
    mapping: Optional[FileMapping] = None
    input_versions: tuple[FileVersion, ...] = ()
    output_versions: tuple[FileVersion, ...] = ()
    captured_at: Optional[str] = None
    stdin: Optional[FileEntry] = None
    cache_versions: tuple[FileVersion, ...] = ()

    @classmethod
    def from_spec(cls, spec):
        """Build the local file manifest for an execution specification."""
        import os

        from kkrunner.files.paths import file_path
        from kkrunner.files.task_file import TaskFile

        directory = os.path.abspath(os.fspath(spec.directory))

        def declared_entry(value, *, required=False):
            path = file_path(value, directory, required=required)
            if path is None:
                return None
            if isinstance(value, TaskFile):
                return FileEntry(local_path=path, optional=value.optional,
                                 kind=value.kind, collect=value.collect, write=value.write)
            from io import IOBase
            writable = not isinstance(value, (str, bytes, os.PathLike))
            if isinstance(value, IOBase):
                writable = value.writable()
            return FileEntry(local_path=path, write=writable)

        primary = declared_entry(spec.input_file)
        if primary is not None and primary.optional:
            raise ValueError(f"Primary input {primary.local_path} cannot be declared optional")

        inputs = {}
        for entry in (primary, *(declared_entry(value, required=True) for value in spec.input_files)):
            if entry is None:
                continue
            if not entry.collect:
                raise ValueError("collect=False is only supported for output_files")
            path = entry.local_path
            previous = inputs.get(path)
            if previous is not None:
                if previous.optional != entry.optional:
                    raise ValueError(f"Conflicting optionality for file {path}: the same input cannot be both required and optional")
                if previous.kind != entry.kind:
                    raise ValueError(f"Conflicting file kinds for input {path}")
                entry = replace(entry, write=previous.write or entry.write)
            inputs[path] = entry

        for value in (spec.output_file, spec.error_file):
            if isinstance(value, TaskFile) and not value.collect:
                raise ValueError("collect=False is not supported for output streams")
        stdout_entry = declared_entry(spec.output_file)
        stderr_entry = declared_entry(spec.error_file)
        stdout = None if stdout_entry is None else stdout_entry.local_path
        stderr = None if stderr_entry is None else stderr_entry.local_path

        outputs = {}
        for value in spec.output_files:
            entry = declared_entry(value, required=True)
            if entry is None or entry.local_path in {stdout, stderr}:
                continue
            path = entry.local_path
            if path in outputs:
                raise ValueError(f"Duplicate file declaration: output {path}")
            outputs[path] = entry

        stdin = file_path(spec.stdin, directory)
        read_paths = inputs.keys() | ({stdin} if stdin is not None else set())
        write_paths = outputs.keys() | ({p for p in (stdout, stderr) if p is not None})
        if read_paths & write_paths:
            raise ValueError("Duplicate file declaration: input/output collision")

        caches = {}
        for value in spec.cache_files:
            entry = declared_entry(value, required=True)
            if entry is None:
                continue
            if not entry.collect:
                raise ValueError("collect=False is only supported for output_files")
            path = entry.local_path
            if path in caches:
                raise ValueError(f"Duplicate file declaration: cache {path}")
            if path in read_paths or path in write_paths:
                raise ValueError(f"Duplicate file declaration: cache collision {path}")
            caches[path] = CacheEntry(entry)
        return cls(
            inputs=tuple(inputs.values()),
            outputs=tuple(outputs.values()),
            caches=tuple(caches.values()),
            stdin=declared_entry(spec.stdin),
            stdout=None if stdout is None else FileEntry(local_path=stdout, write=stdout_entry.write),
            stderr=None if stderr is None else FileEntry(local_path=stderr, write=stderr_entry.write),
        ).initialize_availability()

    def __post_init__(self):
        for name, cls in (("inputs", FileEntry), ("outputs", FileEntry), ("caches", CacheEntry)):
            items = getattr(self, name)
            object.__setattr__(
                self,
                name,
                tuple(item if isinstance(item, cls) else cls(**item) for item in items),
            )
        for name in ("stdout", "stderr", "stdin"):
            item = getattr(self, name)
            if isinstance(item, dict):
                object.__setattr__(self, name, FileEntry(**item))
            elif item is not None and not isinstance(item, FileEntry):
                raise TypeError(f"FileManifest.{name} must be a FileEntry or None")
        if isinstance(self.mapping, dict):
            object.__setattr__(self, "mapping", FileMapping(**self.mapping))
        elif self.mapping is not None and not isinstance(self.mapping, FileMapping):
            raise TypeError("FileManifest.mapping must be a FileMapping or None")
        for name in ("input_versions", "output_versions", "cache_versions"):
            object.__setattr__(
                self,
                name,
                tuple(
                    version if isinstance(version, FileVersion) else FileVersion(**version)
                    for version in getattr(self, name)
                ),
            )

    @property
    def shared_filesystem(self):
        return self.mapping is not None and self.mapping.shared_filesystem

    @property
    def artifacts(self):
        """Output streams, declared outputs and caches (one status per local file)."""
        entries = (self.stdout, self.stderr, *(x for x in self.outputs if x.collect),
                   *(x.file for x in self.caches))
        unique = {}
        for entry in entries:
            if entry is not None and entry.local_path is not None:
                unique[entry.local_path] = entry
        return tuple(unique.values())

    @property
    def failed_artifacts(self):
        return tuple(x for x in self.artifacts if x.availability_state == "failed")

    @property
    def availability_state(self):
        """Derived summary for callers; never stored independently."""
        statuses = tuple(x.availability_state for x in self.artifacts)
        if "failed" in statuses:
            return "failed"
        if not statuses or None in statuses or "pending" in statuses:
            return "pending"
        if all(x == "skipped" for x in statuses):
            return "skipped"
        return "available"

    @property
    def availability_message(self):
        failures = self.failed_artifacts
        return None if not failures else failures[0].availability_message

    def with_remote_stream_paths(self, *, stdout=None, stderr=None):
        """Update recovered stream locations without changing artifact identity.

        Missing or empty recovered paths do not replace persisted locations.
        A stream that was not recorded remains absent.
        """
        changes = {}
        for name, remote in (("stdout", stdout), ("stderr", stderr)):
            current = getattr(self, name)
            if remote and current is not None:
                changes[name] = replace(current, remote_path=remote)
        return replace(self, **changes) if changes else self

    def initialize_availability(self):
        """Mark declared downloadable files pending without marking inputs."""
        return self._map_artifacts(
            lambda item: replace(item, availability_state="pending") if item.availability_state is None else item
        )

    def fail_pending_artifacts(self, message):
        """Mark unresolved artifacts failed without retracting published files."""
        return self._map_artifacts(
            lambda item: replace(item, availability_state="failed", availability_message=message)
            if item.availability_state in (None, "pending") else item
        )

    def _map_artifacts(self, transform):
        return replace(
            self,
            stdout=None if self.stdout is None else transform(self.stdout),
            stderr=None if self.stderr is None else transform(self.stderr),
            outputs=tuple(transform(x) if x.collect else x for x in self.outputs),
            caches=tuple(replace(x, file=transform(x.file)) for x in self.caches),
        )

    def with_file_availability(
        self,
        local_path,
        state,
        message=None,
        *,
        remote_path=None,
        kind="output",
        source_pattern=None,
    ):
        """Update one artifact, adding each wildcard match as it is discovered.

        The pattern declaration retains its own independent status. A child's
        source_pattern identifies the declaration, including after restart.
        Duplicate stream/output declarations of one path are updated together.
        """
        import os

        path = os.fspath(local_path)
        matches = any(x.local_path == path for x in self.artifacts)
        if matches:
            return self._map_artifacts(
                lambda item: (
                    replace(
                        item,
                        availability_state=state,
                        availability_message=message,
                    )
                    if item.local_path == path
                    else item
                )
            )
        file = FileEntry(
            local_path=path,
            remote_path=remote_path,
            availability_state=state,
            availability_message=message,
            source_pattern=source_pattern,
        )
        if kind == "output":
            return replace(self, outputs=(*self.outputs, file))
        if kind == "cache":
            return replace(self, caches=(*self.caches, CacheEntry(file)))
        raise ValueError(f"Unknown artifact kind: {kind!r}")

    @property
    def input_file(self):
        return self.inputs[0].local_path if self.inputs else None

    @property
    def output_file(self):
        return self.stdout.local_path if self.stdout is not None else None

    @property
    def input_entries(self):
        return (*self.inputs, *((self.stdin,) if self.stdin is not None else ()))

    @property
    def input_files(self):
        return tuple(dict.fromkeys(entry.local_path for entry in self.input_entries if entry.local_path is not None))

    def input_version(self, path):
        """Return the version observed before launch for a local manifest path."""
        from pathlib import Path

        normalized = Path(path).expanduser().absolute()
        return next((item for item in self.input_versions if item.path == normalized), None)

    @property
    def output_files(self):
        streams = (self.stdout, self.stderr)
        entries = tuple(entry for entry in streams if entry is not None)
        return tuple(
            dict.fromkeys(entry.local_path for entry in (*entries, *(e for e in self.outputs if e.collect))
                          if entry.local_path is not None)
        )

    @property
    def manifests(self):
        return tuple(dict.fromkeys((*self.input_files, *self.output_files)))

    @property
    def all_versions(self):
        by_path = {str(item.path): item for item in (
            *self.cache_versions, *self.input_versions, *self.output_versions,
        )}
        return tuple(by_path[key] for key in sorted(by_path))

    @property
    def state(self):
        if self.captured_at is None:
            return FileState.CURRENT
        states = tuple(version.state() for version in self.output_versions)
        if all(value is FileState.CURRENT for value in states):
            return FileState.CURRENT
        if FileState.OVERWRITTEN in states:
            return FileState.OVERWRITTEN
        if FileState.UNAVAILABLE in states:
            return FileState.UNAVAILABLE
        return FileState.MISSING
