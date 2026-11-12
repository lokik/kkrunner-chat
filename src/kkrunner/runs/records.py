"""Validated run identity and its crash-durable, atomic store."""

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import Optional

from kkrunner.files.manifest import FileManifest
from kkrunner.runs.base import FailurePhase, RunState
from kkrunner.runs.metadata import freeze_metadata
from kkrunner.runs.model import ExecutionMetadata
from kkrunner.support.durability import durable_mkdir, durable_write_json
from kkrunner.support.file_locks import FileLock

_RUN_RECORD_SCHEMA_VERSION = 1


class CorruptRunRecord(RuntimeError):
    """A persisted run record cannot be decoded or validated."""

    def __init__(self, path, message):
        self.path = Path(path)
        super().__init__(f"Corrupt run record {self.path}: {message}")


@dataclass(frozen=True)
class RunRecord:
    """Single durable identity, file manifest and lifecycle of one calculation."""

    id: str
    state: RunState
    created_at: str
    updated_at: str
    execution: ExecutionMetadata
    files: FileManifest
    finished_at: Optional[str] = None
    return_code: Optional[int] = None
    message: Optional[str] = None
    failure_phase: Optional[FailurePhase] = None
    schema_version: int = _RUN_RECORD_SCHEMA_VERSION
    project_id: Optional[str] = None
    metadata: dict = field(default_factory=dict)
    processing_required: bool = False
    processing_attempt: Optional[str] = None

    def __post_init__(self):
        if (
            not isinstance(self.id, str)
            or not self.id
            or self.id in (".", "..")
            or any(c in self.id for c in ("/", "\\", "\x00"))
        ):
            raise ValueError("Run id must be a nonempty filename component")
        for name in ("created_at", "updated_at", "finished_at"):
            value = getattr(self, name)
            if value is None and name == "finished_at":
                continue
            if not isinstance(value, str):
                raise TypeError(f"{name} must be an ISO timestamp")
            try:
                datetime.fromisoformat(value)
            except ValueError as exc:
                raise ValueError(f"Invalid {name} timestamp: {value!r}") from exc
        if self.return_code is not None and type(self.return_code) is not int:
            raise TypeError("return_code must be an integer or None")
        for name in ("message", "processing_attempt"):
            if getattr(self, name) is not None and not isinstance(getattr(self, name), str):
                raise TypeError(f"{name} must be a string or None")
        object.__setattr__(self, "metadata", freeze_metadata(self.metadata))
        if type(self.processing_required) is not bool:
            raise TypeError("processing_required must be a bool")
        if self.project_id is not None:
            object.__setattr__(self, "project_id", str(self.project_id))
        if isinstance(self.execution, dict):
            object.__setattr__(self, "execution", ExecutionMetadata(**self.execution))
        if isinstance(self.files, dict):
            object.__setattr__(self, "files", FileManifest(**self.files))
        if not isinstance(self.execution, ExecutionMetadata):
            raise TypeError("execution must be ExecutionMetadata")
        if not isinstance(self.files, FileManifest):
            raise TypeError("files must describe a FileManifest")
        if not isinstance(self.state, RunState):
            object.__setattr__(self, "state", RunState(self.state))
        if self.failure_phase is not None and not isinstance(self.failure_phase, FailurePhase):
            object.__setattr__(self, "failure_phase", FailurePhase(self.failure_phase))
        if type(self.schema_version) is not int or self.schema_version != _RUN_RECORD_SCHEMA_VERSION:
            raise ValueError(f"unsupported schema version {self.schema_version}; expected {_RUN_RECORD_SCHEMA_VERSION}")


class RunStore:
    """Run history stored as date-sharded durable JSON files."""

    def __init__(self, directory):
        """Store runs in an explicit directory, with optional project membership per record."""
        self.directory = Path(directory).expanduser()
        self._lock = RLock()

    @staticmethod
    def _bucket(created_at):
        try:
            return datetime.fromisoformat(created_at).date().isoformat()
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid run creation timestamp: {created_at!r}") from exc

    def _path(self, record):
        return self.directory / self._bucket(record.created_at) / f"{record.id}.json"

    def _run_lock_path(self, record):
        return self.directory / ".locks" / f"{record.id}.lock"

    def _path_for_id(self, run_id):
        if (
            not isinstance(run_id, str)
            or not run_id
            or run_id in (".", "..")
            or any(c in run_id for c in ("/", "\\", "\x00"))
        ):
            raise ValueError("Run id must be a nonempty filename component")
        filename = f"{run_id}.json"
        paths = tuple(day / filename for day in self._day_directories(newest=True) if (day / filename).is_file())
        if not paths:
            raise FileNotFoundError(run_id)
        if len(paths) != 1:
            raise CorruptRunRecord(paths[0], "duplicate run identity across date shards")
        return paths[0]

    def processing_lock(self, record):
        """Exclusive live processing ownership, released by the OS after death."""
        return FileLock(self._run_lock_path(record).with_suffix(".processing.lock"))

    @staticmethod
    def _decode(path):
        try:
            with path.open() as file:
                data = json.load(file)
            if not isinstance(data, dict):
                raise TypeError("Run record must be a JSON object")
            version = data.get("schema_version")
            if version != _RUN_RECORD_SCHEMA_VERSION:
                raise ValueError(f"unsupported schema version {version!r}; expected {_RUN_RECORD_SCHEMA_VERSION}")
            record = RunRecord(**data)
            if record.id != path.stem or RunStore._bucket(record.created_at) != path.parent.name:
                raise ValueError("Run identity does not match its persistent location")
            return record
        except CorruptRunRecord:
            raise
        except (json.JSONDecodeError, UnicodeDecodeError, TypeError, ValueError, KeyError) as exc:
            raise CorruptRunRecord(path, str(exc)) from exc

    @staticmethod
    def _json_default(value):
        if isinstance(value, os.PathLike):
            return os.fspath(value)
        raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")

    def _write_unlocked(self, record):
        durable_mkdir(self.directory, parents=True, exist_ok=True, mode=0o700)
        path = self._path(record)
        durable_mkdir(path.parent, parents=True, exist_ok=True, mode=0o700)
        durable_write_json(
            path,
            asdict(record),
            mode=0o600,
            sort_keys=True,
            default=self._json_default,
        )
        return record

    @contextmanager
    def submission_lock(self):
        durable_mkdir(self.directory, parents=True, exist_ok=True, mode=0o700)
        with self._lock, FileLock(self.directory / ".submit.lock"):
            yield

    def save(self, record: RunRecord):
        """Create a record, allowing only an identical idempotent repeat.

        Existing records can change only through update(), under their global
        per-ID lock. An ID is unique across all date shards of this store.
        """
        with self._lock, FileLock(self._run_lock_path(record)):
            try:
                latest = self.get(record.id)
            except FileNotFoundError:
                return self._write_unlocked(record)
            if latest != record:
                raise ValueError("Run already exists; update cannot replace identity or metadata")
            return latest

    def update(self, record: RunRecord, updater):
        with self._lock, FileLock(self._run_lock_path(record)):
            try:
                latest = self.get(record.id)
            except FileNotFoundError:
                latest = record
            if latest.created_at != record.created_at:
                raise ValueError("Run id already belongs to a different persistent identity")
            updated = updater(latest)
            if (
                updated.id != latest.id
                or updated.created_at != latest.created_at
                or updated.metadata != latest.metadata
                or updated.processing_required != latest.processing_required
                or updated.project_id != latest.project_id
            ):
                raise ValueError("Run update cannot change persistent identity or metadata")
            if updated.execution.submission_key != latest.execution.submission_key:
                raise ValueError("Submission key cannot change")
            if latest.execution.backend_id is not None and updated.execution.backend_id != latest.execution.backend_id:
                raise ValueError("Backend identity cannot change")
            return self._write_unlocked(updated)

    def get(self, run_id):
        return self._decode(self._path_for_id(run_id))

    def _day_directories(self, *, newest):
        if not self.directory.is_dir():
            return ()
        days = [path for path in self.directory.iterdir() if path.is_dir()]
        return tuple(sorted(days, key=lambda path: path.name, reverse=newest))

    def _records_in_directory(self, directory, *, newest):
        records = []
        for path in directory.glob("*.json"):
            try:
                records.append(self._decode(path))
            except CorruptRunRecord:
                raise
        return sorted(records, key=lambda record: record.created_at, reverse=newest)

    def runs(self, *, active=None, limit=None, newest=False):
        if limit is not None and limit < 0:
            raise ValueError("limit must be non-negative")
        if limit == 0:
            return ()

        with self._lock:
            records = []
            for day in self._day_directories(newest=newest):
                for record in self._records_in_directory(day, newest=newest):
                    if active is True and record.state.is_terminal:
                        continue
                    if active is False and not record.state.is_terminal:
                        continue
                    records.append(record)
                    if limit is not None and len(records) >= limit:
                        return tuple(records)
            return tuple(records)
