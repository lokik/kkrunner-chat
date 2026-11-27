"""Validated run identity and its crash-durable, atomic store."""

from __future__ import annotations

import json
import os
import re
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from kkrunner.runs.stores import RunStoreProtocol

from kkrunner.files.manifest import FileManifest
from kkrunner.runs.base import FailurePhase, RunState
from kkrunner.runs.metadata import freeze_metadata
from kkrunner.runs.model import ExecutionMetadata
from kkrunner.support.durability import (
    durable_mkdir, durable_replace, durable_write_json, durable_write_text, fsync_directory,
)
from kkrunner.support.file_locks import FileLock

_RUN_RECORD_SCHEMA_VERSION = 1


class ActiveRunDeletionError(RuntimeError):
    """Removing active history requires an explicit dangerous override."""


class ForgottenRunError(RuntimeError):
    """An old observer cannot recreate deliberately removed history."""


class CorruptRunRecord(RuntimeError):
    """A persisted run record cannot be decoded or validated."""

    def __init__(self, path, message):
        self.path = Path(path)
        super().__init__(f"Corrupt run record {self.path}: {message}")


def validate_run_id(run_id):
    if (
        not isinstance(run_id, str)
        or not run_id
        or run_id in (".", "..")
        or any(c in run_id for c in ("/", "\\", "\x00"))
    ):
        raise ValueError("Run id must be a nonempty filename component")

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
    # Native execution lifetime, independent of the store and observation.
    detached: bool = False
    # Runtime owner, not part of record identity or serialized data.
    store: Optional[RunStoreProtocol] = field(default=None, compare=False, repr=False)

    def __post_init__(self):
        validate_run_id(self.id)
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
        if self.message is not None and not isinstance(self.message, str):
            raise TypeError("message must be a string or None")
        object.__setattr__(self, "metadata", freeze_metadata(self.metadata))
        if type(self.detached) is not bool:
            raise TypeError("detached must be a bool")
        if type(self.processing_required) is not bool:
            raise TypeError("processing_required must be a bool")
        if self.project_id is not None:
            object.__setattr__(self, "project_id", str(self.project_id))
        if isinstance(self.execution, dict):
            execution = dict(self.execution)
            # The version-1 JSON field is independent of the Python attribute name.
            if "transport" in execution:
                if "target" in execution:
                    raise ValueError("Duplicate execution target declaration")
                execution["target"] = execution.pop("transport")
            object.__setattr__(self, "execution", ExecutionMetadata(**execution))
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


    @property
    def key(self):
        """Process-local identity, including the owning storage namespace."""
        return (None if self.store is None else self.store.lock_key, self.id)

    def to_dict(self):
        """Serialize the record without its runtime store reference."""
        data = asdict(
            replace(self, store=None),
            dict_factory=lambda items: {
                key: os.fspath(value) if isinstance(value, os.PathLike) else value
                for key, value in items
            },
        )
        del data["store"]
        data["execution"].pop("target")
        data["execution"]["transport"] = self.execution.target.to_dict()
        return data


def validate_update(latest, updated):
    """Enforce the same immutable identity rules in every store."""
    if (
        updated.id != latest.id
        or updated.created_at != latest.created_at
        or updated.metadata != latest.metadata
        or updated.processing_required != latest.processing_required
        or updated.project_id != latest.project_id
        or updated.detached != latest.detached
    ):
        raise ValueError("Run update cannot change persistent identity or metadata")
    if updated.execution.submission_key != latest.execution.submission_key:
        raise ValueError("Submission key cannot change")
    if latest.execution.backend_id is not None and updated.execution.backend_id != latest.execution.backend_id:
        raise ValueError("Backend identity cannot change")


class RunStore:
    """Active records and date-sharded terminal history, with one file per run."""

    durable = True

    def __init__(self, directory=None):
        """Store runs in an explicit directory or the default user state directory."""
        if directory is None:
            root = os.environ.get("XDG_STATE_HOME") or (Path.home() / ".local" / "state")
            directory = Path(root) / "kkrunner" / "runs"
        self.directory = Path(directory).expanduser().resolve()
        self._lock = RLock()

    @property
    def lock_key(self):
        # Distinct objects for the same directory share a submission lock.
        return (0, str(self.directory))

    @staticmethod
    def _bucket(created_at):
        try:
            return datetime.fromisoformat(created_at).date().isoformat()
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid run creation timestamp: {created_at!r}") from exc

    @property
    def _active_directory(self):
        return self.directory / "active"

    @property
    def _layout_marker(self):
        return self.directory / ".active-layout"

    def _path(self, record):
        directory = (self.directory / self._bucket(record.created_at)
                     if record.state.is_terminal else self._active_directory)
        return directory / f"{record.id}.json"

    def _move_record(self, source, destination):
        durable_mkdir(destination.parent, parents=True, exist_ok=True, mode=0o700)
        durable_replace(source, destination)

    def _migrate_layout(self):
        """Under the submission lock, migrate the old date-only layout once.

        No record contents change. Interrupted moves are safe to resume; only
        publish the marker after every nonterminal record is in active/.
        """
        if self._layout_marker.is_file():
            return
        for day in self._day_directories(newest=False):
            for path in day.glob("*.json"):
                if self._forgotten_path(path.stem).is_file():
                    continue
                with FileLock(self._run_lock_path(path.stem)):
                    record = self._decode(path)
                    if record.state.is_terminal:
                        continue
                    destination = self._path(record)
                    if destination.exists():
                        raise CorruptRunRecord(path, "duplicate run identity in active and history")
                    self._move_record(path, destination)
            # Also flush moves interrupted before their final directory barrier.
            fsync_directory(day)
        if self._active_directory.is_dir():
            fsync_directory(self._active_directory)
        durable_write_text(self._layout_marker, "1\n", mode=0o600)

    def _ensure_layout(self):
        if not self._layout_marker.is_file():
            with self.submission_lock():
                pass

    def _run_lock_path(self, run_id):
        validate_run_id(run_id)
        return self.directory / ".locks" / f"{run_id}.lock"

    def _forgotten_path(self, run_id):
        return self.directory / ".forgotten" / f"{run_id}.json"

    def _assert_not_forgotten(self, run_id):
        if self._forgotten_path(run_id).exists():
            raise ForgottenRunError(f"Run {run_id!r} was deliberately removed")

    def _path_for_id(self, run_id):
        validate_run_id(run_id)
        if self._forgotten_path(run_id).is_file():
            raise FileNotFoundError(run_id)
        filename = f"{run_id}.json"
        paths = tuple(folder / filename
                      for folder in (self._active_directory, *self._day_directories(newest=True))
                      if (folder / filename).is_file())
        if not paths:
            raise FileNotFoundError(run_id)
        if len(paths) != 1:
            raise CorruptRunRecord(paths[0], "duplicate run identity across date shards")
        return paths[0]

    def observation_lock(self, record):
        """Own client file access from reservation through final resource cleanup.

        After client death the operating system releases the lock, permitting
        independent recovery. This is not a native execution lock.
        """
        return FileLock(self._run_lock_path(record.id).with_suffix(".observation.lock"))

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
            # Earlier records stored a redundant client processing token.
            # It has no bearing on the durable execution or artifact identity.
            data.pop("processing_attempt", None)
            record = RunRecord(**data)
            if record.id != path.stem or (
                path.parent.name != "active"
                and RunStore._bucket(record.created_at) != path.parent.name
            ):
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

    def _finish_move(self, record, path):
        destination = self._path(record)
        if path != destination:
            self._move_record(path, destination)

    def _write_unlocked(self, record, *, previous_path=None):
        destination = self._path(record)
        path = destination
        if previous_path is not None and previous_path != destination:
            if record.state.is_terminal:
                # Publish terminal state before removing the active entry.
                # A crash can leave a terminal record in active/, never an
                # active reservation hidden in the dated history.
                path = previous_path
            else:
                # Move the old terminal record before publishing active state.
                self._move_record(previous_path, destination)
        durable_mkdir(path.parent, parents=True, exist_ok=True, mode=0o700)
        record = replace(record, store=self)
        durable_write_json(
            path,
            record.to_dict(),
            mode=0o600,
            sort_keys=True,
            default=self._json_default,
        )
        self._finish_move(record, path)
        return record

    @contextmanager
    def submission_lock(self):
        durable_mkdir(self.directory, parents=True, exist_ok=True, mode=0o700)
        with self._lock, FileLock(self.directory / ".submit.lock"):
            self._migrate_layout()
            yield

    def save(self, record: RunRecord):
        """Create a record, allowing only an identical idempotent repeat.

        Existing records can change only through update(), under their global
        per-ID lock. An ID is unique across all date shards of this store.
        """
        self._ensure_layout()
        with self._lock, FileLock(self._run_lock_path(record.id)):
            self._assert_not_forgotten(record.id)
            try:
                path = self._path_for_id(record.id)
            except FileNotFoundError:
                return self._write_unlocked(record)
            latest = replace(self._decode(path), store=self)
            if latest != record:
                raise ValueError("Run already exists; update cannot replace identity or metadata")
            self._finish_move(latest, path)
            return latest

    def update(self, record: RunRecord, updater):
        self._ensure_layout()
        with self._lock, FileLock(self._run_lock_path(record.id)):
            self._assert_not_forgotten(record.id)
            try:
                previous_path = self._path_for_id(record.id)
            except FileNotFoundError:
                if record.state != RunState.NEW:
                    raise
                latest = replace(record, store=self)
                previous_path = None
            else:
                latest = replace(self._decode(previous_path), store=self)
            if latest.created_at != record.created_at:
                raise ValueError("Run id already belongs to a different persistent identity")
            updated = updater(latest)
            validate_update(latest, updated)
            # Returning the authoritative record is an explicit no-op, not
            # a request to repeat its durable publication.
            if previous_path is not None and updated is latest:
                self._finish_move(latest, previous_path)
                return latest
            return self._write_unlocked(updated, previous_path=previous_path)

    def reassign_project(
        self, run_id: str, project_id: str | None, *, expected_project_id: str | None,
    ) -> RunRecord:
        """Explicitly change a run's project without touching execution or files.

        Ordinary lifecycle updates intentionally cannot modify project_id. This
        administrative operation has its own lock and checks the previous
        association to prevent an outdated project browser from overwriting a
        concurrent reassignment. The owning ProjectRepository validates the
        destination and serializes reassignment against project deletion.
        """
        self._ensure_layout()
        with self._lock, FileLock(self._run_lock_path(run_id)):
            previous_path = self._path_for_id(run_id)
            current = replace(self._decode(previous_path), store=self)
            if current.project_id != expected_project_id:
                raise ValueError(f"Run {run_id!r} belongs to a different project now")
            if current.project_id == project_id:
                self._finish_move(current, previous_path)
                return current
            return self._write_unlocked(replace(current, project_id=project_id),
                                        previous_path=previous_path)

    def discard(self, record):
        """Remove an exact, owner-scoped reservation without a tombstone.

        Its observation lock must still be held by the scope's owner.
        """
        with self.submission_lock(), FileLock(self._run_lock_path(record.id)):
            path = self._path_for_id(record.id)
            if self._decode(path) != record:
                raise ValueError("Cannot discard a changed scoped reservation")
            path.unlink()
            fsync_directory(path.parent)

    def forget(self, run, *, force=False):
        """Forget persisted metadata, never the running process or result files.

        Active or unknown jobs require force=True. A durable tombstone prevents
        a stale observer from recreating the removed record on completion.
        """
        with self.submission_lock():
            return self.forget_under_submission_lock(run, force=force)

    def forget_under_submission_lock(self, run, *, force=False):
        """Forget a run while the caller holds this store's submission lock.

        Project deletion already holds the lock for the entire project/history
        operation. Reuse normal record deletion without reacquiring that lock.
        """
        if type(force) is not bool:
            raise TypeError("force must be a bool")
        run_id = run if isinstance(run, str) else run.id
        with self._lock, FileLock(self._run_lock_path(run_id)):
            path = self._path_for_id(run_id)
            record = replace(self._decode(path), store=self)
            if not record.state.is_terminal and not force:
                raise ActiveRunDeletionError(
                    f"Run {run_id!r} is {record.state.value}; its process may "
                    "still be active. Use force=True to forget it without "
                    "stopping the execution."
                )
            tombstone = self._forgotten_path(run_id)
            durable_mkdir(tombstone.parent, parents=True, exist_ok=True, mode=0o700)
            durable_write_json(
                tombstone, {"run_id": run_id, "created_at": record.created_at},
                mode=0o600, sort_keys=True,
            )
            path.unlink()
            fsync_directory(path.parent)
            return record

    def _get_unlocked(self, run_id):
        return replace(self._decode(self._path_for_id(run_id)), store=self)

    def get(self, run_id):
        validate_run_id(run_id)
        if not self.directory.is_dir():
            raise FileNotFoundError(run_id)
        # A record may move between namespaces during a lifecycle update.
        # Reuse the per-ID lock so lookup and read see one location.
        with self._lock, FileLock(self._run_lock_path(run_id)):
            return self._get_unlocked(run_id)

    def _day_directories(self, *, newest):
        if not self.directory.is_dir():
            return ()
        days = [path for path in self.directory.iterdir()
                if re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", path.name) and path.is_dir()]
        return tuple(sorted(days, key=lambda path: path.name, reverse=newest))

    def _records_in_directory(self, directory, *, newest, on_error=None):
        records = []
        for path in directory.glob("*.json"):
            if self._forgotten_path(path.stem).is_file():
                continue
            try:
                records.append(replace(self._decode(path), store=self))
            except FileNotFoundError:
                # A scoped reservation or forgotten record can disappear
                # between directory enumeration and opening the file.
                continue
            except CorruptRunRecord as exc:
                if on_error is None:
                    raise
                on_error(exc)
        return sorted(records, key=lambda record: record.created_at, reverse=newest)

    def runs(self, *, active=None, limit=None, newest=False, on_error=None):
        """List run history.

        Strict by default: an invalid record raises CorruptRunRecord. History
        viewers may supply on_error(exception) to report and skip individual
        unreadable records while preserving their files unchanged. Safety-
        critical consumers (e.g. submission conflict checks) stay strict.
        """
        if limit is not None and limit < 0:
            raise ValueError("limit must be non-negative")
        if limit == 0:
            return ()

        if active is True and self.directory.is_dir() and not self._layout_marker.is_file():
            try:
                self._ensure_layout()
            except CorruptRunRecord:
                if on_error is None:
                    raise
                # A tolerant history viewer may still inspect valid records.
                # Never publish a layout marker while an old shard is unreadable.
        with self._lock:
            migrated = self._layout_marker.is_file()
            if active is True and migrated:
                records = tuple(record for record in self._records_in_directory(
                    self._active_directory, newest=newest, on_error=on_error,
                ) if not record.state.is_terminal)
                return records[:limit]

            # Terminal entries can remain in active/ after an interrupted move.
            # Merge both namespaces by creation date, retaining the existing
            # limit optimization for dated history.
            by_day = {}
            for record in self._records_in_directory(
                self._active_directory, newest=newest, on_error=on_error,
            ):
                by_day.setdefault(self._bucket(record.created_at), []).append(record)
            days = {day.name: day for day in self._day_directories(newest=newest)}
            records = []
            seen = set()
            for name in sorted(days.keys() | by_day.keys(), reverse=newest):
                entries = by_day.get(name, [])
                if name in days:
                    entries.extend(self._records_in_directory(
                        days[name], newest=newest, on_error=on_error,
                    ))
                for record in sorted(entries, key=lambda item: item.created_at, reverse=newest):
                    # A moving record may be observed in both enumerations.
                    # Distinguish that race from duplicate persisted identities
                    # using the normal locked lookup.
                    if record.id in seen:
                        try:
                            self.get(record.id)
                        except FileNotFoundError:
                            pass
                        except CorruptRunRecord as exc:
                            if on_error is None:
                                raise
                            on_error(exc)
                        continue
                    seen.add(record.id)
                    if active is True and record.state.is_terminal:
                        continue
                    if active is False and not record.state.is_terminal:
                        continue
                    records.append(record)
                    if limit is not None and len(records) >= limit:
                        return tuple(records)
            return tuple(records)
