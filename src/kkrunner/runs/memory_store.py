"""Process-local run registry with the same lifecycle and lock contract as RunStore."""
from __future__ import annotations

from dataclasses import replace
from threading import Lock, RLock
from weakref import WeakValueDictionary

from kkrunner.execution import RunState
from kkrunner.runs.records import (
    ActiveRunDeletionError, ForgottenRunError, RunRecord, validate_update, validate_run_id,
)


class MemoryRunStore:
    """Track runs without writing history or coordinating with other processes."""

    durable = False

    def __init__(self):
        self._records = {}
        self._forgotten = set()
        self._lock = RLock()
        self._observation = WeakValueDictionary()

    @property
    def lock_key(self):
        return (1, id(self))

    def submission_lock(self):
        return self._lock

    def observation_lock(self, record):
        with self._lock:
            return self._observation.setdefault(record.id, Lock())

    def _assert_not_forgotten(self, run_id):
        if run_id in self._forgotten:
            raise ForgottenRunError(f"Run {run_id!r} was deliberately removed")

    def get(self, run_id):
        validate_run_id(run_id)
        with self._lock:
            try:
                return self._records[run_id]
            except KeyError:
                raise FileNotFoundError(run_id) from None

    def _write(self, record):
        record = replace(record, store=self)
        self._records[record.id] = record
        return record

    def save(self, record: RunRecord):
        with self._lock:
            self._assert_not_forgotten(record.id)
            latest = self._records.get(record.id)
            if latest is not None:
                if latest != record:
                    raise ValueError("Run already exists; update cannot replace identity or metadata")
                return latest
            return self._write(record)

    def update(self, record: RunRecord, updater):
        with self._lock:
            self._assert_not_forgotten(record.id)
            latest = self._records.get(record.id)
            exists = latest is not None
            if latest is None:
                if record.state != RunState.NEW:
                    raise FileNotFoundError(record.id)
                latest = replace(record, store=self)
            if latest.created_at != record.created_at:
                raise ValueError("Run id already belongs to a different persistent identity")
            updated = updater(latest)
            validate_update(latest, updated)
            if exists and updated is latest:
                return latest
            return self._write(updated)

    def runs(self, *, active=None, limit=None, newest=False, on_error=None):
        if limit is not None and limit < 0:
            raise ValueError("limit must be non-negative")
        with self._lock:
            records = sorted(self._records.values(), key=lambda record: record.created_at, reverse=newest)
            if active is True:
                records = [record for record in records if not record.state.is_terminal]
            elif active is False:
                records = [record for record in records if record.state.is_terminal]
            return tuple(records[:limit])

    def discard(self, record):
        """Remove an exact scoped reservation once its owner leaves the scope."""
        with self._lock:
            if self.get(record.id) != record:
                raise ValueError("Cannot discard a changed scoped reservation")
            del self._records[record.id]

    def forget(self, run, *, force=False):
        if type(force) is not bool:
            raise TypeError("force must be a bool")
        run_id = run if isinstance(run, str) else run.id
        with self._lock:
            record = self.get(run_id)
            if not record.state.is_terminal and not force:
                raise ActiveRunDeletionError(f"Run {run_id!r} is {record.state.value}; its process may still be active")
            self._forgotten.add(run_id)
            del self._records[run_id]
            return record
