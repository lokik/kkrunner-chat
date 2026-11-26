"""Storage contract shared by disk-backed and in-process run registries."""
from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Callable, Protocol

from kkrunner.runs.records import RunRecord


class RunLock(Protocol):
    def acquire(self, *, blocking: bool = True) -> bool: ...
    def release(self) -> None: ...


class RunStoreProtocol(Protocol):
    @property
    def durable(self) -> bool: ...
    @property
    def lock_key(self) -> tuple: ...
    def submission_lock(self) -> AbstractContextManager: ...
    def observation_lock(self, record: RunRecord) -> RunLock: ...
    def save(self, record: RunRecord) -> RunRecord: ...
    def get(self, run_id: str) -> RunRecord: ...
    def update(self, record: RunRecord, updater: Callable[[RunRecord], RunRecord]) -> RunRecord: ...
    def runs(self, *, active=None, limit=None, newest=False, on_error=None) -> tuple[RunRecord, ...]: ...
    def discard(self, record: RunRecord) -> None: ...
    def forget(self, run, *, force=False) -> RunRecord: ...


def store_for(run, default):
    """IDs address the default store; bound records address their own store."""
    if isinstance(run, str):
        return default
    return default if run.store is None else run.store


def resolve_record(run, default):
    """Read the latest authoritative version of an id or record."""
    store = store_for(run, default)
    return store.get(run if isinstance(run, str) else run.id)
