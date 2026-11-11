"""Reusable execution-provider pooling independent of run persistence."""

from __future__ import annotations

from threading import RLock

from kkrunner.runs.base import _close_distinct


class ExecutorPool:
    """Process-local cache of configured client RunEndpoints."""

    def __init__(self):
        self._executors = {}
        self._lock = RLock()

    @staticmethod
    def key_for_profile(profile):
        """Return the endpoint identity used for executor reuse."""
        return profile.endpoint

    def get(self, key, factory):
        with self._lock:
            executor = self._executors.get(key)
            if executor is None:
                executor = factory()
                self._executors[key] = executor
            return executor

    def for_endpoint(self, endpoint):
        """Return the shared provider reconstructed from *endpoint*."""
        from kkrunner.client.endpoint import ExecutionEndpointConfig, executor_from_endpoint

        if isinstance(endpoint, dict):
            endpoint = ExecutionEndpointConfig(**endpoint)
        return self.get(endpoint, lambda: executor_from_endpoint(endpoint))

    def for_profile(self, profile):
        """Return the shared provider suitable for *profile*."""
        return self.for_endpoint(self.key_for_profile(profile))

    def discard(self, key):
        """Forget one pooled provider and close it when supported."""
        with self._lock:
            executor = self._executors.pop(key, None)
        if executor is not None:
            close = getattr(executor, "close", None)
            if close is not None:
                close()
        return executor

    def discard_profile(self, profile):
        return self.discard(self.key_for_profile(profile))

    def close(self):
        """Close every pooled executor, propagating the first close failure."""
        with self._lock:
            executors = tuple(self._executors.values())
            self._executors.clear()
        _close_distinct(executors)


_DEFAULT_EXECUTOR_POOL = ExecutorPool()


def default_executor_pool():
    """Return the process-wide pool used by ordinary calculations."""
    return _DEFAULT_EXECUTOR_POOL
