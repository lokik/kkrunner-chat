"""Reusable execution-provider pooling independent of run persistence."""

from __future__ import annotations

from threading import RLock

from kkrunner.files.streams import _close_distinct


class ClientPool:
    """Process-local cache of clients keyed by transport and backend."""

    def __init__(self):
        self._clients = {}
        self._lock = RLock()

    @staticmethod
    def key_for_profile(profile):
        """Return the client configuration used for instance reuse."""
        return (profile.target, profile.backend)

    def get(self, key, factory):
        with self._lock:
            client = self._clients.get(key)
            if client is None:
                client = factory()
                self._clients[key] = client
            return client

    def for_config(self, transport, backend):
        from kkrunner.backends.config import BackendConfig
        from kkrunner.client.execution_client import ExecutionClient
        from kkrunner.targets import (
            require_persistable_target,
            target_from_dict,
        )
        if isinstance(transport, dict):
            transport = target_from_dict(transport)
        require_persistable_target(transport)
        if isinstance(backend, dict):
            backend = BackendConfig(**backend)
        if not transport.available or not backend.available:
            raise ValueError("Unsupported backend or transport; only local history is available")
        return self.get((transport, backend), lambda: ExecutionClient.from_config(transport, backend))

    def for_profile(self, profile):
        """Return the shared provider suitable for *profile*."""
        return self.for_config(*self.key_for_profile(profile))

    def discard(self, key):
        """Forget one pooled client and close it."""
        with self._lock:
            client = self._clients.pop(key, None)
        if client is not None:
            client.close()
        return client

    def discard_profile(self, profile):
        return self.discard(self.key_for_profile(profile))

    def close(self):
        """Close every pooled client, propagating the first close failure."""
        with self._lock:
            endpoints = tuple(self._clients.values())
            self._clients.clear()
        _close_distinct(endpoints)


_DEFAULT_CLIENT_POOL = ClientPool()


def default_client_pool():
    """Return the process-wide pool used by ordinary calculations."""
    return _DEFAULT_CLIENT_POOL
