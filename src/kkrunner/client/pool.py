"""Reusable execution-provider pooling independent of run persistence."""

from __future__ import annotations

from threading import RLock


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
        from kkrunner.client.execution_client import ExecutionClient, normalize_config
        transport, backend = normalize_config(transport, backend)
        return self.get((transport, backend), lambda: ExecutionClient(backend.create(), target=transport))

    def for_profile(self, profile):
        """Return the shared provider suitable for *profile*."""
        return self.for_config(*self.key_for_profile(profile))

    def discard(self, key):
        """Forget one cached client without affecting its target or runs."""
        with self._lock:
            return self._clients.pop(key, None)

    def discard_profile(self, profile):
        return self.discard(self.key_for_profile(profile))

    def clear(self):
        """Forget cached clients; transport sessions have a separate lifetime."""
        with self._lock:
            self._clients.clear()


_DEFAULT_CLIENT_POOL = ClientPool()


def default_client_pool():
    """Return the process-wide pool used by ordinary calculations."""
    return _DEFAULT_CLIENT_POOL
