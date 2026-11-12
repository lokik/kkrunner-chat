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
        return (profile.transport, profile.backend)

    def get(self, key, factory):
        with self._lock:
            client = self._clients.get(key)
            if client is None:
                client = factory()
                self._clients[key] = client
            return client

    def for_config(self, transport, backend):
        from kkrunner.backends.config import BackendConfig
        from kkrunner.client.endpoint import EndpointClient
        from kkrunner.client.transports import (
            require_persistable_transport,
            transport_from_dict,
        )
        if isinstance(transport, dict):
            transport = transport_from_dict(transport)
        require_persistable_transport(transport)
        if isinstance(backend, dict):
            backend = BackendConfig(**backend)
        return self.get((transport, backend), lambda: EndpointClient.from_config(transport, backend))

    def for_profile(self, profile):
        """Return the shared provider suitable for *profile*."""
        return self.for_config(*self.key_for_profile(profile))

    def discard(self, key):
        """Forget one pooled provider and close it when supported."""
        with self._lock:
            client = self._clients.pop(key, None)
        if client is not None:
            close = getattr(client, "close", None)
            if close is not None:
                close()
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
