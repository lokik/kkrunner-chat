"""Reusable execution-provider pooling independent of run persistence."""

from __future__ import annotations

from threading import RLock

from kkrunner.runs.base import _close_distinct


class EndpointPool:
    """Process-local cache of configured client RunEndpoints."""

    def __init__(self):
        self._endpoints = {}
        self._lock = RLock()

    @staticmethod
    def key_for_profile(profile):
        """Return the endpoint configuration used for instance reuse."""
        return profile.endpoint

    def get(self, key, factory):
        with self._lock:
            endpoint = self._endpoints.get(key)
            if endpoint is None:
                endpoint = factory()
                self._endpoints[key] = endpoint
            return endpoint

    def for_config(self, config):
        """Return the shared endpoint reconstructed from *config*."""
        from kkrunner.client.endpoint import ExecutionEndpointConfig, endpoint_from_config

        if isinstance(config, dict):
            config = ExecutionEndpointConfig(**config)
        return self.get(config, lambda: endpoint_from_config(config))

    def for_profile(self, profile):
        """Return the shared provider suitable for *profile*."""
        return self.for_config(self.key_for_profile(profile))

    def discard(self, key):
        """Forget one pooled provider and close it when supported."""
        with self._lock:
            endpoint = self._endpoints.pop(key, None)
        if endpoint is not None:
            close = getattr(endpoint, "close", None)
            if close is not None:
                close()
        return endpoint

    def discard_profile(self, profile):
        return self.discard(self.key_for_profile(profile))

    def close(self):
        """Close every pooled endpoint, propagating the first close failure."""
        with self._lock:
            endpoints = tuple(self._endpoints.values())
            self._endpoints.clear()
        _close_distinct(endpoints)


_DEFAULT_ENDPOINT_POOL = EndpointPool()


def default_endpoint_pool():
    """Return the process-wide pool used by ordinary calculations."""
    return _DEFAULT_ENDPOINT_POOL
