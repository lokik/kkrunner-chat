"""Serializable backend selection, independent of connection configuration."""

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class BackendConfig:
    name: str = "direct"
    options: Any = ()

    def __post_init__(self):
        from kkrunner.backends import backend_class
        if not isinstance(self.name, str) or not self.name:
            raise ValueError("Backend name must be a nonempty string")
        options = dict(self.options)
        if self.available:
            options = backend_class(self.name).validate_options(**options)
        object.__setattr__(self, "options", tuple(sorted(options.items())))

    @property
    def available(self):
        """Unknown implementations remain readable history, but cannot execute."""
        from kkrunner.backends import available_backend_names
        return self.name in available_backend_names()

    @property
    def supports(self):
        """Capabilities of this configuration without constructing a backend."""
        from kkrunner.backends import backend_class
        if not self.available:
            return frozenset()
        return backend_class(self.name).supports_options(dict(self.options))

    @property
    def execution_mode(self):
        """Execution semantics of the configured implementation, without an instance."""
        from kkrunner.backends import backend_class
        return backend_class(self.name).execution_mode

    def reference_unavailable_reason(self, capability, backend_id, *, stream_paths=()):
        """Explain why a saved reference cannot offer a backend action, if known.

        This advisory check does not establish that the job is alive or that
        the target is reachable. The native operation must verify that itself.
        """
        from kkrunner.backends import backend_class
        if capability not in self.supports:
            return f"The {self.name} backend does not support {capability.value}."
        if backend_id is None:
            return "No saved backend identity"
        return backend_class(self.name).reference_unavailable_reason(
            capability, backend_id, stream_paths=stream_paths,
        )

    def create(self):
        from kkrunner.backends import backend_class
        return backend_class(self.name)(**dict(self.options))
