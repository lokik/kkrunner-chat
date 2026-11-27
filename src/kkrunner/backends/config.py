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
    def execution_mode(self):
        """Execution semantics of the configured implementation, without an instance."""
        from kkrunner.backends import backend_class
        return backend_class(self.name).execution_mode

    def create(self):
        from kkrunner.backends import backend_class
        return backend_class(self.name)(**dict(self.options))
