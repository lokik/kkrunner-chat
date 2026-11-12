"""Serializable backend selection, independent of connection configuration."""

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class BackendConfig:
    name: str = "direct"
    options: Any = ()

    def __post_init__(self):
        from kkrunner.backends import backend_class
        backend = backend_class(self.name)(**dict(self.options))
        object.__setattr__(self, "options", tuple(sorted(backend.options.items())))

    def create(self):
        from kkrunner.backends import backend_class
        return backend_class(self.name)(**dict(self.options))
