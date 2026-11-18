"""Read-only representation of an unsupported persisted transport."""

import json
from dataclasses import dataclass

from ..base import Transport


@dataclass(frozen=True, init=False)
class UnavailableTransport(Transport):
    """Preserve connection metadata without enabling any target operation."""

    settings_json: str
    available = False

    def __init__(self, settings):
        object.__setattr__(self, "settings_json", json.dumps(settings, sort_keys=True, allow_nan=False))

    @property
    def kind(self):
        return self.to_dict()["kind"]

    @property
    def host(self):
        return self.to_dict().get("host", "unknown")

    def to_dict(self):
        return json.loads(self.settings_json)
