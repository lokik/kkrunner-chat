"""Client Run components returned by a client-capable execution endpoint."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional, Protocol

if TYPE_CHECKING:
    from kkrunner.backends.contracts import Job
    from kkrunner.client.remote_files import RemoteFileSession


@dataclass(frozen=True)
class RunParts:
    job: "Job"
    files: Optional["RemoteFileSession"] = None

class RunEndpoint(Protocol):
    """Client facade capable of constructing both sides of a Run lifecycle.

    A backend Job knows nothing about this protocol or remote file staging.
    """

    def create_run_parts(self, spec, *, submission_key=None, callback=None) -> RunParts: ...

    def open_run_parts(self, attachment, *, callback=None, output_listener=None) -> RunParts: ...
