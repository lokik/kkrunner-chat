"""Direct process backend interface and small client Job factory.

SSH direct ships a standalone Python worker, not an installed backend.
"""
from __future__ import annotations
from typing import Protocol
from kkrunner.runs.base import ExecutionSpec
from kkrunner.backends.contracts import Backend
from kkrunner.native.direct.local_adapter import LocalProcessAdapter
from kkrunner.native.direct.ssh_adapter import SshProcessAdapter
from kkrunner.native.direct.worker import WORKER_SOURCE, DIRECT_LIBRARY_SOURCE

class DirectProcessAdapter(Protocol):
    """Process mechanism, not a second Job lifecycle or state owner."""

    expose_pid: bool
    uncertain_after_launch: bool
    detached: bool

    def prepare(self, spec: ExecutionSpec) -> None: ...

    async def launch(self, spec: ExecutionSpec): ...

    async def stop(self, process) -> bool: ...

    def output(self, data: bytes, kind: str): ...

    def detached_output_paths(self, spec: ExecutionSpec) -> dict: ...

    def exit_status(self, code: int): ...

    def close(self, process) -> None: ...


from kkrunner.backends.contracts import Backend
from kkrunner.runs.base import ExecutionSpec

class DirectProcessBackend(Backend):
    """Create direct process Jobs, locally or using an internal transport adapter."""

    name = "direct-process"
    supports_result_file_retrieval = False
    supports_tracked_runs = True
    requires_backend_identity = False
    default_persistent = False
    default_detached = False

    def __init__(self, *, adapter_factory=None):
        # The adapter is a private implementation detail of the direct Job session.
        # Local and raw SSH direct both go through this backend factory.
        self._adapter_factory = adapter_factory

    def create_job(self, spec: ExecutionSpec, *, submission_key=None, callback=None):
        spec.require_complete()
        from kkrunner.client.job import Job
        adapter = self._adapter_factory() if self._adapter_factory is not None else None
        return Job.direct(spec, callback=callback, adapter=adapter)




def stream(request):
    """Run the portable target script with a request encoded as JSON."""
    import json
    import subprocess
    import sys
    from kkrunner.native.direct.worker import WORKER_SOURCE
    return subprocess.call(
        (sys.executable, "-u", "-c", WORKER_SOURCE, json.dumps(request)),
        stdin=sys.stdin.buffer,
        stdout=sys.stdout.buffer,
        stderr=sys.stderr.buffer,
    )


class DirectBackend:
    """Direct process operations, portable SSH worker provided separately."""

    from kkrunner.backends.class_helper import identity as SshHelper

    stream = staticmethod(stream)

# The identity helper serializes this self-contained class, not the host-only
# wrapper above. The two entrypoints share the same worker implementation.
DirectBackend.__source__ = DIRECT_LIBRARY_SOURCE + """
class DirectBackend:
    @staticmethod
    def stream(request):
        return execute(request)
"""
