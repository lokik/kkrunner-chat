"""Direct process backend interface and small client Job factory.

SSH direct ships a standalone Python worker, not an installed backend.
"""
from __future__ import annotations
from kkrunner.runs.base import ExecutionSpec
from kkrunner.backends.contracts import Backend
from kkrunner.native.direct.worker import WORKER_SOURCE, DIRECT_LIBRARY_SOURCE, execute

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





class DirectBackend:
    """Portable target operation; no remote KKRunner installation required."""

    from kkrunner.backends.class_helper import identity as SshHelper

    # The *same* implementation is called on the target; no substitute
    # version of this method is embedded in another class definition.
    stream = staticmethod(execute)


# Direct's static operation needs several separately defined portable
# functions. They are extracted from their real implementations, not copied.
DirectBackend.__method_sources__ = {"stream": DIRECT_LIBRARY_SOURCE}
