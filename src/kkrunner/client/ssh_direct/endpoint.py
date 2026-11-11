"""Untracked direct SSH endpoint over shared remote file synchronization."""

from __future__ import annotations

from kkrunner.client.remote_files import RemoteFileSession
from kkrunner.client.remote_paths import RemotePathContext, query_remote_home
from kkrunner.client.run_parts import RunParts
from kkrunner.client.contracts import RunEndpoint

class SshDirectRunEndpoint(RunEndpoint):
    """One SSH connection per Job, without a target kkrunner-agent.

    The target needs Python 3 and a working SSH connection. A direct Job has no
    durable identity; use AgentRunEndpoint for managed/reconnectable SSH.
    """

    name = "ssh-direct"
    supports_result_file_retrieval = False
    default_persistent = False
    default_detached = False
    requires_backend_identity = False

    def __init__(self, transport, *, filesystem_id=None):
        self.transport = transport
        self.filesystem_id = filesystem_id
        self.computer = getattr(transport, "host", None)
        self.backend = "process"
        self.backend_options = {}
        self.command = ()
        self._remote_home_cache = None

    async def _remote_home(self):
        if self._remote_home_cache is None:
            self._remote_home_cache = await query_remote_home(self.transport)
        return self._remote_home_cache

    def create_run_parts(self, spec, *, submission_key=None, callback=None):
        spec.require_complete()
        from kkrunner.backends.direct import DirectProcessBackend
        from kkrunner.native.direct.ssh_adapter import SshProcessAdapter
        context = RemotePathContext(self, spec.launch)
        backend = DirectProcessBackend(adapter_factory=lambda: SshProcessAdapter(context))
        return RunParts(
            backend.create_job(spec, submission_key=submission_key, callback=callback),
            RemoteFileSession(context, spec),
        )

