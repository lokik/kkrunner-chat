"""Untracked direct SSH endpoint over shared remote file synchronization."""

from __future__ import annotations

from kkrunner.backends.contracts import RecoveryResult, UnsupportedCapability
from kkrunner.client.remote_files import RemoteFileSession
from kkrunner.client.remote_paths import RemotePathContext, query_remote_home
from kkrunner.client.run_parts import RunParts

class SshDirectBackend:
    """One SSH connection per Job, without a target kkrunner-agent.

    The target needs Python 3 and a working SSH connection. A direct Job has no
    durable identity; use AgentBackendProxy for managed/reconnectable SSH.
    """

    name = "ssh-direct"
    supports_result_file_retrieval = False
    default_persistent = False
    default_detached = False

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
        from kkrunner.backends.process.direct.job import ProcessJob
        from .runtime import SshProcessAdapter
        context = RemotePathContext(self, spec.launch)
        return RunParts(
            ProcessJob(spec, callback=callback, adapter=SshProcessAdapter(context)),
            RemoteFileSession(context, spec),
        )

    def open_run_parts(self, attachment, *, callback=None, output_listener=None):
        raise UnsupportedCapability("SSH direct jobs cannot be reattached")

    def recover_submission(self, submission_key, submission, *, submitted_at=None):
        return RecoveryResult.unsupported("SSH direct execution has no durable submission identity")
