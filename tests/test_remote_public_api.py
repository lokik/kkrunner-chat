"""One client and FileSession own managed and remote execution."""

from kkrunner.client import RunClient, SshTransport
from kkrunner.client.file_session import FileSession
from kkrunner.client.job import Job
from kkrunner.remote import __all__ as agent_exports


def test_remote_execution_uses_shared_client_not_an_agent_facade():
    assert RunClient.__module__ == "kkrunner.client.client"
    assert SshTransport.__module__ == "kkrunner.client.transports.ssh"
    assert Job.__module__ == "kkrunner.client.job"
    assert FileSession.__module__ == "kkrunner.client.file_session"
    assert agent_exports == []
    assert not hasattr(Job, "synchronize")
    assert not hasattr(Job, "_sync_output_files")
