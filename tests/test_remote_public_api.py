"""Remote and local managed execution share the same client Job."""

from kkrunner.client.ssh import SshRunEndpoint
from kkrunner.client.remote_files import RemoteFileSession
from kkrunner.client.job import Job
from kkrunner.remote import __all__ as agent_exports


def test_remote_execution_is_a_client_endpoint_not_an_agent_facade():
    assert SshRunEndpoint.__module__ == "kkrunner.client.ssh"
    assert Job.__module__ == "kkrunner.client.job"
    assert RemoteFileSession.__module__ == "kkrunner.client.remote_files"
    assert agent_exports == []
    assert not hasattr(Job, "synchronize")
    assert not hasattr(Job, "_sync_output_files")
