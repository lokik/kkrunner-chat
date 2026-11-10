"""The remote client facade and backend Job have distinct public roles."""

from kkrunner.client.agent import AgentBackendProxy
from kkrunner.client.remote_files import RemoteFileSession
from kkrunner.client.remote_job import RemoteJob
from kkrunner.remote import __all__ as agent_exports


def test_remote_execution_is_a_client_endpoint_not_an_agent_facade():
    assert AgentBackendProxy.__module__ == "kkrunner.client.agent"
    assert RemoteJob.__module__ == "kkrunner.client.remote_job"
    assert RemoteFileSession.__module__ == "kkrunner.client.remote_files"
    assert agent_exports == []
    assert not hasattr(RemoteJob, "synchronize")
    assert not hasattr(RemoteJob, "_sync_output_files")
