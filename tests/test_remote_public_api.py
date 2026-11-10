"""The remote client facade and backend Job have distinct public roles."""

from kkrunner.client.agent import AgentRunEndpoint
from kkrunner.client.remote_files import RemoteFileSession
from kkrunner.client.agent_job import AgentJob
from kkrunner.remote import __all__ as agent_exports


def test_remote_execution_is_a_client_endpoint_not_an_agent_facade():
    assert AgentRunEndpoint.__module__ == "kkrunner.client.agent"
    assert AgentJob.__module__ == "kkrunner.client.agent_job"
    assert RemoteFileSession.__module__ == "kkrunner.client.remote_files"
    assert agent_exports == []
    assert not hasattr(AgentJob, "synchronize")
    assert not hasattr(AgentJob, "_sync_output_files")
