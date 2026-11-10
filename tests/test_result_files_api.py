"""Public result-file terminology and explicit endpoint capabilities."""

from kkrunner.backends import ProcessBackend, PsijBackend
from kkrunner.client.agent import AgentBackendProxy
from kkrunner.files import FileManifest, FileState


def test_result_file_names_use_canonical_public_api():
    manifest = FileManifest()
    assert manifest.state is FileState.CURRENT
    assert FileState.CURRENT.value == "current"
    assert FileState.OVERWRITTEN.value == "overwritten"


def test_result_file_retrieval_is_an_explicit_endpoint_capability():
    assert ProcessBackend.supports_result_file_retrieval is False
    assert PsijBackend.supports_result_file_retrieval is False
    assert AgentBackendProxy.supports_result_file_retrieval is True
