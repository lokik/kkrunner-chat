"""Public result-file terminology and explicit endpoint capabilities."""

from kkrunner.client.local import LocalRunEndpoint
from kkrunner.backends import PsijBackend
from kkrunner.client.ssh import SshRunEndpoint
from kkrunner.files import FileManifest, FileState


def test_result_file_names_use_canonical_public_api():
    manifest = FileManifest()
    assert manifest.state is FileState.CURRENT
    assert FileState.CURRENT.value == "current"
    assert FileState.OVERWRITTEN.value == "overwritten"


def test_result_file_retrieval_is_an_explicit_endpoint_capability():
    assert LocalRunEndpoint.supports_result_file_retrieval is False
    assert LocalRunEndpoint(PsijBackend).supports_result_file_retrieval is False
    assert SshRunEndpoint.supports_result_file_retrieval is True
