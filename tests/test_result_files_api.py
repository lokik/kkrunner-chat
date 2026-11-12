"""Public result-file terminology and client ownership."""

from kkrunner.client import EndpointClient, LocalTransport, SshTransport
from kkrunner.backends import PsijBackend
from kkrunner.files import FileManifest, FileState


def test_result_file_names_use_canonical_public_api():
    manifest = FileManifest()
    assert manifest.state is FileState.CURRENT
    assert FileState.CURRENT.value == "current"
    assert FileState.OVERWRITTEN.value == "overwritten"


def test_result_file_retrieval_is_owned_by_shared_client():
    local = EndpointClient(PsijBackend, transport=LocalTransport())
    remote = EndpointClient(PsijBackend, transport=SshTransport("cluster"))
    assert local.supports_result_file_retrieval is True
    assert remote.supports_result_file_retrieval is True
    assert callable(local.fetch_result_files)
    assert callable(remote.fetch_result_files)
