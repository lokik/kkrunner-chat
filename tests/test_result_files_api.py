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


def test_capture_excludes_skipped_local_copies_and_unmatched_glob_files(tmp_path):
    """An old local match is not an output version of the new remote run."""
    from kkrunner.files import FileEntry
    from kkrunner.runs.files import capture_result_files

    stale = tmp_path / "artifact-old.dat"
    current = tmp_path / "artifact-new.dat"
    wildcard = tmp_path / "artifact-*.dat"
    stale.write_text("older run")
    current.write_text("new run")
    manifest = FileManifest(outputs=(
        FileEntry(local_path=str(wildcard), availability_state="available"),
        FileEntry(local_path=str(stale), availability_state="skipped"),
        FileEntry(local_path=str(current), source_pattern=str(wildcard), availability_state="available"),
    ))
    captured = capture_result_files(manifest)
    assert tuple(str(version.path) for version in captured.output_versions) == (str(current),)
    assert captured.outputs[1].availability_state == "skipped"
    assert captured.outputs[2].availability_state == "available"


def test_capture_still_discovers_pending_local_outputs(tmp_path):
    from kkrunner.files import FileEntry
    from kkrunner.runs.files import capture_result_files

    output = tmp_path / "locally-generated.out"
    output.write_text("current")
    captured = capture_result_files(FileManifest(outputs=(
        FileEntry(local_path=str(output), availability_state="pending"),
    )))
    assert tuple(str(version.path) for version in captured.output_versions) == (str(output),)
    assert captured.outputs[0].availability_state == "available"
