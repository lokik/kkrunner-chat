from __future__ import annotations

import os

from kkrunner.files.versions import FileState, FileVersion


def test_version_tracks_content_through_metadata_replacement_rewrite_and_removal(
    tmp_path,
):
    path = tmp_path / "result.dat"
    original_content = b"same contents"
    path.write_bytes(original_content)
    version = FileVersion.capture(path)
    original = path.stat()

    assert version.digest is not None

    # A changed timestamp does not change content identity.
    os.utime(
        path,
        ns=(original.st_atime_ns, original.st_mtime_ns + 1_000_000_000),
    )
    assert version.current() == version
    assert version.state() is FileState.CURRENT

    # Replacing the inode with identical bytes also retains that identity.
    replacement = tmp_path / "replacement.dat"
    replacement.write_bytes(original_content)
    os.replace(replacement, path)
    assert version.current() == version
    assert version.state() is FileState.CURRENT

    # A same-size rewrite remains distinguishable even if mtime is restored.
    prior = path.stat()
    changed_content = b"other content"
    assert len(changed_content) == len(original_content)
    path.write_bytes(changed_content)
    os.utime(path, ns=(prior.st_atime_ns, prior.st_mtime_ns))
    assert version.state() is FileState.OVERWRITTEN
    assert version.current() != version

    # A missing file takes precedence over its previous content identity.
    path.unlink()
    assert version.state() is FileState.MISSING


def test_metadata_only_snapshot_is_not_available_content_identity(tmp_path):
    path = tmp_path / "result.dat"
    path.write_bytes(b"contents")

    version = FileVersion.capture(path, digest=False)

    assert version.digest is None
    assert version.available is False


def test_metadata_only_capture_cannot_validate_directory_as_regular_file(tmp_path):
    from kkrunner.files import FileState, FileVersion

    directory = tmp_path / "data"
    directory.mkdir()
    wrongly_typed = FileVersion.capture(directory, digest=False)
    assert wrongly_typed.kind == "file"
    assert not wrongly_typed.available
    assert wrongly_typed.state() is FileState.OVERWRITTEN
    assert not wrongly_typed.is_current()

    proper = FileVersion.capture(directory, digest=False, kind="directory")
    # A directory metadata-only identity does not claim verified contents.
    assert not proper.available
