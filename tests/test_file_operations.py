"""Cached local filesystem observations and refresh semantics."""

import pytest

from kkrunner.files import FileOperations, FileState


def test_metadata_refresh_updates_cache(tmp_path):
    ops = FileOperations()

    path = tmp_path / "file.dat"
    path.write_bytes(b"old")
    assert ops.stat(path).st_size == 3
    assert ops.fingerprint(path).size == 3
    from kkrunner.files.fingerprints import FileFingerprint
    assert FileFingerprint.from_stat(path.stat()) == ops.fingerprint(path)
    assert ops.is_file(path)

    path.write_bytes(b"longer")
    assert ops.stat(path).st_size == 3
    assert ops.stat(path, refresh=True).st_size == 6
    assert ops.stat(path).st_size == 6
    assert ops.fingerprint(path).size == 6

    ops.invalidate()
    assert ops.stat(path).st_size == 6


def test_directory_and_glob_refresh_update_cache(tmp_path):
    ops = FileOperations()
    (tmp_path / ".hidden").write_text("secret")
    first = tmp_path / "a.out"
    first.write_bytes(b"a")
    assert ops.listdir(tmp_path) == (".hidden", "a.out")
    assert ops.iterdir(tmp_path) == (tmp_path / ".hidden", first)
    assert ops.glob(tmp_path / "*.out") == (first,)

    second = tmp_path / "b.out"
    second.write_bytes(b"b")
    assert ops.listdir(tmp_path) == (".hidden", "a.out")
    assert ops.glob(tmp_path / "*.out") == (first,)
    assert ops.listdir(tmp_path, refresh=True) == (".hidden", "a.out", "b.out")
    assert ops.glob(tmp_path / "*.out", refresh=True) == (first, second)
    # Refreshed observations are stored for subsequent non-refresh queries.
    assert ops.listdir(tmp_path) == (".hidden", "a.out", "b.out")
    assert ops.glob(tmp_path / "*.out") == (first, second)


def test_recursive_glob_and_iterdir(tmp_path):
    ops = FileOperations()
    nested = tmp_path / "nested"
    deep = nested / "deep"
    deep.mkdir(parents=True)
    (nested / "one.out").write_bytes(b"x")
    (deep / "two.out").write_bytes(b"y")

    assert ops.glob(tmp_path / "**" / "*.out") == (deep / "two.out", nested / "one.out")
    assert ops.iterdir(nested) == (deep, nested / "one.out")
    assert ops.glob(tmp_path / "**" / "*.out", recursive=False) == (nested / "one.out",)


def test_refresh_replaces_captured_file_version(tmp_path):
    ops = FileOperations()
    path = tmp_path / "result.dat"
    path.write_bytes(b"AAA")
    old = ops.capture_version(path, require_digest=True)
    assert old.available and old.digest

    path.write_bytes(b"BBB")
    new = ops.capture_version(path, require_digest=True, refresh=True)
    assert new.digest != old.digest
    assert ops.capture_version(path, require_digest=True) is new
    # Version state is always checked against current bytes.
    assert old.state() is FileState.OVERWRITTEN
    assert new.state() is FileState.CURRENT

    ops.invalidate()
    assert ops.capture_version(path, require_digest=True).digest == new.digest


def test_refreshed_stat_drops_stale_captured_version(tmp_path):
    ops = FileOperations()
    path = tmp_path / "result.dat"
    path.write_bytes(b"old")
    old = ops.capture_version(path)
    assert old.available

    path.write_bytes(b"longer")
    assert ops.stat(path, refresh=True).st_size == 6
    assert ops.capture_version(path).digest != old.digest


def test_version_capture_refresh_invalidates_inconsistent_stat(tmp_path):
    ops = FileOperations()
    path = tmp_path / "result.dat"
    path.write_bytes(b"original")
    assert ops.stat(path).st_size == 8
    old_version = ops.capture_version(path)

    path.write_bytes(b"changed-contents")
    new_version = ops.capture_version(path, refresh=True)
    assert new_version.digest != old_version.digest
    # Version capture reads fresh metadata even when a separate stat was
    # cached.  Subsequent stat must not return the older snapshot.
    assert ops.stat(path).st_size == len(b"changed-contents")


def test_uncached_version_capture_reconciles_prior_stat(tmp_path):
    ops = FileOperations()
    path = tmp_path / "result.dat"
    path.write_bytes(b"a")
    assert ops.stat(path).st_size == 1

    path.write_bytes(b"second")
    version = ops.capture_version(path)
    assert version.available
    assert ops.stat(path).st_size == 6


def test_refresh_stat_invalidates_directory_content_identity(tmp_path):
    ops = FileOperations()
    directory = tmp_path / "results"
    directory.mkdir()
    entry = directory / "file.dat"
    entry.write_bytes(b"old")
    old = ops.capture_version(directory, kind="directory")
    assert old.available

    # Changing a child need not change the directory's metadata.
    entry.write_bytes(b"new")
    ops.stat(directory, refresh=True)
    new = ops.capture_version(directory, kind="directory")
    assert new.digest != old.digest


def test_refresh_listdir_invalidates_directory_stat_and_version(tmp_path):
    ops = FileOperations()
    directory = tmp_path / "results"
    directory.mkdir()
    (directory / "file.dat").write_bytes(b"first")
    old = ops.capture_version(directory, kind="directory")
    stat = ops.stat(directory)
    assert ops.listdir(directory) == ("file.dat",)

    (directory / "file.dat").write_bytes(b"second")
    assert ops.listdir(directory, refresh=True) == ("file.dat",)
    assert ops.capture_version(directory, kind="directory").digest != old.digest
    # Cached metadata was invalidated, not incorrectly treated as fresh.
    assert ops.stat(directory) is not stat


def test_version_state_always_fresh_for_missing_and_restored_content(tmp_path):
    ops = FileOperations()
    path = tmp_path / "result.dat"
    path.write_bytes(b"old")
    version = ops.capture_version(path)
    assert version.state() is FileState.CURRENT
    path.unlink()
    assert version.state() is FileState.MISSING
    path.write_bytes(b"old")
    assert version.state() is FileState.CURRENT


def test_refresh_missing_discards_positive_cache_entries(tmp_path):
    ops = FileOperations()
    path = tmp_path / "result.dat"
    path.write_bytes(b"original")
    ops.stat(path)
    prior = ops.capture_version(path)
    path.unlink()
    assert not ops.exists(path, refresh=True)
    with pytest.raises(FileNotFoundError):
        ops.stat(path)
    assert ops.capture_version(path).fingerprint is None
    assert prior.state() is FileState.MISSING


def test_new_capture_mode_invalidates_stale_existing_version(tmp_path):
    ops = FileOperations()
    path = tmp_path / "result.dat"
    path.write_bytes(b"old")
    old_digest = ops.capture_version(path, require_digest=True)

    path.write_bytes(b"newer contents")
    # A *different* capture mode makes a fresh observation even though
    # digest=True has an older entry. It must invalidate that entry.
    refreshed_metadata = ops.capture_version(path, digest=False)
    assert refreshed_metadata.fingerprint is not None
    current_digest = ops.capture_version(path, require_digest=True)
    assert current_digest.digest != old_digest.digest
    assert current_digest.state() is FileState.CURRENT


def test_failed_strict_capture_invalidates_other_cached_versions(tmp_path):
    ops = FileOperations()
    path = tmp_path / "result.dat"
    path.write_bytes(b"old")
    assert ops.capture_version(path, digest=False).fingerprint is not None
    path.unlink()

    # The strict digest attempt must fail rather than pretending this is the
    # old file. Its fresh failure also invalidates other cached captures.
    with pytest.raises(FileNotFoundError):
        ops.capture_version(path, require_digest=True)
    assert ops.capture_version(path, digest=False).fingerprint is None


def test_missing_and_errors(tmp_path, monkeypatch):
    ops = FileOperations()
    missing = tmp_path / "missing"
    assert not ops.exists(missing)
    assert not ops.is_file(missing)
    with pytest.raises(FileNotFoundError):
        ops.stat(missing)

    def deny(path):
        raise PermissionError("inaccessible")

    monkeypatch.setattr("kkrunner.files.operations.os.stat", deny)
    with pytest.raises(PermissionError):
        ops.exists(missing, refresh=True)


def test_no_cache_mode_is_always_fresh(tmp_path):
    ops = FileOperations(cache=False)
    path = tmp_path / "result"
    path.write_bytes(b"A")
    assert ops.stat(path).st_size == 1
    path.write_bytes(b"AAA")
    assert ops.stat(path).st_size == 3
    assert ops.listdir(tmp_path) == ("result",)
    path.unlink()
    assert ops.listdir(tmp_path) == ()
    assert not ops.exists(path)

    path.write_bytes(b"start")
    before = ops.capture_version(path)
    path.write_bytes(b"other")
    assert ops.capture_version(path).digest != before.digest


def test_refresh_populates_empty_cache(tmp_path):
    ops = FileOperations()
    first = tmp_path / "first.out"
    first.write_bytes(b"A")
    assert ops.stat(first, refresh=True).st_size == 1
    captured = ops.capture_version(first, refresh=True)
    assert ops.listdir(tmp_path, refresh=True) == ("first.out",)
    assert ops.glob(tmp_path / "*.out", refresh=True) == (first,)

    first.write_bytes(b"BBBB")
    second = tmp_path / "second.out"
    second.write_bytes(b"data")
    assert ops.stat(first).st_size == 1
    assert ops.capture_version(first) is captured
    assert ops.listdir(tmp_path) == ("first.out",)
    assert ops.glob(tmp_path / "*.out") == (first,)

    assert ops.stat(first, refresh=True).st_size == 4
    assert ops.capture_version(first).digest != captured.digest
    assert ops.listdir(tmp_path, refresh=True) == ("first.out", "second.out")
    assert ops.glob(tmp_path / "*.out", refresh=True) == (first, second)


def test_version_capture_missing_is_not_cached(tmp_path):
    ops = FileOperations()
    path = tmp_path / "missing"
    missing = ops.capture_version(path)
    assert missing.fingerprint is None
    path.write_bytes(b"hello")
    assert ops.capture_version(path).available
    with pytest.raises(ValueError, match="require_digest"):
        ops.capture_version(path, digest=False, require_digest=True)


def test_symlinked_paths_keep_original_name(tmp_path):
    ops = FileOperations()
    root = tmp_path / "real"
    root.mkdir()
    (root / "value").write_bytes(b"data")
    alias = tmp_path / "alias"
    try:
        alias.symlink_to(root, target_is_directory=True)
    except OSError:
        pytest.skip("Symlinks not supported")
    assert ops.iterdir(alias) == (alias / "value",)
    assert ops.capture_version(alias / "value").path == alias / "value"
    assert ops.fingerprint(alias / "value") == ops.fingerprint(root / "value")
