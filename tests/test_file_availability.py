"""Contracts shared by local project and PSI/J artifact discovery."""

from kkrunner.files.availability import discover_local_files, prepare_target_outputs


def _prepare_outputs(outputs=(), *, protected_paths=(), stdout=None):
    """Invoke the single target-side operation used by every backend."""
    return prepare_target_outputs({
        "output_file": None if stdout is None else str(stdout),
        "error_file": None,
        "output_files": [str(p) for p in outputs],
        "cache_files": [str(p) for p in protected_paths],
        "input_files": [], "stdin": None, "input_file": None,
    })
from kkrunner.files.manifest import CacheEntry, FileEntry, FileManifest


def test_discovers_outputs_and_caches_with_independent_pattern_children(tmp_path):
    (tmp_path / "out-a.dat").write_bytes(b"a")
    (tmp_path / "cache-a.pot").write_bytes(b"old")
    manifest = FileManifest(
        outputs=(FileEntry(local_path=str(tmp_path / "out-*.dat"), availability_state="pending"),),
        caches=(CacheEntry(FileEntry(local_path=str(tmp_path / "cache-*.pot"), availability_state="pending")),),
    )
    found = discover_local_files(manifest)
    assert len(found.outputs) == 2
    assert found.outputs[0].availability_state == "available"
    assert found.outputs[1].source_pattern == str(tmp_path / "out-*.dat")
    assert len(found.caches) == 2
    assert found.caches[0].file.availability_state == "available"
    assert found.caches[1].file.source_pattern == str(tmp_path / "cache-*.pot")
    assert discover_local_files(found) == found


def test_missing_optional_files_are_skipped_without_creating_them(tmp_path):
    missing = tmp_path / "missing.out"
    files = FileManifest(outputs=(FileEntry(local_path=str(missing), optional=True, availability_state="pending"),))
    found = discover_local_files(files)
    assert found.outputs[0].availability_state == "skipped"
    assert not missing.exists()


def test_local_supervisor_discovery_does_not_override_stream_publication(tmp_path):
    output = tmp_path / "previous.out"
    output.write_bytes(b"stale")
    files = FileManifest(
        stdout=FileEntry(local_path=str(output), availability_state="pending"),
        outputs=(FileEntry(local_path=str(tmp_path / "other.out"), optional=True, availability_state="pending"),),
    )
    found = discover_local_files(files, include_streams=False)
    assert found.stdout.availability_state == "pending"
    assert found.outputs[0].availability_state == "skipped"


def test_clear_outputs_preserves_inputs_and_cache(tmp_path):

    input_file = tmp_path / "input.dat"
    cache_file = tmp_path / "cache.dat"
    output_file = tmp_path / "output.dat"
    other = tmp_path / "result-2.dat"
    for path in (input_file, cache_file, output_file, other):
        path.write_text("old")
    _prepare_outputs((tmp_path / "*.dat",), protected_paths=(input_file, cache_file))
    assert input_file.read_text() == "old"
    assert cache_file.read_text() == "old"
    assert not output_file.exists()
    assert not other.exists()


def test_clear_outputs_preserves_wildcard_cache_matches(tmp_path):

    cache = tmp_path / "restart-1.pot"
    output = tmp_path / "result-1.pot"
    cache.write_bytes(b"restart state")
    output.write_bytes(b"stale result")
    _prepare_outputs((tmp_path / "*.pot",), protected_paths=(tmp_path / "restart-*.pot",))
    assert cache.read_bytes() == b"restart state"
    assert not output.exists()


def test_local_wildcard_discovery_uses_same_glob_semantics_as_transport(tmp_path):
    from kkrunner.files.paths import expand_local_candidates

    hidden = tmp_path / ".hidden"
    hidden.mkdir()
    (hidden / "invisible.dat").write_text("hidden")
    shown = tmp_path / "shown"
    shown.mkdir()
    (shown / "file.dat").write_text("visible")
    declaration = str(tmp_path / "**" / "*.dat")
    assert expand_local_candidates(declaration) == (str(shown / "file.dat"),)
    manifest = FileManifest(outputs=(FileEntry(declaration, availability_state="pending"),))
    discovered = discover_local_files(manifest)
    assert [entry.local_path for entry in discovered.outputs] == [declaration, str(shown / "file.dat")]


def test_cleanup_unlinks_symlink_without_deleting_its_target(tmp_path):

    outside = tmp_path / "outside"
    outside.mkdir()
    original = outside / "original.dat"
    original.write_text("important")
    working = tmp_path / "working"
    working.mkdir()
    link = working / "result.dat"
    link.symlink_to(original)
    _prepare_outputs((working / "*.dat",))
    assert not link.is_symlink()
    assert original.read_text() == "important"


def test_protected_output_stream_matches_cache_glob(tmp_path):
    import pytest


    protected = tmp_path / "restart-1.pot"
    protected.write_text("density")
    with pytest.raises(ValueError, match="conflicts with input/cache"):
        _prepare_outputs(protected_paths=(tmp_path / "restart-*.pot",), stdout=protected)


def test_explicit_output_collision_preserves_protected_file(tmp_path):
    import pytest


    input_path = tmp_path / "input.dat"
    input_path.write_bytes(b"input")
    with pytest.raises(ValueError, match="conflicts with input/cache"):
        _prepare_outputs(protected_paths=(input_path,), stdout=input_path)
    assert input_path.read_bytes() == b"input"


def test_output_pattern_can_skip_protected_files(tmp_path):

    keep = tmp_path / "restart.pot"
    drop = tmp_path / "old.pot"
    keep.write_text("restart")
    drop.write_text("old")
    _prepare_outputs((tmp_path / "*.pot",), protected_paths=(keep,))
    _prepare_outputs((tmp_path / "*.pot",), protected_paths=(keep,))
    assert keep.read_text() == "restart"
    assert not drop.exists()


def test_protected_output_rejects_symlink_alias(tmp_path):
    import pytest


    source = tmp_path / "source.dat"
    source.write_bytes(b"density")
    alias = tmp_path / "alias.out"
    alias.symlink_to(source)
    with pytest.raises(ValueError, match="conflicts with input/cache"):
        _prepare_outputs((alias,), protected_paths=(source,))
    assert source.read_bytes() == b"density"


def test_protected_output_rejects_hardlink_alias(tmp_path):
    import os

    import pytest


    source = tmp_path / "source.dat"
    source.write_bytes(b"density")
    alias = tmp_path / "alias.out"
    os.link(source, alias)
    with pytest.raises(ValueError, match="conflicts with input/cache"):
        _prepare_outputs((alias,), protected_paths=(source,))
    assert source.read_bytes() == b"density"


def test_collision_expands_protected_wildcard_once_for_multiple_outputs(tmp_path, monkeypatch):
    import os

    import pytest


    protected_file = tmp_path / "restart-1.pot"
    protected_file.write_bytes(b"restart")
    harmless_output = tmp_path / "output-1.dat"
    harmless_output.write_bytes(b"old result")
    alias_output = tmp_path / "output-2.dat"
    os.link(protected_file, alias_output)
    pattern = tmp_path / "restart-*.pot"
    import glob
    real_expand = glob.glob
    expanded = []

    def observe_expand(path, *args, **kwargs):
        expanded.append(os.fspath(path))
        return real_expand(path, *args, **kwargs)

    monkeypatch.setattr(glob, "glob", observe_expand)
    with pytest.raises(ValueError, match="conflicts with input/cache"):
        _prepare_outputs((harmless_output, alias_output), protected_paths=(pattern, pattern))
    assert expanded == [os.path.abspath(pattern)]
    assert protected_file.read_bytes() == b"restart"



def test_cleanup_preserves_contents_of_protected_directory(tmp_path):
    import pytest

    directory = tmp_path / "inputs"
    directory.mkdir()
    protected = directory / "data.txt"
    protected.write_text("input")
    unrelated = tmp_path / "old.txt"
    unrelated.write_text("old")
    with pytest.raises(ValueError, match="conflicts with input/cache"):
        _prepare_outputs((unrelated, protected), protected_paths=(directory,))
    assert protected.read_text() == "input"
    assert unrelated.read_text() == "old"
    _prepare_outputs((tmp_path / "**" / "*.txt",), protected_paths=(directory,))
    assert protected.read_text() == "input"
    assert not unrelated.exists()



def test_directory_output_cannot_remove_declared_input_symlink(tmp_path):
    import pytest

    data = tmp_path / "source.txt"
    data.write_text("source")
    output = tmp_path / "results"
    output.mkdir()
    alias = output / "input.txt"
    alias.symlink_to(data)
    with pytest.raises(ValueError, match="Directory output contains input/cache"):
        prepare_target_outputs({
            "directory": str(tmp_path),
            "input_files": [str(alias)],
            "output_files": [str(output)],
            "output_directories": [str(output)],
        })
    assert alias.is_symlink()
    assert alias.read_text() == "source"


def test_output_glob_does_not_republish_failed_stdout(tmp_path):
    output = tmp_path / "failed.dat"
    output.write_text("stale")
    result = tmp_path / "result.dat"
    result.write_text("result")
    files = FileManifest(
        stdout=FileEntry(str(output), availability_state="failed",
                         availability_message="not published"),
        outputs=(FileEntry(str(tmp_path / "*.dat"), availability_state="pending"),),
    )
    found = discover_local_files(files, include_streams=False)
    assert found.stdout == files.stdout
    assert found.outputs[0].availability_state == "available"
    assert any(entry.local_path == str(result) and entry.availability_state == "available"
               for entry in found.outputs)


def test_captured_pattern_members_are_literal_names(tmp_path):
    from dataclasses import replace
    from kkrunner.files.availability import capture_result_files

    literal = tmp_path / "result[1].dat"
    literal.write_text("literal")
    pattern = str(tmp_path / "result*.dat")
    declared = FileManifest(outputs=(FileEntry(pattern),)).initialize_availability()
    discovered = discover_local_files(declared)

    # This later file would match an erroneous second expansion of the
    # already discovered name, but is not part of the discovered result.
    unrelated = tmp_path / "result1.dat"
    unrelated.write_text("unrelated")
    for manifest in (discovered, replace(discovered, remote_patterns_resolved=True)):
        captured = capture_result_files(manifest)
        assert tuple(version.path for version in captured.output_versions) == (literal,)
        assert captured.output_versions[0].is_current()



def test_published_artifact_cannot_disappear_during_version_capture(tmp_path):
    import pytest
    from kkrunner.files.availability import capture_result_files

    for kind, source_pattern in (("file", None), ("file", tmp_path / "*.dat"), ("directory", None)):
        path = tmp_path / "result.dat"
        if kind == "directory":
            path.mkdir()
        else:
            path.write_text("result")
        entry = FileEntry(path, availability_state="available",
                          source_pattern=source_pattern, kind=kind)
        files = FileManifest(outputs=(entry,))
        assert len(capture_result_files(files).output_versions) == 1
        if kind == "directory":
            path.rmdir()
        else:
            path.unlink()
        with pytest.raises(OSError):
            capture_result_files(files)


def test_missing_unpublished_input_can_still_be_omitted_from_snapshot(tmp_path):
    from kkrunner.files.availability import snapshot_file_entries

    assert snapshot_file_entries((FileEntry(tmp_path / "optional", optional=True),)) == ()



def test_local_input_capture_uses_same_members_as_durability_barrier(tmp_path, monkeypatch):
    from kkrunner import ExecutionSpec
    from kkrunner.client import ExecutionClient
    from kkrunner.files import availability

    original = tmp_path / "input-first.dat"
    original.write_text("first")
    later = tmp_path / "input-later.dat"
    sync = availability.sync_file
    synced = []

    def synchronize(path):
        sync(path)
        synced.append(path)
        later.write_text("not selected during preparation")

    monkeypatch.setattr(availability, "sync_file", synchronize)
    session = ExecutionClient().file_session(ExecutionSpec(
        ("true",), directory=tmp_path, input_files=("input-*.dat",),
    ))
    try:
        session.prepare_submission()
        assert tuple(version.path for version in session.files.input_versions) == (original,)
        assert synced == [str(original)]
        assert later.exists()
    finally:
        session.close()



def test_primary_input_and_stdin_are_literal_during_output_cleanup(tmp_path):
    for field in ("input_file", "stdin"):
        source = tmp_path / "input[1].dat"
        stale = tmp_path / "input1.dat"
        source.write_text("input")
        stale.write_text("stale")
        prepare_target_outputs({
            "directory": str(tmp_path),
            field: str(source),
            "output_files": [str(tmp_path / "*.dat")],
        })
        assert source.read_text() == "input"
        assert not stale.exists()



def test_missing_literal_primary_or_stdin_still_protects_its_name(tmp_path):
    import pytest

    for field in ("input_file", "stdin"):
        source = tmp_path / "input[1].dat"
        with pytest.raises(ValueError, match="Output stream conflicts"):
            prepare_target_outputs({
                "directory": str(tmp_path),
                field: str(source),
                "output_file": str(source),
            })
        assert not source.exists()


def test_input_stat_error_does_not_authorize_output_cleanup(tmp_path, monkeypatch):
    import os
    import pytest

    source = tmp_path / "input.dat"
    source.write_text("input")
    output = tmp_path / "old.dat"
    output.write_text("old")
    original_stat = os.stat

    def stat(path, *args, **kwargs):
        if os.fspath(path) == str(source):
            raise PermissionError("cannot inspect input")
        return original_stat(path, *args, **kwargs)

    monkeypatch.setattr(os, "stat", stat)
    with pytest.raises(PermissionError, match="cannot inspect input"):
        prepare_target_outputs({
            "directory": str(tmp_path),
            "input_files": [str(source)],
            "output_files": [str(output)],
        })
    assert output.read_text() == "old"



def test_capture_named_stream_does_not_expand_its_name(tmp_path):
    from kkrunner.files.availability import capture_result_files

    source = tmp_path / "out[1].dat"
    source.write_text("stream")
    (tmp_path / "out1.dat").write_text("unrelated")
    files = FileManifest(stdout=FileEntry(source, availability_state="pending"))
    captured = capture_result_files(files)
    assert captured.stdout.availability_state == "available"
    assert captured.outputs == ()
    assert tuple(version.path for version in captured.output_versions) == (source,)
    assert captured.output_versions[0].is_current()


def test_local_input_capture_treats_primary_and_stdin_as_literal(tmp_path):
    from kkrunner import ExecutionSpec
    from kkrunner.client import ExecutionClient

    source = tmp_path / "input[1].dat"
    source.write_text("input")
    (tmp_path / "input1.dat").write_text("unrelated")
    session = ExecutionClient().file_session(ExecutionSpec(
        ("true",), directory=tmp_path, input_file=source, stdin=source,
    ))
    try:
        session.prepare_submission()
        assert tuple(version.path for version in session.files.input_versions) == (source,)
    finally:
        session.close()



def test_output_exclusions_use_literal_directory_identity(tmp_path):
    directory = tmp_path / "cache[1]"
    other = tmp_path / "cache1"
    files = FileManifest(caches=(CacheEntry(FileEntry(directory, kind="directory")),))
    assert files.output_excluded(directory)
    assert files.output_excluded(directory / ".hidden" / "data")
    assert not files.output_excluded(other / "data")
    assert not files.output_excluded(tmp_path / "cache[1]-other" / "data")


def test_output_glob_excludes_cache_symlink_by_its_declared_name(tmp_path):
    target = tmp_path / "source.txt"
    target.write_text("cache data")
    link = tmp_path / "cache-link.dat"
    link.symlink_to(target)
    result = tmp_path / "result.dat"
    result.write_text("output data")
    files = FileManifest(
        outputs=(FileEntry(tmp_path / "*.dat"),),
        caches=(CacheEntry(FileEntry(tmp_path / "cache-*.dat")),),
    )
    assert files.output_excluded(link)
    assert not files.output_excluded(result)
    found = discover_local_files(files)
    assert {entry.local_path for entry in found.outputs if entry.source_pattern is not None} == {str(result)}
    assert {entry.file.local_path for entry in found.caches if entry.file.source_pattern is not None} == {str(link)}

