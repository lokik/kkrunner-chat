import asyncio

import pytest

from kkrunner.files.manifest import FileSyncPolicy
from kkrunner.runs.base import SubmissionNotStarted

from .test_remote import _processed_run, _text_run


def test_remote_executor_expands_input_wildcards(tmp_path):
    workdir = tmp_path / "local" / "calculation"
    workdir.mkdir(parents=True)
    (workdir / "file-a.pot").write_text("a")
    (workdir / "file-b.pot").write_text("b")
    (workdir / "ignored.txt").write_text("ignored")
    executor, spec, _, remote_root = _text_run(
        tmp_path,
        declared_input_files=(workdir / "file*.pot",),
        code="import pathlib; print(':'.join(pathlib.Path(name).read_text() for name in ('file-a.pot', 'file-b.pot')))",
    )
    result = _processed_run(executor, spec).run()
    assert result.output == b"a:b\n"
    remote_workdir = remote_root / "calculation"
    assert (remote_workdir / "file-a.pot").read_text() == "a"
    assert (remote_workdir / "file-b.pot").read_text() == "b"
    assert not (remote_workdir / "ignored.txt").exists()


def test_remote_executor_rejects_unmatched_input_wildcard(tmp_path):
    workdir = tmp_path / "local" / "calculation"
    executor, spec, _, _ = _text_run(tmp_path, declared_input_files=(workdir / "missing*.pot",))
    try:
        _processed_run(executor, spec).run()
    except SubmissionNotStarted as exc:
        assert "input pattern did not match" in str(exc)
    else:
        raise AssertionError("unmatched input wildcard was accepted")


def test_remote_executor_downloads_output_wildcards_without_deleting_local_matches(tmp_path):
    workdir = tmp_path / "local" / "calculation"
    workdir.mkdir(parents=True)
    (workdir / "artifact-stale.dat").write_text("old local result")
    remote_workdir = tmp_path / "remote" / "calculation"
    remote_workdir.mkdir(parents=True)
    (remote_workdir / "artifact-stale.dat").write_text("stale")
    executor, spec, _, _ = _text_run(
        tmp_path,
        declared_output_files=(workdir / "artifact-*.dat",),
        code="import pathlib; pathlib.Path('artifact-a.dat').write_text('a'); pathlib.Path('artifact-b.dat').write_text('b'); print('done')",
    )
    result = _processed_run(executor, spec).run()
    assert result.return_code == 0
    assert (workdir / "artifact-a.dat").read_text() == "a"
    assert (workdir / "artifact-b.dat").read_text() == "b"
    assert (workdir / "artifact-stale.dat").read_text() == "old local result"
    assert not (remote_workdir / "artifact-stale.dat").exists()


def test_remote_executor_syncs_cache_wildcards_in_both_directions(tmp_path):
    workdir = tmp_path / "local" / "calculation"
    workdir.mkdir(parents=True)
    (workdir / "cache-a.dat").write_text("old")
    executor, spec, _, remote_root = _text_run(
        tmp_path,
        declared_cache_files=(workdir / "cache-*.dat",),
        code="import pathlib; a=pathlib.Path('cache-a.dat'); assert a.read_text() == 'old'; a.write_text('new'); pathlib.Path('cache-b.dat').write_text('created'); print('done')",
    )
    result = _processed_run(executor, spec).run()
    assert result.return_code == 0
    assert (workdir / "cache-a.dat").read_text() == "new"
    assert (workdir / "cache-b.dat").read_text() == "created"
    assert (remote_root / "calculation" / "cache-a.dat").read_text() == "new"
    assert (remote_root / "calculation" / "cache-b.dat").read_text() == "created"


def test_exact_and_wildcard_outputs_share_availability_tracking(tmp_path):
    from .test_remote import _SpyTransport

    transport = _SpyTransport()
    workdir = tmp_path / "local" / "calculation"
    exact = workdir / "bands.dat"
    overlap = workdir / "part-a.dat"
    pattern = workdir / "part-*.dat"
    executor, spec, _, remote_root = _text_run(
        tmp_path,
        transport,
        declared_output_files=(exact, overlap, pattern),
        code="from pathlib import Path; Path('bands.dat').write_text('exact'); Path('part-a.dat').write_text('a'); Path('part-b.dat').write_text('b'); Path('part-c.dat').write_text('c')",
    )
    runner = _processed_run(executor, spec)
    runner.run()
    entries = {entry.local_path: entry for entry in runner.files.outputs}
    assert exact.read_text() == "exact"
    assert overlap.read_text() == "a"
    for name, expected in (("part-b.dat", "b"), ("part-c.dat", "c")):
        path = workdir / name
        assert path.read_text() == expected
        assert entries[str(path)].availability_state == "available"
        assert entries[str(path)].source_pattern == str(pattern)
    assert entries[str(exact)].availability_state == "available"
    assert entries[str(overlap)].availability_state == "available"
    assert entries[str(pattern)].availability_state == "available"
    remote_workdir = remote_root / "calculation"
    for name in ("bands.dat", "part-a.dat", "part-b.dat", "part-c.dat"):
        assert sum((src == remote_workdir / name for src, _ in transport.downloads)) == 1


def test_exact_and_wildcard_caches_keep_individual_sync_states(tmp_path):
    workdir = tmp_path / "local" / "calculation"
    workdir.mkdir(parents=True)
    exact = workdir / "restart.dat"
    unchanged = workdir / "generated-base.dat"
    pattern = workdir / "generated-*.dat"
    exact.write_text("before")
    unchanged.write_text("unchanged")
    executor, spec, _, _ = _text_run(
        tmp_path,
        declared_cache_files=(exact, pattern),
        output_files=FileSyncPolicy.MISSING,
        code="from pathlib import Path; Path('restart.dat').write_text('after'); Path('generated-new.dat').write_text('new')",
    )
    runner = _processed_run(executor, spec)
    runner.run()
    entries = {entry.file.local_path: entry.file for entry in runner.files.caches}
    assert exact.read_text() == "before"
    assert unchanged.read_text() == "unchanged"
    assert (workdir / "generated-new.dat").read_text() == "new"
    assert entries[str(exact)].availability_state == "skipped"
    assert entries[str(unchanged)].availability_state == "skipped"
    assert entries[str(pattern)].availability_state == "available"
    new_entry = entries[str(workdir / "generated-new.dat")]
    assert new_entry.availability_state == "available"
    assert new_entry.source_pattern == str(pattern)


def test_remote_executor_tolerates_unmatched_optional_cache_wildcard(tmp_path):
    workdir = tmp_path / "local" / "calculation"
    cache_pattern = workdir / "not-present-*.pot"
    executor, spec, _, remote_root = _text_run(tmp_path, declared_cache_files=(cache_pattern,), code="print('done')")
    runner = _processed_run(executor, spec)
    result = runner.run()
    assert result.return_code == 0
    assert result.output == b"done\n"
    assert not tuple(workdir.glob("not-present-*.pot"))
    assert not tuple((remote_root / "calculation").glob("not-present-*.pot"))


def test_overlapping_exact_and_wildcard_caches_are_transferred_once(tmp_path):
    from .test_remote import _SpyTransport

    transport = _SpyTransport()
    workdir = tmp_path / "local" / "calculation"
    workdir.mkdir(parents=True)
    exact = workdir / "restart-a.dat"
    exact.write_text("old")
    pattern = workdir / "restart-*.dat"
    executor, spec, _, remote_root = _text_run(
        tmp_path,
        transport,
        declared_cache_files=(exact, pattern),
        code="from pathlib import Path; Path('restart-a.dat').write_text('new'); Path('restart-b.dat').write_text('created')",
    )
    runner = _processed_run(executor, spec)
    runner.run()
    remote_a = remote_root / "calculation" / "restart-a.dat"
    remote_b = remote_root / "calculation" / "restart-b.dat"
    assert sum((src == remote_a for src, _ in transport.downloads)) == 1
    assert sum((src == remote_b for src, _ in transport.downloads)) == 1
    entries = {item.file.local_path: item.file for item in runner.files.caches}
    assert entries[str(exact)].availability_state == "available"
    assert entries[str(pattern)].availability_state == "available"
    assert exact.read_text() == "new"


def test_missing_optional_scf_startup_pattern_is_not_fatal(tmp_path):
    """Successful remote SCF need not emit the optional startup DOS."""
    from kkrunner import TaskFile

    workdir = tmp_path / "local" / "calculation"
    executor, spec, _, _ = _text_run(
        tmp_path, declared_output_files=(TaskFile(workdir / "*_SCFSTART.dos", optional=True),), code="print('done')"
    )
    runner = _processed_run(executor, spec)
    result = runner.run()
    assert result.return_code == 0


@pytest.mark.parametrize("optional_first", [True, False])
def test_overlapping_optional_and_required_wildcards_sync_each_file_once(tmp_path, optional_first):
    """Distinct wildcard requirements must share one concrete-transfer set."""
    from kkrunner import TaskFile

    from .test_remote import _SpyTransport

    transport = _SpyTransport()
    workdir = tmp_path / "local" / "calculation"
    optional = TaskFile(workdir / "part-*.dat", optional=True)
    required = TaskFile(workdir / "part-a*.dat")
    declarations = (optional, required) if optional_first else (required, optional)
    executor, spec, _, remote_root = _text_run(
        tmp_path,
        transport,
        declared_output_files=declarations,
        output_files=FileSyncPolicy.ALWAYS,
        code="from pathlib import Path; Path('part-a.dat').write_text('a'); Path('part-b.dat').write_text('b')",
    )
    runner = _processed_run(executor, spec)
    assert runner.run().return_code == 0
    for filename, expected in (("part-a.dat", "a"), ("part-b.dat", "b")):
        assert (workdir / filename).read_text() == expected
        assert sum((source == remote_root / "calculation" / filename for source, _ in transport.downloads)) == 1
    entries = {item.local_path: item for item in runner.files.outputs}
    assert entries[str(optional)].availability_state == "available"
    assert entries[str(required)].availability_state == "available"


def test_missing_optional_exact_remote_output_is_not_fatal(tmp_path):
    """Manifest optionality applies equally to explicit filenames and globs."""
    from kkrunner import TaskFile

    workdir = tmp_path / "local" / "calculation"
    optional_file = workdir / "optional-result.dat"
    executor, spec, _, _ = _text_run(
        tmp_path, declared_output_files=(TaskFile(optional_file, optional=True),), code="print('done')"
    )
    runner = _processed_run(executor, spec)
    result = runner.run()
    assert result.return_code == 0
    entries = {entry.local_path: entry for entry in runner.files.outputs}
    assert entries[str(optional_file)].availability_state == "skipped"


@pytest.mark.parametrize("pattern", [False, True])
def test_remote_missing_optional_input_is_not_staged(tmp_path, pattern):
    """A declared optional input is not sent when neither side has it."""
    from kkrunner import TaskFile

    workdir = tmp_path / "local" / "calculation"
    pathname = "optional-*.pot" if pattern else "optional.pot"
    missing = workdir / pathname
    executor, spec, _, remote_root = _text_run(
        tmp_path, declared_input_files=(TaskFile(missing, optional=True),), code="print('ok')"
    )
    runner = _processed_run(executor, spec)
    assert runner.run().return_code == 0
    assert not tuple((remote_root / "calculation").glob("optional*.pot"))


def test_mandatory_input_alias_cannot_be_made_optional(tmp_path):
    """A required input wins over an optional declaration of the same file."""
    from kkrunner import TaskFile

    workdir = tmp_path / "local" / "calculation"
    missing = workdir / "mandatory.pot"
    executor, spec, _, _ = _text_run(tmp_path, declared_input_files=(TaskFile(missing, optional=True), missing))
    with pytest.raises(ValueError, match="Conflicting optionality"):
        _processed_run(executor, spec).run()


def test_remote_collects_exact_cache_created_during_run(tmp_path):
    """An absent cache is not an input, but can become a new output."""
    from .test_remote import _SpyTransport

    transport = _SpyTransport()
    workdir = tmp_path / "local" / "calculation"
    cache_file = workdir / "fresh-cache.dat"
    executor, spec, _, remote_root = _text_run(
        tmp_path,
        transport,
        declared_cache_files=(cache_file,),
        code="from pathlib import Path; Path('fresh-cache.dat').write_text('fresh')",
    )
    runner = _processed_run(executor, spec)
    assert runner.run().return_code == 0
    assert cache_file.read_text() == "fresh"
    remote_cache = remote_root / "calculation" / "fresh-cache.dat"
    assert all((source != cache_file for source, _ in transport.uploads))
    assert sum((source == remote_cache for source, _ in transport.downloads)) == 1
    entries = {entry.file.local_path: entry.file for entry in runner.files.caches}
    assert entries[str(cache_file)].availability_state == "available"


def test_wildcard_sync_uses_one_exact_batch_in_each_direction(tmp_path):
    from pathlib import Path
    from .test_remote import _OperationLoopbackTransport

    batches = []

    class Observed(_OperationLoopbackTransport):
        async def put_files(self, files, source_directory, destination_directory):
            batches.append(("put", tuple(files)))
            return await super().put_files(files, source_directory, destination_directory)

        async def get_files(self, files, source_directory, destination_directory):
            batches.append(("get", tuple(files)))
            return await super().get_files(files, source_directory, destination_directory)

    workdir = tmp_path / "local" / "calculation"
    workdir.mkdir(parents=True)
    for name in ("tau-one.dat", "tau-two.dat"):
        (workdir / name).write_text(name)
    executor, spec, _, remote_root = _text_run(
        tmp_path, Observed(), declared_cache_files=(workdir / "tau-*.dat",),
        code="from pathlib import Path; [p.write_text('updated') for p in Path('.').glob('tau-*.dat')]",
    )
    _processed_run(executor, spec).run()
    names = ("tau-one.dat", "tau-two.dat")
    expected = {(workdir / name, remote_root / "calculation" / name) for name in names}
    assert [kind for kind, _ in batches] == ["put", "get"]
    assert {tuple(map(Path, pair)) for pair in batches[0][1]} == expected
    assert {tuple(map(Path, pair)) for pair in batches[1][1]} == {(remote, local) for local, remote in expected}
    assert all((workdir / name).read_text() == "updated" for name in names)


def test_never_policy_does_not_claim_missing_wildcard_copies_are_available(tmp_path):
    from .test_remote import _SpyTransport

    transport = _SpyTransport()
    workdir = tmp_path / "local" / "calculation"
    pattern = workdir / "result-*.dat"
    executor, spec, _, _ = _text_run(
        tmp_path, transport, declared_output_files=(pattern,), output_files="never",
        code="from pathlib import Path; Path('result-a.dat').write_text('remote')",
    )
    runner = _processed_run(executor, spec)
    assert runner.run().return_code == 0
    assert not (workdir / "result-a.dat").exists()
    assert transport.downloads == []
    assert all(entry.availability_state == "skipped" for entry in runner.files.outputs)


def test_retry_does_not_publish_disappeared_wildcard_child(tmp_path):
    from kkrunner import ExecutionSpec, RunState
    from kkrunner.client import StagingSpec
    from kkrunner.client.file_context import FileContext
    from kkrunner.client.file_session import FileSession
    from kkrunner.files.manifest import FileEntry, FileManifest, FileMapping
    from kkrunner.runs.base import RunStatus

    from .test_remote import _OperationLoopbackTransport

    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    pattern = local / "result-*.dat"
    stale = local / "result-old.dat"
    stale.write_text("unrelated old local data")
    (remote / "result-new.dat").write_text("current result")

    # A previous attempt discovered the old child but failed to retrieve
    # it. Retrying resets its failed availability to pending; the remote
    # child is now gone while another member of the pattern remains.
    files = FileManifest(
        outputs=(
            FileEntry(str(pattern), str(remote / pattern.name)),
            FileEntry(str(stale), str(remote / stale.name), source_pattern=str(pattern)),
        ),
        mapping=FileMapping(str(local), str(remote)),
    ).initialize_availability()
    staging = StagingSpec.from_mapping(files.mapping)
    context = FileContext(_OperationLoopbackTransport(), staging)
    session = FileSession(
        context, ExecutionSpec(argv=None, directory=local, staging=staging),
        files=files, remote_directory=str(remote),
    )
    try:
        final = session.finalize(RunStatus(RunState.FINISHED, return_code=0))
        artifacts = {entry.local_path: entry for entry in final.outputs}
        assert artifacts[str(stale)].availability_state == "skipped"
        assert artifacts[str(pattern)].availability_state == "available"
        assert artifacts[str(local / "result-new.dat")].availability_state == "available"
        assert {version.path.name for version in final.output_versions} == {"result-new.dat"}
        assert stale.read_text() == "unrelated old local data"
        assert (local / "result-new.dat").read_text() == "current result"
    finally:
        session.close()


@pytest.mark.parametrize("kind", ["output", "cache"])
def test_mapped_wildcard_retains_original_declaration(tmp_path, kind):
    workdir = tmp_path / "local" / "calculation"
    workdir.mkdir(parents=True)
    (workdir / "alias").symlink_to(workdir, target_is_directory=True)
    pattern = workdir / "alias" / "part-*.dat"
    executor, spec, _, _ = _text_run(
        tmp_path, **{f"declared_{kind}_files": (pattern,)},
        code="from pathlib import Path; Path('part-one.dat').write_text('result')",
    )
    run = _processed_run(executor, spec)
    assert run.run().return_code == 0
    entries = run.files.outputs if kind == "output" else tuple(cache.file for cache in run.files.caches)
    declared = {entry.local_path: entry for entry in entries if entry.local_path is not None}
    assert declared[str(pattern)].availability_state == "available"
    assert str(workdir / "part-*.dat") not in declared
    concrete = workdir / "part-one.dat"
    assert declared[str(concrete)].source_pattern == str(pattern)
    assert concrete.read_text() == "result"


def test_all_local_patterns_are_resolved_before_upload(tmp_path):
    from .test_remote import _SpyTransport

    transport = _SpyTransport()
    workdir = tmp_path / "local" / "calculation"
    workdir.mkdir(parents=True)
    (workdir / "present-a.dat").write_text("input")
    executor, spec, _, _ = _text_run(
        tmp_path, transport,
        declared_input_files=(workdir / "present-*.dat", workdir / "missing-*.dat"),
    )
    with pytest.raises(SubmissionNotStarted, match="input pattern did not match"):
        _processed_run(executor, spec).run()
    assert transport.uploads == []



def test_staged_literal_wildcard_member_is_protected_during_output_cleanup(tmp_path):
    from kkrunner.files.availability import prepare_target_outputs

    workdir = tmp_path / "local" / "calculation"
    workdir.mkdir(parents=True)
    original = workdir / "file[1].dat"
    original.write_text("input")
    remote_workdir = tmp_path / "remote" / "calculation"
    remote_workdir.mkdir(parents=True)
    stale = remote_workdir / "file1.dat"
    stale.write_text("stale")
    client, spec, _, _ = _text_run(
        tmp_path, declared_input_files=("file*.dat",),
        declared_output_files=("*.dat",),
    )
    session = client.file_session(spec)
    try:
        session.prepare_submission()
        prepare_target_outputs(session.prepare_job())
        assert (remote_workdir / original.name).read_text() == "input"
        assert not stale.exists()
        assert session.files.input_version(original).is_current()
    finally:
        session.close()
