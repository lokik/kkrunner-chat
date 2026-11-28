"""File reservations distinguish execution from client-side synchronization."""

from dataclasses import replace

import pytest

from kkrunner import ExecutionSpec, RunManager, RunStore
from kkrunner.client import SshTarget
from kkrunner.files.manifest import CacheEntry, FileEntry, FileManifest, FileMapping
from kkrunner.runs.conflicts import conflicting_files, submission_conflicts


@pytest.mark.parametrize("kind", ["stdout", "stderr", "output", "cache"])
@pytest.mark.parametrize("policy", ["never", "changed"])
def test_remote_output_reservation_follows_local_transfer_policy(tmp_path, kind, policy):
    manager = RunManager(RunStore(tmp_path / "runs"))

    def run(host):
        entry = FileEntry(str(tmp_path / "result"), "/work/result")
        declarations = {
            "stdout": {"stdout": entry},
            "stderr": {"stderr": entry},
            "output": {"outputs": (entry,)},
            "cache": {"caches": (CacheEntry(entry),)},
        }[kind]
        return manager.new_record(
            directory=tmp_path, remote_directory="/work", target=SshTarget(host),
            files=FileManifest(
                mapping=FileMapping(str(tmp_path), "/work", output_sync=policy),
                **declarations,
            ),
        )

    first, second = run("first"), run("second")
    assert submission_conflicts(first, second) == (policy != "never")
    assert bool(conflicting_files(first, second)) == (policy != "never")
    # Two runs writing the same file on one target still conflict.
    assert submission_conflicts(first, run("first"))


def test_uncollected_output_reserves_target_not_local_path(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    entry = FileEntry(str(tmp_path / "scratch"), "/work/scratch", collect=False)
    remote = manager.new_record(
        directory=tmp_path, remote_directory="/work", target=SshTarget("first"),
        files=FileManifest(outputs=(entry,), mapping=FileMapping(str(tmp_path), "/work")),
    )
    local = manager.new_record(
        directory=tmp_path, files=FileManifest(outputs=(replace(entry, remote_path=None),)),
    )
    assert not submission_conflicts(remote, local)
    assert conflicting_files(remote, local) == ()
    same_target = replace(remote, id="other")
    assert submission_conflicts(remote, same_target)
    # A local calculation still produces its uncollected file.
    assert submission_conflicts(local, replace(local, id="other-local"))


def test_explicit_local_write_is_reserved_even_without_transfer(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    path = tmp_path / "out"
    with path.open("wb") as stream:
        manifest = FileManifest.from_spec(ExecutionSpec(("true",), directory=str(tmp_path), output_file=stream))
        manifest = replace(
            manifest, stdout=replace(manifest.stdout, remote_path="/work/out"),
            mapping=FileMapping(str(tmp_path), "/work", output_sync="never"),
        )
        remote = manager.new_record(
            directory=tmp_path, remote_directory="/work", target=SshTarget("first"), files=manifest,
        )
        local = manager.new_record(
            directory=tmp_path, files=FileManifest(inputs=(FileEntry(str(path)),)),
        )
        assert submission_conflicts(remote, local)
        assert any(item.first_access == "write" and item.namespace == "local"
                   for item in conflicting_files(remote, local))


def test_shared_filesystem_reserves_local_producer_paths(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    entry = FileEntry(str(tmp_path / "scratch"), "/work/scratch", collect=False)
    mapping = FileMapping(str(tmp_path), "/work", shared_filesystem=True)
    remote = manager.new_record(
        directory=tmp_path, remote_directory="/work", target=SshTarget("first"),
        files=FileManifest(outputs=(entry,), mapping=mapping),
    )
    local = manager.new_record(
        directory=tmp_path, files=FileManifest(inputs=(FileEntry(entry.local_path),)),
    )
    assert submission_conflicts(remote, local)
    assert any(item.first_access == "write" and item.namespace == "local"
               for item in conflicting_files(remote, local))
    other_mount = replace(remote, id="other", execution=replace(
        remote.execution, target=SshTarget("second")))
    assert submission_conflicts(remote, other_mount)


@pytest.mark.parametrize("policy", ["never", "changed"])
def test_collection_reserves_remote_reads_and_local_transfers(tmp_path, policy):
    from kkrunner import RunFileState, RunState

    manager = RunManager(RunStore(tmp_path / "runs"))
    target = SshTarget("cluster")
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    record = manager.new_record(
        directory=source, remote_directory="/work", target=target,
        files=FileManifest(
            inputs=(FileEntry(source / "inp", "/work/inp"),),
            outputs=(FileEntry(source / "out", "/work/out"),
                     FileEntry(source / "scratch", "/work/scratch", collect=False)),
            mapping=FileMapping(source, "/work", output_sync=policy),
        ),
    )
    collecting = replace(record, state=RunState.FINISHED, file_state=RunFileState.COLLECTING)

    def remote(path, *, write):
        entry = FileEntry(destination / path, "/work/" + path)
        return manager.new_record(
            directory=destination, remote_directory="/work", target=target,
            files=FileManifest(
                mapping=FileMapping(destination, "/work", input_sync="never"),
                **({"outputs": (entry,)} if write else {"inputs": (entry,)}),
            ),
        )

    # The producer is no longer using its target inputs or uncollected scratch.
    for path in ("inp", "scratch"):
        assert submission_conflicts(record, remote(path, write=True))
        assert not submission_conflicts(collecting, remote(path, write=True))
        assert not conflicting_files(collecting, remote(path, write=True))

    assert not submission_conflicts(collecting, remote("out", write=False))
    assert submission_conflicts(collecting, remote("out", write=True))
    assert any(item.first_access == "read" and item.namespace == "target"
               for item in conflicting_files(collecting, remote("out", write=True)))

    def local(path, *, write):
        entry = FileEntry(source / path)
        return manager.new_record(
            directory=source,
            files=FileManifest(**({"outputs": (entry,)} if write else {"inputs": (entry,)})),
        )

    assert submission_conflicts(collecting, local("out", write=False)) == (policy != "never")
    assert submission_conflicts(collecting, local("out", write=True))
    assert submission_conflicts(collecting, local("inp", write=True))
    assert not submission_conflicts(collecting, local("scratch", write=True))


@pytest.mark.parametrize("shared", [False, True])
def test_local_collection_reads_published_files(tmp_path, shared):
    from kkrunner import RunFileState, RunState

    manager = RunManager(RunStore(tmp_path / "runs"))
    output = FileEntry(tmp_path / "out", "/work/out" if shared else None)
    if shared:
        record = manager.new_record(
            directory=tmp_path, remote_directory="/work", target=SshTarget("cluster"),
            files=FileManifest(outputs=(output,), mapping=FileMapping(tmp_path, "/work", shared_filesystem=True)),
        )
    else:
        record = manager.new_record(directory=tmp_path, files=FileManifest(outputs=(output,)))
    collecting = replace(record, state=RunState.FINISHED, file_state=RunFileState.COLLECTING)
    reader = manager.new_record(directory=tmp_path, files=FileManifest(inputs=(FileEntry(tmp_path / "out"),)))
    writer = manager.new_record(directory=tmp_path, files=FileManifest(outputs=(FileEntry(tmp_path / "out"),)))
    assert not submission_conflicts(collecting, reader)
    assert submission_conflicts(collecting, writer)
    if shared:
        other = manager.new_record(
            directory=tmp_path / "other", remote_directory="/work", target=SshTarget("cluster"),
            files=FileManifest(outputs=(FileEntry(tmp_path / "other/out", "/work/out"),),
                               mapping=FileMapping(tmp_path / "other", "/work", shared_filesystem=True)),
        )
        assert submission_conflicts(collecting, other)
