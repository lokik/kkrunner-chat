import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from kkrunner.files.manifest import FileSyncPolicy
from kkrunner.backends import SupervisedBackend
from kkrunner.client import ExecutionClient, StagingSpec, SshTarget, LocalTarget
from kkrunner.client.file_context import FileContext
from kkrunner.client.file_session import FileSession
from kkrunner.runs.base import ExecutionSpec, FailurePhase, RunState, SubmissionNotStarted
from kkrunner.runs.run import Run

from .transfer_helpers import LoopbackFileTransfer


class _OperationLoopbackTransport(LoopbackFileTransfer):
    """Local subprocess and file transport viewed as a remote SSH target."""

    host = "loopback"
    is_local = False
    python_command = (sys.executable,)
    start_operation = SshTarget.start_operation
    follow = SshTarget.follow


class _SpyTransport(_OperationLoopbackTransport):
    def __init__(self):
        super().__init__()
        self.uploads = []
        self.downloads = []

    async def put_file(self, source, destination):
        self.uploads.append((Path(source), Path(destination)))
        await super().put_file(source, destination)

    async def get_file(self, source, destination):
        self.downloads.append((Path(source), Path(destination)))
        await super().get_file(source, destination)


def _processed_run(executor, spec):
    """Add test-only result processing above the raw remote runner."""

    def process(outcome):
        output = (b"" if outcome.output_file is None or not Path(outcome.output_file).exists()
                  else Path(outcome.output_file).read_bytes())
        error = (b"" if outcome.error_file is None or not Path(outcome.error_file).exists()
                 else Path(outcome.error_file).read_bytes())
        return SimpleNamespace(return_code=outcome.return_code, output=output, error=error)

    return executor.create_run(spec, tracked=False, process_result=process)


def _executor(target=None, **kwargs):
    backend = kwargs.pop("backend", SupervisedBackend)
    return ExecutionClient(backend, target=target or _OperationLoopbackTransport(), **kwargs)


def _launch(local_root, remote_root, *, input_files=FileSyncPolicy.CHANGED, output_files=FileSyncPolicy.CHANGED):
    return StagingSpec(local_root=local_root, remote_root=remote_root, input_sync=input_files, output_sync=output_files)


def _text_run(
    tmp_path,
    target=None,
    *,
    declared_input_files=(),
    declared_output_files=(),
    declared_cache_files=(),
    code=None,
    input_files=FileSyncPolicy.CHANGED,
    output_files=FileSyncPolicy.CHANGED,
    **executor_kwargs,
):
    local_root = tmp_path / "local"
    workdir = local_root / "calculation"
    workdir.mkdir(parents=True, exist_ok=True)
    input_file = (workdir / "input.txt").open("w+b")
    input_file.write(b"hello")
    input_file.seek(0)
    output_file = workdir / "output.txt"
    remote_root = tmp_path / "remote"
    executor = _executor(target=target, **executor_kwargs)
    spec = ExecutionSpec(
        argv=(sys.executable, "-c", code or "print('ok')"),
        input_file=input_file,
        output_file=output_file,
        directory=str(workdir),
        stdin=input_file,
        input_files=(input_file, *declared_input_files),
        output_files=(output_file, *declared_output_files),
        cache_files=declared_cache_files,
        staging=_launch(local_root, remote_root, input_files=input_files, output_files=output_files),
    )
    return (executor, spec, workdir, remote_root)


def _expose_shared_input(remote_root, name, data):
    """Simulate an input already visible in the remote view of shared storage."""
    remote = remote_root / "calculation" / name
    remote.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(data, str):
        remote.write_text(data)
    else:
        remote.write_bytes(data)
    return remote


def test_remote_provenance_uses_target_input_and_final_cache_without_sync(tmp_path):
    from kkrunner.files.queries import query_files
    from kkrunner.files.versions import file_digest
    from kkrunner.runs.base import RunStatus

    class FilesOnlyTransport(_OperationLoopbackTransport):
        async def run(self, argv, **_kwargs):
            assert argv[:3] == ("mkdir", "-p", "--")
            Path(argv[3]).mkdir(parents=True, exist_ok=True)

        async def query_files(self, sources, *, root=None, digest=False):
            return query_files(sources, root=root, digest=digest)

    client, spec, local, remote = _text_run(
        tmp_path, target=FilesOnlyTransport(),
        input_files=FileSyncPolicy.NEVER, output_files=FileSyncPolicy.NEVER,
        declared_cache_files=("cache.dat",),
    )
    target_input = _expose_shared_input(remote, "input.txt", b"already on target")
    target_cache = _expose_shared_input(remote, "cache.dat", b"old cache")
    parts = client.create_run_parts(spec)
    parts.files.prepare_submission()
    version = parts.files.files.input_version(local / "input.txt")
    assert version.digest == file_digest(target_input)
    assert not version.is_current()
    assert not parts.files.files.cache_versions

    target_input.write_bytes(b"changed after launch")
    target_cache.write_bytes(b"new cache")
    manifest = parts.files.finalize(RunStatus(RunState.FINISHED, return_code=None))
    assert manifest.input_version(local / "input.txt") == version
    assert len(manifest.cache_versions) == 1
    assert manifest.cache_versions[0].path == local / "cache.dat"
    assert manifest.cache_versions[0].digest == file_digest(target_cache)


def test_remote_executor_local_transport_streams_output(tmp_path):
    input_file = (tmp_path / "input.txt").open("w+b")
    input_file.write(b"hello")
    input_file.seek(0)
    output_file = (tmp_path / "output.txt").open("wb")
    remote_root = tmp_path / "remote"
    executor = _executor()
    spec = ExecutionSpec(
        argv=(sys.executable, "-c", "import sys; print(sys.stdin.buffer.read().decode().upper())"),
        input_file=input_file,
        output_file=output_file,
        directory=str(tmp_path),
        stdin=input_file,
        staging=_launch(tmp_path, remote_root),
    )
    seen = []
    runner = _processed_run(executor, spec)
    runner.add_output_listener(lambda text, kind: seen.append((text, kind)))
    result = runner.run()
    assert runner.state == RunState.FINISHED
    assert result.return_code == 0
    assert result.output == b"HELLO\n"
    assert result.error == b""
    assert seen == [("HELLO\n", "out")]
    assert runner.backend_id is not None
    # An untracked loopback transport is intentionally not persistable;
    # inspect the runtime binding and mapped workspace, not a RunRecord.
    assert runner._job.execution.target is executor.target
    assert Path(runner._file_session.remote_directory) == remote_root


def test_remote_input_file_and_stdin_staged_only_once(tmp_path):
    transport = _SpyTransport()
    executor, spec, _, remote_root = _text_run(tmp_path, transport)
    runner = executor.create_run(spec, tracked=False)

    try:
        runner._file_session.prepare_submission()
        request = runner._file_session.prepare_job()
        target = remote_root / "calculation" / "input.txt"
        assert request["input_file"] == str(target)
        assert request["stdin"] == str(target)
        assert request["input_files"] == [str(target)]
        assert [destination for _, destination in transport.uploads] == [target]
    finally:
        spec.input_file.close()


def test_remote_executor_stages_declared_input_files(tmp_path):
    workdir = tmp_path / "calculation"
    workdir.mkdir()
    input_file = (workdir / "input.txt").open("w+b")
    input_file.write(b"hello")
    input_file.seek(0)
    potential_file = workdir / "start.pot"
    potential_file.write_text("potential")
    output_file = (workdir / "output.txt").open("wb")
    remote_root = tmp_path / "remote"
    executor = _executor()
    spec = ExecutionSpec(
        argv=(
            sys.executable,
            "-c",
            "import pathlib, sys; print(sys.stdin.buffer.read().decode() + ':' + pathlib.Path('start.pot').read_text())",
        ),
        input_file=input_file,
        output_file=output_file,
        directory=str(workdir),
        stdin=input_file,
        input_files=(input_file, potential_file),
        staging=_launch(tmp_path, remote_root),
    )
    result = _processed_run(executor, spec).run()
    assert result.return_code == 0
    assert result.output == b"hello:potential\n"
    assert (remote_root / "calculation" / "start.pot").read_text() == "potential"


def test_remote_executor_uses_remote_only_input(tmp_path):
    transport = _SpyTransport()
    workdir = tmp_path / "local" / "calculation"
    workdir.mkdir(parents=True)
    remote_only = workdir / "remote-only.dat"
    remote_file = tmp_path / "remote" / "calculation" / "remote-only.dat"
    remote_file.parent.mkdir(parents=True)
    remote_file.write_text("remote")
    executor, spec, _, _ = _text_run(
        tmp_path,
        transport,
        declared_input_files=(remote_only,),
        code="import pathlib; print(pathlib.Path('remote-only.dat').read_text())",
    )
    result = _processed_run(executor, spec).run()
    assert result.output == b"remote\n"
    assert not remote_only.exists()
    assert all((dst != remote_file for _, dst in transport.uploads))


@pytest.mark.parametrize(
    "policy, local_value, remote_value, expected, uploads",
    [
        ("changed", "new", "old", "new", 1),
        ("changed", "same", "same", "same", 0),
        ("always", "local", "remote", "local", 1),
        ("missing", "local", "remote", "remote", 0),
        ("never", "local", "local", "local", 0),
    ],
    ids=("changed", "unchanged", "always", "missing", "never"),
)
def test_remote_input_file_policies(tmp_path, policy, local_value, remote_value, expected, uploads):
    transport = _SpyTransport()
    workdir = tmp_path / "local" / "calculation"
    workdir.mkdir(parents=True)
    artifact = workdir / "artifact.dat"
    artifact.write_text(local_value)
    remote_artifact = tmp_path / "remote" / "calculation" / "artifact.dat"
    remote_artifact.parent.mkdir(parents=True)
    remote_artifact.write_text(remote_value)
    executor, spec, _, remote_root = _text_run(
        tmp_path,
        transport,
        declared_input_files=(artifact,),
        input_files=policy,
        code="import pathlib; print(pathlib.Path('artifact.dat').read_text())",
    )
    if policy == "never":
        _expose_shared_input(remote_root, "input.txt", b"hello")
    result = _processed_run(executor, spec).run()
    assert result.output == (expected + "\n").encode()
    assert sum((dst == remote_artifact for _, dst in transport.uploads)) == uploads


@pytest.mark.parametrize("direction", ("input", "output"))
@pytest.mark.parametrize("kind", ("file", "directory"))
def test_remote_never_policy_ignores_missing_declared_file(tmp_path, direction, kind):
    from kkrunner import TaskFile

    transport = _SpyTransport()
    missing = tmp_path / "local" / "calculation" / "missing.dat"
    declaration = TaskFile(missing, kind=kind)
    kwargs = (
        {"declared_input_files": (declaration,), "input_files": "never"}
        if direction == "input"
        else {"declared_output_files": (declaration,), "output_files": "never"}
    )
    executor, spec, _, remote_root = _text_run(tmp_path, transport, code="print('done')", **kwargs)
    if direction == "input":
        _expose_shared_input(remote_root, "input.txt", b"hello")
    result = _processed_run(executor, spec).run()
    assert result.return_code == 0
    assert not missing.exists()
    if direction == "input":
        assert all((src != missing for src, _ in transport.uploads))
    else:
        assert all((dst != missing for _, dst in transport.downloads))


def test_remote_input_policy_always_requires_local_file(tmp_path):
    missing = tmp_path / "local" / "calculation" / "missing.dat"
    remote = tmp_path / "remote" / "calculation" / "missing.dat"
    remote.parent.mkdir(parents=True)
    remote.write_text("remote")
    executor, spec, _, _ = _text_run(tmp_path, declared_input_files=(missing,), input_files="always")
    with pytest.raises(SubmissionNotStarted, match="Local input file"):
        _processed_run(executor, spec).run()


@pytest.mark.parametrize("policy", tuple(FileSyncPolicy))
@pytest.mark.parametrize(
    "case, local_value, generated_value",
    [
        ("same", "same", "same"),
        ("different", "local", "remote"),
        ("missing-on-server", "local", None),
        ("missing-locally", None, "remote"),
    ],
)
def test_remote_output_file_policies(tmp_path, policy, case, local_value, generated_value):
    """All four policies obey the non-destructive source/target matrix."""
    from kkrunner import TaskFile

    transport = _SpyTransport()
    workdir = tmp_path / "local" / "calculation"
    workdir.mkdir(parents=True)
    artifact = workdir / "artifact.dat"
    if local_value is not None:
        artifact.write_text(local_value)
    code = (
        "print('done')" if generated_value is None
        else f"import pathlib; pathlib.Path('artifact.dat').write_text({generated_value!r}); print('done')"
    )
    executor, spec, _, remote_root = _text_run(
        tmp_path, transport, declared_output_files=(TaskFile(artifact, optional=True),),
        output_files=policy, code=code,
    )
    runner = _processed_run(executor, spec)
    assert runner.run().return_code == 0
    remote_artifact = remote_root / "calculation" / "artifact.dat"
    assert remote_artifact.exists() == (generated_value is not None)
    if generated_value is not None:
        assert remote_artifact.read_text() == generated_value

    download = (
        generated_value is not None and policy != FileSyncPolicy.NEVER
        and (
            policy == FileSyncPolicy.ALWAYS or local_value is None
            or (policy == FileSyncPolicy.CHANGED and local_value != generated_value)
        )
    )
    expected_local = generated_value if download else local_value
    if expected_local is None:
        assert not artifact.exists()
    else:
        assert artifact.read_text() == expected_local
    primary = (remote_root / "calculation" / "output.txt", workdir / "output.txt")
    expected_downloads = ([] if policy == FileSyncPolicy.NEVER else [primary])
    if download:
        expected_downloads.append((remote_artifact, artifact))
    assert transport.downloads == expected_downloads

    state = (
        "available" if generated_value is not None
        and (download or local_value == generated_value) else "skipped"
    )
    entry = next(x for x in runner.files.outputs if x.local_path == str(artifact))
    assert entry.availability_state == state


@pytest.mark.parametrize("policy", (FileSyncPolicy.ALWAYS, FileSyncPolicy.CHANGED, FileSyncPolicy.MISSING))
def test_remote_executor_does_not_reuse_stale_output(tmp_path, policy):
    """A missing required server result fails but preserves local data."""
    transport = _SpyTransport()
    workdir = tmp_path / "local" / "calculation"
    workdir.mkdir(parents=True)
    artifact = workdir / "artifact.dat"
    artifact.write_text("old local result")
    remote_artifact = tmp_path / "remote" / "calculation" / "artifact.dat"
    remote_artifact.parent.mkdir(parents=True)
    remote_artifact.write_text("stale remote result")
    executor, spec, _, _ = _text_run(
        tmp_path, transport, declared_output_files=(artifact,),
        output_files=policy, code="print('done')",
    )
    runner = _processed_run(executor, spec)
    with pytest.raises(FileNotFoundError, match="was not created"):
        runner.run()
    assert not remote_artifact.exists()
    assert artifact.read_text() == "old local result"
    entry = next(x for x in runner.files.outputs if x.local_path == str(artifact))
    assert entry.availability_state == "failed"


@pytest.mark.parametrize(
    "policy, local_value, generated_value, expected, transfer, state",
    [
        (FileSyncPolicy.ALWAYS, "same", "same", "same", True, "available"),
        (FileSyncPolicy.CHANGED, "same", "same", "same", False, "available"),
        (FileSyncPolicy.CHANGED, "old", "new", "new", True, "available"),
        (FileSyncPolicy.MISSING, "old", "new", "old", False, "skipped"),
        (FileSyncPolicy.MISSING, None, "new", "new", True, "available"),
        (FileSyncPolicy.NEVER, "old", "new", "old", False, "skipped"),
        (FileSyncPolicy.ALWAYS, "old", None, "old", False, "skipped"),
    ],
)
def test_remote_directory_output_policies(tmp_path, policy, local_value, generated_value, expected, transfer, state):
    """Whole output directories follow the same non-destructive policies."""
    from kkrunner import TaskFile

    class DirectorySpy(_SpyTransport):
        def __init__(self):
            super().__init__()
            self.directory_downloads = []

        async def get_directory(self, source, destination):
            self.directory_downloads.append((Path(source), Path(destination)))
            await super().get_directory(source, destination)

    transport = DirectorySpy()
    workdir = tmp_path / "local" / "calculation"
    workdir.mkdir(parents=True)
    local = workdir / "result-tree"
    if local_value is not None:
        local.mkdir()
        (local / "value.dat").write_text(local_value)
    code = (
        "print('done')" if generated_value is None
        else f"from pathlib import Path; p=Path('result-tree'); p.mkdir(); (p/'value.dat').write_text({generated_value!r})"
    )
    executor, spec, _, remote_root = _text_run(
        tmp_path, transport, code=code, output_files=policy,
        declared_output_files=(TaskFile(local, kind="directory", optional=True),),
    )
    runner = _processed_run(executor, spec)
    assert runner.run().return_code == 0
    remote = remote_root / "calculation" / "result-tree"
    assert remote.exists() == (generated_value is not None)
    if generated_value is not None:
        assert (remote / "value.dat").read_text() == generated_value
    if expected is None:
        assert not local.exists()
    else:
        assert (local / "value.dat").read_text() == expected
    assert transport.directory_downloads == ([(remote, local)] if transfer else [])
    entry = next(x for x in runner.files.outputs if x.local_path == str(local))
    assert entry.availability_state == state


def test_remote_cache_creation_content_and_mtime_changes_in_one_run(tmp_path):
    """Four independent cache outcomes should need only one remote calculation."""
    transport = _SpyTransport()
    workdir = tmp_path / "local" / "calculation"
    workdir.mkdir(parents=True)
    changed = workdir / "changed-cache.dat"
    created = workdir / "created-cache.dat"
    unchanged = workdir / "unchanged-cache.dat"
    touched = workdir / "mtime-cache.dat"
    changed.write_text("old")
    unchanged.write_text("same")
    touched.write_text("same")
    executor, spec, _, remote_root = _text_run(
        tmp_path,
        transport,
        declared_cache_files=(changed, created, unchanged, touched),
        code="import os; from pathlib import Path; changed=Path('changed-cache.dat'); assert changed.read_text()=='old'; changed.write_text('new'); Path('created-cache.dat').write_text('generated'); assert Path('unchanged-cache.dat').read_text()=='same'; touched=Path('mtime-cache.dat'); assert touched.read_text()=='same'; info=touched.stat(); os.utime(touched, ns=(info.st_atime_ns, info.st_mtime_ns+1000000000)); print('done')",
    )
    result = _processed_run(executor, spec).run()
    remote_directory = remote_root / "calculation"
    assert result.return_code == 0
    assert changed.read_text() == "new"
    assert created.read_text() == "generated"
    assert unchanged.read_text() == "same"
    assert touched.read_text() == "same"
    for path in (changed, unchanged, touched):
        remote_path = remote_directory / path.name
        assert sum((dst == remote_path for _, dst in transport.uploads)) == 1
    assert all((dst != remote_directory / created.name for _, dst in transport.uploads))
    for path in (changed, created, touched):
        remote_path = remote_directory / path.name
        assert sum(((src, dst) == (remote_path, path) for src, dst in transport.downloads)) == 1
    assert all((src != remote_directory / unchanged.name for src, _ in transport.downloads))


@pytest.mark.parametrize("direction", ("input", "output"))
def test_remote_cache_never_policy_skips_only_requested_direction(tmp_path, direction):
    transport = _SpyTransport()
    cache = tmp_path / "local" / "calculation" / "cache.dat"
    cache.parent.mkdir(parents=True)
    cache.write_text("local")
    kwargs = {"input_files": "never"} if direction == "input" else {"output_files": "never"}
    executor, spec, _, remote_root = _text_run(
        tmp_path,
        transport,
        declared_cache_files=(cache,),
        code="from pathlib import Path; Path('cache.dat').write_text('remote'); print('done')",
        **kwargs,
    )
    if direction == "input":
        _expose_shared_input(remote_root, "input.txt", b"hello")
        _expose_shared_input(remote_root, "cache.dat", "local")
    remote_cache = remote_root / "calculation" / "cache.dat"
    result = _processed_run(executor, spec).run()
    assert result.return_code == 0
    if direction == "input":
        assert all((dst != remote_cache for _, dst in transport.uploads))
        assert cache.read_text() == "remote"
        assert (remote_cache, cache) in transport.downloads
    else:
        assert cache.read_text() == "local"
        assert all((src != remote_cache for src, _ in transport.downloads))


def test_remote_sync_policy_rejects_unknown_value(tmp_path):
    with pytest.raises(ValueError, match="input_sync"):
        _launch(tmp_path, tmp_path / "remote", input_files="sometimes")


@pytest.mark.parametrize(
    "root_mode, remote_root, expected",
    [
        ("default", None, (Path.home() / "calculations" / "Fe").as_posix()),
        ("remote-only", "/scratch/tester", "/scratch/tester/calculations/Fe"),
        ("both", "/scratch/tester/projects", "/scratch/tester/projects/project/Fe"),
    ],
)
def test_remote_directory_mapping(tmp_path, root_mode, remote_root, expected):
    endpoint = _executor()
    if root_mode == "both":
        local_root = tmp_path / "local"
        local = local_root / "project" / "Fe"
        launch = StagingSpec(local_root=local_root, remote_root=remote_root)
    else:
        local = Path.home() / "calculations" / "Fe"
        launch = StagingSpec() if remote_root is None else StagingSpec(remote_root=remote_root)
    context = FileContext(endpoint.target, launch)
    mapped = asyncio.run(context.map_directory(local))
    assert mapped == expected


def test_remote_directory_mapping_rejects_path_outside_local_root(tmp_path):
    local_root = tmp_path / "local"
    endpoint = _executor()
    context = FileContext(endpoint.target, StagingSpec(local_root=local_root, remote_root="/scratch/tester"))
    with pytest.raises(ValueError, match="outside mapped root"):
        asyncio.run(context.map_directory(tmp_path / "elsewhere"))


def test_remote_none_stream_files_are_live_only(tmp_path):
    workdir = tmp_path / "calculation"
    workdir.mkdir()
    executor = _executor()
    spec = ExecutionSpec(
        argv=(
            sys.executable,
            "-c",
            "import sys,time; time.sleep(.1); print('out',flush=True); print('err',file=sys.stderr,flush=True)",
        ),
        input_file=None,
        output_file=None,
        error_file=None,
        directory=str(workdir),
        staging=_launch(tmp_path, tmp_path / "remote"),
    )
    observed = []
    runner = executor.create_run(spec, tracked=False)
    runner.add_output_listener(lambda text, kind: observed.append((kind, text)))
    outcome = runner.run()
    assert runner.state == RunState.FINISHED
    assert outcome.return_code == 0
    assert outcome.output_file is None
    assert outcome.error_file is None
    assert {kind: "".join(text for channel, text in observed if channel == kind)
            for kind in ("out", "err")} == {"out": "out\n", "err": "err\n"}
    assert list((tmp_path / "remote").rglob("stdout")) == []
    assert list((tmp_path / "remote").rglob("stderr")) == []


@pytest.mark.parametrize(
    "policy,source,destination,expected",
    [
        (FileSyncPolicy.NEVER, "new", None, False),
        (FileSyncPolicy.NEVER, "new", "old", False),
        (FileSyncPolicy.ALWAYS, "new", "new", True),
        (FileSyncPolicy.ALWAYS, None, "old", False),
        (FileSyncPolicy.MISSING, "new", None, True),
        (FileSyncPolicy.MISSING, "new", "old", False),
        (FileSyncPolicy.CHANGED, "new", None, True),
        (FileSyncPolicy.CHANGED, "new", "old", True),
        (FileSyncPolicy.CHANGED, "new", "new", False),
        (FileSyncPolicy.CHANGED, None, None, False),
        (FileSyncPolicy.CHANGED, None, "old", False),
    ],
)
def test_file_sync_transfer_decision_is_direction_independent(policy, source, destination, expected):
    assert policy.should_transfer(source, destination) is expected


def test_remote_argv_reuses_staged_path_identity(tmp_path):
    """Relative, absolute and Path arguments share the staging resolver."""
    work = tmp_path / "work"
    source = work / "input.dat"
    runner = _executor().create_run_parts(
        ExecutionSpec(("program",), directory=str(work))
    ).files
    assert isinstance(runner, FileSession)
    arguments = ("program", "input.dat", source, "--verbose", "./missing.dat")
    staged = {str(source): "/remote/work/input.dat"}
    assert runner._remote_argv(arguments, staged) == [
        "program",
        "/remote/work/input.dat",
        "/remote/work/input.dat",
        "--verbose",
        "./missing.dat",
    ]


@pytest.mark.parametrize("preflight", (False, True))
def test_remote_preparation_failure_has_one_classification(tmp_path, preflight):
    """Direct submission and persisted preflight must report the same failure."""
    directory = tmp_path / "local"
    directory.mkdir()
    spec = ExecutionSpec(
        argv=("ignored",),
        directory=str(directory),
        input_file=str(directory / "missing.inp"),
        output_file=None,
        staging=StagingSpec(local_root=str(tmp_path), remote_root=str(tmp_path / "remote"), input_sync="always"),
    )
    runner = _executor().create_run(spec, tracked=False)
    if preflight:
        with pytest.raises(SubmissionNotStarted, match="Local input file"):
            runner._file_session.prepare_submission()
        # File preparation alone does not submit or mutate Run state.
        assert runner.state == RunState.NEW
        assert runner.failure_phase is None

    with pytest.raises(SubmissionNotStarted, match="Local input file"):
        runner.submit()
    assert runner.failure_phase == FailurePhase.SUBMISSION
    assert "Local input file" in runner.failure_message
    assert runner.backend_id is None
    assert runner.state == RunState.FAILED
    with pytest.raises(RuntimeError, match="already submitted"):
        runner.submit()
    with pytest.raises(RuntimeError, match="cannot be restarted"):
        runner.start()


@pytest.mark.parametrize("preflight", (False, True))
def test_remote_submit_failure_is_not_misclassified_as_failed_preparation(tmp_path, monkeypatch, preflight):
    directory = tmp_path / "local"
    directory.mkdir()
    spec = ExecutionSpec(
        argv=("ignored",),
        directory=str(directory),
        input_file=None,
        output_file=None,
        staging=StagingSpec(local_root=str(tmp_path), remote_root=str(tmp_path / "remote")),
    )
    executor = ExecutionClient(SupervisedBackend, target=_OperationLoopbackTransport())
    runner = executor.create_run(spec, tracked=False)

    async def fail_after_preparation(_request, **_kwargs):
        raise OSError("backend submission uncertain")

    # Submission now belongs to the selected backend, not ExecutionClient.
    monkeypatch.setattr(SupervisedBackend, "submit_and_follow", staticmethod(fail_after_preparation))
    if preflight:
        runner._file_session.prepare_submission()
    with pytest.raises(OSError, match="backend submission uncertain"):
        runner.submit()
    assert runner.failure_phase is None
    assert runner.backend_id is None
    assert runner.state == RunState.UNKNOWN
    assert "backend submission uncertain" in runner.failure_message
    with pytest.raises(RuntimeError, match="already submitted"):
        runner.submit()
    with pytest.raises(RuntimeError, match="cannot be restarted"):
        runner.start()


@pytest.mark.parametrize("policy,remote_value,expected,upload", [
    ("always", "same", "same", True),
    ("changed", "same", "same", False),
    ("changed", "old", "same", True),
    ("missing", "old", "old", False),
    ("missing", None, "same", True),
    ("never", "old", "old", False),
])
def test_remote_directory_input_policy(tmp_path, policy, remote_value, expected, upload):
    from kkrunner import TaskFile

    class DirectorySpy(_SpyTransport):
        def __init__(self):
            super().__init__()
            self.directory_uploads = []

        async def put_directory(self, source, destination):
            self.directory_uploads.append((Path(source), Path(destination)))
            await super().put_directory(source, destination)

    transport = DirectorySpy()
    local = tmp_path / "local/calculation/tree"
    local.mkdir(parents=True)
    (local / "value").write_text("same")
    remote = tmp_path / "remote/calculation/tree"
    if remote_value is not None:
        remote.mkdir(parents=True)
        (remote / "value").write_text(remote_value)
        if remote_value == "old":
            (remote / "obsolete").write_text("obsolete")
    executor, spec, _, remote_root = _text_run(
        tmp_path, transport, input_files=policy,
        declared_input_files=(TaskFile(local, kind="directory"),),
        code=f"from pathlib import Path; assert Path('tree/value').read_text() == {expected!r}",
    )
    if policy == "never":
        _expose_shared_input(remote_root, "input.txt", b"hello")
    assert _processed_run(executor, spec).run().return_code == 0
    assert transport.directory_uploads == ([(local, remote)] if upload else [])
    assert (remote / "value").read_text() == expected
    if upload:
        assert not (remote / "obsolete").exists()
        assert not (remote / "tree").exists()


@pytest.mark.parametrize("policy,modify,expected,transfer", [
    ("changed", True, "new", True),
    ("always", True, "new", True),
    ("missing", True, "old", False),
    ("never", True, "old", False),
    ("changed", False, "old", False),
    ("always", False, "old", False),
])
def test_remote_directory_cache_policy(tmp_path, policy, modify, expected, transfer):
    from kkrunner import TaskFile

    class DirectorySpy(_SpyTransport):
        def __init__(self):
            super().__init__()
            self.directory_downloads = []

        async def get_directory(self, source, destination):
            self.directory_downloads.append((Path(source), Path(destination)))
            await super().get_directory(source, destination)

    local = tmp_path / "local/calculation/cache-tree"
    local.mkdir(parents=True)
    (local / "value").write_text("old")
    (local / "obsolete").write_text("old")
    transport = DirectorySpy()
    executor, spec, _, remote_root = _text_run(
        tmp_path, transport, output_files=policy,
        declared_cache_files=(TaskFile(local, kind="directory"),),
        code=("from pathlib import Path; p=Path('cache-tree'); "
              "(p/'value').write_text('new'); (p/'obsolete').unlink()" if modify else "pass"),
    )
    assert _processed_run(executor, spec).run().return_code == 0
    assert (local / "value").read_text() == expected
    assert (local / "obsolete").exists() is not transfer
    remote = remote_root / "calculation/cache-tree"
    assert transport.directory_downloads == ([(remote, local)] if transfer else [])


@pytest.mark.parametrize("policy", (FileSyncPolicy.ALWAYS, FileSyncPolicy.MISSING))
def test_existence_based_staging_only_hashes_final_target_inputs(tmp_path, monkeypatch, policy):
    from kkrunner.client import file_session
    from kkrunner.files.versions import file_digest

    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    names = ("input", "part-1.dat", "part-2.dat")
    for name in names:
        (local / name).write_text("new " + name)
    for name in names[:2]:
        (remote / name).write_text("old " + name)

    class Target(_OperationLoopbackTransport):
        def __init__(self):
            super().__init__()
            self.queries = []
            self.uploads = []

        async def query_files(self, sources, *, root=None, digest=False):
            self.queries.append((tuple(sources), digest, tuple(self.uploads)))
            return await super().query_files(sources, root=root, digest=digest)

        async def put_file(self, source, destination):
            await super().put_file(source, destination)
            self.uploads.append(Path(destination).name)

    target = Target()
    client = _executor(target)
    spec = ExecutionSpec(
        argv=("unused",), directory=local, input_file="input",
        input_files=("part-*.dat",),
        staging=_launch(local, remote, input_files=policy),
    )

    def unexpected_hash(*args, **kwargs):
        pytest.fail("Existence-based input staging must not hash local files")

    monkeypatch.setattr(file_session, "file_digest", unexpected_hash)
    session = client.file_session(spec)
    try:
        session.prepare_submission()
        expected_uploads = set(names if policy == FileSyncPolicy.ALWAYS else names[2:])
        assert set(target.uploads) == expected_uploads
        hashed = [(sources, uploads) for sources, digest, uploads in target.queries if digest]
        assert len(hashed) == 1
        sources, uploads = hashed[0]
        assert set(map(Path, sources)) == {remote / name for name in names}
        assert set(uploads) == expected_uploads
        if policy == FileSyncPolicy.ALWAYS:
            assert len(target.queries) == 1
        for name in names:
            version = session.files.input_version(local / name)
            expected = "new " if name in expected_uploads else "old "
            assert (remote / name).read_text() == expected + name
            assert version.digest == file_digest(remote / name)
    finally:
        session.close()


def test_always_output_sync_does_not_hash_before_transfer(tmp_path, monkeypatch):
    from kkrunner.client import file_session
    from kkrunner.files.versions import file_digest
    from kkrunner.runs.base import RunStatus

    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    remote.mkdir()

    class Target(_OperationLoopbackTransport):
        async def query_files(self, sources, *, root=None, digest=False):
            assert not digest, "ALWAYS does not compare output digests"
            return await super().query_files(sources, root=root, digest=digest)

    client = _executor(Target())
    spec = ExecutionSpec(
        argv=("unused",), directory=local, output_files=("exact.dat", "part-*.dat"),
        staging=_launch(local, remote, output_files=FileSyncPolicy.ALWAYS),
    )
    session = client.file_session(spec)
    try:
        session.prepare_submission()
        for name in ("exact.dat", "part-1.dat", "part-2.dat"):
            (local / name).write_text("old")
            (remote / name).write_text("new " + name)

        def unexpected_hash(*args, **kwargs):
            pytest.fail("ALWAYS output staging must not compare local contents")

        monkeypatch.setattr(file_session, "file_digest", unexpected_hash)
        files = session.finalize(RunStatus(RunState.FINISHED, return_code=0))
        assert {version.path.name for version in files.output_versions} == {
            "exact.dat", "part-1.dat", "part-2.dat",
        }
        for version in files.output_versions:
            assert version.path.read_text() == "new " + version.path.name
            assert version.digest == file_digest(version.path)
    finally:
        session.close()
