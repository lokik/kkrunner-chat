"""Direct SSH execution without an agent; local transport exercises the wire worker."""

import asyncio
import json
import sys
from threading import Event

import pytest

from kkrunner import ExecutionSpec, Run, RunState
from kkrunner.client import LocalTransport, EndpointClient, SshTransport, StagingSpec
from kkrunner.files.queries import finalize_upload, query_files
from kkrunner.backends import DirectBackend
from kkrunner.backends.contracts import BackendCapability
from kkrunner.client.contracts import Operation
from kkrunner.client.transports.rpc import operation_command, operation_result

_FINALIZE_SOURCE = operation_command(Operation.standalone(finalize_upload), python=(sys.executable,))[-1]
_QUERY_SOURCE = operation_command(Operation.standalone(query_files), python=(sys.executable,))[-1]
WORKER_SOURCE = operation_command(
    Operation.for_backend(DirectBackend, "stream"), python=(sys.executable,), streaming=True,
)[-1]


class LoopbackSshTransport(SshTransport):
    def __init__(self):
        super().__init__("loopback", python_command=(sys.executable,))

    def _command(self, argv, cwd=None):
        assert cwd is None
        return tuple(argv)


def test_remote_helper_sources_are_valid_python():
    for source in (WORKER_SOURCE, _FINALIZE_SOURCE, _QUERY_SOURCE):
        compile(source, "<ssh direct target>", "exec")


def test_direct_remote_stages_runs_streams_and_downloads_without_agent(tmp_path):
    local = tmp_path / "local"
    remote = tmp_path / "remote"
    directory = local / "job"
    directory.mkdir(parents=True)
    remote.mkdir()
    (directory / "input.dat").write_text("hello")
    spec = ExecutionSpec(
        argv=(sys.executable, "-c",
              "from pathlib import Path; import sys; "
              "print(Path('input.dat').read_text(), flush=True); "
              "print('error', file=sys.stderr, flush=True); "
              "Path('answer.dat').write_text('42')"),
        directory=str(directory),
        input_files=("input.dat",),
        output_file=str(directory / "stdout.txt"),
        error_file=str(directory / "stderr.txt"),
        output_files=(str(directory / "answer.dat"),),
        staging=StagingSpec(local_root=str(local), remote_root=str(remote)),
    )
    run = Run.untracked(EndpointClient(DirectBackend, transport=LoopbackSshTransport()), spec)
    assert run.spec is spec
    observed = []
    run.add_output_listener(lambda data, kind: observed.append((kind, data)))
    outcome = run.run()
    assert outcome.return_code == 0
    assert run.state == RunState.FINISHED
    assert run.backend_id is not None  # live session identity, not a durable reattach token
    assert run.spec is spec
    assert run.spec.output_file == str(directory / "stdout.txt")
    assert run.spec.error_file == str(directory / "stderr.txt")
    assert (directory / "stdout.txt").read_text() == "hello\n"
    assert (directory / "stderr.txt").read_text() == "error\n"
    assert (directory / "answer.dat").read_text() == "42"
    assert (remote / "job" / "stdout.txt").read_text() == "hello\n"
    assert (remote / "job" / "answer.dat").read_text() == "42"
    assert {kind for kind, _ in observed} == {"out", "err"}


def test_direct_ssh_persistence_is_separate_from_job_mechanism(tmp_path):
    spec = ExecutionSpec(argv=("true",), directory=str(tmp_path))
    backend = EndpointClient(DirectBackend, transport=LoopbackSshTransport())
    parts = backend.create_run_parts(spec, submission_key="client-submission-key")
    from kkrunner.client.job import Job
    assert type(parts.job) is Job
    assert BackendCapability.REATTACH not in backend.backend.supports
    assert BackendCapability.RECOVER not in backend.backend.supports


def test_direct_ssh_cancellation_stops_remote_process(tmp_path):
    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    spec = ExecutionSpec(
        argv=(sys.executable, "-u", "-c",
              "import time; print('ready', flush=True); time.sleep(30)"),
        directory=str(local),
        output_file=str(local / "stdout.txt"),
        staging=StagingSpec(local_root=str(local), remote_root=str(remote)),
    )
    run = Run.untracked(EndpointClient(DirectBackend, transport=LoopbackSshTransport()), spec)
    ready = Event()
    run.add_output_listener(lambda data, kind: ready.set() if "ready" in data else None)
    run.submit()
    assert ready.wait(8)
    run.cancel()
    run.wait()
    assert run.state == RunState.CANCELLED


def test_direct_ssh_query_source_reports_metadata(tmp_path):
    target = tmp_path / "a.txt"
    target.write_bytes(b"123")
    req = json.dumps({"paths": [str(target)], "root": None, "digest": True})
    result = asyncio.run(LocalTransport().run((sys.executable, "-c", _QUERY_SOURCE), input=json.dumps({
        "args": [[str(target)]], "kwargs": {"root": None, "digest": True},
    }).encode()))
    value = operation_result(result.stdout.decode())
    assert value[0]["status"] == "ok"
    assert value[0]["size"] == 3
    assert len(value[0]["digest"]) == 64


def test_direct_ssh_uses_same_job_type_as_local_direct(tmp_path):
    from kkrunner.client import EndpointClient
    from kkrunner.client.job import Job
    local = EndpointClient(DirectBackend).create_run_parts(ExecutionSpec(argv=("true",), directory=str(tmp_path))).job
    remote = EndpointClient(DirectBackend, transport=LoopbackSshTransport()).create_run_parts(
        ExecutionSpec(argv=("true",), directory=str(tmp_path))
    ).job
    assert type(local) is Job
    assert type(remote) is Job
    assert local.execution.transport.is_local
    assert not remote.execution.transport.is_local

def test_direct_ssh_rejects_merged_streams_before_staging(tmp_path):
    root = tmp_path / "local"
    root.mkdir()
    spec = ExecutionSpec(
        argv=(sys.executable, "-c", "print('never launched')"),
        directory=root,
        output_file=str(root / "combined.log"),
        error_file=str(root / "combined.log"),
        staging=StagingSpec(local_root=str(root), remote_root=str(tmp_path / "remote")),
    )
    run = Run.untracked(EndpointClient(DirectBackend, transport=LoopbackSshTransport()), spec)
    with pytest.raises(Exception, match="distinct output files"):
        run.submit()
    assert not (root / "combined.log").exists()


def test_direct_endpoint_round_trip_and_normalized_identity():
    from kkrunner.client.transports import transport_from_dict
    from kkrunner.client.transports import require_persistable_transport
    from dataclasses import asdict

    transport = SshTransport("user@example.net", options=("-p", "2200"))
    client = EndpointClient(DirectBackend, transport=transport)
    configured = client.backend_config
    restored = EndpointClient.from_config(asdict(transport), asdict(configured))
    assert restored.transport == transport
    assert restored.backend_config == configured
    assert isinstance(restored.transport, SshTransport)
    assert not restored.transport.is_local

    python = SshTransport("user@example.net", python_command=("python3.12",))
    restored_python = EndpointClient.from_config(asdict(python), asdict(configured))
    assert restored_python.transport == python
    assert restored_python.transport.python_command == ("python3.12",)


def test_remote_worker_does_not_delete_cache_matched_by_output_glob(tmp_path):
    root = tmp_path
    cache = root / "restart.pot"
    stale = root / "stale.pot"
    cache.write_text("protected")
    stale.write_text("obsolete")
    request = {
        "directory": str(root),
        "argv": [sys.executable, "-c", "print('done')"],
        "output_files": [str(root / "*.pot")],
        "cache_files": [str(root / "restart*.pot")],
    }
    from kkrunner.files.availability import prepare_target_outputs
    prepare_target_outputs(request)
    async def run_worker():
        # Keep the control pipe open while the target runs: EOF means SSH
        # disconnection and must cancel a direct process.
        process = await LocalTransport().start(
            (sys.executable, "-u", "-c", WORKER_SOURCE, json.dumps({"args": [request], "kwargs": {}})),
            stdin=asyncio.subprocess.PIPE,
        )
        try:
            out, err = await asyncio.wait_for(
                asyncio.gather(process.stdout.read(), process.stderr.read()),
                timeout=10,
            )
            code = await asyncio.wait_for(process.wait(), timeout=10)
            return code, out, err
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()
            process.stdin.close()

    code, out, err = asyncio.run(run_worker())
    assert code == 0, err.decode()
    assert cache.read_text() == "protected"
    assert not stale.exists()


def test_direct_local_cancel_after_termination_does_not_relabel_success(tmp_path):
    run = Run.untracked(
        EndpointClient(DirectBackend),
        ExecutionSpec(argv=(sys.executable, "-c", "print('finished')"), directory=str(tmp_path)),
    )
    assert run.run().return_code == 0
    run.cancel()
    assert run.state is RunState.FINISHED


def test_direct_ssh_preparation_preserves_spec(tmp_path):
    """Binding a direct backend to SSH never mutates the user's declaration."""
    from kkrunner.client.backend_execution import DirectExecution

    spec = ExecutionSpec(argv=("true",), directory=tmp_path)
    client = EndpointClient(DirectBackend, transport=LoopbackSshTransport())
    parts = client.create_run_parts(spec)
    assert isinstance(parts.job.execution, DirectExecution)
    assert parts.job.spec is spec
    assert parts.files.spec is spec
    assert parts.job.execution.transport is client.transport
