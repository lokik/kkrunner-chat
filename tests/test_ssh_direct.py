"""Direct SSH execution without an agent; local transport exercises the wire worker."""

import asyncio
import json
import sys
from threading import Event

import pytest

from kkrunner import ExecutionSpec, Run, RunState
from kkrunner.client import LocalTransport, SshDirectRunEndpoint
from kkrunner.client.ssh_direct.transport import _FINALIZE_SOURCE, _QUERY_SOURCE
from kkrunner.client.ssh_direct.worker import WORKER_SOURCE
from kkrunner.runs import LaunchSpec


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
        launch=LaunchSpec(local_root=str(local), remote_root=str(remote)),
    )
    run = Run.direct(SshDirectRunEndpoint(LocalTransport()), spec)
    assert run.spec is spec
    observed = []
    run.add_output_listener(lambda data, kind: observed.append((kind, data)))
    outcome = run.run()
    assert outcome.return_code == 0
    assert run.state == RunState.FINISHED
    assert run.backend_id is None
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
    backend = SshDirectRunEndpoint(LocalTransport())
    parts = backend.create_run_parts(spec, submission_key="client-submission-key")
    from kkrunner.client.job import Job
    assert type(parts.job) is Job
    from kkrunner.client.contracts import ReopenableRunEndpoint
    assert not isinstance(backend, ReopenableRunEndpoint)
    assert not hasattr(backend, "recover_submission")


def test_direct_ssh_cancellation_stops_remote_process(tmp_path):
    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    spec = ExecutionSpec(
        argv=(sys.executable, "-u", "-c",
              "import time; print('ready', flush=True); time.sleep(30)"),
        directory=str(local),
        output_file=str(local / "stdout.txt"),
        launch=LaunchSpec(local_root=str(local), remote_root=str(remote)),
    )
    run = Run.direct(SshDirectRunEndpoint(LocalTransport()), spec)
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
    result = asyncio.run(LocalTransport().run((sys.executable, "-c", _QUERY_SOURCE, req)))
    value = json.loads(result.stdout)
    assert value[0]["status"] == "ok"
    assert value[0]["size"] == 3
    assert len(value[0]["digest"]) == 64


def test_direct_ssh_uses_same_job_type_as_local_direct(tmp_path):
    from kkrunner.backends.process.backend import DirectProcessBackend
    from kkrunner.client.job import Job
    local = DirectProcessBackend().create_job(ExecutionSpec(argv=("true",), directory=str(tmp_path)))
    remote = SshDirectRunEndpoint(LocalTransport()).create_run_parts(
        ExecutionSpec(argv=("true",), directory=str(tmp_path))
    ).job
    assert type(local) is Job
    assert type(remote) is Job
    assert local.adapter.expose_pid is True
    assert remote.adapter.expose_pid is False

def test_direct_ssh_rejects_merged_streams_before_staging(tmp_path):
    root = tmp_path / "local"
    root.mkdir()
    spec = ExecutionSpec(
        argv=(sys.executable, "-c", "print('never launched')"),
        directory=root,
        output_file=str(root / "combined.log"),
        error_file=str(root / "combined.log"),
        launch=LaunchSpec(local_root=str(root), remote_root=str(tmp_path / "remote")),
    )
    run = Run.direct(SshDirectRunEndpoint(LocalTransport()), spec)
    with pytest.raises(Exception, match="distinct output files"):
        run.submit()
    assert not (root / "combined.log").exists()


def test_direct_endpoint_round_trip_and_normalized_identity():
    from kkrunner.client.endpoint import ExecutionEndpointConfig, executor_from_endpoint, endpoint_config_for_executor
    from kkrunner.client.ssh_direct import SshDirectTransport

    endpoint = endpoint_config_for_executor(
        SshDirectRunEndpoint(SshDirectTransport("user@example.net", options=("-p", "2200")))
    )
    assert endpoint.executor == "ssh-direct"
    restored = executor_from_endpoint(endpoint)
    assert isinstance(restored, SshDirectRunEndpoint)
    assert endpoint_config_for_executor(restored) == endpoint
    configured = SshDirectRunEndpoint(
        SshDirectTransport("user@example.net", python_command=("python3.12",))
    )
    normalized_python = endpoint_config_for_executor(configured)
    assert normalized_python.python_command == ("python3.12",)
    assert endpoint_config_for_executor(executor_from_endpoint(normalized_python)) == normalized_python
    normalized = ExecutionEndpointConfig(
        executor="ssh-direct", host="user@example.net",
        remote_command=("unrelated-agent",), psij_backend="slurm",
    )
    assert normalized.remote_command == ()
    assert normalized.psij_backend is None
    assert normalized == ExecutionEndpointConfig(executor="ssh-direct", host="user@example.net")


def test_remote_worker_does_not_delete_cache_matched_by_output_glob(tmp_path):
    root = tmp_path
    cache = root / "restart.pot"
    stale = root / "stale.pot"
    cache.write_text("protected")
    stale.write_text("obsolete")
    request = {
        "directory": str(root),
        "argv": [sys.executable, "-c", "print('done')"],
        "client_sync": {
            "output_patterns": [str(root / "*.pot")],
            "cache_patterns": [str(root / "restart*.pot")],
        },
    }
    async def run_worker():
        # Keep the control pipe open while the target runs: EOF means SSH
        # disconnection and must cancel a direct process.
        process = await LocalTransport().start(
            (sys.executable, "-u", "-c", WORKER_SOURCE, json.dumps(request)),
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
    from kkrunner.backends.process.backend import DirectProcessBackend
    job = DirectProcessBackend().create_job(
        ExecutionSpec(argv=(sys.executable, "-c", "print('finished')"), directory=str(tmp_path))
    )
    assert job.run().return_code == 0
    job.cancel()
    assert job.state == RunState.FINISHED

def test_direct_ssh_adapter_preparation_keeps_spec(tmp_path):
    """Direct adapters return the command specification, never mutate Job state."""
    from kkrunner.client.ssh_direct.runtime import SshProcessAdapter
    from kkrunner.client.remote_paths import RemotePathContext

    spec = ExecutionSpec(argv=("true",), directory=tmp_path)
    backend = SshDirectRunEndpoint(LocalTransport())
    adapter = SshProcessAdapter(RemotePathContext(backend, spec.launch))
    with pytest.raises(ValueError, match="staged request"):
        adapter.prepare(spec)
    adapter.configure_submission(
        {"directory": str(tmp_path)},
        stream_offsets={"out": None, "err": None},
        stream_writer=lambda data, kind: None,
        stream_paths=(None, None),
    )
    assert adapter.prepare(spec) is None
