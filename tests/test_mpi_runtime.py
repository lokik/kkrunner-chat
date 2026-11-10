import asyncio
import json
import warnings
from dataclasses import asdict

import pytest

from kkrunner import ExecutionSpec
from kkrunner.backends import mpi as mpi_runtime
from kkrunner.backends.process.primitives import materialize_local_spec
from kkrunner.client.agent import AgentBackendProxy
from kkrunner.runs.base import LaunchSpec, MpiNotFoundWarning, MpiSpec


@pytest.mark.parametrize(
    "slurm, available, expected, probed",
    [
        (True, "srun", ("srun",), ("srun",)),
        (True, "mpirun", ("mpirun",), ("srun", "mpirun")),
        (False, "mpiexec", ("mpiexec",), ("mpirun", "mpirun.opmpirun", "mpirun.mpich", "mpiexec")),
    ],
)
def test_mpi_launcher_selection_and_probe_order(monkeypatch, slurm, available, expected, probed):
    if slurm:
        monkeypatch.setenv("SLURM_NODELIST", "node[01-02]")
    else:
        monkeypatch.delenv("SLURM_NODELIST", raising=False)
    seen = []

    def which(name):
        seen.append(name)
        return f"/usr/bin/{name}" if name == available else None

    monkeypatch.setattr(mpi_runtime.shutil, "which", which)
    assert mpi_runtime.find_default_mpi_launcher() == expected
    assert seen == list(probed)


def test_mpi_spec_distinguishes_required_auto_and_explicit_launchers():
    required = LaunchSpec.from_mpi(True).mpi
    automatic = LaunchSpec.from_mpi("auto").mpi
    explicit = LaunchSpec.from_mpi(("custom-mpi", "--bind")).mpi
    assert required.required is True
    assert required.autodetect is True
    assert required.allows_serial_fallback is False
    assert automatic.required is False
    assert automatic.autodetect is True
    assert automatic.allows_serial_fallback is True
    assert explicit.required is True
    assert explicit.autodetect is False
    assert explicit.launcher == ("custom-mpi", "--bind")


def test_launch_spec_json_round_trip_uses_nested_mpi_spec():
    launch = LaunchSpec.from_mpi(4)
    encoded = json.loads(json.dumps(asdict(launch)))
    assert encoded["mpi"] == {"required": True, "autodetect": True, "launcher": [], "process_count": 4, "warning": True}
    assert "process_count" not in {key for key in encoded if key != "mpi"}
    assert "mpi_warning" not in encoded
    assert LaunchSpec(**encoded) == launch


def test_auto_disables_mpi_when_only_one_cpu_is_available(monkeypatch):
    monkeypatch.setattr(mpi_runtime.os, "sched_getaffinity", lambda _pid: {3})

    def must_not_probe():
        raise AssertionError("single-CPU auto mode must not probe MPI launchers")

    monkeypatch.setattr(mpi_runtime, "find_default_mpi_launcher", must_not_probe)
    spec = LaunchSpec.from_mpi("auto").mpi
    assert mpi_runtime.resolve_mpi_command(spec) is None


@pytest.mark.parametrize("affinity", [None, {2, 7}], ids=("no-sched-getaffinity", "multiple-cpus"))
def test_auto_mpi_probes_host_when_cpu_count_is_not_one(monkeypatch, affinity):
    if affinity is None:
        monkeypatch.delattr(mpi_runtime.os, "sched_getaffinity", raising=False)
    else:
        monkeypatch.setattr(mpi_runtime.os, "sched_getaffinity", lambda _pid: affinity)
    monkeypatch.setattr(mpi_runtime, "find_default_mpi_launcher", lambda: ("mpirun",))
    spec = LaunchSpec.from_mpi("auto").mpi
    assert mpi_runtime.resolve_mpi_command(spec) == ("mpirun",)


def test_explicit_mpi_command_does_not_probe_host(monkeypatch):

    def must_not_probe():
        raise AssertionError("explicit MPI command must not probe the host")

    monkeypatch.setattr(mpi_runtime, "find_default_mpi_launcher", must_not_probe)
    spec = LaunchSpec.from_mpi(("custom-mpi", "--bind")).mpi
    assert mpi_runtime.resolve_mpi_command(spec) == ("custom-mpi", "--bind")


@pytest.mark.parametrize(("launcher", "count_option"), (("mpirun", "-np"), ("srun", "-n")))
def test_process_count_is_materialized_for_launcher(monkeypatch, launcher, count_option):
    monkeypatch.setattr(mpi_runtime, "find_default_mpi_launcher", lambda: (launcher,))
    spec = LaunchSpec.from_mpi(4).mpi
    assert mpi_runtime.resolve_mpi_command(spec) == (launcher, count_option, "4")


def test_resolver_rejects_unparsed_mpi_values():
    try:
        mpi_runtime.resolve_mpi_command("auto")
    except TypeError as exc:
        assert "MpiSpec" in str(exc)
    else:
        raise AssertionError("raw MPI values must be normalized before resolution")


def test_mpi_process_count_is_portable_launch_intent():
    assert LaunchSpec.from_mpi(16) == LaunchSpec(mpi=MpiSpec(required=True, autodetect=True, process_count=16))


def test_local_executor_materializes_mpi_on_execution_host(monkeypatch, tmp_path):
    monkeypatch.setattr("kkrunner.backends.mpi.find_default_mpi_launcher", lambda: ("srun",))
    input_file = (tmp_path / "run.inp").open("w+")
    spec = ExecutionSpec(
        argv=("serial-program",),
        mpi_argv=("parallel-program", input_file.name),
        launch=LaunchSpec.from_mpi(16),
        input_file=input_file,
        output_file=None,
        directory=str(tmp_path),
        stdin=input_file,
    )
    resolved = materialize_local_spec(spec)
    assert resolved.argv == ("srun", "-n", "16", "parallel-program", input_file.name)
    assert resolved.stdin is input_file
    input_file.close()


def test_required_local_mpi_without_launcher_fails(monkeypatch, tmp_path):
    monkeypatch.setattr("kkrunner.backends.mpi.find_default_mpi_launcher", lambda: None)
    input_file = (tmp_path / "run.inp").open("w+")
    spec = ExecutionSpec(
        argv=("serial-program",),
        mpi_argv=("parallel-program", input_file.name),
        launch=LaunchSpec.from_mpi(True),
        input_file=input_file,
        output_file=None,
        directory=str(tmp_path),
        stdin=input_file,
    )
    with pytest.raises(FileNotFoundError, match="no MPI launcher"):
        materialize_local_spec(spec)
    input_file.close()


@pytest.mark.parametrize("warn", (True, False), ids=("warn", "silent"))
def test_local_auto_mpi_fallback_respects_warning_option(monkeypatch, tmp_path, warn):
    monkeypatch.setattr("kkrunner.backends.process.primitives.resolve_mpi_command", lambda mpi: None)
    input_file = (tmp_path / "run.inp").open("w+")
    spec = ExecutionSpec(
        argv=("serial-program",),
        mpi_argv=("parallel-program", input_file.name),
        launch=LaunchSpec(mpi=MpiSpec(autodetect=True, warning=warn)),
        input_file=input_file,
        output_file=None,
        directory=str(tmp_path),
        stdin=input_file,
    )
    try:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            resolved = materialize_local_spec(spec)
        assert len(caught) == int(warn)
        if warn:
            assert issubclass(caught[0].category, MpiNotFoundWarning)
            assert "without MPI" in str(caught[0].message)
        assert resolved.argv == ("serial-program",)
        assert resolved.stdin is input_file
    finally:
        input_file.close()


@pytest.mark.parametrize("warn", (True, False), ids=("warn", "silent"))
def test_remote_endpoint_mpi_warning_probe_respects_option(monkeypatch, warn):
    endpoint = object.__new__(AgentBackendProxy)
    endpoint.backend = "process"
    calls = []

    async def request(action, *args, payload=None):
        calls.append((action, args, payload))
        if action == "resolve-mpi":
            return {"mpi": None}
        return {"state": "queued", "directory": "/remote/run"}

    monkeypatch.setattr(endpoint, "_json_request", request)
    payload = {
        "directory": "/remote/run",
        "launch": {
            "mpi": {"required": False, "autodetect": True, "launcher": [], "process_count": None, "warning": warn}
        },
    }
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        asyncio.run(endpoint._submit(payload))
    assert [call[0] for call in calls] == (["resolve-mpi", "submit"] if warn else ["submit"])
    assert len(caught) == int(warn)
    if warn:
        assert issubclass(caught[0].category, MpiNotFoundWarning)
        assert "remote host" in str(caught[0].message)


def test_mpi_stdin_is_explicit_and_serial_fallback_is_unchanged(monkeypatch, tmp_path):
    monkeypatch.setattr("kkrunner.backends.process.primitives.resolve_mpi_command", lambda mpi: ("mpirun",))
    spec = ExecutionSpec(
        argv=("serial",),
        mpi_argv=("parallel",),
        launch=LaunchSpec.from_mpi(True),
        input_file=None,
        output_file=None,
        directory=str(tmp_path),
        stdin=str(tmp_path / "separate-stdin.dat"),
    )
    assert materialize_local_spec(spec).stdin == spec.stdin
    disabled = materialize_local_spec(__import__("dataclasses").replace(spec, mpi_uses_stdin=False))
    assert disabled.stdin is None
