"""MPI intent stays in ExecutionSpec; launchers resolve only on the target."""

import json
import shutil
from dataclasses import asdict, replace

import pytest

from kkrunner import ExecutionSpec
from kkrunner.execution import MpiSpec
from kkrunner.backends.support import process as mpi_runtime
from kkrunner.client.execution_request import execution_request


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

    monkeypatch.setattr(shutil, "which", which)
    assert mpi_runtime.resolve_mpi_options({"required": True, "autodetect": True}) == expected
    assert seen == list(probed)


def test_mpi_spec_distinguishes_required_auto_and_explicit_launchers():
    required = MpiSpec.from_value(True)
    automatic = MpiSpec.from_value("auto")
    explicit = MpiSpec.from_value(("custom-mpi", "--bind"))
    assert required.required and required.autodetect
    assert not required.allows_serial_fallback
    assert automatic.autodetect and not automatic.required
    assert automatic.allows_serial_fallback
    assert explicit.required and not explicit.autodetect
    assert explicit.launcher == ("custom-mpi", "--bind")


def test_mpi_spec_json_roundtrip_and_immutable_execution_spec():
    mpi = MpiSpec.from_value(4)
    encoded = json.loads(json.dumps(asdict(mpi)))
    assert encoded == {
        "required": True, "autodetect": True, "launcher": [],
        "process_count": 4, "warning": True,
    }
    assert MpiSpec(**encoded) == mpi
    spec = ExecutionSpec(("serial",), mpi_argv=("parallel",), mpi=mpi)
    before = asdict(spec)
    request = execution_request(spec)
    assert request["mpi"] == encoded
    assert request["mpi_argv"] == ["parallel"]
    assert asdict(spec) == before


def test_auto_disables_mpi_when_only_one_cpu_is_available(monkeypatch):
    monkeypatch.setattr(mpi_runtime.os, "sched_getaffinity", lambda _pid: {3})
    monkeypatch.setattr(shutil, "which", lambda _: pytest.fail("single CPU must not probe MPI"))
    assert mpi_runtime.resolve_mpi_command(MpiSpec.from_value("auto")) is None


@pytest.mark.parametrize("affinity", [None, {2, 7}])
def test_auto_probes_host_when_multiple_cpus_are_available(monkeypatch, affinity):
    if affinity is None:
        monkeypatch.delattr(mpi_runtime.os, "sched_getaffinity", raising=False)
    else:
        monkeypatch.setattr(mpi_runtime.os, "sched_getaffinity", lambda _pid: affinity)
    monkeypatch.setattr(shutil, "which", lambda n: "/bin/mpirun" if n == "mpirun" else None)
    assert mpi_runtime.resolve_mpi_command(MpiSpec.from_value("auto")) == ("mpirun",)


def test_explicit_mpi_command_does_not_probe_host(monkeypatch):
    monkeypatch.setattr(shutil, "which", lambda _: pytest.fail("explicit launcher must not probe host"))
    mpi = MpiSpec.from_value(("custom-mpi", "--bind"))
    assert mpi_runtime.resolve_mpi_command(mpi) == ("custom-mpi", "--bind")


@pytest.mark.parametrize("launcher,option", [("mpirun", "-np"), ("srun", "-n")])
def test_process_count_is_materialized_on_execution_host(monkeypatch, launcher, option):
    monkeypatch.setattr(shutil, "which", lambda n: "/bin/" + n if n == launcher else None)
    if launcher == "srun":
        monkeypatch.setenv("SLURM_NODELIST", "node1")
    else:
        monkeypatch.delenv("SLURM_NODELIST", raising=False)
    assert mpi_runtime.resolve_mpi_command(MpiSpec.from_value(4)) == (launcher, option, "4")


def test_resolver_requires_validated_mpi_spec():
    with pytest.raises(TypeError, match="MpiSpec"):
        mpi_runtime.resolve_mpi_command("auto")


def test_target_prepares_serial_or_parallel_command_without_changing_source(monkeypatch, tmp_path):
    monkeypatch.setenv("SLURM_NODELIST", "node1")
    monkeypatch.setattr(shutil, "which", lambda name: "/bin/srun" if name == "srun" else None)
    spec = ExecutionSpec(
        ("serial-program",), mpi_argv=("parallel-program", "run.inp"),
        mpi=MpiSpec.from_value(16), stdin="run.inp", directory=str(tmp_path),
    )
    request = execution_request(spec)
    outcome = mpi_runtime.prepare_mpi(request)
    assert outcome["request"]["argv"] == ["srun", "-n", "16", "parallel-program", "run.inp"]
    assert outcome["request"]["stdin"] == str(tmp_path / "run.inp")
    assert request["argv"] == ["serial-program"]
    assert spec.argv == ("serial-program",)


def test_required_mpi_without_launcher_fails(monkeypatch, tmp_path):
    monkeypatch.delenv("SLURM_NODELIST", raising=False)
    monkeypatch.setattr(shutil, "which", lambda _: None)
    spec = ExecutionSpec(("serial",), mpi_argv=("parallel",),
                         mpi=MpiSpec.from_value(True), directory=str(tmp_path))
    with pytest.raises(FileNotFoundError, match="no MPI launcher"):
        mpi_runtime.prepare_mpi(execution_request(spec))


@pytest.mark.parametrize("warn", [True, False])
def test_auto_mpi_fallback_retains_serial_command_and_warning_policy(monkeypatch, warn):
    monkeypatch.delenv("SLURM_NODELIST", raising=False)
    monkeypatch.setattr(shutil, "which", lambda _: None)
    spec = ExecutionSpec(("serial",), mpi_argv=("parallel",),
                         mpi=MpiSpec.from_value("auto", warning=warn))
    resolved = mpi_runtime.prepare_mpi(execution_request(spec))
    assert resolved["request"]["argv"] == ["serial"]
    assert bool(resolved["warning"]) is warn
    if warn:
        assert "without MPI" in resolved["warning"]


def test_mpi_stdin_policy_is_explicit_and_serial_fallback_unchanged():
    spec = ExecutionSpec(("serial",), mpi_argv=("parallel",),
                         mpi=MpiSpec.from_value(("mpirun",)), stdin="separate.inp")
    with_stdin = mpi_runtime.prepare_mpi(execution_request(spec))
    without_stdin = mpi_runtime.prepare_mpi(execution_request(replace(spec, mpi_uses_stdin=False)))
    assert with_stdin["request"]["stdin"] is not None
    assert without_stdin["request"]["stdin"] is None
    assert without_stdin["use_stdin"] is False
