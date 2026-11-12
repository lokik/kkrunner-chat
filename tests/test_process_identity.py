"""Identity must survive client restarts without trusting a recycled PID."""

import json
import os
import subprocess
import sys
from types import SimpleNamespace

import psutil
import pytest

from kkrunner.support.process_identity import ProcessIdentity


def fake_process(monkeypatch, *, created=1_700_001_234.56, status=psutil.STATUS_RUNNING, running=True):
    # Synthetic psutil identities must also have a verifiable Linux boot ID.
    # Production intentionally rejects PID+start_time without that provenance.
    import kkrunner.support.process_identity as identity_module
    monkeypatch.setattr(
        identity_module, "_read_boot_id",
        lambda: "12345678-1234-4234-8234-123456789abc",
    )
    instances = []

    class Process:
        def __init__(self, pid):
            self.pid = pid
            instances.append(self)

        def create_time(self):
            return created() if callable(created) else created

        def status(self):
            return status() if callable(status) else status

        def is_running(self):
            return running() if callable(running) else running

    monkeypatch.setattr(psutil, "Process", Process)
    return instances


def test_linux_identity_ignores_clock_adjustment_between_observations(monkeypatch):
    monkeypatch.setattr(psutil, "LINUX", True)
    clock = SimpleNamespace(boot=1_700_000_000.0)
    monkeypatch.setattr(psutil, "boot_time", lambda: clock.boot)
    fake_process(monkeypatch, created=lambda: clock.boot + 1234.56)
    original = ProcessIdentity.read(42)
    for offset in (3600, -7200, 0.25):
        clock.boot += offset
        assert original.is_running()
        assert ProcessIdentity.read(42).start_time == pytest.approx(1234.56, abs=1e-6, rel=0)


def test_clock_adjustment_during_read_retries_with_fresh_process(monkeypatch):
    monkeypatch.setattr(psutil, "LINUX", True)
    readings = iter((1000.0, 4600.0, 4600.0, 4600.0))
    monkeypatch.setattr(psutil, "boot_time", lambda: next(readings))
    created = iter((1100.0, 4700.0))
    instances = fake_process(monkeypatch, created=lambda: next(created))
    identity = ProcessIdentity.read(42)
    assert identity.start_time == pytest.approx(100.0, abs=1e-6, rel=0)
    assert len(instances) == 2


def test_persistently_unstable_clock_does_not_confirm_identity(monkeypatch):
    monkeypatch.setattr(psutil, "LINUX", True)
    calls = iter(range(1000, 1010))
    monkeypatch.setattr(psutil, "boot_time", lambda: float(next(calls)))
    instances = fake_process(monkeypatch)
    assert not ProcessIdentity(42, 100.0, "linux-boot").is_running()
    assert len(instances) == 3


@pytest.mark.parametrize("linux,basis", [(True, "linux-boot"), (False, "epoch")])
def test_recycled_pid_is_rejected(monkeypatch, linux, basis):
    monkeypatch.setattr(psutil, "LINUX", linux)
    monkeypatch.setattr(psutil, "boot_time", lambda: 1_700_000_000.0)
    created = SimpleNamespace(value=1_700_001_234.56)
    fake_process(monkeypatch, created=lambda: created.value)
    identity = ProcessIdentity.read(42)
    assert identity.time_basis == basis
    created.value += 0.001
    assert not identity.is_running()


def test_pid_reuse_during_read_is_rejected(monkeypatch):
    monkeypatch.setattr(psutil, "LINUX", False)
    fake_process(monkeypatch, running=False)
    assert ProcessIdentity.read(42) is None


@pytest.mark.parametrize("error", [psutil.NoSuchProcess(42), psutil.AccessDenied(42), psutil.ZombieProcess(42)])
@pytest.mark.parametrize("operation", ["Process", "create_time", "status", "is_running", "boot_time"])
def test_failed_read_never_confirms_identity(monkeypatch, error, operation):
    monkeypatch.setattr(psutil, "LINUX", True)
    monkeypatch.setattr(psutil, "boot_time", lambda: 1_700_000_000.0)
    fake_process(monkeypatch)

    def fail(*args):
        raise error

    if operation in ("Process", "boot_time"):
        monkeypatch.setattr(psutil, operation, fail)
    else:
        monkeypatch.setattr(psutil.Process, operation, fail)
    assert not ProcessIdentity(42, 1234.56, "linux-boot").is_running()


@pytest.mark.parametrize("status", [psutil.STATUS_ZOMBIE, psutil.STATUS_DEAD])
def test_dead_process_is_not_running(monkeypatch, status):
    monkeypatch.setattr(psutil, "LINUX", False)
    fake_process(monkeypatch, status=status)
    assert ProcessIdentity.read(42) is None


def test_other_platforms_use_epoch_and_never_read_boot_time(monkeypatch):
    monkeypatch.setattr(psutil, "LINUX", False)
    monkeypatch.setattr(psutil, "boot_time", lambda: pytest.fail("Only Linux normalizes against boot time"))
    fake_process(monkeypatch, created=1_700_000_123.456)
    assert ProcessIdentity.read(42) == ProcessIdentity(42, 1_700_000_123.456, "epoch")


def test_tolerance_only_covers_float_roundoff(monkeypatch):
    monkeypatch.setattr(psutil, "LINUX", False)
    created = 1_700_000_123.456
    fake_process(monkeypatch, created=created)
    assert ProcessIdentity(42, created + 0.0000005, "epoch").is_running()
    assert not ProcessIdentity(42, created + 0.00001, "epoch").is_running()
    assert not ProcessIdentity(42, created, "linux-boot").is_running()


@pytest.mark.parametrize("pid", [True, 0, -1, "42", None])
def test_invalid_pid_is_not_probed(monkeypatch, pid):
    monkeypatch.setattr(psutil, "Process", lambda pid: pytest.fail("Invalid PID must not reach psutil"))
    assert ProcessIdentity.read(pid) is None


@pytest.mark.parametrize(
    "value",
    [
        None,
        {},
        {"pid": True, "start_time": 1.0, "time_basis": "epoch"},
        {"pid": 42, "start_time": True, "time_basis": "epoch"},
        {"pid": 42, "start_time": float("nan"), "time_basis": "epoch"},
        {"pid": 42, "start_time": float("inf"), "time_basis": "epoch"},
        {"pid": 42, "start_time": -1.0, "time_basis": "epoch"},
        {"pid": 42, "start_time": 1.0, "time_basis": "ticks"},
    ],
)
def test_malformed_persisted_identity_is_rejected(value):
    with pytest.raises(ValueError, match="Invalid process identity"):
        ProcessIdentity.from_dict(value)


def test_identity_roundtrips_in_a_new_interpreter():
    identity = ProcessIdentity.read(os.getpid())
    assert identity is not None
    code = """
import json, sys
from kkrunner.support.process_identity import ProcessIdentity
identity = ProcessIdentity.from_dict(json.loads(sys.argv[1]))
assert identity.is_running(), identity
"""
    subprocess.run([sys.executable, "-c", code, json.dumps(identity.to_dict())], check=True, timeout=10)


@pytest.fixture
def synthetic_linux_proc(tmp_path, monkeypatch):
    if not psutil.LINUX:
        pytest.skip("The synthetic proc fixture exercises the real Linux psutil backend")
    # PROCFS_PATH is public psutil API; psutil itself reads these synthetic files.
    monkeypatch.setattr(psutil, "PROCFS_PATH", str(tmp_path))
    pid = 42
    process_dir = tmp_path / str(pid)
    process_dir.mkdir()

    def write(*, boot=1_700_000_000, ticks=123456, state="S"):
        fields = ["0"] * 50
        fields[0], fields[19] = state, str(ticks)
        (process_dir / "stat").write_text(f"{pid} (synthetic process) " + " ".join(fields) + "\n")
        (tmp_path / "stat").write_text(f"btime {boot}\n")

    write()
    return write, pid


def test_real_psutil_normalized_time_survives_forward_and_backward_clock_steps(synthetic_linux_proc):
    write, pid = synthetic_linux_proc
    original = ProcessIdentity.read(pid)
    assert original is not None
    cached_process = psutil.Process(pid)
    cached_created = cached_process.create_time()
    for boot in (1_700_003_600, 1_699_996_400, 1_700_086_400):
        write(boot=boot)
        # A cached Process demonstrates why subtracting a new boot time is unsafe.
        assert cached_process.create_time() == cached_created
        assert abs(cached_process.create_time() - psutil.boot_time() - original.start_time) > 1
        fresh = psutil.Process(pid).create_time() - psutil.boot_time()
        assert fresh == pytest.approx(original.start_time, abs=1e-6, rel=0)
        assert original.is_running()


def test_real_psutil_retries_a_clock_step_between_calls(synthetic_linux_proc, monkeypatch):
    write, pid = synthetic_linux_proc
    boot_time = psutil.boot_time
    calls = []

    def adjust_after_first_read():
        value = boot_time()
        calls.append(value)
        if len(calls) == 1:
            write(boot=1_700_003_600)
        return value

    monkeypatch.setattr(psutil, "boot_time", adjust_after_first_read)
    identity = ProcessIdentity.read(pid)
    assert identity is not None
    assert len(calls) == 4
    assert identity.start_time == pytest.approx(123456 / os.sysconf("SC_CLK_TCK"), abs=1e-6, rel=0)


def test_real_psutil_detects_reuse_between_creation_time_and_status(synthetic_linux_proc, monkeypatch):
    write, pid = synthetic_linux_proc
    status = psutil.Process.status

    def recycled_status(process):
        write(ticks=123457)
        return status(process)

    monkeypatch.setattr(psutil.Process, "status", recycled_status)
    assert ProcessIdentity.read(pid) is None


def test_normalized_identity_survives_clock_step_and_reader_restart(synthetic_linux_proc, tmp_path):
    write, pid = synthetic_linux_proc
    identity = ProcessIdentity.read(pid)
    write(boot=1_700_003_600)
    code = """
import json, sys, psutil
from kkrunner.support.process_identity import ProcessIdentity
psutil.PROCFS_PATH = sys.argv[1]
assert ProcessIdentity.from_dict(json.loads(sys.argv[2])).is_running()
"""
    subprocess.run([sys.executable, "-c", code, str(tmp_path), json.dumps(identity.to_dict())], check=True, timeout=10)
