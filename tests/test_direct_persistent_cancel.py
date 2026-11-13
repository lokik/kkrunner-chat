"""Cancellation of direct jobs using identity persisted across client restarts."""
import os
import signal
import subprocess
import sys
import time

import pytest

from kkrunner.backends.direct.backend import DirectBackend
from kkrunner.backends.contracts import BackendCapability
from kkrunner.client import EndpointClient, LocalTransport
from kkrunner.client.contracts import JobAttachment
from kkrunner import RunManager, RunState, RunStore
from kkrunner.backends.config import BackendConfig


pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="Direct process identity uses Linux procfs"
)


def _start_child():
    return subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        start_new_session=True,
    )


def _identity(pid):
    with open(f"/proc/{pid}/stat", encoding="ascii") as source:
        fields = source.read().rsplit(")", 1)[1].split()
    with open("/proc/sys/kernel/random/boot_id", encoding="ascii") as source:
        boot = source.read().strip()
    assert int(fields[2]) == pid
    assert int(fields[3]) == pid
    return f"direct:{pid}:{int(fields[19])}:{boot}"


def test_direct_backend_advertises_cancel_capability():
    assert BackendCapability.CANCEL in DirectBackend().supports


def test_direct_cancel_rejects_legacy_session_identity():
    with pytest.raises(NotImplementedError, match="persistent PID"):
        DirectBackend.cancel("old-random-session-id")


def test_direct_cancel_rejects_invalid_native_identity():
    with pytest.raises(ValueError, match="Invalid direct process reference"):
        DirectBackend.cancel("direct:0:42:00000000-0000-0000-0000-000000000000")


def test_direct_backend_cancels_verified_process_group():
    child = _start_child()
    try:
        identity = _identity(child.pid)
        result = DirectBackend.cancel(identity, timeout=2)
        assert result["state"] in ("cancelled", "running")
        child.wait(timeout=4)
        assert child.returncode in (-signal.SIGTERM, -signal.SIGKILL)
        assert DirectBackend.cancel(identity)["state"] == "finished"
    finally:
        if child.poll() is None:
            os.killpg(child.pid, signal.SIGKILL)
            child.wait()


def test_direct_cancel_does_not_signal_reused_pid(tmp_path, monkeypatch):
    child = _start_child()
    try:
        identity = _identity(child.pid)
        components = identity.split(":")
        components[2] = str(int(components[2]) + 1)
        wrong_identity = ":".join(components)
        seen = []
        monkeypatch.setattr(os, "killpg", lambda pid, signum: seen.append((pid, signum)))
        result = DirectBackend.cancel(wrong_identity)
        assert result["state"] == "finished"
        assert not seen
        assert child.poll() is None
    finally:
        child.terminate()
        child.wait()


def test_restart_manager_cancels_saved_direct_job(tmp_path):
    child = _start_child()
    try:
        manager = RunManager(RunStore(tmp_path / "runs"))
        record = manager.new_record(
            directory=tmp_path,
            transport=LocalTransport(),
            backend=BackendConfig("direct"),
        )
        record = manager.transition(
            record,
            state=RunState.RUNNING,
            execution=record.execution.submitted(_identity(child.pid)),
        )
        restarted = RunManager(RunStore(tmp_path / "runs"))
        outcome = restarted.cancel(record.id)
        assert outcome.state in (RunState.RUNNING, RunState.CANCELLED)
        child.wait(timeout=4)
        assert restarted.store.get(record.id).execution.backend_id == record.execution.backend_id
    finally:
        if child.poll() is None:
            os.killpg(child.pid, signal.SIGKILL)
            child.wait()
