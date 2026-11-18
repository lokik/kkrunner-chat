"""Cancellation of direct jobs using identity persisted across client restarts."""
import os
import signal
import subprocess
import sys
import time

import pytest

from kkrunner.backends.implementations.direct.backend import DirectBackend
from kkrunner.backends.contracts import BackendCapability
from kkrunner.client import EndpointClient, LocalTransport
from kkrunner.client.contracts import JobAttachment
from kkrunner import RunManager, RunState, RunStore
from kkrunner.backends.config import BackendConfig
from kkrunner.client.contracts import Operation
from kkrunner.transports.implementations.ssh.rpc import operation_command
from kkrunner.transports.rpc import operation_payload, operation_result


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


@pytest.mark.parametrize("remote", (False, True), ids=("local", "source-delivered"))
@pytest.mark.parametrize("leader_already_exited", (False, True))
def test_cancellation_terminates_descendant_ignoring_term(tmp_path, remote, leader_already_exited):
    """A dead leader must neither confirm cancellation nor prevent escalation."""
    descendant = (
        "import os,signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        "print(os.getpid(), flush=True); time.sleep(60)"
    )
    leader = subprocess.Popen(
        [sys.executable, "-c",
         ("import subprocess,sys,time; "
         "child=subprocess.Popen([sys.executable, '-c', sys.argv[1]], stdout=subprocess.PIPE); "
         "print(child.stdout.readline().decode().strip(), flush=True); time.sleep(60)"), descendant],
        start_new_session=True, stdout=subprocess.PIPE, text=True,
    )
    try:
        descendant_pid = int(leader.stdout.readline())
        identity = _identity(leader.pid)
        if leader_already_exited:
            leader.terminate()
            leader.wait(timeout=5)
        assert DirectBackend.status(identity)["state"] == "running"
        if remote:
            command = operation_command(Operation.for_backend(DirectBackend, "cancel"),
                                        python=(sys.executable, "-I", "-S"))
            reply = subprocess.run(command, input=operation_payload((identity,), {"timeout": 0.05}),
                                   capture_output=True, text=True, check=True, cwd=tmp_path, timeout=5)
            outcome = operation_result(reply.stdout)
        else:
            outcome = DirectBackend.cancel(identity, timeout=0.05)
        assert outcome["state"] in ("cancelled", "running")
        deadline = time.monotonic() + 5
        while DirectBackend.status(identity)["state"] == "running" and time.monotonic() < deadline:
            time.sleep(0.01)
        assert DirectBackend.status(identity)["state"] == "finished"
        try:
            with open(f"/proc/{descendant_pid}/stat") as source:
                state = source.read().rsplit(")", 1)[1].split()[0]
        except FileNotFoundError:
            pass
        else:
            assert state in ("Z", "X", "x")
    finally:
        try:
            os.killpg(leader.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        leader.wait(timeout=5)
        leader.stdout.close()


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
