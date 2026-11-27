"""Direct execution can recover liveness without claiming an exit code."""

import subprocess
import sys

import pytest

from kkrunner import ExecutionOutcome, RunState, RunStore
from kkrunner.backends import DirectBackend
from kkrunner.backends.config import BackendConfig
from kkrunner.backends.contracts import BackendCapability
from kkrunner.client import ExecutionClient, SshTarget
from kkrunner.client.contracts import JobAttachment
from kkrunner.files import FileEntry, FileManifest, FileMapping
from kkrunner.runs import ExecutionMetadata, WorkingDirectory
from kkrunner.runs.manager import RunManager
from kkrunner.runs.base import FailurePhase

@pytest.fixture
def ssh_loopback(monkeypatch):
    original = SshTarget._command

    def command(self, argv, cwd=None):
        if self.host == "loopback":
            assert cwd is None
            return tuple(argv)
        return original(self, argv, cwd=cwd)

    monkeypatch.setattr(SshTarget, "_command", command)
    return SshTarget("loopback", python_command=(sys.executable,))


def _linux_ref(pid):
    with open(f"/proc/{pid}/stat", encoding="ascii") as stream:
        fields = stream.read().rsplit(")", 1)[1].split()
    with open("/proc/sys/kernel/random/boot_id", encoding="ascii") as stream:
        boot = stream.read().strip()
    return f"direct:{pid}:{fields[19]}:{boot}"


@pytest.fixture
def process():
    child = subprocess.Popen(
        (sys.executable, "-c", "import time; time.sleep(30)"),
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    try:
        yield child
    finally:
        if child.poll() is None:
            child.terminate()
        child.wait(timeout=5)


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux procfs identity")
def test_direct_status_is_independent_of_live_session(process):
    ref = _linux_ref(process.pid)
    assert DirectBackend().supports == frozenset({BackendCapability.STATUS, BackendCapability.CANCEL, BackendCapability.REATTACH})
    running = DirectBackend.status(ref)
    assert running["state"] == "running"
    assert running["return_code"] is None
    process.terminate()
    process.wait(timeout=5)
    finished = DirectBackend.status(ref)
    assert finished["state"] == "finished"
    assert finished["return_code"] is None
    assert "exit code unavailable" in finished["error"]


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux procfs identity")
def test_recycled_pid_does_not_make_old_direct_job_run_again(process):
    ref = _linux_ref(process.pid)
    kind, pid, tick, boot = ref.split(":")
    recycled = f"{kind}:{pid}:{int(tick) + 1}:{boot}"
    assert DirectBackend.status(recycled)["state"] == "finished"
    assert DirectBackend.status("old-session-uuid")["state"] == "unknown"


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux procfs identity")
def test_ssh_status_uses_portable_operation_and_no_supervisor(process, tmp_path, ssh_loopback):
    ref = _linux_ref(process.pid)
    endpoint = ExecutionClient(DirectBackend, target=ssh_loopback)
    attachment = JobAttachment(
        ExecutionMetadata(
            endpoint.target, WorkingDirectory(str(tmp_path), str(tmp_path)),
            ref, backend=BackendConfig("direct"),
        ),
        FileManifest(),
    )
    assert endpoint.status(attachment)[0].state == RunState.RUNNING
    process.terminate()
    process.wait(timeout=5)
    result = endpoint.status(attachment)[0]
    assert result.state == RunState.FINISHED
    assert result.return_code is None


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux procfs identity")
def test_refresh_and_reservation_release_ended_unknown_code_job(process, tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    stdout = tmp_path / "missing.out"
    manifest = FileManifest(stdout=FileEntry(local_path=str(stdout)))
    record = manager.new_record(
        directory=tmp_path, backend=BackendConfig("direct"),
        files=manifest, processing_required=True,
    )
    record = manager.transition(
        record, state=RunState.RUNNING,
        execution=record.execution.submitted(_linux_ref(process.pid)),
    )
    assert manager.refresh((record,))[0].state == RunState.RUNNING
    process.terminate()
    process.wait(timeout=5)

    competing = manager.new_record(
        directory=tmp_path, backend=BackendConfig("direct"), files=manifest,
    )
    from kkrunner.runs import DuplicateSubmissionError
    with pytest.raises(DuplicateSubmissionError):
        manager.reserve(competing)
    assert manager.store.get(record.id).state == RunState.RUNNING
    reserved = manager.reserve(competing, recover_conflicts=True)
    assert reserved.state == RunState.SUBMITTING
    completed = manager.store.get(record.id)
    assert completed.state == RunState.FINISHED
    assert completed.return_code is None
    assert completed.files.stdout.availability_state == "skipped"


def test_uncertain_exit_can_be_refined_by_authoritative_outcome(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = manager.new_record(directory=tmp_path)
    record = manager.transition(record, state=RunState.RUNNING)
    unknown = manager.complete(record, state=RunState.FINISHED, return_code=None)
    assert unknown.state == RunState.FINISHED
    confirmed = manager.complete(
        unknown, state=RunState.FAILED, return_code=7,
        failure_phase=FailurePhase.EXECUTION, message="Exited with code 7",
    )
    assert confirmed.state == RunState.FAILED
    assert confirmed.return_code == 7


def test_unverified_completion_does_not_claim_postprocessing(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = manager.new_record(directory=tmp_path, processing_required=True)
    record = manager.transition(record, state=RunState.RUNNING)
    from kkrunner.runs.base import RunStatus
    pending = manager.update(record, RunStatus(RunState.FINISHED, return_code=None))
    assert pending.state == RunState.DOWNLOADING
    assert pending.return_code is None
    finished = manager.complete(pending, state=RunState.FINISHED, artifacts_available=True)
    assert finished.state == RunState.FINISHED
    assert finished.return_code is None


@pytest.mark.parametrize("code,expected", ((0, RunState.DOWNLOADING), (7, RunState.FAILED)))
def test_liveness_observation_preserves_known_exit_outcome(tmp_path, code, expected):
    from kkrunner.runs.base import RunStatus

    manager = RunManager(RunStore(tmp_path / "runs"))
    record = manager.new_record(directory=tmp_path, processing_required=True)
    record = manager.transition(record, state=RunState.DOWNLOADING, return_code=code)
    observed = manager.update(record, RunStatus(RunState.FINISHED, return_code=None))
    assert observed.return_code == code
    assert observed.state == expected


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux procfs identity")
def test_ssh_refresh_collects_available_outputs_without_exit_code(process, tmp_path, ssh_loopback):
    local = tmp_path / "local"
    remote = tmp_path / "remote"
    local.mkdir()
    remote.mkdir()
    (remote / "out.log").write_text("remote output\\n")
    (remote / "result.dat").write_text("partial result")
    endpoint = ExecutionClient(DirectBackend, target=ssh_loopback)
    manager = RunManager(RunStore(tmp_path / "runs"))
    manager.register_client(endpoint)
    manifest = FileManifest(
        stdout=FileEntry(
            local_path=str(local / "out.log"), remote_path=str(remote / "out.log"),
        ),
        outputs=(
            FileEntry(local_path=str(local / "result.dat"), remote_path=str(remote / "result.dat")),
            FileEntry(local_path=str(local / "optional.dat"), remote_path=str(remote / "optional.dat")),
        ),
        mapping=FileMapping(local_root=str(local), remote_root=str(remote)),
    )
    record = manager.new_record(
        directory=local, remote_directory=str(remote), target=endpoint.target,
        backend=endpoint.backend_config, files=manifest, processing_required=True,
    )
    record = manager.transition(
        record, state=RunState.RUNNING,
        execution=record.execution.submitted(_linux_ref(process.pid)),
    )
    process.terminate()
    process.wait(timeout=5)
    pending = manager.refresh((record,))[0]
    assert pending.state == RunState.DOWNLOADING
    assert not (local / "out.log").exists()
    assert not (local / "result.dat").exists()
    finished = manager.refresh((pending,), retrieve_ended=True)[0]
    assert finished.state == RunState.FINISHED
    assert finished.return_code is None
    assert (local / "out.log").read_text() == "remote output\\n"
    assert (local / "result.dat").read_text() == "partial result"
    assert finished.files.outputs[1].availability_state == "skipped"


@pytest.mark.parametrize("backend", ("direct", "kkrunner", "psij"))
def test_unreachable_status_does_not_claim_completion(tmp_path, monkeypatch, backend):
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = manager.new_record(
        directory=tmp_path, backend=BackendConfig(backend),
    )
    record = manager.transition(
        record, state=RunState.RUNNING,
        execution=record.execution.submitted("direct:123:4:fake-boot"),
    )
    def unreachable(self, *attachments):
        raise OSError("Host unreachable")
    monkeypatch.setattr(ExecutionClient, "status", unreachable)
    refreshed = manager.refresh((record,))[0]
    assert refreshed.state == RunState.UNKNOWN
    assert "Host unreachable" in refreshed.message


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux procfs identity")
def test_live_observer_keeps_reservation_during_postprocess(process, tmp_path):
    from kkrunner.runs import DuplicateSubmissionError

    manager = RunManager(RunStore(tmp_path / "runs"))
    manifest = FileManifest(stdout=FileEntry(local_path=str(tmp_path / "output.txt")))
    original = manager.new_record(directory=tmp_path, backend=BackendConfig("direct"), files=manifest)
    original = manager.transition(
        original, state=RunState.RUNNING,
        execution=original.execution.submitted(_linux_ref(process.pid)),
    )
    replacement = manager.new_record(directory=tmp_path, backend=BackendConfig("direct"), files=manifest)
    with manager.store.observation_lock(original):
        process.terminate()
        process.wait(timeout=5)
        assert manager.refresh((original,))[0].state == RunState.RUNNING
        with pytest.raises(DuplicateSubmissionError):
            manager.reserve(replacement)
    assert manager.reserve(replacement, recover_conflicts=True).state == RunState.SUBMITTING
    assert manager.store.get(original.id).state == RunState.FINISHED


def test_update_only_persists_supplied_status(tmp_path, monkeypatch):
    from kkrunner.runs.base import RunStatus

    manager = RunManager(RunStore(tmp_path / "runs"))
    local = tmp_path / "local"
    remote = tmp_path / "remote"
    manifest = FileManifest(stdout=FileEntry(
        local_path=str(local / "stdout"), remote_path=str(remote / "stdout"),
    ))
    record = manager.new_record(
        directory=local, remote_directory=str(remote),
        target=SshTarget("example"), backend=BackendConfig("direct"),
        files=manifest,
    )
    record = manager.transition(
        record, state=RunState.RUNNING,
        execution=record.execution.submitted("direct:123:4:fake-boot"),
    )

    def unexpected(*args, **kwargs):
        pytest.fail("Updating status must not use a client or inspect result files")

    monkeypatch.setattr(manager, "client_for", unexpected)
    monkeypatch.setattr("kkrunner.runs.manager.capture_result_files", unexpected)
    updated = manager.update(record, RunStatus(RunState.FINISHED))
    assert updated.state == RunState.DOWNLOADING
    assert updated.files == record.files
    assert manager.store.get(record.id) == updated
    assert not local.exists()
