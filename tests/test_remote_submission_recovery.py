import hashlib
import pytest

from kkrunner import RunState
from kkrunner.remote import service as remote_service
from kkrunner.backends.handles import _decode_handle
from kkrunner.backends.process import operations as process_operations
from kkrunner.backends.psij import agent as scheduler_operations


def test_direct_remote_process_without_pending_identity_returns_none(tmp_path, monkeypatch):
    monkeypatch.setenv("KKRunner_REMOTE_STATE_DIR", str(tmp_path))
    assert remote_service.recover_remote("a" * 32, {"executor": "process", "directory": "/scratch/case"}) is None


def test_psij_recovery_uses_backend_native_identity(monkeypatch):
    calls = []

    class Backend:
        backend = "slurm"
        url = None

        def recover_native(self, run_id, directory, *, submitted_at=None):
            calls.append((run_id, directory, submitted_at))
            return {
                "native_id": "scheduler-42",
                "state": RunState.RUNNING,
                "directory": "/scratch/case",
                "stdout": "/scratch/case/run.out",
                "stderr": "/scratch/case/run.err",
                "return_code": None,
            }

    monkeypatch.setattr(scheduler_operations, "_psij_executor", lambda options: Backend())
    recovered = remote_service.recover_remote(
        "client-run",
        {"executor": "psij", "executor_options": {"backend": "slurm", "url": None}, "directory": "/scratch/case"},
        submitted_at="2026-10-13T08:00:00+02:00",
    )
    assert calls == [("client-run", "/scratch/case", "2026-10-13T08:00:00+02:00")]
    assert recovered["state"] == RunState.RUNNING.value
    assert recovered["directory"] == "/scratch/case"
    assert recovered["output_file"] == "/scratch/case/run.out"
    handle = _decode_handle(recovered["remote_id"])
    assert handle == {"kind": "psij", "backend": "slurm", "url": None, "native_id": "scheduler-42"}


def test_psij_recovery_returns_none_when_backend_has_no_match(monkeypatch):

    class Backend:
        backend = "slurm"
        url = None

        def recover_native(self, run_id, directory, *, submitted_at=None):
            return None

    monkeypatch.setattr(scheduler_operations, "_psij_executor", lambda options: Backend())
    assert (
        remote_service.recover_remote(
            "client-run", {"executor": "psij", "executor_options": {"backend": "slurm"}, "directory": "/scratch/case"}
        )
        is None
    )


def test_ticket_and_outcome_share_configured_state_root(tmp_path, monkeypatch):
    from kkrunner.backends.process import mailbox as remote_mailbox

    key = "f" * 32
    monkeypatch.setenv("KKRunner_REMOTE_STATE_DIR", str(tmp_path / "state"))
    ticket = process_operations._submission_ticket(key)
    receipt = remote_mailbox._path(key)
    assert ticket == tmp_path / "state" / "remote-submissions" / (hashlib.sha256(key.encode()).hexdigest() + ".json")
    assert receipt == tmp_path / "state" / "remote-outcomes" / f"{key}.json"
    process_operations._publish_submission(key, {"remote_id": "job-12"})
    assert process_operations._recover_submission(key) == {"remote_id": "job-12"}
    assert remote_service.acknowledge_submission("process", key) == {"acknowledged": True}
    assert process_operations._recover_submission(key) is None
    assert remote_service.acknowledge_submission("process", key) == {"acknowledged": False}


def test_ticket_uses_xdg_state_home_without_override(tmp_path, monkeypatch):
    key = "e" * 32
    monkeypatch.delenv("KKRunner_REMOTE_STATE_DIR", raising=False)
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "xdg"))
    assert (
        process_operations._submission_ticket(key) == tmp_path / "xdg" / "kkrunner" / "remote-submissions" / (hashlib.sha256(key.encode()).hexdigest() + ".json")
    )


def test_ticket_validation_is_independent_from_receipt_layout():
    with pytest.raises(ValueError, match="Invalid persistent run id"):
        process_operations._submission_ticket("../invalid")
