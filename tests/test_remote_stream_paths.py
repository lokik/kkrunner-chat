"""Backend execution handles contain identity, never file path metadata."""

from kkrunner.remote.handles import decode_handle, encode_handle
from kkrunner.backends.contracts import JobRef, RecoveryResult


def test_scheduler_handle_contains_only_native_execution_identity():
    identity = {"kind": "psij", "executor_name": "slurm", "url": None, "native_id": "123"}
    handle = encode_handle(identity)
    assert decode_handle(handle) == identity
    assert all(key not in decode_handle(handle) for key in (
        "directory", "output_file", "error_file", "run_id",
    ))


def test_recovery_stream_locations_are_separate_from_job_reference():
    handle = encode_handle({"kind": "psij", "executor_name": "slurm", "url": None, "native_id": "123"})
    recovery = RecoveryResult.found(
        JobRef("psij", handle),
        directory="/scratch/work", output_file="run.out", error_file="run.err",
    )
    assert recovery.ref.token == handle
    assert "run.out" not in decode_handle(handle).values()
    assert recovery.output_file == "run.out"
    assert recovery.error_file == "run.err"
