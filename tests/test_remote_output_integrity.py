"""Primary output files use ordinary terminal synchronization policies."""

import asyncio

import pytest

from kkrunner.client.file_session import FileSession
from kkrunner.client.file_context import FileContext
from kkrunner.client import ExecutionClient, StagingSpec
from kkrunner.files.manifest import FileEntry, FileManifest
from kkrunner.runs.base import ExecutionSpec


def _session(tmp_path, *, stdout=b"same", stderr=b"error"):
    local_out = tmp_path / "local.out"
    local_err = tmp_path / "local.err"
    remote_out = tmp_path / "remote.out"
    remote_err = tmp_path / "remote.err"
    local_out.write_bytes(stdout)
    local_err.write_bytes(stderr)
    remote_out.write_bytes(b"same")
    remote_err.write_bytes(b"error")
    manifest = FileManifest(
        stdout=FileEntry(str(local_out), str(remote_out)),
        stderr=FileEntry(str(local_err), str(remote_err)),
    ).initialize_availability()
    client = ExecutionClient()
    session = FileSession(
        FileContext(client, StagingSpec()),
        ExecutionSpec(argv=(), directory=tmp_path),
        files=manifest,
    )
    return session, local_out, local_err, remote_out, remote_err


@pytest.mark.parametrize("channel", ("stdout", "stderr"))
def test_equal_size_stale_output_is_replaced(tmp_path, channel):
    session, local_out, local_err, _, _ = _session(
        tmp_path,
        stdout=b"bad!" if channel == "stdout" else b"same",
        stderr=b"wrong" if channel == "stderr" else b"error",
    )
    asyncio.run(session._sync_output_files(required=True))
    assert getattr(session.files, channel).availability_state == "available"
    assert local_out.read_bytes() == b"same"
    assert local_err.read_bytes() == b"error"


@pytest.mark.parametrize("channel", ("stdout", "stderr"))
def test_missing_authoritative_stream_is_not_accepted(tmp_path, channel):
    session, _, _, remote_out, remote_err = _session(tmp_path)
    (remote_out if channel == "stdout" else remote_err).unlink()
    with pytest.raises(FileNotFoundError, match="was not created"):
        asyncio.run(session._sync_output_files(required=True))
    assert getattr(session.files, channel).availability_state == "failed"


def test_matching_output_is_not_rewritten(tmp_path):
    session, local_out, local_err, _, _ = _session(tmp_path)
    before = (local_out.stat(), local_err.stat())
    asyncio.run(session._sync_output_files(required=True))
    assert local_out.stat().st_mtime_ns == before[0].st_mtime_ns
    assert local_err.stat().st_mtime_ns == before[1].st_mtime_ns
    assert session.files.failed_artifacts == ()
