"""Authoritative stdout/stderr verification belongs to the client file session."""

import asyncio
from types import SimpleNamespace

import pytest

from kkrunner.client.file_session import FileSession
from kkrunner.client.file_context import FileContext
from kkrunner.client import EndpointClient, StagingSpec
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
    client = EndpointClient()
    session = FileSession(
        FileContext(client, StagingSpec()),
        ExecutionSpec(argv=(), directory=tmp_path),
        files=manifest,
    )
    return session, local_out, local_err, remote_out, remote_err


@pytest.mark.parametrize("channel", ("stdout", "stderr"))
def test_equal_size_corruption_is_rejected(tmp_path, channel):
    session, local_out, local_err, _, _ = _session(
        tmp_path,
        stdout=b"bad!" if channel == "stdout" else b"same",
        stderr=b"wrong" if channel == "stderr" else b"error",
    )
    with pytest.raises(RuntimeError, match=f"{channel} differs from remote"):
        asyncio.run(session._verify_streams())
    assert getattr(session.files, channel).availability_state == "failed"
    assert (local_out if channel == "stdout" else local_err).exists()


@pytest.mark.parametrize("channel", ("stdout", "stderr"))
def test_missing_authoritative_stream_is_not_accepted(tmp_path, channel):
    session, _, _, remote_out, remote_err = _session(tmp_path)
    (remote_out if channel == "stdout" else remote_err).unlink()
    with pytest.raises(FileNotFoundError, match=f"Remote {channel} output is missing"):
        asyncio.run(session._verify_streams())
    assert getattr(session.files, channel).availability_state == "failed"


def test_successful_stream_integrity_verification_restores_timestamps(tmp_path):
    session, local_out, local_err, remote_out, remote_err = _session(tmp_path)
    asyncio.run(session._verify_streams())
    assert local_out.stat().st_mtime_ns == remote_out.stat().st_mtime_ns
    assert local_err.stat().st_mtime_ns == remote_err.stat().st_mtime_ns
    assert session.files.failed_artifacts == ()
