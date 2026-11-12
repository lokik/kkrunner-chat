"""Focused tests for optional remote artifact availability."""

import asyncio

import pytest

from kkrunner.files.manifest import FileEntry, FileManifest, FileSyncPolicy
from kkrunner.client.file_session import FileSession
from kkrunner.client.file_context import FileContext
from kkrunner.client import EndpointClient, StagingSpec
from kkrunner.runs.base import ExecutionSpec


def _runner(tmp_path):
    local = str(tmp_path / "local-optional.out")
    remote = str(tmp_path / "remote-optional.out")
    manifest = FileManifest(outputs=(FileEntry(local_path=local, remote_path=remote, availability_state="pending"),))
    client = EndpointClient()
    context = FileContext(client, StagingSpec())
    runner = FileSession(
        context,
        ExecutionSpec(argv=(), input_file=None, output_file=None, directory=str(tmp_path)),
        files=manifest,
    )
    return (runner, local)


def test_optional_missing_remote_output_is_skipped(tmp_path):
    runner, local = _runner(tmp_path)
    asyncio.run(runner._sync_output_files(required=False))
    assert runner.files.outputs[0].availability_state == "skipped"
    assert not (tmp_path / "local-optional.out").exists()


def test_required_missing_remote_output_is_failed(tmp_path):
    runner, local = _runner(tmp_path)
    with pytest.raises(FileNotFoundError, match="was not created"):
        asyncio.run(runner._sync_output_files(required=True))
    assert runner.files.outputs[0].availability_state == "failed"
