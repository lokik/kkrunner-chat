"""Explicit caller-owned Job waiting and completion contracts."""

import asyncio
import sys

import pytest

from kkrunner import ExecutionSpec, RunState
from kkrunner.client import ExecutionClient


def test_wait_without_native_identity_fails_immediately(tmp_path):
    spec = ExecutionSpec((sys.executable, "-c", "pass"), directory=tmp_path)
    job = ExecutionClient().create_run_parts(spec).job
    with pytest.raises(RuntimeError, match="Job has not been submitted"):
        job.wait()
    assert job.state == RunState.NEW


def test_wait_and_run_async_reuse_completed_result(tmp_path):
    spec = ExecutionSpec((sys.executable, "-c", "pass"), directory=tmp_path)
    seen = []
    parts = ExecutionClient().create_run_parts(spec, callback=seen.append)
    try:
        parts.files.prepare_submission()
        parts.prepare_job()
        job = parts.job
        result = job.run()
        assert result.state == RunState.FINISHED
        assert result.return_code == 0
        assert job.wait() == result
        assert asyncio.run(job.run_async()) == result
        assert seen == [result]
    finally:
        parts.close()
