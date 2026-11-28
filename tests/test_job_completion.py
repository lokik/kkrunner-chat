"""Completion callback behavior on a caller-owned Job."""

import sys

import pytest

from kkrunner import ExecutionSpec, RunState
from kkrunner.backends.contracts import SubmissionNotStarted
from kkrunner.client import ExecutionClient


@pytest.mark.parametrize("fails", [False, True])
def test_job_callback_receives_native_outcome_once(tmp_path, fails):
    outcomes = []

    def callback(result):
        outcomes.append(result)
        if fails:
            raise ValueError("callback failed")

    parts = ExecutionClient().create_run_parts(
        ExecutionSpec([sys.executable, "-c", "pass"], directory=tmp_path),
        callback=callback,
    )
    try:
        parts.files.prepare_submission()
        parts.prepare_job()
        if fails:
            with pytest.raises(ValueError, match="callback failed"):
                parts.job.run()
            with pytest.raises(ValueError, match="callback failed"):
                parts.job.wait()
        else:
            assert parts.job.run() == parts.job.wait()
        assert parts.job.state == RunState.FINISHED
        assert outcomes == [parts.job.status()]
    finally:
        parts.close()


def test_job_execution_error_has_priority_over_callback_error(tmp_path):
    outcomes = []

    def callback(result):
        outcomes.append(result)
        raise ValueError("callback failed")

    parts = ExecutionClient().create_run_parts(
        ExecutionSpec([str(tmp_path / "missing-executable")], directory=tmp_path),
        callback=callback,
    )
    try:
        parts.files.prepare_submission()
        parts.prepare_job()
        with pytest.raises(SubmissionNotStarted) as original:
            parts.job.run()
        with pytest.raises(SubmissionNotStarted) as repeated:
            parts.job.wait()
        assert repeated.value is original.value
        assert len(outcomes) == 1
        assert outcomes[0].state == RunState.FAILED
    finally:
        parts.close()


@pytest.mark.parametrize("level", ["job", "run"])
def test_result_is_not_published_before_completion_callback_finishes(tmp_path, level):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    entered, release = Event(), Event()
    failure = ValueError("callback failed")

    def callback(result):
        entered.set()
        assert release.wait(5)
        raise failure

    client = ExecutionClient()
    spec = ExecutionSpec([sys.executable, "-c", "pass"], directory=tmp_path)
    if level == "job":
        parts = client.create_run_parts(spec, callback=callback)
        parts.files.prepare_submission()
        parts.prepare_job()
        handle = parts.job
        close = parts.close
    else:
        handle = client.create_run(spec, callback=callback)
        close = handle.close
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            first = pool.submit(handle.run)
            try:
                assert entered.wait(5)
                with pytest.raises(RuntimeError, match="active caller"):
                    handle.wait()
            finally:
                release.set()
            with pytest.raises(ValueError) as caught:
                first.result(timeout=5)
        assert caught.value is failure
        with pytest.raises(ValueError) as repeated:
            handle.wait()
        assert repeated.value is failure
    finally:
        close()
