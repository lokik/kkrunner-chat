"""Regressions found during independent extraction review."""

import json
from threading import Event, Thread

import pytest

from kkrunner.runs import CorruptRunRecord
from kkrunner import RunState, RunStore
from kkrunner.runs.manager import RunManager


@pytest.mark.parametrize("data", [[], None, 42])
def test_non_object_json_root_raises_corrupt_record(tmp_path, data):
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = manager.new_record(directory=tmp_path)
    manager.store.save(record)
    path = manager.store._path(record)
    path.write_text(json.dumps(data))

    with pytest.raises(CorruptRunRecord, match="must be a JSON object") as error:
        manager.store.get(record.id)
    assert error.value.path == path


def test_cancel_during_failed_submit_does_not_reenter_submission_lock():
    from .test_cancellation import _controlled_job

    job, execution = _controlled_job(fail_submit=True)
    finished = Event()
    errors = []

    def submit():
        try:
            job.submit()
        except BaseException as exc:
            errors.append(exc)
        finally:
            finished.set()

    submitting = Thread(target=submit, daemon=True)
    submitting.start()
    try:
        assert execution.entered.wait(3)
        future = job.cancel()
    finally:
        execution.release.set()

    assert finished.wait(3), "failed submission deadlocked"
    submitting.join(3)
    assert not submitting.is_alive()
    assert future.result(timeout=3) is None
    assert len(errors) == 1
    assert isinstance(errors[0], RuntimeError)
    assert str(errors[0]) == "submission rejected"
