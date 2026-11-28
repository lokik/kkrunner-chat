"""Regressions found during independent extraction review."""

import json

import pytest

from kkrunner.runs import CorruptRunRecord
from kkrunner import RunStore
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
