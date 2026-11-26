"""History visibility does not decide whether a Run protects its files."""

import subprocess
import sys
import time
from dataclasses import replace

import pytest

from kkrunner import ExecutionSpec, Run, RunManager, RunState, RunStore
from kkrunner.client import EndpointClient
from kkrunner.runs import DuplicateSubmissionError
from kkrunner.files import FileManifest


@pytest.mark.parametrize("first_tracked, second_tracked", [(False, False), (False, True), (True, False)])
def test_reservations_are_shared_independently_of_history(tmp_path, first_tracked, second_tracked):
    store = tmp_path / "runs"
    manager = RunManager(RunStore(store), submitting_grace=0)
    other = RunManager(RunStore(store), submitting_grace=0)
    spec = ExecutionSpec((sys.executable, "-c", "print('done')"), directory=tmp_path,
                         output_file="out", preprocessing_files=("input",))
    client = EndpointClient()
    with RunManager.as_default(other):
        second = Run.create(client, replace(spec, preprocessing_files=()), persistent=second_tracked)

    def preprocess():
        assert first.state == RunState.PREPROCESSING
        assert len(other.store.runs()) == int(first_tracked)
        assert len(other.store.runs(active=True, include_reservations=True)) == 1
        with pytest.raises(DuplicateSubmissionError):
            second.submit()
        (tmp_path / "input").write_text("prepared")

    with RunManager.as_default(manager):
        first = Run.create(client, spec, persistent=first_tracked, preprocess=preprocess)
    assert (first.record is not None) == first_tracked
    first.run()
    assert first.state == RunState.FINISHED
    other.refresh()
    assert len(other.store.runs(include_reservations=True)) == int(first_tracked)
    assert (tmp_path / "out").read_text().strip() == "done"


def test_hidden_terminal_reservation_stays_until_owner_cleanup(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = manager.new_record(directory=tmp_path, retain_history=False)
    with manager.store.observation_lock(record):
        reserved = manager.reserve(record)
        terminal = manager.complete(reserved, state=RunState.FINISHED)
        assert not manager.store.discard_terminal_reservation(terminal)
    assert manager.store.discard_terminal_reservation(terminal)
    with pytest.raises(FileNotFoundError):
        manager.store.get(record.id)
    with pytest.raises(FileNotFoundError):
        manager.transition(terminal, message="stale observer")


def test_hidden_interrupted_preprocessing_is_collected(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    record = manager.new_record(directory=tmp_path, retain_history=False)
    with manager.store.preprocessing_lock(record):
        manager.reserve(record, preprocessing=True)
    manager.refresh()
    assert not manager.store.runs(include_reservations=True)


def test_project_manager_can_reserve_without_project(tmp_path):
    from kkrunner.projects.manager import ProjectRunManager
    from kkrunner.projects.repository import ProjectRepository

    manager = ProjectRunManager(project_repository=ProjectRepository(tmp_path / "projects"))
    spec = ExecutionSpec((sys.executable, "-c", "pass"), directory=tmp_path)
    with RunManager.as_default(manager):
        run = Run.create(EndpointClient(), spec, persistent=False)
    assert run.record is None
    run.run()
    assert not manager.store.runs()


def test_save_only_preprocessing_conflicts_and_never_enters_history(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    other = RunManager(RunStore(tmp_path / "runs"))
    spec = ExecutionSpec((), directory=tmp_path, preprocessing_files=("input",))
    reader = other.new_record(directory=tmp_path, files=FileManifest.from_spec(
        ExecutionSpec((), directory=tmp_path, input_file="input")))
    with manager.preprocessing_reservation(spec):
        assert not manager.store.runs()
        with pytest.raises(DuplicateSubmissionError):
            other.reserve(reader)
        (tmp_path / "input").write_text("prepared")
    assert not manager.store.runs(include_reservations=True)
    assert other.reserve(reader).state == RunState.SUBMITTING


def test_save_only_preprocessing_failure_does_not_leave_reservation(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    spec = ExecutionSpec((), directory=tmp_path, preprocessing_files=("input",))
    with pytest.raises(ValueError, match="bad input"):
        with manager.preprocessing_reservation(spec):
            raise ValueError("bad input")
    assert not manager.store.runs(include_reservations=True)


@pytest.mark.parametrize("crash", [False, True])
def test_preprocessing_reservation_is_interprocess(tmp_path, crash):
    store = tmp_path / "runs"
    manager = RunManager(RunStore(store))
    spec = ExecutionSpec((), directory=tmp_path, preprocessing_files=("input",))
    child_script = """
import sys
from pathlib import Path
from kkrunner import ExecutionSpec, RunManager, RunStore
root, store = map(Path, sys.argv[1:])
manager = RunManager(RunStore(store))
with manager.preprocessing_reservation(
    ExecutionSpec((), directory=root, preprocessing_files=("input",))
):
    (root / "ready").touch()
    sys.stdin.read()
"""
    child = subprocess.Popen(
        [sys.executable, "-c", child_script, str(tmp_path), str(store)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    try:
        deadline = time.monotonic() + 10
        while not (tmp_path / "ready").exists() and child.poll() is None and time.monotonic() < deadline:
            time.sleep(0.02)
        assert (tmp_path / "ready").exists(), "child did not enter preprocessing"
        with pytest.raises(DuplicateSubmissionError):
            with manager.preprocessing_reservation(spec):
                pytest.fail("conflicting preprocessing was allowed")
        if crash:
            child.kill()
        child.communicate(timeout=10)
        if not crash:
            assert child.returncode == 0
        # The OS releases ownership even when the child is killed. Recovery
        # must not query a backend because preprocessing never launched one.
        with manager.preprocessing_reservation(spec):
            pass
        manager.refresh()
        assert not manager.store.runs(include_reservations=True)
    finally:
        if child.poll() is None:
            child.kill()
            child.communicate(timeout=10)


def test_persistent_terminal_record_keeps_live_cleanup_reservation(tmp_path):
    manager = RunManager(RunStore(tmp_path / "runs"))
    spec = ExecutionSpec((), directory=tmp_path, preprocessing_files=("input",))
    first = manager.new_record(directory=tmp_path, files=FileManifest.from_spec(spec))
    with manager.store.observation_lock(first):
        reserved = manager.reserve(first)
        manager.complete(reserved, state=RunState.FINISHED)
        with pytest.raises(DuplicateSubmissionError):
            with manager.preprocessing_reservation(spec):
                pytest.fail("live cleanup was not protected")
    with manager.preprocessing_reservation(spec):
        pass
    assert len(manager.store.runs()) == 1
