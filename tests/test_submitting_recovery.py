from threading import Event
from types import SimpleNamespace

import pytest

from kkrunner.runs import DuplicateSubmissionError, RunStatus
from kkrunner.backends.contracts import JobRef, RecoveryResult
from kkrunner.client.run_parts import RunParts
from kkrunner.client.pool import ClientPool
from kkrunner.client import EndpointClient
from kkrunner.backends import SupervisedBackend
from kkrunner.backends.contracts import JobStatus
from kkrunner import ExecutionSpec
from kkrunner import RunState, RunStore
from kkrunner.runs.manager import RunManager


class _Runner:
    def __init__(self):
        self.state = RunState.NEW
        self.backend_id = None
        self.result = SimpleNamespace(return_code=None)
        self.exception = None
        self.failure_phase = None
        self.failure_message = None
        self._done = Event()

    @property
    def requires_stream_retrieval(self):
        return False

    def configure_detached(self, detached):
        pass

    def configure_submission(self, request, **options):
        pass

    def status(self):
        return JobStatus(self.state, return_code=self.result.return_code)

    def submit(self):
        self.state = RunState.QUEUED
        self.backend_id = "new-job"

    def wait(self):
        self._done.wait()
        return self.result

    def cancel(self):
        self.state = RunState.CANCELLED
        self._done.set()

    def add_output_listener(self, callback):
        pass

    def add_output_error_listener(self, callback):
        pass


class _RecoveringExecutor(EndpointClient):
    def __init__(self, recovered):
        super().__init__(SupervisedBackend)
        self.recovered = recovered
        self.recovery_calls = []

    def create_run_parts(self, spec, *, submission_key=None, callback=None):
        runner = _Runner()
        runner.spec = spec
        parts = super().create_run_parts(spec, submission_key=submission_key, callback=callback)
        parts.job = runner
        return parts

    def status(self, *attachments):
        return tuple(
            RunStatus(RunState.QUEUED, backend_id=item.execution.backend_id)
            for item in attachments
        )

    def recover_submission(self, run_id, submission, *, submitted_at=None):
        self.recovery_calls.append((run_id, submission, submitted_at))
        if self.recovered is NotImplemented:
            raise NotImplementedError
        return self.recovered


def _run(manager, executor, tmp_path):
    return manager.create_run(
        executor,
        spec=ExecutionSpec(argv=("test-executable",), directory=str(tmp_path),
                           output_file=str(tmp_path / "shared.out")),
        process_result=lambda outcome: outcome,
        metadata={"test": {"task": "DOS", "dataset": "case"}},
    )


def test_confirmed_absent_stale_submission_releases_namespace(tmp_path):
    executor = _RecoveringExecutor(RecoveryResult.absent())
    manager = RunManager(RunStore(tmp_path / "runs"), client_pool=ClientPool(), submitting_grace=0)
    abandoned = _run(manager, executor, tmp_path)
    manager.reserve(abandoned.record)
    replacement = _run(manager, executor, tmp_path)
    replacement.submit()
    old = manager.store.get(abandoned.id)
    assert old.state == RunState.FAILED
    assert "not found" in old.message
    assert replacement.record.state == RunState.QUEUED
    assert executor.recovery_calls[0][0] == abandoned.record.execution.submission_key
    assert executor.recovery_calls[0][2] is not None


@pytest.mark.parametrize(
    "recovered, grace, expected_state, expected_backend, recovery_calls",
    [
        pytest.param(
            RecoveryResult.found(JobRef("process", "existing-job")),
            0,
            RunState.QUEUED,
            "existing-job",
            1,
            id="recovered-backend-blocks-duplicate",
        ),
        pytest.param(NotImplemented, 0, RunState.SUBMITTING, None, 1, id="unsupported-recovery-keeps-namespace"),
        pytest.param(RecoveryResult.indeterminate(), 0, RunState.SUBMITTING, None, 1, id="unknown-recovery-keeps-namespace"),
        pytest.param(RecoveryResult.absent(), 3600, RunState.SUBMITTING, None, 0, id="fresh-submitting-is-not-recovered"),
    ],
)
def test_unresolved_or_recovered_submission_keeps_duplicate_protection(
    tmp_path, recovered, grace, expected_state, expected_backend, recovery_calls
):
    executor = _RecoveringExecutor(recovered)
    manager = RunManager(RunStore(tmp_path / "runs"), client_pool=ClientPool(), submitting_grace=grace)
    uncertain = _run(manager, executor, tmp_path)
    manager.reserve(uncertain.record)
    replacement = _run(manager, executor, tmp_path)
    with pytest.raises(DuplicateSubmissionError):
        replacement.submit()
    stored = manager.store.get(uncertain.id)
    assert stored.state == expected_state
    assert stored.execution.backend_id == expected_backend
    assert len(executor.recovery_calls) == recovery_calls
