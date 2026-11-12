"""Early cancellation is a client lifecycle state, not a backend operation."""

import pytest

from kkrunner import ExecutionSpec, RunState
from kkrunner.backends import SupervisedBackend
from kkrunner.client import EndpointClient
from kkrunner.runs.base import SubmissionNotStarted


def test_managed_cancel_before_submit_never_contacts_backend(tmp_path, monkeypatch):
    spec = ExecutionSpec(argv=(), directory=str(tmp_path))
    job = EndpointClient(SupervisedBackend).create_run_parts(spec).job

    def unexpected_cancel(*args, **kwargs):
        pytest.fail("Cancellation of an unsubmitted Job must not reach the backend")

    monkeypatch.setattr(SupervisedBackend, "cancel", staticmethod(unexpected_cancel))
    assert job.cancel().result() is None
    assert job.state is RunState.CANCELLED
    assert job.wait().return_code is None
    with pytest.raises(SubmissionNotStarted, match="cancelled before submission"):
        job.submit()
