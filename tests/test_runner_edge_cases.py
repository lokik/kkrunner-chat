import pytest

from kkrunner import ExecutionSpec
from kkrunner.client.remote_job import RemoteJob


def test_remote_cancel_before_submit_is_rejected(tmp_path):
    spec = ExecutionSpec(argv=(), input_file=None, output_file=None, directory=str(tmp_path))
    job = RemoteJob(object(), spec)
    with pytest.raises(RuntimeError, match="no confirmed backend identity"):
        job.cancel()
