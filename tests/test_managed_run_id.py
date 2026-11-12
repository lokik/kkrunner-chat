from kkrunner import ExecutionSpec
from kkrunner.client import EndpointClient, StagingSpec, SshTransport
from kkrunner.backends import SupervisedBackend


class _Transport:
    host = "cluster"


def test_remote_project_run_parts_retain_manager_run_id(tmp_path):
    backend = EndpointClient(SupervisedBackend, transport=SshTransport("cluster"))
    spec = ExecutionSpec(
        argv=("true",),
        input_file=None,
        output_file=None,
        directory=str(tmp_path),
        staging=StagingSpec(local_root=tmp_path, remote_root="/scratch/work"),
    )
    parts = backend.create_run_parts(spec, submission_key="kkrunner-run-id")
    assert parts.submission_builder({"directory": "/scratch/work"})["submission_key"] == "kkrunner-run-id"
    assert parts.job.backend_id is None
