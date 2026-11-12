"""Translate an execution declaration to a native PSI/J job.

Submission and observation belong to PsijBackend. This module only builds the
native specification and prepares its named output files before submission.
"""

from pathlib import Path

from kkrunner.backends.support.process import prepare_mpi
from kkrunner.files.availability import prepare_target_outputs

from .slurm import submission_marker


def prepare_job(psij, request, *, executor_name, submission_key):
    """Build a native Job from validated target-only execution values."""
    argv, resources, launcher = request["argv"], None, None
    mpi = request["mpi"]
    use_mpi = request["mpi_argv"] is not None and (
        mpi["required"] or mpi["autodetect"] or mpi["launcher"]
    )
    if use_mpi:
        if mpi["launcher"]:
            argv = prepare_mpi(request)["request"]["argv"]
        else:
            argv = request["mpi_argv"]
            if mpi["process_count"] is not None:
                resources = psij.ResourceSpecV1(process_count=mpi["process_count"])
            launcher = "srun" if executor_name.lower() == "slurm" else "mpirun"
    attributes = None
    if executor_name.lower() == "slurm":
        attributes = psij.JobAttributes(custom_attributes={
            "slurm.comment": submission_marker(submission_key),
        })
    job_spec = psij.JobSpec(
        executable=argv[0],
        arguments=argv[1:],
        directory=request["directory"],
        environment={"KKRunner_RUN_ID": submission_key},
        stdin_path=(
            request["stdin"] if not use_mpi or request["mpi_uses_stdin"] else None
        ),
        stdout_path=request["output_file"],
        stderr_path=request["error_file"],
        resources=resources,
        attributes=attributes,
        launcher=launcher,
    )
    return psij.Job(job_spec)


def prepare_outputs(request):
    """Check declared inputs/caches and initialize scheduler-owned streams."""
    prepare_target_outputs(request)
    for path in {request.get("output_file"), request.get("error_file")} - {None}:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_bytes(b"")
