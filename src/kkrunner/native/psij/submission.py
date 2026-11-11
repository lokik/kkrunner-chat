"""Construct and submit a native PSI/J job; no client observation lifecycle."""

import os
from pathlib import Path
from threading import Lock

from kkrunner.files.availability import prepare_target_outputs
from kkrunner.files.paths import file_path
from kkrunner.files.streams import close_output_files
from kkrunner.backends.contracts import JobFailurePhase
from kkrunner.runs.base import ExecutionOutcome, SubmissionNotStarted
from kkrunner.runs.states import RunState
from . import slurm

_STATE_MAP = {
    "NEW": RunState.NEW,
    "STAGE_IN": RunState.QUEUED,
    "QUEUED": RunState.QUEUED,
    "ACTIVE": RunState.RUNNING,
    "STAGE_OUT": RunState.RUNNING,
    "COMPLETED": RunState.FINISHED,
    "FAILED": RunState.FAILED,
    "CANCELED": RunState.CANCELLED,
}

def psij_state(status, *, attached=False, return_code=None):
    """Interpret native PSI/J job facts consistently across all observers.

    A newly attached job reports NEW before PSI/J has received its real
    scheduler status. That is UNKNOWN, not evidence of a new submission.
    A completed job with a nonzero exit code is an execution failure.
    """
    name = getattr(getattr(status, "state", None), "name", None)
    if attached and name == "NEW":
        return RunState.UNKNOWN
    state = _STATE_MAP.get(name, RunState.UNKNOWN)
    if return_code is None:
        return_code = getattr(status, "exit_code", None)
    if state == RunState.FINISHED and return_code not in (None, 0):
        return RunState.FAILED
    return state

def _path(value, directory=None):
    if value is None:
        return None
    try:
        return file_path(value, directory, required=True)
    except ValueError as exc:
        raise ValueError("PSI/J execution requires named input/output files") from exc


class PsijSubmission:
    """One scheduler submission with an explicit no-resubmit boundary."""

    def __init__(self, executor, spec, *, run_id=None):
        self.executor, self.spec, self._run_id = executor, spec, run_id
        self._psij = executor._psij
        self._stdout_path = _path(spec.output_file, spec.directory)
        self._stderr_path = _path(spec.error_file, spec.directory)
        self._stdin_path = _path(spec.stdin, spec.directory)
        self.result = ExecutionOutcome(
            output_file=self._stdout_path, error_file=self._stderr_path,
            input_file=_path(spec.input_file, spec.directory),
        )
        self._submit_lock = Lock()
        self._submission_attempted = False
        self.failure_phase = None
        self.failure_message = None
        self._job = self._create_job()

    @property
    def backend_id(self):
        identity = self._job.native_id
        return None if identity is None else str(identity)

    @property
    def return_code(self):
        return getattr(self._job.status, "exit_code", None)

    @property
    def state(self):
        if self._submission_attempted and self.backend_id is None:
            return RunState.UNKNOWN
        return psij_state(self._job.status)

    def _job_command(self):
        launch = self.spec.launch
        if not launch or self.spec.mpi_argv is None:
            return tuple(self.spec.argv), None, None
        mpi = launch.mpi
        if mpi.launcher:
            return (*mpi.launcher, *self.spec.mpi_argv), None, None
        resources = None
        if mpi.process_count is not None:
            resources = self._psij.ResourceSpecV1(process_count=mpi.process_count)
        launcher = "srun" if self.executor.backend.lower() == "slurm" else "mpirun"
        return tuple(self.spec.mpi_argv), resources, launcher

    def _project_metadata(self):
        if self._run_id is None:
            return None, None
        environment = {"KKRunner_RUN_ID": self._run_id}
        attributes = None
        if self.executor.backend.lower() == "slurm":
            attributes = self._psij.JobAttributes(custom_attributes={"slurm.comment": slurm.submission_marker(self._run_id)})
        return environment, attributes

    def _create_job(self):
        argv, resources, launcher = self._job_command()
        if not argv:
            raise ValueError("Cannot submit a PSI/J run without a command")
        environment, attributes = self._project_metadata()
        job_spec = self._psij.JobSpec(
            executable=os.fspath(argv[0]),
            arguments=[os.fspath(arg) for arg in argv[1:]],
            directory=self.spec.directory,
            environment=environment,
            stdin_path=(
                self._stdin_path
                if not (self.spec.launch and self.spec.mpi_argv is not None) or self.spec.mpi_uses_stdin
                else None
            ),
            stdout_path=self._stdout_path,
            stderr_path=self._stderr_path,
            resources=resources,
            attributes=attributes,
            launcher=launcher,
        )
        return self._psij.Job(job_spec)

    def _prepare_io(self):
        if self.spec.input_file is not None and hasattr(self.spec.input_file, "flush"):
            self.spec.input_file.flush()
        protected = [
            _path(value, self.spec.directory)
            for value in (self.spec.input_file, self.spec.stdin, *self.spec.input_files, *self.spec.cache_files)
        ]
        for stream in (self.spec.output_file, self.spec.error_file):
            if stream is not None and hasattr(stream, "flush"):
                stream.flush()
        close_output_files(self.spec.output_file, self.spec.error_file)
        prepare_target_outputs({
            "input_file": protected[0],
            "stdin": protected[1],
            "input_files": protected[2:2 + len(self.spec.input_files)],
            "output_file": self._stdout_path,
            "error_file": self._stderr_path,
            "client_sync": {
                "output_files": [_path(value, self.spec.directory) for value in self.spec.output_files],
                "cache_files": [{"path": path} for path in protected[2 + len(self.spec.input_files):]],
            },
        })
        for path in {self._stdout_path, self._stderr_path} - {None}:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            Path(path).write_bytes(b"")

    def submit_native(self):
        """Prepare before crossing the scheduler's uncertain submission boundary."""
        with self._submit_lock:
            if self._submission_attempted:
                raise RuntimeError("PSI/J job has already been submitted or submission is uncertain")
            try:
                self._prepare_io()
            except Exception as exc:
                self.failure_phase = JobFailurePhase.SUBMISSION
                self.failure_message = str(exc)
                raise SubmissionNotStarted(str(exc)) from exc
            self._submission_attempted = True
            self.executor._job_executor.submit(self._job)
