"""Installed PSI/J runtime access, independent of client endpoints and Runs."""


from kkrunner.backends.contracts import JobFailurePhase, JobStatus
from kkrunner.runs.states import RunState
from .slurm import recover, status
from .submission import PsijSubmission, psij_state

def _load_psij():
    try:
        import psij
    except ImportError as exc:
        raise ImportError(
            "PSI/J support requires the optional 'psij-python' package. "
            "Install it with 'python -m pip install \"kkrunner[psij]\"'."
        ) from exc
    return psij


class PsijExecutor:
    """Own native PSI/J objects for one target operation."""

    def __init__(self, executor_name="slurm", *, url=None, config=None, job_executor=None):
        self.executor_name, self.url = executor_name, url
        self._psij = _load_psij()
        self._job_executor = job_executor if job_executor is not None else self._psij.JobExecutor.get_instance(
            executor_name, url=url, config=config,
        )

    def create_submission(self, spec, *, submission_key=None):
        from uuid import uuid4
        spec.require_complete()
        return PsijSubmission(self, spec, run_id=submission_key or uuid4().hex)

    def attach_job(self, native_id):
        job = self._psij.Job()
        self._job_executor.attach(job, str(native_id))
        return job

    def status(self, native_id: str) -> JobStatus:
        if self.executor_name.lower() == "slurm" and self.url is None:
            return status(str(native_id))
        job = self.attach_job(native_id)
        native_status = job.status
        state = psij_state(native_status, attached=True)
        return JobStatus(
            state=state, return_code=getattr(native_status, "exit_code", None),
            message=getattr(native_status, "message", None),
            failure_phase=JobFailurePhase.EXECUTION if state == RunState.FAILED else None,
        )

    def recover(self, run_id, directory, *, submitted_at=None):
        """Return Slurm's native recovery facts without a client RunRecord."""
        if self.executor_name.lower() != "slurm" or self.url is not None:
            raise NotImplementedError(
                f"PSI/J executor {self.executor_name!r} at {self.url!r} cannot "
                "recover uncertain submissions using local Slurm commands"
            )
        return recover(run_id, directory, submitted_at=submitted_at)
