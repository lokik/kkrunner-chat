"""Installed PSI/J runtime access, independent of client endpoints and Runs."""


from kkrunner.backends.contracts import JobFailurePhase, JobStatus
from kkrunner.runs.states import RunState
from . import slurm
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

    def __init__(self, backend="slurm", *, url=None, config=None, job_executor=None):
        self.backend, self.url = backend, url
        self._psij = _load_psij()
        self._job_executor = job_executor if job_executor is not None else self._psij.JobExecutor.get_instance(
            backend, url=url, config=config,
        )

    def create_native_job(self, spec, *, submission_key=None):
        from uuid import uuid4
        spec.require_complete()
        return PsijSubmission(self, spec, run_id=submission_key or uuid4().hex)

    def attach_native_job(self, native_id):
        job = self._psij.Job()
        self._job_executor.attach(job, str(native_id))
        return job

    def status_native(self, native_id: str) -> JobStatus:
        if self.backend.lower() == "slurm" and self.url is None:
            return slurm.status_native(str(native_id))
        job = self.attach_native_job(native_id)
        status = job.status
        state = psij_state(status, attached=True)
        return JobStatus(
            state=state, return_code=getattr(status, "exit_code", None),
            message=getattr(status, "message", None),
            failure_phase=JobFailurePhase.EXECUTION if state == RunState.FAILED else None,
        )

    def recover_native(self, run_id, directory, *, submitted_at=None):
        """Return Slurm's native recovery facts without a client RunRecord."""
        if self.backend.lower() != "slurm" or self.url is not None:
            raise NotImplementedError(
                f"PSI/J backend {self.backend!r} at {self.url!r} cannot "
                "recover uncertain submissions using local Slurm commands"
            )
        return slurm.recover_native(run_id, directory, submitted_at=submitted_at)
