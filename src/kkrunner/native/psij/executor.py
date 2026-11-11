"""PSI/J native executor, job cache and callback subscriptions."""

import logging
from collections import OrderedDict
from datetime import timedelta
from threading import RLock

from kkrunner.runs.base import ExecutionSpec, FailurePhase, RunState, RunStatus
from kkrunner.backends.contracts import RecoverableBackend
from . import slurm
from .job import PsijJob, psij_state


_log = logging.getLogger(__name__)


def _ignore_job_status(job, status):
    """PSI/J 0.9.11 wraps None as a broken callable on unsubscribe."""
    pass


def _load_psij():
    try:
        import psij
    except ImportError as exc:
        raise ImportError(
            "PSI/J support requires the optional 'psij-python' package. "
            "Install it with 'python -m pip install \"kkrunner[psij]\"'."
        ) from exc
    return psij


class PsijBackend(RecoverableBackend):
    """Execute calculations through a PSI/J backend such as Slurm or PBS."""

    name = "psij"
    supports_result_file_retrieval = False
    default_persistent = True
    default_detached = True

    def __init__(
        self,
        backend="slurm",
        *,
        url=None,
        config=None,
        job_executor=None,
        poll_interval=0.1,
    ):
        self.backend = backend
        self.url = url
        self.poll_interval = poll_interval
        self._native_configuration = config is not None or job_executor is not None
        self._psij = _load_psij()
        self._job_executor = (
            job_executor
            if job_executor is not None
            else self._psij.JobExecutor.get_instance(backend, url=url, config=config)
        )
        self._jobs = OrderedDict()
        self._observers = {}
        self._jobs_lock = RLock()
        self._max_idle_jobs = 64

    def create_native_job(self, spec: ExecutionSpec, *, submission_key=None, callback=None):
        """Target-side PSI/J helper. Native state never crosses the Endpoint."""
        from uuid import uuid4
        spec.require_complete()
        return PsijJob(self, spec, callback=callback, run_id=submission_key or uuid4().hex)

    def create_job(self, spec: ExecutionSpec, *, submission_key=None, callback=None):
        """Create the same client Job as remote and supervised operations."""
        if getattr(self, "_native_configuration", False):
            raise ValueError(
                "A native PSI/J JobExecutor or config object cannot cross the "
                "context-independent endpoint boundary; use backend/url options"
            )
        from kkrunner.client.local_managed import local_managed_job
        return local_managed_job(
            spec, backend="psij",
            options={"backend": self.backend, "url": self.url},
            submission_key=submission_key, callback=callback,
        )

    def status(self, *runs):
        from kkrunner.client.endpoint import endpoint_config_for_executor

        expected = endpoint_config_for_executor(self)
        statuses = []
        for run in runs:
            if run.execution.endpoint != expected:
                raise ValueError("Run does not belong to this PSI/J endpoint")
            statuses.append(self.status_native(run.execution.backend_id))
        return tuple(statuses)

    def _trim_jobs(self):
        idle = sum(job not in self._observers for job in self._jobs.values())
        for native_id, job in tuple(self._jobs.items()):
            if idle <= self._max_idle_jobs:
                break
            if job not in self._observers:
                del self._jobs[native_id]
                idle -= 1

    def _cache_submitted_job(self, job):
        native_id = job.native_id
        if native_id is not None:
            with self._jobs_lock:
                self._jobs[str(native_id)] = job
                self._jobs.move_to_end(str(native_id))
                self._trim_jobs()

    def attach_native_job(self, native_id):
        native_id = str(native_id)
        with self._jobs_lock:
            job = self._jobs.get(native_id)
            if job is None:
                job = self._psij.Job()
                self._job_executor.attach(job, native_id)
                self._jobs[native_id] = job
            self._jobs.move_to_end(native_id)
            self._trim_jobs()
            return job

    def _observe_job(self, job, observer):
        with self._jobs_lock:
            observers = self._observers.get(job)
            if observers is None:
                # Register the first observer *before* installing the native
                # callback: PSI/J may call it synchronously. Roll back when
                # registration fails so a later attach can retry.
                observers = {observer}
                self._observers[job] = observers
                try:
                    job.set_job_status_callback(self._dispatch_job_status)
                except BaseException:
                    del self._observers[job]
                    raise
            else:
                observers.add(observer)

        def detach():
            with self._jobs_lock:
                current = self._observers.get(job)
                if current is None:
                    return
                current.discard(observer)
                if current:
                    return
                del self._observers[job]
                # PSI/J 0.9.11 incorrectly wraps None as a callback.
                # This is best-effort teardown: failing to unregister must
                # not replace a monitor's original failure or its terminal
                # result. The dispatcher has no subscribers after removal.
                try:
                    job.set_job_status_callback(_ignore_job_status)
                except Exception:
                    _log.exception("Failed to disable PSI/J job status callback")
                native_id = job.native_id
                if (
                    native_id is not None
                    and self._jobs.get(str(native_id)) is job
                    and psij_state(job.status)
                    in (
                        RunState.FINISHED,
                        RunState.FAILED,
                        RunState.CANCELLED,
                    )
                ):
                    del self._jobs[str(native_id)]
                self._trim_jobs()

        return detach

    def _dispatch_job_status(self, job, status):
        with self._jobs_lock:
            observers = tuple(self._observers.get(job, ()))
        for observer in observers:
            observer(status)

    def status_native(self, native_id):
        """Return the currently known native job status without waiting."""
        native_id = str(native_id)
        if self.backend.lower() == "slurm" and self.url is None:
            return slurm.status_native(native_id)

        job = self.attach_native_job(native_id)
        status = getattr(job, "status", None)
        state = psij_state(status, attached=True)
        return_code = getattr(status, "exit_code", None)
        if state in (RunState.FINISHED, RunState.FAILED, RunState.CANCELLED):
            with self._jobs_lock:
                if job not in self._observers and self._jobs.get(native_id) is job:
                    del self._jobs[native_id]
        return RunStatus(
            state=state,
            backend_id=native_id,
            return_code=return_code,
            message=getattr(status, "message", None),
            failure_phase=FailurePhase.EXECUTION if state == RunState.FAILED else None,
        )

    def wait_status_native(self, native_id, timeout):
        """Wait up to *timeout* seconds for a known state of a native job.

        Unlike status_native(), this method may wait. A timeout returns UNKNOWN
        if the scheduler has still not provided a meaningful job state.
        """
        timeout = float(timeout)
        if timeout < 0:
            raise ValueError("Status timeout must not be negative")
        status = self.status_native(native_id)
        if status.state != RunState.UNKNOWN or timeout == 0:
            return status

        job = self.attach_native_job(native_id)
        # The PSI/J Job object waits for notifications from its executor.
        # There is no repeated scheduler status query or sleep loop.
        job.wait(
            timeout=timedelta(seconds=timeout),
            target_states=[self._psij.JobState.QUEUED],
        )
        return self.status_native(native_id)

    def recover_native(self, run_id, directory, *, submitted_at=None):
        """Return Slurm's native recovery facts without a client RunRecord."""
        if self.backend.lower() != "slurm" or self.url is not None:
            raise NotImplementedError(
                f"PSI/J backend {self.backend!r} at {self.url!r} cannot "
                "recover uncertain submissions using local Slurm commands"
            )
        return slurm.recover_native(run_id, directory, submitted_at=submitted_at)

    def recover_submission(self, run_id, submission, *, submitted_at=None):
        facts = self.recover_native(
            run_id,
            submission.execution.directory.target,
            submitted_at=submitted_at,
        )
        if facts is None:
            from kkrunner.backends.contracts import RecoveryResult
            return RecoveryResult.indeterminate("Scheduler cannot prove submission absence")
        from kkrunner.backends.contracts import JobRef, RecoveryResult
        return RecoveryResult.found(
            JobRef("psij", facts["native_id"]),
            directory=facts.get("directory"),
            output_file=facts.get("stdout"),
            error_file=facts.get("stderr"),
        )

    def attach_native_observer(self, context, *, callback=None, output_listener=None, start=False):
        """Internal PSI/J observer for native scheduler callback mechanics.

        Client-facing reattachment uses the shared Job instead.
        """
        from kkrunner.client.endpoint import endpoint_config_for_executor

        if context.endpoint != endpoint_config_for_executor(self):
            raise ValueError("Run does not belong to this PSI/J endpoint")
        if context.ref.backend != "psij":
            raise ValueError("Job belongs to a different backend")
        job = self.attach_native_job(context.ref.token)
        spec = ExecutionSpec(
            argv=None,
            input_file=context.input_file,
            output_file=context.output_file,
            error_file=context.error_file,
            directory=context.directory.local,
        )
        runner = PsijJob(
            self,
            spec,
            callback=callback,
            output_listener=output_listener,
            start=False,
            job=job,
            attached=True,
        )
        if start:
            runner.start()
        return runner

    def attach_job(self, context, *, callback=None, output_listener=None, start=False):
        from kkrunner.client.endpoint import endpoint_config_for_executor

        if context.endpoint != endpoint_config_for_executor(self):
            raise ValueError("Run does not belong to this PSI/J endpoint")
        if context.ref.backend != "psij":
            raise ValueError("Job belongs to a different backend")
        from kkrunner.client.local_managed import attach_local_managed
        spec = ExecutionSpec(
            argv=None,
            input_file=context.input_file,
            output_file=context.output_file,
            error_file=context.error_file,
            directory=context.directory.local,
        )
        facts = self.status_native(context.ref.token)
        return attach_local_managed(
            spec, context.ref.token, backend="psij",
            options={"backend": self.backend, "url": self.url},
            callback=callback, output_listener=output_listener,
            status={
                "state": facts.state.value,
                "return_code": facts.return_code,
                "failure_phase": None if facts.failure_phase is None else facts.failure_phase.value,
                "error": facts.message,
            },
            start=start,
        )


