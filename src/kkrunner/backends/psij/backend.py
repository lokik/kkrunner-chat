"""Reusable PSI/J executor, native job cache and status subscriptions."""

from __future__ import annotations

import logging
from collections import OrderedDict
from datetime import timedelta
from threading import RLock

from kkrunner.files.manifest import FileManifest
from kkrunner.runs.base import ExecutionSpec, FailurePhase, RunState, RunStatus

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


class PsijBackend:
    """Execute calculations through a PSI/J backend such as Slurm or PBS."""

    name = "psij"
    supports_result_file_retrieval = False

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

    def create_run_parts(self, spec: ExecutionSpec, *, submission_key=None, callback=None):
        from kkrunner.client.run_parts import RunParts
        return RunParts(self.create_job(spec, submission_key=submission_key, callback=callback))

    def open_run_parts(self, attachment, *, callback=None, output_listener=None):
        from kkrunner.client.run_parts import RunParts
        return RunParts(self.open_job(
            attachment, callback=callback, output_listener=output_listener, start=False,
        ))

    def create_job(self, spec: ExecutionSpec, *, submission_key=None, callback=None):
        spec.require_complete()
        job = PsijJob(self, spec, callback=callback, run_id=submission_key)
        job.set_manifest(FileManifest.from_spec(spec))
        return job

    def status(self, *runs):
        from kkrunner.client.endpoint import endpoint_for_backend

        expected = endpoint_for_backend(self)
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

    def open_job(self, run, callback=None, output_listener=None, start=True):
        from kkrunner.client.endpoint import endpoint_for_backend

        if run.execution.endpoint != endpoint_for_backend(self):
            raise ValueError("Run does not belong to this PSI/J endpoint")
        backend_id = run.execution.backend_id
        if not backend_id:
            raise ValueError("PSI/J run lacks backend ID")
        job = self.attach_native_job(backend_id)
        spec = ExecutionSpec(
            argv=None,
            input_file=run.files.input_file,
            output_file=run.files.output_file,
            error_file=None if run.files.stderr is None else run.files.stderr.local_path,
            directory=run.execution.directory.local,
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
        runner.set_manifest(run.files)
        if start:
            runner.start()
        return runner
