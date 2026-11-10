"""One PSI/J Job: submission, observation, output and cancellation."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from threading import Event, Lock

from kkrunner.files.availability import prepare_target_outputs
from kkrunner.files.paths import file_path
from kkrunner.runs.base import (
    ExecutionOutcome, ExecutionSpec, FailurePhase, OutputObservers, RunState,
    SubmissionNotStarted, close_output_files, finish_monitor,
    request_cancellation, start_monitor, wait_for_monitor,
)
from kkrunner.support.growing_file_tail import follow_growing_files

from . import slurm

_OBSERVATION_STOPPED = object()


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


class PsijJob:
    """One raw job managed by a PSI/J executor."""

    def __init__(
        self,
        executor,
        spec: ExecutionSpec,
        callback=None,
        *,
        output_listener=None,
        start=False,
        job=None,
        attached=False,
        run_id=None,
    ):
        self.executor = executor
        self.spec = spec
        self.callback = callback
        self._run_id = run_id

        self._stdout_path = _path(spec.output_file, spec.directory)
        self._stderr_path = _path(spec.error_file, spec.directory)
        self._stdin_path = _path(spec.stdin, spec.directory)
        self.result = ExecutionOutcome(
            output_file=self._stdout_path,
            input_file=_path(spec.input_file, spec.directory),
            error_file=self._stderr_path,
        )

        self._output_observers = OutputObservers(output_listener)
        self._lock = Lock()
        self._submit_lock = Lock()
        self._done = Event()
        self._exception = None
        self._callback_exception = None
        self._return_code = None
        self._failure_phase = None
        self._failure_message = None
        self._thread = None
        self._attached = attached
        self._submission_attempted = False
        self._cancel_future = None
        self._observed_state = None
        self._observation_stopped = Event()
        self._observation_signal_lock = Lock()
        self._observation_wakeup = None

        self._psij = executor._psij
        self._job = job or self._create_job()

        if start:
            self.start()

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

    @property
    def state(self):
        if self._observed_state is not None:
            return self._observed_state
        if self._exception is not None:
            # Monitor exceptions are not evidence of scheduler failure.
            return RunState.UNKNOWN
        if self._submission_attempted and self.backend_id is None and self._thread is None:
            # A submission error may have occurred after scheduler acceptance.
            # PSI/J's still-NEW native status is not proof of non-submission.
            return RunState.UNKNOWN
        return psij_state(self._job.status, attached=self._attached, return_code=self.return_code)

    @property
    def exception(self):
        return self._exception

    @property
    def return_code(self):
        if self._observed_state is not None:
            # A completed wait() is authoritative even without an exit code.
            return self._return_code
        if self._return_code is not None:
            return self._return_code
        return getattr(getattr(self._job, "status", None), "exit_code", None)

    @property
    def failure_phase(self):
        return self._failure_phase

    @property
    def failure_message(self):
        return self._failure_message

    @property
    def backend_id(self):
        native_id = getattr(self._job, "native_id", None)
        return None if native_id is None else str(native_id)

    @property
    def ref(self):
        from kkrunner.backends.contracts import JobRef
        return None if self.backend_id is None else JobRef("psij", self.backend_id)

    def status(self):
        from kkrunner.backends.contracts import job_status_from_facts
        return job_status_from_facts(
            self.state, return_code=self.return_code,
            failure_phase=self.failure_phase, message=self.failure_message,
        )

    def add_output_listener(self, callback):
        self._output_observers.add(callback)

    def add_output_error_listener(self, callback):
        self._output_observers.add_error_listener(callback)

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

    async def _monitor(self):
        paths = {}
        for kind, path in (("out", self._stdout_path), ("err", self._stderr_path)):
            # Merged stderr and stdout denote one physical stream.
            if path is not None and path not in paths.values():
                paths[kind] = path

        # PSI/J calls status callbacks on backend threads. Forward terminal
        # facts and explicit detach requests into this monitor's event loop.
        loop = asyncio.get_running_loop()
        terminal = loop.create_future()
        completed = []

        def publish(status):
            if not terminal.done():
                terminal.set_result(status)

        def schedule(status):
            try:
                if not loop.is_closed():
                    loop.call_soon_threadsafe(publish, status)
            except RuntimeError:
                # Loop shutdown after an observer has detached.
                pass

        def receive(status):
            if self._observation_stopped.is_set():
                return
            if psij_state(status) not in (
                RunState.FINISHED,
                RunState.FAILED,
                RunState.CANCELLED,
            ):
                return
            completed.append(status)
            schedule(status)

        with self._observation_signal_lock:
            self._observation_wakeup = lambda: schedule(_OBSERVATION_STOPPED)
            if self._observation_stopped.is_set():
                self._observation_wakeup()

        detach = None
        read_error = None
        try:
            if not self._observation_stopped.is_set():
                detach = self.executor._observe_job(self._job, receive)
                # A job may finish before registration or during it.
                receive(self._job.status)
            if not paths:
                status = await terminal
            else:

                async def terminal_status():
                    return terminal.result() if terminal.done() else None

                status = await follow_growing_files(
                    paths,
                    lambda kind, data: self._output_observers.emit_bytes(data, kind),
                    terminal_status,
                    poll_interval=max(self.executor.poll_interval, 1.0),
                )
        except OSError as exc:
            read_error = exc
            status = completed[-1] if completed else None
        finally:
            if detach is not None:
                detach()
            with self._observation_signal_lock:
                self._observation_wakeup = None

        if status is _OBSERVATION_STOPPED:
            status = None

        observed_state = psij_state(status) if status is not None else RunState.UNKNOWN
        if observed_state not in (
            RunState.FINISHED,
            RunState.FAILED,
            RunState.CANCELLED,
        ):
            self._observed_state = RunState.UNKNOWN
            self._failure_phase = None
            self._failure_message = (
                str(read_error) if read_error is not None else "PSI/J observation has no terminal job status"
            )
            # The job might still be writing its outputs. Do not finalize
            # their manifest on an inconclusive scheduler observation.
            return self.result

        self._return_code = getattr(status, "exit_code", None)
        # The scheduler may know FAILED/CANCELED without knowing the process
        # exit status; retain None rather than inventing an exit code.
        self.result.return_code = self._return_code
        self._observed_state = observed_state
        if observed_state == RunState.FAILED:
            self._failure_phase = FailurePhase.EXECUTION
            self._failure_message = getattr(status, "message", None) or (
                f"PSI/J job failed with exit code {self._return_code}"
                if self._return_code is not None
                else "PSI/J job failed"
            )
        if read_error is not None and observed_state == RunState.FINISHED:
            self._observed_state = RunState.FAILED
            self._failure_phase = FailurePhase.OUTPUT
            self._failure_message = str(read_error)
            return self.result
        if read_error is not None:
            # A failed or cancelled scheduler job remains terminal regardless
            # of whether its output stream could be read.
            return self.result
        return self.result

    def _record_observation_error(self, exc):
        if self._exception is None:
            self._exception = exc
        if self._observed_state not in (RunState.FAILED, RunState.CANCELLED):
            # Neither monitor failure nor output observer cleanup proves that
            # the scheduler job failed; result delivery remains inconclusive.
            self._observed_state = RunState.UNKNOWN
            self._failure_phase = None
        self._failure_message = self._failure_message or str(exc)

    def _thread_main(self):
        try:
            asyncio.run(self._monitor())
        except BaseException as exc:
            self._record_observation_error(exc)
        finally:
            try:
                self._output_observers.finish_bytes()
            except BaseException as exc:
                self._record_observation_error(exc)
            finally:
                finish_monitor(self)

    def start(self):
        if self._attached:
            return start_monitor(self)
        return self.submit()

    def submit_native(self):
        """Commit one scheduler submission without starting client observation."""
        with self._submit_lock:
            if self._attached:
                raise RuntimeError("Cannot submit an attached PSI/J job")
            if self._submission_attempted or self._thread is not None or self.state != RunState.NEW:
                raise RuntimeError("PSI/J job has already been submitted or submission is uncertain")
            try:
                self._prepare_io()
            except Exception as exc:
                # No scheduler interaction has happened yet.
                self._failure_phase = FailurePhase.SUBMISSION
                self._failure_message = str(exc)
                raise SubmissionNotStarted(str(exc)) from exc
            # Mark the boundary before calling PSI/J: a raised exception may
            # follow scheduler acceptance, so never submit this Job twice.
            self._submission_attempted = True
            self.executor._job_executor.submit(self._job)
            self.executor._cache_submitted_job(self._job)

    def submit(self):
        self.submit_native()
        start_monitor(self)

    def wait(self):
        if self._thread is None:
            raise RuntimeError("Run has not been started")
        return wait_for_monitor(self)

    def run(self):
        if self._thread is None:
            self.start()
        return self.wait()

    async def run_async(self):
        if self._thread is None:
            self.start()
        return await asyncio.to_thread(self.wait)

    def stop_observation(self):
        """Stop monitoring without cancelling the scheduler calculation."""
        self._observation_stopped.set()
        with self._observation_signal_lock:
            wakeup = self._observation_wakeup
        if wakeup is not None:
            wakeup()

    def cancel(self):
        if self._thread is None and not self._attached:
            raise RuntimeError("Run has not been submitted")
        return request_cancellation(self, self._job.cancel)
