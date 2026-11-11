"""PSI/J functional backend with native scheduler support and stateless operations."""
from __future__ import annotations

import hashlib
import shlex
import subprocess
from datetime import datetime

from kkrunner.runs.base import FailurePhase, RunState, RunStatus


_SLURM_STATE_MAP = {
    "PENDING": RunState.QUEUED,
    "CONFIGURING": RunState.QUEUED,
    "SUSPENDED": RunState.QUEUED,
    "RUNNING": RunState.RUNNING,
    "COMPLETING": RunState.RUNNING,
    "RESIZING": RunState.RUNNING,
    "COMPLETED": RunState.FINISHED,
    "CANCELLED": RunState.CANCELLED,
    "CANCELED": RunState.CANCELLED,
    "BOOT_FAIL": RunState.FAILED,
    "DEADLINE": RunState.FAILED,
    "FAILED": RunState.FAILED,
    "NODE_FAIL": RunState.FAILED,
    "OUT_OF_MEMORY": RunState.FAILED,
    "PREEMPTED": RunState.FAILED,
    "REVOKED": RunState.FAILED,
    "TIMEOUT": RunState.FAILED,
}


def _scheduler_output(argv):
    return subprocess.run(
        tuple(argv),
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ).stdout


def _slurm_state(value):
    value = (value or "").strip().upper().split("+", 1)[0]
    return _SLURM_STATE_MAP.get(value, RunState.UNKNOWN)


def _slurm_exit_code(value):
    if not value:
        return None
    code = value.split(":", 1)[0]
    try:
        return int(code)
    except ValueError:
        return None


def _slurm_details(native_id):
    output = _scheduler_output(("scontrol", "--oneliner", "show", "job", str(native_id)))
    fields = {}
    for token in shlex.split(output):
        name, separator, value = token.partition("=")
        if separator:
            fields[name] = value
    return fields


def submission_marker(run_id):
    """Short, scheduler-safe lookup marker for an arbitrary valid Run ID."""
    return "kkrunner:" + hashlib.blake2s(run_id.encode("utf8"), digest_size=16).hexdigest()


def _slurm_current_matches(run_id):
    marker = submission_marker(run_id)
    output = _scheduler_output(("squeue", "--noheader", "--me", "--format=%i|%k|%T|%Z"))
    matches = []
    for line in output.splitlines():
        fields = line.split("|", 3)
        if len(fields) != 4:
            continue
        native_id, comment, state, directory = (field.strip() for field in fields)
        if comment == marker:
            matches.append((native_id, state, directory))
    return tuple(matches)


def _slurm_accounting_stores_comments():
    output = _scheduler_output(("scontrol", "show", "config"))
    for line in output.splitlines():
        name, separator, value = line.partition("=")
        if separator and name.strip() == "AccountingStoreFlags":
            flags = {flag.strip().lower() for flag in value.split(",")}
            return "job_comment" in flags
    return False


def _slurm_time(value):
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone()
    return parsed.strftime("%Y-%m-%dT%H:%M:%S")


def _slurm_accounting_matches(run_id, submitted_at):
    marker = submission_marker(run_id)
    output = _scheduler_output(
        (
            "sacct",
            "-X",
            "-n",
            "-P",
            f"--starttime={_slurm_time(submitted_at)}",
            "--format=JobIDRaw,Comment,State,WorkDir,StdOut,StdErr,ExitCode",
        )
    )
    matches = []
    for line in output.splitlines():
        fields = line.rstrip("|").split("|")
        if len(fields) != 7:
            continue
        native_id, comment, state, directory, stdout, stderr, exit_code = fields
        if comment.strip() == marker:
            matches.append(
                (
                    native_id.strip(),
                    state.strip(),
                    directory.strip(),
                    stdout.strip() or None,
                    stderr.strip() or None,
                    _slurm_exit_code(exit_code.strip()),
                )
            )
    return tuple(matches)


def status_native(native_id):
    """Read scheduler facts for a local Slurm job without initializing PSI/J."""
    native_id = str(native_id)
    # A PSI/J attach initially reports NEW; short-lived remote commands
    # cannot wait for an asynchronous update or retain the Job cache.
    # Query the same scheduler facts for local and SSH PSI/J runs.
    try:
        details = _slurm_details(native_id)
    except (FileNotFoundError, subprocess.CalledProcessError):
        details = {}

    if details:
        state = _slurm_state(details.get("JobState"))
        return_code = _slurm_exit_code(details.get("ExitCode"))
    else:
        try:
            output = _scheduler_output(
                (
                    "sacct",
                    "-X",
                    "-n",
                    "-P",
                    "-j",
                    native_id,
                    "--format=JobIDRaw,State,ExitCode",
                )
            )
        except (FileNotFoundError, subprocess.CalledProcessError):
            output = ""
        state = RunState.UNKNOWN
        return_code = None
        for line in output.splitlines():
            fields = line.rstrip("|").split("|")
            if len(fields) == 3 and fields[0].strip() == native_id:
                state = _slurm_state(fields[1])
                return_code = _slurm_exit_code(fields[2].strip())
                break

    if not state.is_terminal:
        return_code = None
    elif state == RunState.FINISHED and return_code is None:
        return_code = 0
    elif state == RunState.FINISHED and return_code != 0:
        state = RunState.FAILED
    return RunStatus(
        state=state,
        backend_id=native_id,
        return_code=return_code,
        failure_phase=FailurePhase.EXECUTION if state == RunState.FAILED else None,
    )


def recover_native(run_id, directory, *, submitted_at=None):
    """Recover an uncertain local Slurm submission from scheduler facts."""
    matches = _slurm_current_matches(run_id)
    if len(matches) > 1:
        raise RuntimeError(f"Multiple live Slurm jobs carry KKRunner run id {run_id!r}")
    if matches:
        native_id, state, workdir = matches[0]
        try:
            details = _slurm_details(native_id)
        except (FileNotFoundError, subprocess.CalledProcessError):
            # The job may have finished between squeue and scontrol.
            # Recover its terminal facts from accounting instead.
            details = None
        if details:
            return {
                "native_id": str(native_id),
                "state": _slurm_state(state),
                "directory": details.get("WorkDir") or workdir or directory,
                "stdout": details.get("StdOut"),
                "stderr": details.get("StdErr"),
                "return_code": None,
            }

    if submitted_at is None or not _slurm_accounting_stores_comments():
        raise NotImplementedError(
            "Slurm cannot safely determine whether this submission already "
            "finished because job comments are unavailable in accounting"
        )
    accounting = _slurm_accounting_matches(run_id, submitted_at)
    if len(accounting) > 1:
        raise RuntimeError(f"Multiple Slurm accounting records carry KKRunner run id {run_id!r}")
    if not accounting:
        # An empty sacct result is not authoritative negative evidence:
        # Slurm accounting can lag behind the scheduler. Do not release
        # the client's SUBMITTING reservation and risk a duplicate job.
        raise NotImplementedError(
            "Slurm has no matching job in squeue or accounting yet; submission acceptance is still uncertain"
        )
    native_id, state, workdir, stdout, stderr, return_code = accounting[0]
    recovered_state = _slurm_state(state)
    if recovered_state == RunState.FINISHED and return_code not in (None, 0):
        recovered_state = RunState.FAILED
    return {
        "native_id": str(native_id),
        "state": recovered_state,
        "directory": workdir or directory,
        "stdout": stdout,
        "stderr": stderr,
        "return_code": return_code,
    }


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
            attributes = self._psij.JobAttributes(custom_attributes={"comment": submission_marker(self._run_id)})
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


import logging
from collections import OrderedDict
from datetime import timedelta
from threading import RLock

from kkrunner.runs.base import ExecutionSpec, FailurePhase, RunState, RunStatus
from kkrunner.backends.contracts import RecoverableBackend


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
            return status_native(native_id)

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
        return recover_native(run_id, directory, submitted_at=submitted_at)

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


import asyncio

from kkrunner.remote.handles import decode_handle, encode_handle
from kkrunner.remote.messages import request_to_spec, status_reply
from kkrunner.runs.base import FailurePhase, RunState
from kkrunner.support.growing_file_tail import follow_job_files

def _psij_executor(options):

    options = options or {}
    return PsijBackend(options.get("backend") or "slurm", url=options.get("url"))

def _submit_impl(request):
    executor = _psij_executor(request.get("executor_options"))
    spec = request_to_spec(request)
    runner = executor.create_native_job(spec, submission_key=request.get("id"))
    # PSI/J's _prepare_io() owns cleanup of its complete declared manifest.
    # Running a second cleanup here would duplicate it and risk wildcard caches.
    runner.submit_native()
    backend_id = runner.backend_id
    if backend_id is None:
        raise RuntimeError("PSI/J submission did not return a backend id")
    remote_id = encode_handle(
        {
            "kind": "psij",
            "backend": executor.backend,
            "url": executor.url,
            "native_id": backend_id,
        }
    )
    return {
        "remote_id": remote_id,
        "state": runner.state.value,
        "directory": request["directory"],
        "input_file": request.get("input_file"),
        "output_file": runner.result.output_file,
        "error_file": runner.result.error_file,
        "return_code": runner.return_code,
        "failure_phase": (None if runner.failure_phase is None else runner.failure_phase.value),
    }



def _status_impl(remote_id):
    handle = decode_handle(remote_id)
    if handle["kind"] != "psij":
        raise ValueError("Not a PSI/J job reference")
    if str(handle["backend"]).lower() == "slurm" and handle.get("url") is None:
        status = status_native(handle["native_id"])
    else:
        executor = _psij_executor(handle)
        status = executor.status_native(handle["native_id"])
    return status_reply(
        status.state, status.return_code,
        failure_phase=status.failure_phase, error=status.message,
    )


def _cancel_impl(remote_id):
    handle = decode_handle(remote_id)
    if handle["kind"] != "psij":
        raise ValueError("Not a PSI/J job reference")
    executor = _psij_executor(handle)
    job = executor.attach_native_job(handle["native_id"])
    job.cancel()
    return status(remote_id)


def _acknowledge_impl(remote_id):
    return {"acknowledged": False}


def _acknowledge_submission_impl(run_id):
    return {"acknowledged": False}


def _recover_impl(run_id, request, *, submitted_at=None):
    backend = _psij_executor(request.get("executor_options"))
    facts = backend.recover_native(
        run_id, request["directory"], submitted_at=submitted_at,
    )
    if facts is None:
        return None
    native_id = facts["native_id"]
    remote_id = encode_handle(
        {
            "kind": "psij",
            "backend": backend.backend,
            "url": backend.url,
            "native_id": native_id,
        }
    )
    state = facts["state"]
    return {
        "remote_id": remote_id,
        "state": state.value,
        "return_code": facts["return_code"],
        "failure_phase": (FailurePhase.EXECUTION.value if state == RunState.FAILED else None),
        "directory": facts["directory"],
        "output_file": facts["stdout"],
        "error_file": facts["stderr"],
    }


async def _follow_scheduler_async(remote_id, streams, paths, offsets, poll_interval):
    """Follow scheduler-owned files while PSI/J waits for terminal completion."""

    handle = decode_handle(remote_id)
    executor = _psij_executor(handle)
    job = executor.attach_native_job(handle["native_id"])

    def emit(kind, data):
        streams[kind].write(data)
        streams[kind].flush()

    async def terminal(_native_status):
        facts = await asyncio.to_thread(_status_impl, remote_id)
        if RunState(facts["state"]).is_terminal:
            return facts
        native = job.status
        state = psij_state(native)
        return status_reply(
            state,
            getattr(native, "exit_code", None),
            failure_phase=(FailurePhase.EXECUTION if state == RunState.FAILED else None),
            error=getattr(native, "message", None),
        )

    return await follow_job_files(
        paths,
        emit,
        job.wait,
        terminal=terminal,
        offsets=offsets,
        poll_interval=max(poll_interval, 1.0),
    )

def _follow_impl(
    remote_id, stdout, stderr, *, stdout_offset=0, stderr_offset=0,
    poll_interval=0.1, stdout_path=None, stderr_path=None,
    observation_started=None,
):
    handle = decode_handle(remote_id)
    if handle["kind"] != "psij":
        raise ValueError("Not a PSI/J job reference")
    offsets = {"out": stdout_offset, "err": stderr_offset}
    paths = {"out": stdout_path, "err": stderr_path}
    streams = {"out": stdout, "err": stderr}
    return asyncio.run(_follow_scheduler_async(remote_id, streams, paths, offsets, poll_interval))


def stream(job_ref, **options):
    import sys
    from kkrunner.backends.psij import follow
    return follow(job_ref, sys.stdout.buffer, sys.stderr.buffer, **options)

# Public functional operations deliberately import their implementation inside
# the call. They carry no backend object, closure, or module-global mutable state.
def submit(request):
    from kkrunner.backends.psij import _submit_impl
    return _submit_impl(request)


def status(job_ref):
    from kkrunner.backends.psij import _status_impl
    return _status_impl(job_ref)


def cancel(job_ref):
    from kkrunner.backends.psij import _cancel_impl
    return _cancel_impl(job_ref)


def recover(submission_key, request, *, submitted_at=None):
    from kkrunner.backends.psij import _recover_impl
    return _recover_impl(submission_key, request, submitted_at=submitted_at)


def acknowledge(job_ref):
    from kkrunner.backends.psij import _acknowledge_impl
    return _acknowledge_impl(job_ref)


def acknowledge_submission(submission_key):
    from kkrunner.backends.psij import _acknowledge_submission_impl
    return _acknowledge_submission_impl(submission_key)


def follow(job_ref, stdout, stderr, **options):
    from kkrunner.backends.psij import _follow_impl
    return _follow_impl(job_ref, stdout, stderr, **options)


