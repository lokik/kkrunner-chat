"""Local subprocess execution backend."""

from __future__ import annotations

import asyncio
from threading import Event, Lock, Thread

from kkrunner.backends.process.primitives import materialize_local_spec, pump_stream, start_local_process, stop_local_process
from kkrunner.files.availability import clear_declared_outputs, discover_local_files, validate_output_collisions
from kkrunner.files.manifest import FileManifest
from kkrunner.files.paths import file_path
from kkrunner.runs.base import (
    ExecutionOutcome,
    ExecutionSpec,
    FailurePhase,
    OutputObservers,
    RunState,
    SubmissionNotStarted,
    close_output_files,
    finish_monitor,
    materialize_output_streams,
    wait_for_monitor,
)


class ProcessJob:
    """One local subprocess execution.

    The runner owns only process lifecycle, output-file writing and raw output
    observation. It deliberately performs no task-specific parsing.
    """

    def __init__(self, spec: ExecutionSpec, callback=None):
        self.spec = spec
        self.callback = callback
        self.result = ExecutionOutcome(
            output_file=file_path(spec.output_file, spec.directory),
            input_file=file_path(spec.input_file, spec.directory),
            error_file=file_path(spec.error_file, spec.directory),
        )

        self._state = RunState.NEW
        self._output_observers = OutputObservers()
        self._lock = Lock()
        self._cancel_requested = Event()
        self._started = Event()
        self._done = Event()

        self._thread = None
        self._loop = None
        self._process = None
        self._pid = None
        self._stop_task = None
        self._exception = None
        self._callback_exception = None
        self._failure_phase = None
        self._failure_message = None

    @property
    def state(self):
        return self._state

    @property
    def ref(self):
        # A raw local process has no durable supervisor/receipt identity.
        return None

    def status(self):
        from kkrunner.backends.contracts import job_status_from_facts
        return job_status_from_facts(
            self.state, return_code=self.return_code,
            failure_phase=self.failure_phase, message=self.failure_message,
        )

    @property
    def exception(self):
        return self._exception

    @property
    def backend_id(self):
        return None if self._pid is None else str(self._pid)

    @property
    def failure_phase(self):
        return self._failure_phase

    @property
    def failure_message(self):
        return self._failure_message

    @property
    def return_code(self):
        return self.result.return_code

    def add_output_listener(self, callback):
        self._output_observers.add(callback)

    def add_output_error_listener(self, callback):
        self._output_observers.add_error_listener(callback)

    def _output_target(self, kind):
        # A missing stderr target means live observation only, not an
        # implicit merge into the saved stdout artifact.
        return self.spec.error_file if kind == "err" else self.spec.output_file

    def _forward_output(self, data, kind):
        target = self._output_target(kind)
        if target is not None:
            target.write(data)
            target.flush()
        self._output_observers.emit_bytes(data, kind)

    def _ensure_stop_task(self):
        if self._process is None or self._process.returncode is not None:
            return None
        if self._stop_task is None or self._stop_task.done():
            self._stop_task = asyncio.create_task(stop_local_process(self._process))
        return self._stop_task

    def cancel(self):
        self._cancel_requested.set()
        loop = self._loop
        if loop is None:
            if self._state == RunState.NEW:
                self._state = RunState.CANCELLED
            return
        try:
            loop.call_soon_threadsafe(self._ensure_stop_task)
        except RuntimeError:
            pass

    async def _execute(self):
        self._loop = asyncio.get_running_loop()
        if self._cancel_requested.is_set():
            self._state = RunState.CANCELLED
            self._started.set()
            return self.result

        try:
            protected = [
                file_path(value, self.spec.directory)
                for value in (self.spec.input_file, self.spec.stdin, *self.spec.input_files, *self.spec.cache_files)
            ]
            # Preserve already-open stdout/stderr handles: an unlink would
            # disconnect their inode from the named destination.
            outputs = [file_path(value, self.spec.directory) for value in self.spec.output_files]
            validate_output_collisions(
                (*outputs, self.result.output_file, self.result.error_file),
                protected=protected,
            )
            self.spec = materialize_output_streams(self.spec)
            clear_declared_outputs(
                outputs,
                protected=(*protected, self.result.output_file, self.result.error_file),
            )
            try:
                self._process = await start_local_process(self.spec)
            except BaseException as exc:
                self._failure_phase = FailurePhase.LAUNCH
                self._failure_message = str(exc)
                self._state = RunState.FAILED
                raise

            self._pid = self._process.pid
            self._state = RunState.RUNNING
            self._started.set()

            if self._cancel_requested.is_set():
                stop_task = self._ensure_stop_task()
                if stop_task is not None:
                    await stop_task

            wait_task = asyncio.create_task(self._process.wait())
            stdout_task = asyncio.create_task(
                pump_stream(
                    self._process.stdout,
                    lambda data: self._forward_output(data, "out"),
                )
            )
            stderr_task = asyncio.create_task(
                pump_stream(
                    self._process.stderr,
                    lambda data: self._forward_output(data, "err"),
                )
            )
            tasks = (wait_task, stdout_task, stderr_task)
            try:
                return_code, _, _ = await asyncio.gather(*tasks)
            finally:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

            self.result.return_code = return_code
            if self._cancel_requested.is_set():
                self._state = RunState.CANCELLED
            elif return_code == 0:
                self._state = RunState.FINISHED
            else:
                self._state = RunState.FAILED
                self._failure_phase = FailurePhase.EXECUTION
                self._failure_message = f"Process exited with status {return_code}"
            if self._state == RunState.FINISHED:
                files = discover_local_files(FileManifest.from_spec(self.spec))
                if files.failed_artifacts:
                    self._state = RunState.FAILED
                    self._failure_phase = FailurePhase.ARTIFACTS
                    self._failure_message = files.availability_message
            return self.result
        except BaseException:
            self._started.set()
            if self._state not in (RunState.CANCELLED, RunState.FAILED):
                self._state = RunState.FAILED
                self._failure_phase = FailurePhase.EXECUTION
            if self._process is not None and self._process.returncode is None:
                await stop_local_process(self._process)
            raise
        finally:
            if self._stop_task is not None:
                await asyncio.gather(self._stop_task, return_exceptions=True)
            self._output_observers.finish_bytes()
            close_output_files(self.spec.output_file, self.spec.error_file)
            self._process = None
            self._loop = None

    def _thread_main(self):
        try:
            asyncio.run(self._execute())
        except BaseException as exc:
            self._exception = exc
            if self._failure_message is None:
                self._failure_message = str(exc)
        finally:
            finish_monitor(self)

    def submit(self):
        with self._lock:
            if self._state == RunState.CANCELLED and self._thread is None:
                raise RuntimeError("Run was cancelled before submission")
            if self._state != RunState.NEW:
                raise RuntimeError(f"Run is already {self._state.value}")
            self._state = RunState.SUBMITTING
            self._thread = Thread(target=self._thread_main, daemon=True)
            self._thread.start()

        self._started.wait()
        if self._pid is None and self._state == RunState.FAILED:
            self._done.wait()
            error = self._exception or RuntimeError(self._failure_message or "Launch failed")
            raise SubmissionNotStarted(str(error)) from self._exception

    def wait(self):
        if self._state == RunState.NEW:
            raise RuntimeError("Run has not been submitted")
        if self._state == RunState.CANCELLED and self._thread is None:
            return self.result
        return wait_for_monitor(self)

    def run(self):
        if self._state == RunState.NEW:
            self.submit()
        return self.wait()

    async def run_async(self):
        if self._state == RunState.NEW:
            self.submit()
        return await asyncio.to_thread(self.wait)


class ProcessBackend:
    """Create raw runners executed as local subprocesses."""

    name = "process"
    supports_result_file_retrieval = False

    def create_job(self, spec: ExecutionSpec, *, submission_key=None, callback=None):
        if submission_key is None:
            return ProcessJob(materialize_local_spec(spec), callback=callback)
        from kkrunner.backends.process.managed import SupervisedProcessJob
        return SupervisedProcessJob(
            self, spec, run_id=submission_key, callback=callback,
        )

    def recover_submission(self, run_id, submission, *, submitted_at=None):
        from kkrunner.backends.process.operations import recover_remote

        reply = recover_remote(
            run_id,
            {
                "executor": "local",
                "directory": submission.execution.directory.local,
            },
        )
        from kkrunner.backends.contracts import JobRef, RecoveryResult
        if reply is None:
            return RecoveryResult.indeterminate("No submission receipt was found")
        return RecoveryResult.found(
            JobRef("process", reply["remote_id"]),
            directory=reply.get("directory"),
            output_file=reply.get("output_file"),
            error_file=reply.get("error_file"),
        )

    def open_job(self, run, callback=None, output_listener=None, start=True):
        from kkrunner.backends.process.managed import attach_local

        return attach_local(
            self,
            run,
            callback=callback,
            output_listener=output_listener,
            start=start,
        )

    def acknowledge_submission(self, run):
        from kkrunner.backends.process.operations import acknowledge_submission

        acknowledge_submission(run.id)

    def acknowledge(self, run):
        from kkrunner.backends.process.operations import acknowledge_remote

        acknowledge_remote(run.execution.backend_id)

    def status(self, *runs):
        from kkrunner.backends.process.managed import local_status

        statuses = []
        for run in runs:
            if run.execution.endpoint.executor != "local":
                raise ValueError("Run does not belong to the local backend")
            statuses.append(local_status(run.execution.backend_id, run.files))
        return tuple(statuses)
