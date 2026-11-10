"""Transport-neutral direct DirectProcessJob: a single execution lifecycle.

The adapter implements process creation, target-specific output persistence,
termination and final file validation. Local and SSH direct share the same
state transitions, callback, streaming, waiting and cancellation semantics.
"""

from __future__ import annotations

import asyncio
from threading import Event, Lock, Thread
from kkrunner.backends.process.direct.contracts import DirectProcessAdapter

from kkrunner.backends.process.primitives import pump_stream
from kkrunner.files.paths import file_path
from kkrunner.support.growing_file_tail import follow_growing_files
from kkrunner.runs.base import (
    ExecutionOutcome, ExecutionSpec, FailurePhase, OutputObservers, RunState,
    SubmissionNotStarted, finish_monitor, wait_for_monitor,
)



class DirectProcessJob:
    """A client-owned, non-reconnectable direct process on any transport."""

    def __init__(self, spec: ExecutionSpec, callback=None, *, adapter: DirectProcessAdapter | None = None):
        if adapter is None:
            from .local import LocalProcessAdapter
            adapter = LocalProcessAdapter()
        self.adapter = adapter
        self._detached = False
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
        self._output_error = None

    @property
    def state(self):
        return self._state

    @property
    def ref(self):
        # Direct execution does not provide a durable backend identity.
        return None

    @property
    def backend_id(self):
        return str(self._pid) if self._pid is not None and self.adapter.expose_pid else None

    @property
    def exception(self):
        return self._exception

    @property
    def failure_phase(self):
        return self._failure_phase

    @property
    def failure_message(self):
        return self._failure_message

    @property
    def return_code(self):
        return self.result.return_code

    @property
    def output_error(self):
        """Client stream delivery errors, independent of backend execution."""
        return self._output_error

    def configure_detached(self, detached):
        """Set process lifetime before submission, independently of RunRecord."""
        if self._state != RunState.NEW:
            raise RuntimeError("Cannot change detached policy after submission")
        self.adapter.configure_detached(detached)
        self._detached = detached

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

    def configure_submission(self, request, *, stream_offsets, stream_writer, stream_paths):
        """Used only by staged direct transports; never an agent submit."""
        if self._state != RunState.NEW:
            raise RuntimeError("Cannot configure a submitted direct job")
        configure = getattr(self.adapter, "configure_submission", None)
        if configure is None:
            raise RuntimeError("Direct adapter does not support staged submissions")
        configure(request, stream_offsets=stream_offsets,
                  stream_writer=stream_writer, stream_paths=stream_paths)

    def _forward_output(self, data, kind):
        if self._output_error is None:
            self._output_error = self.adapter.output(data, kind)
        self._output_observers.emit_bytes(data, kind)

    def _ensure_stop_task(self):
        if self._process is None or self._process.returncode is not None:
            return None
        if self._stop_task is None:
            self._stop_task = asyncio.create_task(self.adapter.stop(self._process))
        return self._stop_task

    def cancel(self):
        if self._state.is_terminal:
            return
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
            try:
                self.adapter.prepare(self.spec)
                self._process = await self.adapter.launch(self.spec)
            except BaseException as exc:
                self._failure_phase = FailurePhase.LAUNCH
                self._failure_message = str(exc)
                self._state = RunState.FAILED
                raise

            self._pid = self._process.pid
            self._state = RunState.RUNNING
            self._started.set()

            # Start draining both streams before a stop() operation can wait
            # for the child. Otherwise full pipes can deadlock cancellation.
            wait_task = asyncio.create_task(self._process.wait())
            stdout_task = (
                asyncio.create_task(pump_stream(
                    self._process.stdout, lambda data: self._forward_output(data, "out"),
                ))
                if self._process.stdout is not None else None
            )
            stderr_task = (
                asyncio.create_task(pump_stream(
                    self._process.stderr, lambda data: self._forward_output(data, "err"),
                ))
                if self._process.stderr is not None else None
            )
            tasks = (wait_task,) + tuple(
                task for task in (stdout_task, stderr_task) if task is not None
            )
            paths = self.adapter.detached_output_paths(self.spec) if self._detached else {}
            if paths:
                async def terminal():
                    return wait_task.result() if wait_task.done() else None

                # Exactly the same file-following mechanism used by PSI/J.
                # The adapter writes directly to disk, so these bytes go
                # only to observers; forwarding them back to output() would
                # duplicate output in the declared files.
                observer = asyncio.create_task(follow_growing_files(
                    paths,
                    lambda kind, data: self._output_observers.emit_bytes(data, kind),
                    terminal,
                    wakeup=wait_task,
                ))
                tasks += (observer,)
            if self._cancel_requested.is_set():
                self._ensure_stop_task()
            try:
                return_code = (await asyncio.gather(*tasks))[0]
            finally:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

            # Backend-specific statuses (e.g. SSH transport disconnection)
            # must be considered before declaring target execution success.
            override = self.adapter.exit_status(return_code)
            if override is not None:
                self._state, self._failure_phase, self._failure_message = override
            else:
                self.result.return_code = return_code
                if self._cancel_requested.is_set() and self._stop_task is not None:
                    # Cancellation is confirmed only when the adapter can
                    # establish that the process exited through its channel.
                    confirmed = True
                    if self._stop_task is not None:
                        try:
                            confirmed = await self._stop_task
                        except Exception as exc:
                            confirmed = False
                            self._failure_message = str(exc)
                    if not confirmed:
                        self._state = RunState.UNKNOWN
                        self._failure_message = self._failure_message or "Unable to confirm direct process cancellation"
                    else:
                        self._state = RunState.CANCELLED
                elif return_code == 0:
                    self._state = RunState.FINISHED
                else:
                    self._state = RunState.FAILED
                    self._failure_phase = FailurePhase.EXECUTION
                    self._failure_message = f"Process exited with status {return_code}"
            return self.result
        except BaseException:
            self._started.set()
            if self._state not in (RunState.CANCELLED, RunState.FAILED):
                if self._pid is not None and self.adapter.uncertain_after_launch:
                    self._state = RunState.UNKNOWN
                    self._failure_phase = None
                else:
                    self._state = RunState.FAILED
                    self._failure_phase = (
                        FailurePhase.EXECUTION if self._pid is not None else FailurePhase.LAUNCH
                    )
            if (
                self._process is not None
                and self._process.returncode is None
                and (not self._detached or self._cancel_requested.is_set())
            ):
                # Observation failure on a detached process must never turn
                # into an implicit backend cancellation.
                await self.adapter.stop(self._process)
            raise
        finally:
            if self._stop_task is not None:
                await asyncio.gather(self._stop_task, return_exceptions=True)
            self._output_observers.finish_bytes()
            self.adapter.close(self._process)
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
