"""Client Job lifecycle for direct sessions and independently executable managed operations.

The Job owns client-side submission state, observation, cancellation and
listeners, but never owns the backend process or client file synchronization.
Its controller provides context-independent target operations through the
selected Endpoint. It works identically with local or remote controllers.
"""

from __future__ import annotations

import asyncio
import subprocess
from threading import Event, Lock, Thread

from kkrunner.runs.base import (
    ExecutionOutcome, FailurePhase, OutputObservers, RunState,
    finish_monitor, request_cancellation, start_monitor, wait_for_monitor,
)
from kkrunner.support.async_utils import run_coro_sync as _run_sync


class Job:
    """One client handle for a direct or managed backend execution."""

    def __init__(
        self, context, spec, *, callback=None,
        backend_id=None, remote_directory=None, status=None,
        output_listener=None, request_factory=None, completion_cleanup=None, adapter=None,
    ):
        self.context = context
        from kkrunner.backends.direct import DirectSession
        self._direct_session = None if adapter is None else DirectSession(adapter)
        self._started = Event()
        self.spec = spec
        self.callback = callback
        self._backend_id = backend_id
        self._remote_directory = remote_directory
        self._state = RunState.NEW
        self._lock = Lock()
        self._thread = None
        self._done = Event()
        self._exception = None
        self._callback_exception = None
        self._failure_phase = None
        self._failure_message = None
        self._return_code = None
        self._cancel_future = None
        self._observation_stopped = Event()
        self._follow_process = None
        self._loop = None
        self._request = None
        self._request_factory = request_factory
        self._completion_cleanup = completion_cleanup
        self._stream_offsets = None
        self._stream_paths = (None, None)
        self._stream_writer = None
        self._output_observers = OutputObservers(output_listener)
        self._output_error = None
        from kkrunner.files.paths import file_path
        self.result = ExecutionOutcome(
            input_file=file_path(spec.input_file, spec.directory),
            output_file=file_path(spec.output_file, spec.directory),
            error_file=file_path(spec.error_file, spec.directory),
        )
        if status is not None:
            self._apply_backend_facts(status)

    @classmethod
    def direct(cls, spec, *, callback=None, adapter=None):
        """Create the direct session variant of the same client Job type."""
        if adapter is None:
            from kkrunner.backends.direct import LocalProcessAdapter
            adapter = LocalProcessAdapter()
        return cls(None, spec, callback=callback, adapter=adapter)

    @property
    def _direct(self):
        return self._direct_session is not None

    @property
    def adapter(self):
        """Direct adapter for mechanism-specific introspection, not Job state."""
        return None if self._direct_session is None else self._direct_session.adapter

    @property
    def backend_id(self):
        if self._direct:
            session = self._direct_session
            return str(session.pid) if session.pid is not None and session.adapter.expose_pid else None
        return self._backend_id

    @property
    def ref(self):
        if self._direct:
            return None
        from kkrunner.backends.contracts import JobRef
        return None if self._backend_id is None else JobRef(self.context.backend, self._backend_id)

    @property
    def observing(self):
        """Whether an observation worker has taken over stream ownership."""
        return self._thread is not None

    @property
    def output_error(self):
        """Local stream-delivery error; never evidence of backend failure."""
        return self._output_error

    @property
    def state(self):
        if self._direct:
            return self._state
        if self._thread is not None:
            return self._state
        if self._backend_id is None or self._state.is_terminal:
            return self._state
        facts = _run_sync(self.context._status(self._remote_directory, self._backend_id))
        self._apply_backend_facts(facts)
        return self._state

    def status(self):
        from kkrunner.backends.contracts import job_status_from_facts
        if self._direct:
            return job_status_from_facts(
                self.state, return_code=self.return_code,
                failure_phase=self.failure_phase, message=self.failure_message,
            )
        if self._backend_id is None:
            return job_status_from_facts(self._state)
        facts = _run_sync(self.context._status(self._remote_directory, self._backend_id))
        return job_status_from_facts(
            facts.get("state", "unknown"),
            return_code=facts.get("return_code"),
            failure_phase=facts.get("failure_phase"),
            message=facts.get("error"),
        )

    @property
    def return_code(self):
        return self.result.return_code if self._direct else self._return_code

    @property
    def failure_phase(self):
        return self._failure_phase

    @property
    def failure_message(self):
        return self._failure_message

    @property
    def exception(self):
        return self._exception

    def add_output_listener(self, callback):
        self._output_observers.add(callback)

    def add_output_error_listener(self, callback):
        self._output_observers.add_error_listener(callback)

    def _apply_backend_facts(self, facts):
        self._state = RunState(facts.get("state", "unknown"))
        self._return_code = facts.get("return_code")
        self.result.return_code = self._return_code
        phase = facts.get("failure_phase")
        self._failure_phase = None if phase is None else FailurePhase(phase)
        self._failure_message = facts.get("error")


    def configure_detached(self, detached):
        """Set direct-process lifetime policy independently of Run persistence."""
        if not self._direct:
            raise RuntimeError("Detached policy is fixed by the managed backend")
        if self._state != RunState.NEW:
            raise RuntimeError("Cannot change detached policy after submission")
        self._direct_session.configure_detached(detached)

    def _forward_output(self, data, kind):
        if self._output_error is None:
            self._output_error = self._direct_session.output(data, kind)
        if not self._observation_stopped.is_set():
            self._output_observers.emit_bytes(data, kind)

    def _emit_detached_output(self, kind, data):
        if not self._observation_stopped.is_set():
            self._output_observers.emit_bytes(data, kind)

    def _direct_started(self, pid):
        self._state = RunState.RUNNING
        self._started.set()

    async def _execute(self):
        """Apply native direct execution facts to the single Job state."""
        session = self._direct_session
        try:
            outcome = await session.execute(
                self.spec, on_started=self._direct_started,
                on_output=self._forward_output,
                on_detached_output=self._emit_detached_output,
                on_finish=self._output_observers.finish_bytes,
            )
            self.result.return_code = outcome.return_code
            self._state = outcome.state
            self._failure_phase = outcome.failure_phase
            self._failure_message = outcome.message
            self._started.set()
            return self.result
        except BaseException:
            self._started.set()
            if self._state not in (RunState.CANCELLED, RunState.FAILED):
                if session.pid is not None and (
                    session.adapter.uncertain_after_launch or session.detached
                ):
                    self._state = RunState.UNKNOWN
                    self._failure_phase = None
                else:
                    self._state = RunState.FAILED
                    self._failure_phase = (
                        FailurePhase.EXECUTION if session.pid is not None else FailurePhase.LAUNCH
                    )
            raise

    def configure_submission(self, request, *, stream_offsets, stream_writer, stream_paths):
        if self._direct:
            if self._state != RunState.NEW:
                raise RuntimeError("Cannot configure a submitted direct job")
            return self._direct_session.configure_submission(
                request, stream_offsets=stream_offsets,
                stream_writer=stream_writer, stream_paths=stream_paths,
            )
        if self._request is not None or self._backend_id is not None:
            raise RuntimeError("Job submission is already configured")
        self.configure_observation(
            stream_offsets=stream_offsets, stream_writer=stream_writer,
            stream_paths=stream_paths,
        )
        # Publish the request only after all local observation configuration
        # succeeded. A pre-submit exception must not leave a submit-ready Job.
        self._request = request

    def configure_observation(self, *, stream_offsets, stream_writer, stream_paths):
        if self._thread is not None:
            raise RuntimeError("Cannot reconfigure a running observer")
        self._stream_offsets = dict(stream_offsets)
        self._stream_writer = stream_writer
        self._stream_paths = stream_paths

    def prepare_submission(self):
        """Construct the target request before crossing the submission boundary."""
        if self._request is not None:
            return
        if self._request_factory is None:
            return  # Remote FileSession supplies a staged request later.
        request = self._request_factory()
        from kkrunner.files.paths import file_path
        output = file_path(self.spec.output_file, self.spec.directory)
        error = file_path(self.spec.error_file, self.spec.directory)
        self.configure_submission(
            request,
            stream_offsets={"out": 0, "err": 0},
            stream_writer=None,
            stream_paths=(output, error),
        )

    def submit(self):
        from kkrunner.runs.base import SubmissionNotStarted
        if self._direct:
            with self._lock:
                if self._state == RunState.CANCELLED and self._thread is None:
                    raise RuntimeError("Run was cancelled before submission")
                if self._state != RunState.NEW:
                    raise RuntimeError(f"Run is already {self._state.value}")
                self._state = RunState.SUBMITTING
                self._thread = Thread(target=self._thread_main, daemon=True)
                self._thread.start()
            self._started.wait()
            if self._direct_session.pid is None and self._state == RunState.FAILED:
                self._done.wait()
                error = self._exception or RuntimeError(self._failure_message or "Launch failed")
                raise SubmissionNotStarted(str(error)) from self._exception
            return

        try:
            self.prepare_submission()
        except SubmissionNotStarted:
            raise
        except Exception as exc:
            self._state = RunState.FAILED
            self._failure_phase = FailurePhase.SUBMISSION
            self._failure_message = str(exc)
            raise SubmissionNotStarted(str(exc)) from exc
        with self._lock:
            if self._state != RunState.NEW or self._request is None:
                raise RuntimeError("Job is not prepared for submission")
            request = self._request
            self._request = None
            self._state = RunState.SUBMITTING
            self._remote_directory = request["directory"]
            try:
                facts = _run_sync(self.context._submit(request))
            except SubmissionNotStarted as exc:
                # The target proves that no calculation was accepted.
                self._state = RunState.FAILED
                self._failure_phase = FailurePhase.SUBMISSION
                self._failure_message = str(exc)
                raise
            except BaseException:
                # An accepted submission may have lost its reply.
                self._state = RunState.UNKNOWN
                raise
            remote_id = facts.get("remote_id")
            if not remote_id:
                self._state = RunState.UNKNOWN
                raise RuntimeError("Backend submit returned without job identity")
            self._backend_id = str(remote_id)
            self._apply_backend_facts(facts)
        self.start()

    async def _pump(self, source, kind):
        while True:
            data = await source.read(65536)
            if not data:
                return
            if self._stream_writer is not None:
                self._stream_writer(data, kind)
            self._output_observers.emit_bytes(data, kind)

    async def _monitor(self):
        self._loop = asyncio.get_running_loop()
        stdout_task = stderr_task = wait_task = None
        try:
            if self._observation_stopped.is_set():
                self._state = RunState.UNKNOWN
                return self.result
            try:
                self._follow_process = await self.context._follow(
                    self._remote_directory, self._backend_id,
                    offsets=self._stream_offsets,
                    stdout_path=self._stream_paths[0],
                    stderr_path=self._stream_paths[1],
                )
            except (OSError, ConnectionError, subprocess.SubprocessError, RuntimeError, ValueError) as exc:
                self._state = RunState.UNKNOWN
                self._failure_phase = None
                self._failure_message = str(exc)
                return self.result
            if self._observation_stopped.is_set():
                self._terminate_follow()
            stdout_task = asyncio.create_task(self._pump(self._follow_process.stdout, "out"))
            stderr_task = asyncio.create_task(self._pump(self._follow_process.stderr, "err"))
            stream_error = None
            wait_task = asyncio.create_task(self._follow_process.wait())
            try:
                follow_result, _, _ = await asyncio.gather(
                    wait_task, stdout_task, stderr_task,
                )
                # A plain operation has no framed terminal receipt; its
                # nonzero exit is an observation failure, not a Job outcome.
                if follow_result not in (None, 0):
                    raise subprocess.CalledProcessError(
                        follow_result, "backend follow operation",
                    )
            except Exception as exc:
                stream_error = exc
                self._output_error = exc
                # A failed local writer must not leave a FOLLOW subprocess
                # alive while the client queries the authoritative job state.
                self._terminate_follow()
                # gather() does not cancel other tasks when a stream writer
                # fails. Reap the helper before querying backend status, while
                # its stdout/stderr readers are still available for draining.
                await self._reap_follow(wait_task)
            if self._observation_stopped.is_set():
                self._state = RunState.UNKNOWN
                self._failure_phase = None
                self._failure_message = "Observation detached"
                return self.result
            facts = getattr(self._follow_process, "terminal", None)
            if facts is None:
                try:
                    facts = await self.context._status(self._remote_directory, self._backend_id)
                except (OSError, ConnectionError, subprocess.SubprocessError, RuntimeError, ValueError) as exc:
                    self._state = RunState.UNKNOWN
                    self._failure_phase = None
                    self._failure_message = str(stream_error or exc)
                    return self.result
            self._apply_backend_facts(facts)
            if not self._state.is_terminal:
                self._state = RunState.UNKNOWN
                self._failure_phase = None
                self._failure_message = str(stream_error or "Output follow ended without terminal backend status")
            return self.result
        except BaseException as exc:
            if self._observation_stopped.is_set():
                self._state = RunState.UNKNOWN
                self._failure_phase = None
                self._failure_message = "Observation detached"
                return self.result
            self._state = RunState.UNKNOWN
            self._failure_phase = None
            self._failure_message = self._failure_message or str(exc)
            self._exception = exc
            raise
        finally:
            for task in (stdout_task, stderr_task):
                if task is not None and not task.done():
                    task.cancel()
            tasks = tuple(task for task in (stdout_task, stderr_task) if task is not None)
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            # A wait task may still be outstanding after a broken or
            # unresponsive helper; do not leave an asyncio Task unobserved.
            if wait_task is not None and not wait_task.done():
                self._terminate_follow()
                await self._reap_follow(wait_task)
                if not wait_task.done():
                    wait_task.cancel()
                    await asyncio.gather(wait_task, return_exceptions=True)
            self._loop = None

    async def _reap_follow(self, wait_task):
        """Bounded reap of an already terminated observation helper.

        Read tasks stay active while wait() drains; force-kill is only for
        a helper that ignores termination. The caller retains the original
        stream error rather than replacing it with cleanup diagnostics.
        """
        try:
            await asyncio.wait_for(asyncio.shield(wait_task), timeout=2)
        except asyncio.TimeoutError:
            try:
                self._follow_process.kill()
            except (ProcessLookupError, RuntimeError):
                pass
            try:
                await asyncio.wait_for(asyncio.shield(wait_task), timeout=2)
            except Exception:
                pass
        except Exception:
            pass

    def _terminate_follow(self):
        if self._follow_process is not None:
            try:
                self._follow_process.terminate()
            except (ProcessLookupError, RuntimeError):
                pass

    def stop_observation(self):
        self._observation_stopped.set()
        if self._direct:
            # No backend identity exists for reattach. Stopping delivery must
            # not send an implicit cancel to a detached process.
            return
        if self._loop is not None:
            try:
                self._loop.call_soon_threadsafe(self._terminate_follow)
            except RuntimeError:
                pass

    def _thread_main(self):
        """Finalize observation, decoder and local handles independently."""
        if self._direct:
            try:
                asyncio.run(self._execute())
            except BaseException as exc:
                self._exception = exc
                if self._failure_message is None:
                    self._failure_message = str(exc)
            finally:
                finish_monitor(self)
            return
        try:
            asyncio.run(self._monitor())
        except BaseException as exc:
            if self._exception is None:
                self._exception = exc
                self._failure_message = self._failure_message or str(exc)
            if not self._state.is_terminal:
                self._state = RunState.UNKNOWN
                self._failure_phase = None
        finally:
            for cleanup in (self._output_observers.finish_bytes, self._completion_cleanup):
                if cleanup is None:
                    continue
                try:
                    cleanup()
                except BaseException as exc:
                    if self._output_error is None:
                        self._output_error = exc
                    # A confirmed terminal backend result is independent of
                    # client output decoding and cleanup. Let Run classify
                    # delivery failures; do not make Job.wait() raise an
                    # execution error after the backend has succeeded.
                    if not self._state.is_terminal:
                        if self._exception is None:
                            self._exception = exc
                            self._failure_message = str(exc)
                        self._state = RunState.UNKNOWN
                        self._failure_phase = None
            finish_monitor(self)

    def start(self):
        if self._direct:
            if self._state == RunState.NEW:
                return self.submit()
            raise RuntimeError("A direct process cannot restart its control session")
        return start_monitor(self, missing_backend="Job has no backend identity")

    def wait(self):
        if self._direct and self._state == RunState.CANCELLED and self._thread is None:
            return self.result
        return wait_for_monitor(self)

    def run(self):
        if self._direct:
            if self._state == RunState.NEW:
                self.submit()
            return self.wait()
        if self._thread is None:
            if self._backend_id is None:
                self.submit()
            else:
                self.start()
        return self.wait()

    async def run_async(self):
        return await asyncio.to_thread(self.run)

    def cancel(self):
        if self._direct:
            if not self._state.is_terminal:
                self._direct_session.request_cancel()
                if self._state == RunState.NEW:
                    self._state = RunState.CANCELLED
            return
        if self._backend_id is None:
            raise RuntimeError("Job has no confirmed backend identity")
        return request_cancellation(
            self,
            lambda: _run_sync(self.context._cancel(self._remote_directory, self._backend_id)),
        )
