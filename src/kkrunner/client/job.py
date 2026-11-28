"""Generic client handle for a backend execution.

Job owns client submission/observation state and listeners. Its backend binding
owns execution semantics; the transport owns invocation resources. No process
identity, return-code interpretation or backend-mode selection lives here.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import Future
from threading import Event, RLock

from kkrunner.backends.contracts import SubmissionNotStarted, job_status_from_facts
from kkrunner.client.contracts import BackendExecution, OutputSubscription
from kkrunner.execution import merge_terminal_outcome
from kkrunner.runs.base import (
    FailurePhase, OutputObservers, RunState,
)
from kkrunner.support.async_utils import run_coro_sync as _run_sync


class Job:
    """One client handle for any selected execution backend."""

    def __init__(self, execution: BackendExecution, spec, *, callback=None, output_listener=None):
        self.execution = execution
        self.spec = spec
        self.callback = callback
        self._state = RunState.NEW
        self._lock = RLock()
        self._settled = False
        self._exception = None
        self._callback_exception = None
        self._failure_phase = None
        self._failure_message = None
        self._loop = None
        self._request = None
        self._cancel_future = None
        self._cancel_task = None
        self._observer = None
        self._observation_stopped = Event()
        self._output_observers = OutputObservers(output_listener)
        self._output_requested = None
        self._output_error = None
        self._return_code = None
        self._output_pending = False
        self._file_error = None
        self._apply_status(execution.snapshot)

    @property
    def ref(self):
        return self.execution.ref

    @property
    def backend_id(self):
        return None if self.ref is None else self.ref.token

    @property
    def observing(self):
        return self._loop is not None

    @property
    def output_error(self):
        """A client delivery error is independent of the backend's outcome."""
        return self._output_error

    @property
    def state(self):
        """Last observed state; inspecting a Job never performs I/O."""
        return self._state

    @property
    def return_code(self):
        return self._return_code

    @property
    def result(self):
        """The observed backend facts, not a client file result."""
        return self.status()

    @property
    def failure_phase(self):
        return self._failure_phase

    @property
    def failure_message(self):
        return self._failure_message

    @property
    def exception(self):
        return self._exception

    def status(self):
        with self._lock:
            return job_status_from_facts(
                self._state, return_code=self.return_code,
                failure_phase=self.failure_phase, message=self.failure_message,
                output_pending=self._output_pending, file_error=self._file_error,
            )

    def refresh_status(self):
        before = self.status()
        observed = _run_sync(self.execution.status())
        with self._lock:
            if observed.state.is_terminal or observed.file_error is not None or self.status() == before:
                self._apply_status(observed)
            return self.status()

    def _apply_status(self, status):
        with self._lock:
            if self._state.is_terminal:
                # Publication is independent of the native exit. A late
                # receipt can settle publication without relabeling the exit.
                if self._output_pending and (
                    (status.state.is_terminal and not status.output_pending)
                    or (status.state == RunState.UNKNOWN and status.file_error is not None)
                ):
                    self._output_pending = False
                    self._file_error = status.file_error
                state, code = merge_terminal_outcome(
                    self._state, self._return_code, status.state, status.return_code,
                )
                if (state, code) == (self._state, self._return_code):
                    return
                if state != status.state:
                    self._return_code = code
                    return
            else:
                self._output_pending = status.output_pending
                self._file_error = status.file_error
            self._state = status.state
            self._return_code = status.return_code
            phase = status.failure_phase
            self._failure_phase = None if phase is None else FailurePhase(phase.value)
            self._failure_message = status.message

    def _observation_lost(self, message):
        with self._lock:
            if not self._state.is_terminal:
                self._state = RunState.UNKNOWN
                self._failure_phase = None
                self._failure_message = message

    def add_output_listener(self, callback):
        self._output_observers.add(callback)
        self._schedule(self._update_output_subscription)

    def remove_output_listener(self, callback):
        """Unsubscribe without stopping observation of the calculation."""
        self._output_observers.remove(callback)
        self._schedule(self._update_output_subscription)

    def _update_output_subscription(self):
        if self._output_requested is not None:
            self._output_requested.set(
                not self._observation_stopped.is_set() and self._output_observers.has_listeners,
            )

    def add_output_error_listener(self, callback):
        self._output_observers.add_error_listener(callback)

    def configure_detached(self, detached):
        if type(detached) is not bool:
            raise TypeError("detached must be bool")
        if self._state != RunState.NEW:
            raise RuntimeError("Cannot change execution policy after submission")
        self.execution.configure_detached(detached)

    def configure_observation(self, *, stream_offsets, stream_paths):
        if self.observing or self._settled:
            raise RuntimeError("Cannot reconfigure a running or settled observer")
        self.execution.configure_observation(stream_offsets=stream_offsets, stream_paths=stream_paths)

    def configure_submission(self, request, **observation):
        if self._state != RunState.NEW or self._request is not None or self.ref is not None:
            raise RuntimeError("Job submission is already configured")
        self.configure_observation(**observation)
        self._request = request

    def _observed_output(self, data, kind):
        if not self._observation_stopped.is_set():
            try:
                self._output_observers.emit_bytes(data, kind)
            finally:
                # A failed last callback is removed by OutputObservers.
                self._update_output_subscription()

    def submit(self):
        """Submit independently, without observing the accepted calculation."""
        return _run_sync(self.submit_async())

    async def submit_async(self, *, on_submitted=None):
        if not self.execution.can_stop_observation:
            from kkrunner.backends.contracts import UnsupportedCapability
            raise UnsupportedCapability("This execution requires run() or run_async()")
        await self._execute(on_submitted=on_submitted, submit_only=True)
        return self.status()

    async def _monitor(self, on_submitted=None, *, submit_only=False):
        with self._lock:
            # Standalone submission does not subscribe, even if listeners
            # have been registered for a later explicit wait().
            subscription = OutputSubscription(
                not submit_only and not self._observation_stopped.is_set()
                and self._output_observers.has_listeners,
            )
            self._output_requested = None if submit_only else subscription
        try:
            if self._request is not None:
                request, self._request = self._request, None
                try:
                    status = await self.execution.submit(
                        request, self._observed_output, output_requested=subscription,
                    )
                except BaseException:
                    self._apply_status(self.execution.snapshot)
                    raise
                self._apply_status(status)
                if on_submitted is not None:
                    await on_submitted()
            # A cancellation queued during submission must survive a request
            # to detach observation before the observer is created.
            self._ensure_cancel_task(starting=True)
            if submit_only:
                return
            with self._lock:
                detached = self._observation_stopped.is_set() and self.execution.can_stop_observation
                if not detached:
                    self._observer = asyncio.create_task(self.execution.observe(
                        self._observed_output, output_requested=self._output_requested,
                    ))
            if detached:
                self._observation_lost("Observation detached")
                return
            self._ensure_cancel_task()
            try:
                status = await self._observer
            except asyncio.CancelledError:
                if not self._observation_stopped.is_set():
                    raise
                self._observation_lost("Observation detached")
                return
            except Exception as exc:
                self._output_error = self._output_error or exc
                try:
                    status = await self.execution.status()
                except Exception:
                    self._observation_lost(str(exc))
                    return
                self._apply_status(status)
                if not self._state.is_terminal:
                    self._observation_lost(str(exc))
                return
            self._output_error = self._output_error or self.execution.output_error
            self._apply_status(status)
            if not self._state.is_terminal:
                self._observation_lost(status.message or "Observation ended without terminal backend status")
        finally:
            with self._lock:
                self._observer = None
                self._output_requested = None
                cancel_task = self._cancel_task
            try:
                if isinstance(cancel_task, asyncio.Task):
                    await asyncio.gather(cancel_task, return_exceptions=True)
                elif isinstance(cancel_task, Future):
                    # An idle cancellation may already have been dispatched
                    # when this caller starts observing the same execution.
                    await asyncio.gather(asyncio.wrap_future(cancel_task), return_exceptions=True)
                try:
                    await self.execution.close()
                except BaseException as exc:
                    self._output_error = self._output_error or exc
                    if not self._state.is_terminal:
                        self._observation_lost(str(exc))
            finally:
                with self._lock:
                    if self._cancel_task is cancel_task and isinstance(cancel_task, asyncio.Task):
                        self._cancel_task = None

    async def _execute(self, *, on_submitted=None, submit_only=False):
        with self._lock:
            if not submit_only and self._settled:
                return self._result()
            if self._loop is not None:
                raise RuntimeError("Job already has an active caller")
            if submit_only or self.ref is None:
                if self._state == RunState.CANCELLED:
                    raise SubmissionNotStarted("Job was cancelled before submission")
                if self._state != RunState.NEW or self._request is None or self.ref is not None:
                    raise RuntimeError("Job is not prepared for submission")
                self._state = RunState.SUBMITTING
            self._loop = asyncio.get_running_loop()
        try:
            await self._monitor(on_submitted, submit_only=submit_only)
        except BaseException as exc:
            self._exception = exc
            self._observation_lost(str(exc))
        finally:
            if not submit_only or self._exception is not None:
                try:
                    if not self._observation_stopped.is_set():
                        self._output_observers.finish_bytes()
                except BaseException as exc:
                    self._output_error = self._output_error or exc
                    self._observation_lost(str(exc))
                try:
                    if self.callback is not None:
                        self.callback(self.result)
                except BaseException as exc:
                    self._callback_exception = exc
                finally:
                    self._settled = True
            # The active caller owns the decoder and completion callback as
            # well as the backend connection. Do not admit a second observer
            # between transport closure and completion.
            with self._lock:
                self._loop = None
                future = self._cancel_future if self._cancel_task is None else None
                terminal = self._state.is_terminal
            if future is not None and not future.done():
                if terminal:
                    future.set_result(None)
                else:
                    future.set_exception(RuntimeError(
                        "Observation ended before cancellation was dispatched"
                    ))
        return self._result()

    def _result(self):
        if self._exception is not None:
            raise self._exception
        if self._callback_exception is not None:
            raise self._callback_exception
        return self.result

    def _ensure_cancel_task(self, *, starting=False):
        with self._lock:
            future = self._cancel_future
            if (future is None or future.done() or self._cancel_task is not None
                    or self.ref is None or (not starting and self._observer is None)):
                return
            if not self._state.is_terminal:
                task = asyncio.create_task(self.execution.cancel())
                self._cancel_task = task
                task.add_done_callback(lambda task: self._cancel_completed(task, future))
                return
        future.set_result(None)

    def _cancel_completed(self, task, future):
        # Resolve the particular request, not whichever request is current
        # when this callback runs. A caller may immediately submit another.
        try:
            result = task.result()
        except BaseException as exc:
            future.set_exception(exc)
        else:
            self._confirm_cancellation(result)
            future.set_result(result)

    def _confirm_cancellation(self, status):
        # A cancel request can discover that execution has already ended.
        # Acceptance of the request alone is not proof of termination.
        if status is not None and (status.state.is_terminal or status.file_error is not None):
            self._apply_status(status)

    def cancel(self):
        """Cancel in the active caller loop, or synchronously when idle."""
        with self._lock:
            future = self._cancel_future
            if future is not None and not future.done():
                return future
            future = Future()
            future.set_running_or_notify_cancel()
            if self._state.is_terminal:
                future.set_result(None)
                return future
            self._cancel_future = future
            self._cancel_task = None
            if self._state == RunState.NEW:
                self._state = RunState.CANCELLED
                future.set_result(None)
                return future
            if self._loop is not None:
                self._schedule(self._ensure_cancel_task)
                return future
            if self.ref is None:
                future.set_exception(RuntimeError("No execution reference available for cancellation"))
                return future
            self._cancel_task = future  # dispatched outside the observation loop

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            try:
                status = _run_sync(self.execution.cancel())
                self._confirm_cancellation(status)
                future.set_result(status)
            except BaseException as exc:
                future.set_exception(exc)
        else:
            task = loop.create_task(self.execution.cancel())
            task.add_done_callback(lambda task: self._cancel_completed(task, future))
        return future

    async def cancel_async(self):
        return await asyncio.wrap_future(self.cancel())

    def _schedule(self, action):
        loop = self._loop
        if loop is not None:
            try:
                loop.call_soon_threadsafe(action)
            except RuntimeError:
                pass  # Observation has already closed its loop.

    def stop_observation(self):
        """Stop listeners; interrupt observation only if the execution survives."""
        with self._lock:
            if self._observation_stopped.is_set():
                return
            self._observation_stopped.set()
            observer = self._observer
        self._schedule(self._update_output_subscription)
        if observer is not None and self.execution.can_stop_observation:
            self._schedule(observer.cancel)

    def wait(self):
        """Explicitly observe an accepted execution in the caller's thread."""
        if self._settled:
            return self._result()
        return _run_sync(self.wait_async())

    async def wait_async(self):
        if self._settled:
            return self._result()
        if self._state == RunState.CANCELLED and self.ref is None:
            return self.result
        if self.ref is None:
            raise RuntimeError("Job has not been submitted")
        return await self._execute()

    def run(self):
        return _run_sync(self.run_async())

    async def run_async(self, *, on_submitted=None):
        """Drive native submission and observation in the caller's event loop."""
        if self._settled:
            return self._result()
        return await self._execute(on_submitted=on_submitted)
