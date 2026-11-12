"""Backend-independent calculation execution contracts."""

from __future__ import annotations

import asyncio
import codecs
import os
import warnings
from concurrent.futures import Future
from dataclasses import dataclass
from threading import Event, Lock, Thread, current_thread
from typing import Any, Optional, Sequence

from kkrunner.backends.contracts import SubmissionNotStarted as SubmissionNotStarted
from kkrunner.client.staging import StagingSpec
from kkrunner.execution import FailurePhase, MpiSpec, RunState
from kkrunner.files.manifest import FileManifest
from kkrunner.files.streams import _close_distinct
from kkrunner.runs.model import Execution


def dispatch_cancellation(action):
    """Run a blocking backend cancellation off the calling/GUI thread.

    The returned Future exposes errors, while the normal monitor remains
    authoritative for the terminal run state.
    """
    future = Future()

    def worker():
        if future.set_running_or_notify_cancel():
            try:
                future.set_result(action())
            except BaseException as exc:
                future.set_exception(exc)

    Thread(target=worker, daemon=True).start()
    return future


def start_monitor(runner, *, missing_backend=None):
    """Start a runner's monitor once, under its lifecycle lock."""
    with runner._lock:
        if runner._thread is not None:
            return
        if missing_backend is not None and runner._backend_id is None:
            raise RuntimeError(missing_backend)
        runner._thread = Thread(target=runner._thread_main, daemon=True)
        runner._thread.start()


def request_cancellation(runner, action):
    """Dispatch cancellation once and reuse its Future on repeat requests."""
    with runner._lock:
        if runner._cancel_future is None:
            runner._cancel_future = dispatch_cancellation(action)
        return runner._cancel_future


def finish_monitor(runner):
    """Deliver the callback and unblock waiters after backend-specific cleanup."""
    try:
        if runner.callback is not None:
            runner.callback(runner.result)
    except BaseException as exc:
        runner._callback_exception = exc
    finally:
        runner._done.set()


def wait_for_monitor(runner):
    """Await a started monitor; propagate execution errors before callback errors."""
    if runner._thread is None:
        if runner._state == RunState.NEW:
            raise RuntimeError("Run has not been submitted")
        raise RuntimeError("Attached run has not been started")
    # The monitor delivers its callback before waking ordinary waiters. The
    # callback may inspect its own result without waiting for itself to return.
    if current_thread() is not runner._thread:
        runner._done.wait()
    if runner._exception is not None:
        raise runner._exception
    if runner._callback_exception is not None:
        raise runner._callback_exception
    return runner.result


class OutputObserverWarning(RuntimeWarning):
    """An output observer failed while the calculation itself keeps running."""


class OutputObservers:
    """Deliver raw output without allowing observers to affect execution."""

    def __init__(self, listener=None):
        self._listeners = [] if listener is None else [listener]
        self._error_listeners = []
        self._lock = Lock()
        self._decoders = {}

    def add(self, callback):
        with self._lock:
            self._listeners.append(callback)

    def add_error_listener(self, callback):
        with self._lock:
            self._error_listeners.append(callback)

    def emit_bytes(self, data, kind):
        """Preserve UTF-8 characters spanning successive byte chunks."""
        if not data:
            return
        with self._lock:
            decoder = self._decoders.setdefault(kind, codecs.getincrementaldecoder("utf8")(errors="replace"))
            text = decoder.decode(data)
        if text:
            self.emit(text, kind)

    def finish_bytes(self):
        """Flush incomplete UTF-8 sequences once at the end of observation."""
        with self._lock:
            pending = [(kind, decoder.decode(b"", final=True)) for kind, decoder in self._decoders.items()]
            self._decoders.clear()
        for kind, text in pending:
            if text:
                self.emit(text, kind)

    def emit(self, text, kind):
        with self._lock:
            listeners = tuple(self._listeners)
        for callback in listeners:
            try:
                callback(text, kind)
            except Exception as exc:
                with self._lock:
                    try:
                        self._listeners.remove(callback)
                    except ValueError:
                        pass
                    handlers = tuple(self._error_listeners)
                if not handlers:
                    warnings.warn(
                        f"Output observer {callback!r} failed: {exc}",
                        OutputObserverWarning,
                        stacklevel=4,
                    )
                    continue
                for handler in handlers:
                    try:
                        handler(callback, exc)
                    except Exception as handler_exc:
                        warnings.warn(
                            f"Output observer error handler {handler!r} failed: {handler_exc}",
                            OutputObserverWarning,
                            stacklevel=4,
                        )


@dataclass
class ExecutionOutcome:
    """Raw execution facts available before task-specific interpretation."""

    output_file: Any = None
    input_file: Any = None
    error_file: Any = None
    return_code: Optional[int] = None


class ExecutionResources:
    """Closable resources whose ownership is transferred to a calculation run."""

    def __init__(self, *resources):
        self._resources = tuple(resources)
        self._closed = False
        self._lock = Lock()

    @property
    def closed(self):
        with self._lock:
            return self._closed

    def close(self, *, suppress_errors=False):
        with self._lock:
            if self._closed:
                return
            self._closed = True
            resources = self._resources
            self._resources = ()

        _close_distinct(resources, suppress_errors=suppress_errors)


class ResourceOwnedRunner:
    """Runner proxy owning resources and the public submit/cancel boundary."""

    def __init__(self, runner, resources: ExecutionResources):
        self._runner = runner
        self._resources = resources
        self._submission_lock = Lock()
        self._cancel_requested = Event()
        self._submitted = runner.state != RunState.NEW
        self._cancel_dispatched = False

    def __getattr__(self, name):
        return getattr(self._runner, name)

    @property
    def result(self):
        return self._runner.result

    @property
    def state(self):
        if self._cancel_requested.is_set():
            with self._submission_lock:
                if not self._submitted:
                    return RunState.CANCELLED
        return self._runner.state

    @property
    def backend_id(self):
        return self._runner.backend_id

    @property
    def exception(self):
        return self._runner.exception

    def _release_after_failure(self):
        # submit() calls this while holding _submission_lock. Read backend
        # state directly: our public state property takes that same lock when
        # cancellation has arrived, and would deadlock the submission thread.
        state = self._runner.state
        if state == RunState.NEW or state.is_terminal:
            self._resources.close(suppress_errors=True)

    def _raise_if_cancelled_before_submit(self):
        if not self._cancel_requested.is_set():
            return
        with self._submission_lock:
            if not self._submitted:
                raise asyncio.CancelledError("Run was cancelled before submission")

    def submit(self):
        with self._submission_lock:
            if self._cancel_requested.is_set():
                self._resources.close()
                raise asyncio.CancelledError("Run was cancelled before submission")
            try:
                result = self._runner.submit()
            except BaseException:
                self._release_after_failure()
                raise
            self._submitted = True
            cancel_after_submit = self._cancel_requested.is_set()
        if cancel_after_submit:
            self.cancel()
        return result

    def wait(self):
        self._raise_if_cancelled_before_submit()
        try:
            result = self._runner.wait()
        except BaseException:
            self._release_after_failure()
            raise
        if self.state.is_terminal:
            self._resources.close()
        return result

    def run(self):
        self._raise_if_cancelled_before_submit()
        if self.state == RunState.NEW:
            self.submit()
        return self.wait()

    async def run_async(self):
        self._raise_if_cancelled_before_submit()
        if self.state == RunState.NEW:
            self.submit()
        try:
            run_async = getattr(self._runner, "run_async", None)
            if run_async is not None:
                result = await run_async()
            else:
                result = await asyncio.to_thread(self._runner.wait)
        except BaseException:
            self._release_after_failure()
            raise
        if self.state.is_terminal:
            self._resources.close()
        return result

    def cancel(self):
        self._cancel_requested.set()
        with self._submission_lock:
            if not self._submitted:
                self._resources.close()
                return None
            if self._cancel_dispatched:
                return None
            result = self._runner.cancel()
            self._cancel_dispatched = True
            return result

    def close(self):
        if self._resources.closed:
            return
        state = self.state
        if state != RunState.NEW and not state.is_terminal:
            raise RuntimeError(f"Cannot close execution resources while run is {state.value}")
        self._resources.close()

    def add_output_listener(self, callback):
        return self._runner.add_output_listener(callback)

    def add_output_error_listener(self, callback):
        return self._runner.add_output_error_listener(callback)


@dataclass(frozen=True)
class RunStatus:
    """Client projection of execution facts, identity and file availability."""

    state: RunState
    backend_id: Optional[str] = None
    execution: Optional[Execution] = None
    files: Optional[FileManifest] = None
    return_code: Optional[int] = None
    message: Optional[str] = None
    failure_phase: Optional[FailurePhase] = None

    def __post_init__(self):
        if not isinstance(self.state, RunState):
            object.__setattr__(self, "state", RunState(self.state))
        if self.failure_phase is not None and not isinstance(self.failure_phase, FailurePhase):
            object.__setattr__(self, "failure_phase", FailurePhase(self.failure_phase))


def _pathlike(value):
    from kkrunner.files.task_file import TaskFile

    if isinstance(value, TaskFile):
        return value
    if isinstance(value, os.PathLike):
        return os.fspath(value)
    return value


@dataclass(frozen=True)
class ExecutionSpec:
    """Client declaration of a command, files, MPI and staging policy.

    Path-valued file fields are resolved relative to ``directory``. For
    example, ``output_file="stdout.txt"`` saves stdout in that working directory.
    ``input_file`` identifies the main input for tracking and transfer;
    ``stdin`` selects the file connected to the process's standard input.
    Declaring an input file does not add its name to ``argv`` or redirect stdin.

    ``argv=None`` means the original command is unknown, as on reattachment.
    This is a partial, observation-only specification, not a new launch request.
    ``argv=()`` remains a known empty command. Other reconstructed fields can
    also be incomplete; they describe only facts available to the observer.
    """

    argv: Optional[Sequence[str]]
    input_file: Any = None
    output_file: Any = None
    directory: str = "."
    stdin: Any = None
    error_file: Any = None
    input_files: Sequence[Any] = ()
    output_files: Sequence[Any] = ()
    cache_files: Sequence[Any] = ()
    mpi_argv: Optional[Sequence[str]] = None
    mpi: MpiSpec = MpiSpec()
    staging: StagingSpec = StagingSpec()
    # The MPI command may have a different stdin contract from serial argv.
    # Generic executors preserve stdin unless the caller explicitly opts out.
    mpi_uses_stdin: bool = True

    def __post_init__(self):
        for name, expected in (("mpi", MpiSpec), ("staging", StagingSpec)):
            value = getattr(self, name)
            if isinstance(value, dict):
                value = expected(**value)
            elif not isinstance(value, expected):
                raise TypeError(f"ExecutionSpec.{name} must be a {expected.__name__}")
            object.__setattr__(self, name, value)
        if type(self.mpi_uses_stdin) is not bool:
            raise TypeError("mpi_uses_stdin must be bool")
        object.__setattr__(self, "directory", os.fspath(self.directory))
        for name in ("input_file", "output_file", "stdin", "error_file"):
            object.__setattr__(self, name, _pathlike(getattr(self, name)))
        for name in ("input_files", "output_files", "cache_files"):
            object.__setattr__(
                self,
                name,
                tuple(_pathlike(value) for value in getattr(self, name)),
            )
        if self.argv is not None:
            object.__setattr__(self, "argv", tuple(os.fspath(value) for value in self.argv))
        if self.mpi_argv is not None:
            object.__setattr__(
                self,
                "mpi_argv",
                tuple(os.fspath(value) for value in self.mpi_argv),
            )

    @property
    def is_partial(self) -> bool:
        """Whether the original command is unknown (usually after reattachment)."""
        return self.argv is None

    def require_complete(self):
        """Reject an observation-only specification as a new submission."""
        if self.is_partial:
            raise ValueError("Cannot submit a partial ExecutionSpec: original argv is unknown")
        return self

