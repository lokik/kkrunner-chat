"""Client observation of a direct backend call, over local or SSH channels.

Owns client observation tasks, cancellation requests and output callbacks.
The target backend owns the calculation process and its group termination.
The client Job owns lifecycle state, Run persistence and result processing.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from threading import Event
from typing import Callable

from kkrunner.backends.support.process import pump_stream
from kkrunner.runs.base import FailurePhase, RunState
from kkrunner.support.growing_file_tail import follow_growing_files


@dataclass(frozen=True)
class ObservedOutcome:
    """One observed native process outcome, without client delivery state."""

    state: RunState
    return_code: int | None = None
    failure_phase: FailurePhase | None = None
    message: str | None = None


class DirectObservation:
    """Own one direct observation session, leaving lifecycle decisions to Job."""

    def __init__(self, adapter):
        self.adapter = adapter
        self.detached = False
        self.pid = None
        self.identity = None
        self._process = None
        self._loop = None
        self._stop_task = None
        self._cancel_requested = Event()

    def configure_detached(self, detached):
        self.adapter.configure_detached(detached)
        self.detached = detached

    def configure_submission(self, request, *, stream_offsets, stream_writer, stream_paths):
        self.adapter.configure_submission(
            request, stream_offsets=stream_offsets,
            stream_writer=stream_writer, stream_paths=stream_paths,
        )

    def output(self, data, kind):
        return self.adapter.output(data, kind)

    def request_cancel(self):
        """Request termination, even if launch has not entered the event loop."""
        self._cancel_requested.set()
        loop = self._loop
        if loop is not None:
            try:
                loop.call_soon_threadsafe(self._ensure_stop_task)
            except RuntimeError:
                pass

    def _ensure_stop_task(self):
        process = self._process
        if process is None or process.returncode is not None:
            return None
        if self._stop_task is None:
            self._stop_task = asyncio.create_task(self.adapter.stop(process))
        return self._stop_task

    async def execute(
        self, spec, *, on_started: Callable, on_output: Callable,
        on_detached_output: Callable, on_finish: Callable,
    ) -> ObservedOutcome:
        """Execute without writing any Job state or emitting Job callbacks."""
        self._loop = asyncio.get_running_loop()
        if self._cancel_requested.is_set():
            try:
                return ObservedOutcome(RunState.CANCELLED)
            finally:
                self._loop = None

        try:
            self.adapter.prepare(spec)
            self._process = await self.adapter.launch(spec)
            self.pid = self._process.pid
            self.identity = getattr(self._process, "identity", None)
            on_started(self.pid)

            # Pump before stopping: a full pipe must not deadlock cancellation.
            wait_task = asyncio.create_task(self._process.wait())
            stdout_task = (
                asyncio.create_task(pump_stream(
                    self._process.stdout, lambda data: on_output(data, "out"),
                ))
                if self._process.stdout is not None else None
            )
            stderr_task = (
                asyncio.create_task(pump_stream(
                    self._process.stderr, lambda data: on_output(data, "err"),
                ))
                if self._process.stderr is not None else None
            )
            tasks = (wait_task,) + tuple(
                task for task in (stdout_task, stderr_task) if task is not None
            )
            paths = self.adapter.detached_output_paths(spec) if self.detached else {}
            if paths:
                async def terminal():
                    return wait_task.result() if wait_task.done() else None

                # Detached output is already written by the child; do not
                # send it through adapter.output a second time.
                tasks += (asyncio.create_task(follow_growing_files(
                    paths, on_detached_output, terminal, wakeup=wait_task,
                )),)
            if self._cancel_requested.is_set():
                self._ensure_stop_task()
            try:
                return_code = (await asyncio.gather(*tasks))[0]
            finally:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

            override = self.adapter.exit_status(return_code)
            if override is not None:
                state, phase, message = override
                return ObservedOutcome(state, failure_phase=phase, message=message)

            if self._cancel_requested.is_set() and self._stop_task is not None:
                try:
                    confirmed = await self._stop_task
                except Exception as exc:
                    return ObservedOutcome(
                        RunState.UNKNOWN, return_code=return_code,
                        message=str(exc),
                    )
                return ObservedOutcome(
                    RunState.CANCELLED if confirmed else RunState.UNKNOWN,
                    return_code=return_code,
                    message=None if confirmed else "Unable to confirm direct process cancellation",
                )
            if return_code == 0:
                return ObservedOutcome(RunState.FINISHED, return_code=0)
            return ObservedOutcome(
                RunState.FAILED, return_code=return_code,
                failure_phase=FailurePhase.EXECUTION,
                message=f"Process exited with status {return_code}",
            )
        except BaseException:
            if (
                self._process is not None
                and self._process.returncode is None
                and (not self.detached or self._cancel_requested.is_set())
            ):
                # A detached child survives observation failure unless
                # cancellation was explicitly requested.
                await self.adapter.stop(self._process, discard_output=True)
            raise
        finally:
            try:
                if self._stop_task is not None:
                    await asyncio.gather(self._stop_task, return_exceptions=True)
                on_finish()
            finally:
                try:
                    self.adapter.close(self._process)
                finally:
                    self._process = None
                    self._loop = None
