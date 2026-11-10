"""Direct-process client session mechanics shared by local and raw SSH.

These operate on the single client Job's process session and never create a
second Job hierarchy. Target-side portable process code is separate.
"""

from __future__ import annotations

import asyncio
from threading import Thread

from kkrunner.backends.process.primitives import pump_stream
from kkrunner.support.growing_file_tail import follow_growing_files
from kkrunner.runs.base import (
    FailurePhase, RunState,
    SubmissionNotStarted, finish_monitor,
)

def forward_output(job, data, kind):
    if job._output_error is None:
        job._output_error = job.adapter.output(data, kind)
    if not job._observation_stopped.is_set():
        job._output_observers.emit_bytes(data, kind)



def ensure_stop_task(job):
    if job._process is None or job._process.returncode is not None:
        return None
    if job._stop_task is None:
        job._stop_task = asyncio.create_task(job.adapter.stop(job._process))
    return job._stop_task



def cancel(job):
    if job._state.is_terminal:
        return
    job._cancel_requested.set()
    loop = job._loop
    if loop is None:
        if job._state == RunState.NEW:
            job._state = RunState.CANCELLED
        return
    try:
        loop.call_soon_threadsafe(job._ensure_stop_task)
    except RuntimeError:
        pass



async def execute(job):
    job._loop = asyncio.get_running_loop()
    if job._cancel_requested.is_set():
        job._state = RunState.CANCELLED
        job._started.set()
        return job.result
    try:
        try:
            job.adapter.prepare(job.spec)
            job._process = await job.adapter.launch(job.spec)
        except BaseException as exc:
            job._failure_phase = FailurePhase.LAUNCH
            job._failure_message = str(exc)
            job._state = RunState.FAILED
            raise

        job._pid = job._process.pid
        job._state = RunState.RUNNING
        job._started.set()

        # Start draining both streams before a stop() operation can wait
        # for the child. Otherwise full pipes can deadlock cancellation.
        wait_task = asyncio.create_task(job._process.wait())
        stdout_task = (
            asyncio.create_task(pump_stream(
                job._process.stdout, lambda data: job._forward_output(data, "out"),
            ))
            if job._process.stdout is not None else None
        )
        stderr_task = (
            asyncio.create_task(pump_stream(
                job._process.stderr, lambda data: job._forward_output(data, "err"),
            ))
            if job._process.stderr is not None else None
        )
        tasks = (wait_task,) + tuple(
            task for task in (stdout_task, stderr_task) if task is not None
        )
        paths = job.adapter.detached_output_paths(job.spec) if job._detached else {}
        if paths:
            async def terminal():
                return wait_task.result() if wait_task.done() else None

            # Exactly the same file-following mechanism used by PSI/J.
            # The adapter writes directly to disk, so these bytes go
            # only to observers; forwarding them back to output() would
            # duplicate output in the declared files.
            observer = asyncio.create_task(follow_growing_files(
                paths,
                lambda kind, data: (
                    None if job._observation_stopped.is_set()
                    else job._output_observers.emit_bytes(data, kind)
                ),
                terminal,
                wakeup=wait_task,
            ))
            tasks += (observer,)
        if job._cancel_requested.is_set():
            job._ensure_stop_task()
        try:
            return_code = (await asyncio.gather(*tasks))[0]
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        # Backend-specific statuses (e.g. SSH transport disconnection)
        # must be considered before declaring target execution success.
        override = job.adapter.exit_status(return_code)
        if override is not None:
            job._state, job._failure_phase, job._failure_message = override
        else:
            job.result.return_code = return_code
            if job._cancel_requested.is_set() and job._stop_task is not None:
                # Cancellation is confirmed only when the adapter can
                # establish that the process exited through its channel.
                confirmed = True
                if job._stop_task is not None:
                    try:
                        confirmed = await job._stop_task
                    except Exception as exc:
                        confirmed = False
                        job._failure_message = str(exc)
                if not confirmed:
                    job._state = RunState.UNKNOWN
                    job._failure_message = job._failure_message or "Unable to confirm direct process cancellation"
                else:
                    job._state = RunState.CANCELLED
            elif return_code == 0:
                job._state = RunState.FINISHED
            else:
                job._state = RunState.FAILED
                job._failure_phase = FailurePhase.EXECUTION
                job._failure_message = f"Process exited with status {return_code}"
        return job.result
    except BaseException:
        job._started.set()
        if job._state not in (RunState.CANCELLED, RunState.FAILED):
            if job._pid is not None and (job.adapter.uncertain_after_launch or job._detached):
                job._state = RunState.UNKNOWN
                job._failure_phase = None
            else:
                job._state = RunState.FAILED
                job._failure_phase = (
                    FailurePhase.EXECUTION if job._pid is not None else FailurePhase.LAUNCH
                )
        if (
            job._process is not None
            and job._process.returncode is None
            and (not job._detached or job._cancel_requested.is_set())
        ):
            # Observation failure on a detached process must never turn
            # into an implicit backend cancellation.
            await job.adapter.stop(job._process)
        raise
    finally:
        if job._stop_task is not None:
            await asyncio.gather(job._stop_task, return_exceptions=True)
        job._output_observers.finish_bytes()
        job.adapter.close(job._process)
        job._process = None
        job._loop = None



def thread_main(job):
    try:
        asyncio.run(job._execute())
    except BaseException as exc:
        job._exception = exc
        if job._failure_message is None:
            job._failure_message = str(exc)
    finally:
        finish_monitor(job)



def submit(job):
    with job._lock:
        if job._state == RunState.CANCELLED and job._thread is None:
            raise RuntimeError("Run was cancelled before submission")
        if job._state != RunState.NEW:
            raise RuntimeError(f"Run is already {job._state.value}")
        job._state = RunState.SUBMITTING
        job._thread = Thread(target=job._thread_main, daemon=True)
        job._thread.start()
    job._started.wait()
    if job._pid is None and job._state == RunState.FAILED:
        job._done.wait()
        error = job._exception or RuntimeError(job._failure_message or "Launch failed")
        raise SubmissionNotStarted(str(error)) from job._exception


